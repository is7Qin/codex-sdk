package codexsdk

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
)

// 本文件是 Responses 出站 WS 层的 T0 基线（spec §7 的 P1–P3 判定入口）。
//
// 口径纪律（T2/T3/T4 替换**测量对象**，但 bench 名、输入、计数口径不变）：
//   - 输入固定为 wsBenchPayload（3.7KB 请求体）与 wsBenchFrame（1KB delta 帧），
//     两者在包初始化时按目标长度精确构造，任何一次运行都同长；
//   - WS 两条链路每轮含**一次真实握手**：loopback 上的 httptest 假上游，所以
//     数字里含拨号 + 升级 + 帧往返，不是纯 CPU 成本（这是 spec P3 的"整链路"口径）；
//   - 结果一律写入 benchSink（包级 var），防止整条调用被编译器判定为无副作用。

// benchSink 汇总各 bench 的结果长度。必须是包级变量：局部变量被优化掉时整条
// 被测链也会被删，数字会变成 0。
var benchSink int

// wsBenchPayload 是 P1 的输入：3.7KB 量级的 Responses 请求体。
//
// 顶层键覆盖 spec 要求的面（model / instructions / input / tools / reasoning /
// text / stream:true / store:false / client_metadata），且全部落在 WS 白名单内——
// 否则 buildResponsesCreateFrame 的过滤会把它们删掉，帧会缩水成"只剩 type"，
// 3.7KB 口径立刻失真。
var wsBenchPayload = buildWSBenchPayload(3700)

// wsBenchFrame 是 P2 的输入：1024B 的 response.output_text.delta 帧（1KB 级）。
var wsBenchFrame = buildWSBenchFrame(1024)

// buildWSBenchPayload 构造长度恰为 target 字节的请求体。
//
// 两遍法：先用零填充量算出骨架长度，再把差值全部填进 instructions 的字符串
// （'i' 无需 JSON 转义，故字节数 = 字符数，长度可精确控制）。这样目标长度是
// 构造期常量而不是"跑起来量一下"，P1 的 3.7KB 口径不会随编辑漂移。
func buildWSBenchPayload(target int) []byte {
	build := func(pad int) []byte {
		var b strings.Builder
		b.Grow(target + 512)
		b.WriteString(`{"model":"gpt-5.5","instructions":"`)
		b.WriteString(strings.Repeat("i", pad))
		b.WriteString(`","input":[`)
		b.WriteString(`{"type":"message","role":"user","content":[{"type":"input_text","text":"` +
			strings.Repeat("u", 480) + `"}]},`)
		b.WriteString(`{"type":"message","role":"assistant","content":[{"type":"output_text","text":"` +
			strings.Repeat("a", 480) + `"}]}]`)
		b.WriteString(`,"tools":[{"type":"function","name":"shell","description":"run a command",` +
			`"parameters":{"type":"object","properties":{"command":{"type":"array","items":{"type":"string"}}},` +
			`"required":["command"],"additionalProperties":false}}]`)
		b.WriteString(`,"reasoning":{"effort":"medium","summary":"auto"},"text":{"verbosity":"medium"}`)
		b.WriteString(`,"stream":true,"store":false`)
		b.WriteString(`,"client_metadata":{"session_id":"sess-bench","thread_id":"thread-bench"}}`)
		return []byte(b.String())
	}
	return build(target - len(build(0)))
}

// buildWSBenchFrame 构造长度恰为 target 字节的单帧（同两遍法，填充进 delta）。
func buildWSBenchFrame(target int) []byte {
	build := func(pad int) []byte {
		var b strings.Builder
		b.Grow(target + 256)
		b.WriteString(`{"type":"response.output_text.delta","item_id":"msg_bench",` +
			`"output_index":0,"content_index":0,"delta":"`)
		b.WriteString(strings.Repeat("d", pad))
		b.WriteString(`"}`)
		return []byte(b.String())
	}
	return build(target - len(build(0)))
}

// benchWSUpstream 是 bench 用的假 Responses WS 上游：httptest + websocket.Accept。
//
// 与 test 侧 wsUpstream 同形（先收客户端那一帧，再回放脚本，最后停在读循环上），
// 但只需要固定脚本，故不复用（wsUpstream 的构造签名收 *testing.T，benchmark 里
// 拿不到；既有测试文件不得改动）。
type benchWSUpstream struct {
	srv *httptest.Server
	// reps 是每次升级回放的帧脚本。构造后只读 → 跨 goroutine 无需加锁。
	reps []string
}

// newBenchWSUpstream 起服务并固定脚本：created → 4×delta → completed
// （completed 之后由客户端补 [DONE]，见 sseView 的终态规则）。
func newBenchWSUpstream(b *testing.B) *benchWSUpstream {
	b.Helper()
	u := &benchWSUpstream{reps: []string{
		wsCreatedEv, wsDeltaEv, wsDeltaEv, wsDeltaEv, wsDeltaEv, wsCompletedEv,
	}}
	u.srv = httptest.NewServer(http.HandlerFunc(u.handle))
	b.Cleanup(func() {
		u.srv.CloseClientConnections()
		u.srv.Close()
	})
	return u
}

func (u *benchWSUpstream) handle(w http.ResponseWriter, r *http.Request) {
	c, err := websocket.Accept(w, r, nil)
	if err != nil {
		return
	}
	defer c.CloseNow()
	// 客户端每连接只发一帧 response.create；先收满一帧再回放，与真实上游同序
	// （收不到帧就说明 bench 语义已经不成立，直接退场，由客户端侧报错暴露）。
	if _, _, rerr := c.Read(r.Context()); rerr != nil {
		return
	}
	for _, f := range u.reps {
		if werr := c.Write(r.Context(), websocket.MessageText, []byte(f)); werr != nil {
			return
		}
	}
	// **常驻读循环**（不是可选的收尾）：coder/websocket 的 Ping 自己不读连接，
	// 只等"某次 Reader 调用把 pong 读上来"。没有在飞的 Read，心跳就会在 ping
	// 超时后 CloseNow——那是 bench 环境造出来的假故障，会把 P3 的数字污染成
	// "连接被提前掐断"。客户端 CloseNow 时读返回错误 → 退出，httptest 收尾不挂。
	for {
		if _, _, rerr := c.Read(r.Context()); rerr != nil {
			return
		}
	}
}

// benchFixedTransport 是 newFixedTransport 的 bench 版：后者签名收 *testing.T，
// benchmark 拿不到。这里复用同一个 fixedTransport 类型（同一份 URL 校验 +
// 改写逻辑），只把失败断言换成 *testing.B。
//
// expected 必须是 **https** 字面量：coder/websocket 会把 wss:// 改写成 https://
// 再发请求，用 DefaultResponsesWSURL（wss）作期望值会让四个 bench 全部命中不到。
func benchFixedTransport(b *testing.B, expected, srvBase string) *fixedTransport {
	b.Helper()
	u, err := url.Parse(srvBase)
	if err != nil {
		b.Fatalf("parse srvBase %q: %v", srvBase, err)
	}
	return &fixedTransport{expected: expected, srvURL: u}
}

// benchWSClient 构造与本仓库既有 WS 用例同形的 client（只差 benchmark 侧不需要
// 会话 / 透传项）：传输选 WS、关掉每帧自动 turn_id、出口走 fixedTransport。
func benchWSClient(b *testing.B, up *benchWSUpstream) *HTTPClient {
	b.Helper()
	return NewHTTPClient(PAT("pat-bench"),
		WithResponsesTransport(TransportWS),
		WithTurnAuto(false),
		WithTransport(benchFixedTransport(b, "https://chatgpt.com/backend-api/codex/responses", up.srv.URL)),
	)
}

// BenchmarkFrameCodecPrepare3KB：spec §7 P1 —— 帧构造 + 归一（3.7KB 体）。
//
// 测量体是生产发送的等价链：frameCodec.encode(encodeWSResponses) = HTTP→WS 协议
// 形状（单遍快路径 encodeOnePass；非紧凑 / 转义键 / 重复键 / 白名单外键才回退
// buildResponsesCreateFrame）+ 伪装层归一（白名单 / store 归零 / client_metadata
// 组装）。codec 在循环外构造（无状态），循环内构造等于把配置
// 成本摊进被测链。traceAuto/turnAuto 保持 true —— 每帧生成 traceparent 与
// turn_id 是真实路径。
func BenchmarkFrameCodecPrepare3KB(b *testing.B) {
	if n := len(wsBenchPayload); n < 3600 || n > 3800 {
		b.Fatalf("wsBenchPayload 长度 %d 越界（要求 3600–3800）", n)
	}
	fc := newFrameCodec(&options{
		meta: &CodexMeta{
			InstallationID: "bench-installation",
			SessionID:      "bench-session",
			ThreadID:       "bench-thread",
			WindowID:       "bench-thread:0",
		},
		session:   &Session{SessionID: "bench-session", ThreadID: "bench-thread", WindowID: "bench-thread:0"},
		traceAuto: true,
		turnAuto:  true,
	})
	b.ReportAllocs()
	b.SetBytes(int64(len(wsBenchPayload)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		out, err := fc.encode(wsBenchPayload, encodeWSResponses, frameInput{Now: time.Now()})
		if err != nil {
			b.Fatalf("frameCodec.encode: %v", err)
		}
		benchSink = len(out)
	}
}

// BenchmarkSSEViewRender1KB：spec §7 P2 —— 帧 → SSE 渲染（1KB 帧）。
//
// T3 的测量对象是**零拷贝分段视图**：一帧被切成 prefix / raw / suffix 三段后
// 逐段读尽（帧体不进任何中间块）。名字、输入（wsBenchFrame 1KB）、每轮口径
// （一帧 → 一次完整渲染 + 读尽）与 T0/T2 不变；但 T0 测的是"构建一块"，这里是
// "三段渲染 + 读尽"，所以 **ns/op 不与 T0 纵向比**，P2 只判 `0 B/op, 0 allocs/op`。
// 结果必须被 benchSink 消费：读出的字节数不落地的话，整条链会被优化掉。
func BenchmarkSSEViewRender1KB(b *testing.B) {
	if n := len(wsBenchFrame); n < 900 || n > 1100 {
		b.Fatalf("wsBenchFrame 长度 %d 越界（要求 1KB 级）", n)
	}
	st := &benchStaticStream{frames: [][]byte{wsBenchFrame}}
	ctx := context.Background()
	buf := make([]byte, 4096)
	// 视图对象是**每请求一次**的东西（生产路径在 streamBodyWS 里 newSSEView 一次，
	// 288B 计入每请求成本而不是每帧成本）。P2 的口径是「一帧 → 三段渲染 + 读尽」
	// 这一步：循环内重置视图状态、不重新分配视图，否则测到的是对象创建本身。
	view := newSSEView(ctx, st, wsStreamReadLimit)
	b.ReportAllocs()
	b.SetBytes(int64(len(wsBenchFrame)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		st.i = 0
		*view = sseView{ctx: ctx, stream: st, limit: wsStreamReadLimit, seg: sseSegCount}
		written := 0
		for {
			n, err := view.Read(buf)
			written += n
			if err != nil {
				if err != io.EOF {
					b.Fatalf("sseView.Read: %v", err)
				}
				break
			}
		}
		benchSink = written
	}
}

// benchStaticStream 是 P2 的帧流桩：按序吐固定帧（**不复制**帧切片，与
// wsExchange 的交付语义同形），吐完返回 io.EOF。
type benchStaticStream struct {
	frames [][]byte
	i      int
}

func (s *benchStaticStream) Next(context.Context) ([]byte, error) {
	if s.i >= len(s.frames) {
		return nil, io.EOF
	}
	f := s.frames[s.i]
	s.i++
	return f, nil
}

func (s *benchStaticStream) Close() error { return nil }

// BenchmarkWSStreamBodyPerRequest：spec §7 P3 —— WS 每请求分配（整链路含握手）。
//
// 每轮 = 拨号 + 升级 + 发一帧 + 读尽 6 帧脚本（含末帧补的 [DONE]）+ 关闭，
// 这才是网关真实支付的每请求成本（T2/T4 的 allocs 降幅以此为准）。
func BenchmarkWSStreamBodyPerRequest(b *testing.B) {
	up := newBenchWSUpstream(b)
	hc := benchWSClient(b, up)
	ctx := b.Context() // benchmark 结束时取消：收尾阶段不再有残留调用
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		resp, err := hc.StreamBody(ctx, wsBenchPayload)
		if err != nil {
			b.Fatalf("StreamBody: %v", err)
		}
		n, cerr := io.Copy(io.Discard, resp.Body)
		if cerr != nil {
			b.Fatalf("读 body: %v", cerr)
		}
		if cerr := resp.Body.Close(); cerr != nil {
			b.Fatalf("关 body: %v", cerr)
		}
		benchSink = int(n)
	}
}

// BenchmarkWSResponsesPerRequest：同上，但调 Responses —— 多出来的是
// SSE 扫描 + 聚合器 + 合成体序列化（P3 的第二个观测面）。
func BenchmarkWSResponsesPerRequest(b *testing.B) {
	up := newBenchWSUpstream(b)
	hc := benchWSClient(b, up)
	ctx := b.Context()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		resp, err := hc.Responses(ctx, wsBenchPayload)
		if err != nil {
			b.Fatalf("Responses: %v", err)
		}
		benchSink = len(resp.Raw)
	}
}
