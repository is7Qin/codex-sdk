package codexsdk

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/klauspost/compress/zstd"
	"github.com/tidwall/gjson"
)

func newZstdWriterForTest(lower bool) (*zstd.Encoder, error) {
	return zstd.NewWriter(nil, zstdEncoderOptions(lower)...)
}

// TestInvalidRequestRejectedBeforeNetwork：model 含控制字节、为空，或
// service_tier 含控制字节时，HTTP→HTTP、HTTP→WS、Dial(WithFirstFrame) 与
// Client.Send 都在本地返回 ErrInvalidRequestBody：不发请求、不拨号、不入网，
// 也不包成拨号类 / 传输错误。
func TestInvalidRequestRejectedBeforeNetwork(t *testing.T) {
	bad := []string{
		`{"model":"gpt\r\nX-Evil: 1","input":"hi"}`,
		`{"model":"a\u0000","input":"hi"}`,
		`{"model":"","input":"hi"}`,
		`{"model":"gpt-5.5","service_tier":"fl\nex","input":"hi"}`,
		`{"input":"hi"}`,
		`[]`,
	}
	var hits atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { hits.Add(1); sseOK(w) }))
	t.Cleanup(srv.Close)
	hc := NewHTTPClient(PAT("p"), WithTransport(newFixedTransport(t, DefaultResponsesURL, srv.URL)))

	up := newTSUpstream(t, nil)
	whc := NewHTTPClient(PAT("p"), WithResponsesTransport(TransportWS), WithTransport(newFixedTransport(t, DefaultResponsesURL, up.srv.URL)))

	for _, body := range bad {
		if _, err := hc.StreamBody(context.Background(), []byte(body), testID()); !errors.Is(err, ErrInvalidRequestBody) {
			t.Fatalf("HTTP %s: 期望 ErrInvalidRequestBody, got %v", body, err)
		}
		_, err := whc.StreamBody(context.Background(), []byte(body), testID())
		var de *DialError
		if !errors.Is(err, ErrInvalidRequestBody) || errors.As(err, &de) {
			t.Fatalf("HTTP→WS %s: 期望 ErrInvalidRequestBody（非拨号错误）, got %v", body, err)
		}
		if _, err := Dial(context.Background(), PAT("p"), testID(), WithTransport(newFixedTransport(t, DefaultResponsesURL, up.srv.URL)), WithFirstFrame([]byte(body))); !errors.Is(err, ErrInvalidRequestBody) {
			t.Fatalf("Dial(WithFirstFrame) %s: 期望 ErrInvalidRequestBody, got %v", body, err)
		}
	}
	if n := hits.Load(); n != 0 {
		t.Fatalf("HTTP 上游收到 %d 个请求，期望 0", n)
	}
	if n := up.upgrades.Load(); n != 0 {
		t.Fatalf("WS 上游收到 %d 次握手，期望 0", n)
	}

	// Client.Send：无效体本地拒绝，上游收到 0 帧；之后的有效帧照常发出。
	c, err := Dial(context.Background(), PAT("p"), testID(), WithTransport(newFixedTransport(t, DefaultResponsesURL, up.srv.URL)))
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer c.CloseNow()
	for _, body := range append(bad, `{}`, `not json`, ``) {
		if err := c.Send(context.Background(), []byte(body)); !errors.Is(err, ErrInvalidRequestBody) {
			t.Fatalf("Send(%q): 期望 ErrInvalidRequestBody, got %v", body, err)
		}
	}
	if err := c.Send(context.Background(), []byte(`{"model":"gpt-5.5","input":[]}`)); err != nil {
		t.Fatalf("Send(有效帧): %v", err)
	}
	up.waitFrames(t, 1)
	if n := len(up.sentFrames(t)); n != 1 {
		t.Fatalf("上游收到 %d 帧，期望只有 1 个有效帧", n)
	}
}

// TestWSStaticIdentityIncrementalNoReinject：WS→WS 静态身份按连接去重指令/工具
// 插入项。带 previous_response_id 的增量帧：与本连接上次插入的内容相同 → 只删
// 顶层字段、不再插入；不同 → 插入并计数。不带 previous_response_id 的全量帧
// 照常插入。
func TestWSStaticIdentityIncrementalNoReinject(t *testing.T) {
	up := newTSUpstream(t, func(i int, _ []byte) []string {
		rid := "resp_inc_" + strconv.Itoa(i)
		return []string{tsCreatedFrame(rid), tsCompletedFrame(rid)}
	})
	// lite 模型：工具项 + 指令消息两项插入。
	full := []byte(`{"model":"gpt-6.1-sol","instructions":"sys A","tools":[{"type":"function","name":"f"}],"input":[{"role":"user","content":"hi"}]}`)
	c, err := Dial(context.Background(), PAT("p"), testID(), WithTransport(newFixedTransport(t, DefaultResponsesURL, up.srv.URL)), WithFirstFrame(full))
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer c.CloseNow()
	send := func(b string) {
		t.Helper()
		if err := c.Send(context.Background(), []byte(b)); err != nil {
			t.Fatalf("Send: %v", err)
		}
		if _, err := c.Recv(context.Background()); err != nil { // created
			t.Fatalf("Recv: %v", err)
		}
		if _, err := c.Recv(context.Background()); err != nil { // completed
			t.Fatalf("Recv: %v", err)
		}
	}
	r0 := ShapeStatsSnapshot().IncrementalReinjects
	send(string(full))
	incSame := `{"model":"gpt-6.1-sol","previous_response_id":"resp_inc_0","instructions":"sys A","tools":[{"type":"function","name":"f"}],"input":[{"type":"function_call_output","call_id":"c","output":"ok"}]}`
	send(incSame)
	incDiff := strings.Replace(incSame, "sys A", "sys B", 1)
	send(incDiff)
	send(strings.Replace(incDiff, "resp_inc_0", "resp_inc_2", 1)) // 与上一次（B）相同：不插
	frames := up.sentFrames(t)
	if len(frames) != 4 {
		t.Fatalf("上游帧数 = %d", len(frames))
	}
	countInserted := func(f []byte) int {
		n := 0
		gjson.GetBytes(f, "input").ForEach(func(_, el gjson.Result) bool {
			if el.Get("type").String() == "additional_tools" || el.Get("role").String() == "developer" {
				n++
			}
			return true
		})
		return n
	}
	for i, want := range []int{2, 0, 2, 0} {
		f := frames[i]
		if got := countInserted(f); got != want {
			t.Fatalf("帧 %d 插入项 = %d，期望 %d: %s", i, got, want, f)
		}
		if gjson.GetBytes(f, "instructions").Exists() || gjson.GetBytes(f, "tools").Exists() {
			t.Fatalf("帧 %d 顶层 instructions / tools 应删除: %s", i, f)
		}
	}
	if gjson.GetBytes(frames[1], "input.0.type").String() != "function_call_output" ||
		gjson.GetBytes(frames[1], "previous_response_id").String() != "resp_inc_0" {
		t.Fatalf("增量帧应只带客户端输入: %s", frames[1])
	}
	if !strings.Contains(gjson.GetBytes(frames[2], "input.1.content.0.text").String(), "sys B") {
		t.Fatalf("内容变化时应插入新指令: %s", frames[2])
	}
	if d := ShapeStatsSnapshot().IncrementalReinjects - r0; d != 1 {
		t.Fatalf("重新插入计数增量 = %d，期望 1", d)
	}
}

// TestWSFirstIncrementalInjectNotCounted：连接上的第一个插入发生在增量帧上
// （此前没有全量帧、lastInject==0）时照常插入，但不计入 IncrementalReinjects；
// 之后插入项变化才计数。
func TestWSFirstIncrementalInjectNotCounted(t *testing.T) {
	up := newTSUpstream(t, func(i int, _ []byte) []string {
		rid := "resp_fi_" + strconv.Itoa(i)
		return []string{tsCreatedFrame(rid), tsCompletedFrame(rid)}
	})
	c, err := Dial(context.Background(), PAT("p"), testID(), WithTransport(newFixedTransport(t, DefaultResponsesURL, up.srv.URL)))
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer c.CloseNow()
	send := func(b string) {
		t.Helper()
		if err := c.Send(context.Background(), []byte(b)); err != nil {
			t.Fatalf("Send: %v", err)
		}
		for i := 0; i < 2; i++ { // created / completed
			if _, err := c.Recv(context.Background()); err != nil {
				t.Fatalf("Recv: %v", err)
			}
		}
	}
	inc := `{"model":"gpt-6.1-sol","previous_response_id":"resp_x","instructions":"sys A","tools":[{"type":"function","name":"f"}],"input":[{"type":"function_call_output","call_id":"c","output":"ok"}]}`
	r0 := ShapeStatsSnapshot().IncrementalReinjects
	send(inc)
	if d := ShapeStatsSnapshot().IncrementalReinjects - r0; d != 0 {
		t.Fatalf("首个增量帧的插入不应计数，增量 = %d", d)
	}
	send(inc) // 相同：不插、不计
	send(strings.Replace(inc, "sys A", "sys B", 1))
	if d := ShapeStatsSnapshot().IncrementalReinjects - r0; d != 1 {
		t.Fatalf("插入项变化后计数增量 = %d，期望 1", d)
	}
	frames := up.sentFrames(t)
	if len(frames) != 3 {
		t.Fatalf("上游帧数 = %d", len(frames))
	}
	for i, want := range []int{2, 0, 2} {
		if got := len(gjson.GetBytes(frames[i], "input").Array()) - 1; got != want {
			t.Fatalf("帧 %d 插入项 = %d，期望 %d: %s", i, got, want, frames[i])
		}
	}
}

// TestEmptyInstallationIDOmitted：InstallationID 为空时 client_metadata 与
// turn-metadata 两处都省略该键（键序其余不变）。
func TestEmptyInstallationIDOmitted(t *testing.T) {
	id := testID()
	id.InstallationID = ""
	for _, s := range []surface{surfaceHTTP, surfaceWSFromHTTP} {
		sh := mustShape(t, []byte(`{"model":"gpt-5.5","input":[]}`), s, id)
		var out []byte
		if s == surfaceHTTP {
			out = assembleHTTP(&sh)
		} else {
			out, _ = assembleWS(&sh, wsAssembly{})
		}
		cm := gjson.GetBytes(out, "client_metadata")
		if cm.Get("x-codex-installation-id").Exists() {
			t.Fatalf("client_metadata 不应带 x-codex-installation-id: %s", cm.Raw)
		}
		for _, tm := range [][]byte{sh.turnMeta, sh.prewarmMeta, []byte(cm.Get("x-codex-turn-metadata").String())} {
			if !gjson.ValidBytes(tm) || bytes.Contains(tm, []byte("installation_id")) || !bytes.HasPrefix(tm, []byte(`{"session_id":`)) {
				t.Fatalf("turn-metadata 应省略 installation_id: %s", tm)
			}
		}
	}
	// 非空时两处都写，turn-metadata 以 installation_id 开头。
	sh := mustShape(t, []byte(`{"model":"gpt-5.5"}`), surfaceHTTP, testID())
	if !bytes.HasPrefix(sh.turnMeta, []byte(`{"installation_id":"inst-t","session_id":`)) ||
		gjson.GetBytes(assembleHTTP(&sh), "client_metadata.x-codex-installation-id").String() != "inst-t" {
		t.Fatalf("非空 InstallationID 两处都应写: %s", sh.turnMeta)
	}
}

// rawHeaderServer 是只读请求头的 TCP 服务端：记录 HTTP/1.1 线上原始头行（头名
// 大小写原样），回 400 并关闭。
type rawHeaderServer struct {
	ln   net.Listener
	reqs chan string
}

func newRawHeaderServer(t *testing.T) *rawHeaderServer {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	s := &rawHeaderServer{ln: ln, reqs: make(chan string, 16)}
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				defer c.Close()
				br := bufio.NewReader(c)
				var sb strings.Builder
				for {
					l, err := br.ReadString('\n')
					sb.WriteString(l)
					if err != nil || l == "\r\n" {
						break
					}
				}
				s.reqs <- sb.String()
				_, _ = io.WriteString(c, "HTTP/1.1 400 Bad Request\r\nContent-Length: 2\r\nConnection: close\r\n\r\n{}")
			}(c)
		}
	}()
	t.Cleanup(func() { _ = ln.Close() })
	return s
}

// redirectTransport 把请求改投到 addr（Go 的 *http.Transport 照常写头）。
type redirectTransport struct{ addr string }

func (r redirectTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	c := req.Clone(req.Context())
	c.URL = &url.URL{Scheme: "http", Host: r.addr, Path: req.URL.Path}
	c.Host = r.addr
	return (&http.Transport{}).RoundTrip(c)
}

// TestHeaderOverridesGoManagedNoDuplicates：WithHeader 设置由 Go 管理的头
// （Host、Content-Length、Transfer-Encoding、Connection、Accept-Encoding、
// User-Agent）时保持规范写法交给 Go：线上每个头名（大小写不敏感）至多出现一次，
// Content-Length 不被覆盖成冲突值，Accept-Encoding 用调用方的值且 Go 不再补 gzip。
func TestHeaderOverridesGoManagedNoDuplicates(t *testing.T) {
	cases := []struct {
		key, val string
		want     string // 期望线上的值（"" = 不检查值）
	}{
		{"Host", "evil.example", ""},
		{"content-length", "5", ""},
		{"Transfer-Encoding", "chunked", ""},
		{"connection", "keep-alive", ""},
		{"accept-encoding", "identity", "identity"},
		{"user-agent", "custom-ua", "custom-ua"},
		{"X-Custom", "v", "v"},
	}
	for _, tc := range cases {
		srv := newRawHeaderServer(t)
		hc := NewHTTPClient(PAT("tok"), WithTransport(redirectTransport{srv.ln.Addr().String()}), WithHeader(tc.key, tc.val))
		_, _ = hc.StreamBody(context.Background(), []byte(`{"model":"gpt-5.4","input":"hi"}`), testID())
		var raw string
		select {
		case raw = <-srv.reqs:
		case <-time.After(5 * time.Second):
			t.Fatalf("%s: 没有收到请求", tc.key)
		}
		counts := map[string]int{}
		values := map[string]string{}
		for _, line := range strings.Split(raw, "\r\n")[1:] {
			k, v, ok := strings.Cut(line, ":")
			if !ok {
				continue
			}
			lk := strings.ToLower(k)
			counts[lk]++
			values[lk] = strings.TrimSpace(v)
		}
		for k, n := range counts {
			if n > 1 {
				t.Fatalf("WithHeader(%s): 头 %s 出现 %d 次\n%s", tc.key, k, n, raw)
			}
		}
		lk := strings.ToLower(tc.key)
		if tc.want != "" && values[lk] != tc.want {
			t.Fatalf("WithHeader(%s): 线上值 = %q，期望 %q\n%s", tc.key, values[lk], tc.want, raw)
		}
		if lk == "content-length" && values[lk] == "5" {
			t.Fatalf("Content-Length 不应被覆盖成调用方的值\n%s", raw)
		}
		if lk == "host" && values[lk] == "evil.example" {
			t.Fatalf("Host 应由 Go 按请求目标写出\n%s", raw)
		}
	}
}

// TestZstdLowerEncoderMemSameBytes：WithLowerEncoderMem 只改变编码器内部分配，
// 压缩输出逐字节不变（官方抓包体与 <256B / 1–128KB / >128KB 三档），帧头为官方
// 28b52ffd0058。依赖升级改变这一点时本测试失败。
func TestZstdLowerEncoderMemSameBytes(t *testing.T) {
	bodies := [][]byte{[]byte(`{"model":"gpt-5.4"}`), bytes.Repeat([]byte(`{"type":"message","content":"hello 12345"},`), 300), bytes.Repeat([]byte(`{"a":"bcdefg","n":12345},`), 20000)}
	for _, name := range []string{"http-body-turn.zst", "http-body-followup.zst"} {
		raw, err := decodeZstd(loadOfficial(t, name))
		if err != nil {
			t.Fatal(err)
		}
		bodies = append(bodies, raw)
	}
	compress := func(lower bool, body []byte) []byte {
		enc, err := newZstdWriterForTest(lower)
		if err != nil {
			t.Fatal(err)
		}
		var buf bytes.Buffer
		enc.Reset(&buf)
		_, _ = enc.Write(body)
		_ = enc.Flush()
		_ = enc.Close()
		return buf.Bytes()
	}
	for i, b := range bodies {
		def, low, got := compress(false, b), compress(true, b), zstdCompress(b)
		if !bytes.Equal(def, low) || !bytes.Equal(got, def) {
			t.Fatalf("体 %d（%d 字节）：低内存编码器输出与默认不同", i, len(b))
		}
		if !bytes.HasPrefix(got, []byte{0x28, 0xb5, 0x2f, 0xfd, 0x00, 0x58}) {
			t.Fatalf("体 %d 帧头 = %x", i, got[:6])
		}
	}
}

// TestZstdEncoderPoolBounded：空闲编码器列表有上限；并发压缩超过上限时临时
// 新建、多余的丢弃，结果都正确。
func TestZstdEncoderPoolBounded(t *testing.T) {
	body := bytes.Repeat([]byte(`{"type":"message","content":"x"},`), 2000)
	var wg sync.WaitGroup
	for i := 0; i < 4*cap(zstdIdle)+4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			out, err := decodeZstd(zstdCompress(body))
			if err != nil || !bytes.Equal(out, body) {
				t.Errorf("还原失败: %v", err)
			}
		}()
	}
	wg.Wait()
	if n := len(zstdIdle); n > cap(zstdIdle) || cap(zstdIdle) < 1 {
		t.Fatalf("空闲编码器 %d / 上限 %d", n, cap(zstdIdle))
	}
}

// TestAnalyzeConversationInputFirstInputZeroCopy：输入分析与整形同样只认第一次
// 出现的 input（含转义拼写），且不复制 input（零分配）。
func TestAnalyzeConversationInputFirstInputZeroCopy(t *testing.T) {
	payload := []byte(`{"model":"m","input":[{"role":"user","content":"a"},{"type":"function_call_output","output":"x"}],"inp\u0075t":[{"role":"user","content":"b"}]}`)
	got := AnalyzeConversationInput(payload)
	if got.Items != 2 || !got.FollowUp() || got.Anchor != 0 {
		t.Fatalf("应只认第一次出现的 input: %+v", got)
	}
	plain := []byte(`{"model":"m","input":[{"role":"user","content":"a"},{"type":"reasoning"},{"type":"compaction","x":1},{"type":"function_call_output","output":"x"}]}`)
	if n := testing.AllocsPerRun(100, func() { _ = AnalyzeConversationInput(plain) }); n != 0 {
		t.Fatalf("AnalyzeConversationInput 分配 %v 次，期望 0", n)
	}
}
