package codexsdk

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	coderws "github.com/coder/websocket"
	"github.com/tidwall/gjson"
)

// 本文件是连接内核与单连接臂的验收面：每个用例只经公开 API（W9/W10/W11/W16
// 例外——它们钉的是队列/终止的模型行为，只经 push/pop/close/terminate），
// 断言的是"错误面 + 帧字节 + 终止原因"，不断言任何具体类型。
//
// 假上游的形态：行为全部由 handler 决定（写帧 / 挂住 / 停读），因此每个用例都
// 能确定性触发目标路径，不依赖 sleep 抢时序以外的巧合。

const contractPayload = `{"model":"gpt-5.6","input":[]}`

// contractUpstream 是契约用例的假 Responses WS 上游。
type contractUpstream struct {
	srv   *httptest.Server
	mu    sync.Mutex
	dials int
}

// newContractUpstream 起假上游；handler 决定每次升级做什么。
func newContractUpstream(t *testing.T, handler func(c *coderws.Conn, r *http.Request)) *contractUpstream {
	t.Helper()
	u := &contractUpstream{}
	u.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		u.mu.Lock()
		u.dials++
		u.mu.Unlock()
		c, err := coderws.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer c.CloseNow()
		handler(c, r)
	}))
	t.Cleanup(func() {
		u.srv.CloseClientConnections()
		u.srv.Close()
	})
	return u
}

func (u *contractUpstream) dialCount() int {
	u.mu.Lock()
	defer u.mu.Unlock()
	return u.dials
}

// writeFrames 顺序写出脚本帧（写失败即返回：对端已断，脚本没有意义了）。
func writeFrames(c *coderws.Conn, r *http.Request, frames ...string) bool {
	for _, f := range frames {
		if err := c.Write(r.Context(), coderws.MessageText, []byte(f)); err != nil {
			return false
		}
	}
	return true
}

// serveAndStall 写帧后停在对端读循环上：连接保持打开（客户端一断即返回）。
// 这个形态同时服务 pong（常驻读），因此"停读即不服务 pong"由不读的 handler 表达。
func serveAndStall(c *coderws.Conn, r *http.Request, frames ...string) {
	if !writeFrames(c, r, frames...) {
		return
	}
	for {
		if _, _, err := c.Read(r.Context()); err != nil {
			return
		}
	}
}

// contractClient 构造指向假上游的 WS 臂客户端。
func contractClient(t *testing.T, srvURL string, opts ...Option) *HTTPClient {
	t.Helper()
	base := []Option{
		WithResponsesTransport(TransportWS),
		WithTransport(newFixedTransport(t, "https://chatgpt.com/backend-api/codex/responses", srvURL)),
		WithTurnAuto(false),
		WithTraceAuto(false),
	}
	return NewHTTPClient(PAT("pat-contract"), append(base, opts...)...)
}

// contractResponseBody 打开一次 StreamBody 并返回 body（调用方负责关闭）。
func contractResponseBody(t *testing.T, hc *HTTPClient, ctx context.Context) io.ReadCloser {
	t.Helper()
	resp, err := hc.StreamBody(ctx, []byte(contractPayload), Identity{})
	if err != nil {
		t.Fatalf("StreamBody: %v", err)
	}
	return resp.Body
}

// deltaFrame 构造一个小 delta 帧（带唯一序号，便于确认帧序）。
func deltaFrame(i int) string {
	return `{"type":"response.output_text.delta","output_index":0,"content_index":0,"delta":"` +
		strings.Repeat("x", 8) + `#` + itoa(i) + `"}`
}

// itoa 是 strconv.Itoa 的最小替代（避免为一行引入 import）。
func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	var buf [20]byte
	pos := len(buf)
	for i > 0 {
		pos--
		buf[pos] = byte('0' + i%10)
		i /= 10
	}
	return string(buf[pos:])
}

// ---- W1 ----

// TestWSQueueSaturationIsBackpressure：读到首帧后停 4s，上游持续灌 400 个小帧
// → 传输层必须报**可归因的背压**（ErrWSBackpressure），而不是任何被误读成
// "use of closed network connection" 的传输错误（常驻读者保证没人中途放弃读）。
func TestWSQueueSaturationIsBackpressure(t *testing.T) {
	const total = 400
	frames := make([]string, 0, total)
	for i := 0; i < total; i++ {
		frames = append(frames, deltaFrame(i))
	}
	up := newContractUpstream(t, func(c *coderws.Conn, r *http.Request) {
		// 先收客户端那一帧，再持续灌帧并停在对端读循环上（不主动关闭）。
		if _, _, err := c.Read(r.Context()); err != nil {
			return
		}
		serveAndStall(c, r, frames...)
	})
	hc := contractClient(t, up.srv.URL)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	body := contractResponseBody(t, hc, ctx)
	defer func() { _ = body.Close() }()

	// 读到首帧后停 4s：队列（256 帧 / 8MiB）在这段时间内被灌满。
	first := make([]byte, 64)
	if n, err := body.Read(first); n == 0 || err != nil {
		t.Fatalf("首帧读取失败: n=%d err=%v", n, err)
	}
	time.Sleep(4 * time.Second)

	_, err := io.Copy(io.Discard, body)
	if !errors.Is(err, ErrWSBackpressure) {
		t.Fatalf("读尽后错误 = %v，想要 ErrWSBackpressure", err)
	}
	if strings.Contains(strings.ToLower(err.Error()), "use of closed network connection") {
		t.Fatalf("错误应是可归因的背压，而不是传输层关闭: %v", err)
	}
}

// ---- W2 ----

// TestWSConsumeCancelIsContextError：流中途 cancel() → 读到可 errors.Is 的
// ctx 错误（取消不得伪装成 io.EOF）。
func TestWSConsumeCancelIsContextError(t *testing.T) {
	up := newContractUpstream(t, func(c *coderws.Conn, r *http.Request) {
		if _, _, err := c.Read(r.Context()); err != nil {
			return
		}
		serveAndStall(c, r, wsCreatedEv)
	})
	hc := contractClient(t, up.srv.URL)

	ctx, cancel := context.WithCancel(context.Background())
	body := contractResponseBody(t, hc, ctx)
	defer func() { _ = body.Close() }()

	// 取到 created（确定性：第 2 次 Read 之前上游不会再发任何帧）。
	head := make([]byte, 8)
	if n, err := body.Read(head); n == 0 || err != nil {
		t.Fatalf("首帧读取失败: n=%d err=%v", n, err)
	}
	cancel()

	_, err := io.Copy(io.Discard, body)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("取消后错误 = %v，想要 context.Canceled", err)
	}
	if errors.Is(err, io.EOF) {
		t.Fatalf("取消不得伪装成 EOF: %v", err)
	}
}

// ---- W3 ----

// TestWSDialRefusedIsNotHTTPError：连接被拒（无升级响应）→ 原始错误链
// （含 *net.OpError / *url.Error），**不是** *HTTPError{0}。
func TestWSDialRefusedIsNotHTTPError(t *testing.T) {
	hc := NewHTTPClient(PAT("pat-refused"),
		WithResponsesTransport(TransportWS),
		// 127.0.0.1:1 恒拒绝（确定性触发，不依赖端口耗尽）。
		WithTransport(newFixedTransport(t, "https://chatgpt.com/backend-api/codex/responses", "http://127.0.0.1:1")),
		WithTurnAuto(false), WithTraceAuto(false),
	)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, err := hc.StreamBody(ctx, []byte(contractPayload), Identity{})
	if err == nil {
		t.Fatal("期望拨号失败")
	}
	var he *HTTPError
	if errors.As(err, &he) {
		t.Fatalf("无响应失败不应包成 *HTTPError: %+v", he)
	}
	var opErr *net.OpError
	var urlErr *url.Error
	if !errors.As(err, &opErr) && !errors.As(err, &urlErr) {
		t.Fatalf("错误链缺 *net.OpError/*url.Error: %v", err)
	}
}

// ---- W4 ----

// TestWSDialCanceledCtxIsCanceled：预取消 ctx → errors.Is(ctx) 成立，且无
// *HTTPError（D3：取消不是"上游 0 状态码"）。
func TestWSDialCanceledCtxIsCanceled(t *testing.T) {
	hc := contractClient(t, "http://127.0.0.1:1")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := hc.StreamBody(ctx, []byte(contractPayload), Identity{})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("错误 = %v，想要 context.Canceled", err)
	}
	var he *HTTPError
	if errors.As(err, &he) {
		t.Fatalf("取消不应包成 *HTTPError: %+v", he)
	}
}

// ---- W5 ----

// TestWSUpgradeFailureKeepsCause：上游拒绝升级（400 + body）→ *HTTPError{400}
// 且 Raw 带上游 body；errors.As 能回溯到 *DialError（cause 链不丢）。
func TestWSUpgradeFailureKeepsCause(t *testing.T) {
	const upBody = `{"error":{"message":"instructions must not be empty","type":"invalid_request_error"}}`
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(upBody))
	}))
	t.Cleanup(srv.Close)

	hc := contractClient(t, srv.URL)
	_, err := hc.StreamBody(context.Background(), []byte(contractPayload), Identity{})
	var he *HTTPError
	if !errors.As(err, &he) {
		t.Fatalf("错误 = %T: %v，想要 *HTTPError", err, err)
	}
	if he.StatusCode != http.StatusBadRequest {
		t.Fatalf("StatusCode = %d，想要 400", he.StatusCode)
	}
	if !strings.Contains(string(he.Raw), "instructions must not be empty") {
		t.Fatalf("Raw 未带上游 body: %q", string(he.Raw))
	}
	var de *DialError
	if !errors.As(err, &de) {
		t.Fatalf("cause 链应可回溯到 *DialError: %v", err)
	}
	if len(de.Body) == 0 {
		t.Fatal("DialError.Body 应非空（错误分类面对称）")
	}
}

// ---- W5b ----

// TestWSConsecutiveHugeFramesBackpressure：消费者暂停时连续两个超预算巨帧 ⇒
// ErrWSBackpressure（已知取舍：单帧可超预算，连续两帧不行）。
func TestWSConsecutiveHugeFramesBackpressure(t *testing.T) {
	huge := `{"type":"response.output_text.delta","delta":"` + strings.Repeat("h", 9<<20) + `"}`
	up := newContractUpstream(t, func(c *coderws.Conn, r *http.Request) {
		if _, _, err := c.Read(r.Context()); err != nil {
			return
		}
		serveAndStall(c, r, huge, huge)
	})
	hc := contractClient(t, up.srv.URL)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	body := contractResponseBody(t, hc, ctx)
	defer func() { _ = body.Close() }()

	// 消费者暂停：等上游把两帧都推上线（pump 在帧间不会等消费侧）。
	time.Sleep(500 * time.Millisecond)

	_, err := io.Copy(io.Discard, body)
	if !errors.Is(err, ErrWSBackpressure) {
		t.Fatalf("错误 = %v，想要 ErrWSBackpressure", err)
	}
}

// ---- W6 ----

// TestWSHugeFramePassesEmptyQueue：单帧 9MiB、队列空 → 照常交付（合法巨帧
// 不能因为比字节闸大就被判死）。
func TestWSHugeFramePassesEmptyQueue(t *testing.T) {
	payload := strings.Repeat("z", 9<<20)
	huge := `{"type":"response.output_text.delta","delta":"` + payload + `"}`
	up := newContractUpstream(t, func(c *coderws.Conn, r *http.Request) {
		if _, _, err := c.Read(r.Context()); err != nil {
			return
		}
		// 写完巨帧即正常收尾：用例断言的是"这一帧被完整交付"，不是流长度。
		if !writeFrames(c, r, huge) {
			return
		}
		_ = c.Close(coderws.StatusNormalClosure, "")
	})
	hc := contractClient(t, up.srv.URL)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	body := contractResponseBody(t, hc, ctx)
	defer func() { _ = body.Close() }()

	got, err := io.ReadAll(body)
	if err != nil {
		t.Fatalf("读尽巨帧: %v", err)
	}
	if !strings.Contains(string(got), payload) {
		t.Fatalf("巨帧未被完整交付（读到 %d 字节）", len(got))
	}
}

// ---- W7 ----

// TestWSStalledConsumerHitsTimeout：停读 + WithTimeout(200ms) → DeadlineExceeded，
// 且 1s 内 goroutine 回落到基线附近（预热 5 次、容差 ±4、禁 parallel）。
func TestWSStalledConsumerHitsTimeout(t *testing.T) {
	up := newContractUpstream(t, func(c *coderws.Conn, r *http.Request) {
		if _, _, err := c.Read(r.Context()); err != nil {
			return
		}
		serveAndStall(c, r, wsCreatedEv)
	})
	hc := contractClient(t, up.srv.URL, WithTimeout(200*time.Millisecond))

	run := func() error {
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		body := contractResponseBody(t, hc, ctx)
		defer func() { _ = body.Close() }()
		time.Sleep(300 * time.Millisecond) // 停读：让 exchange 的 deadline 到点
		_, err := io.Copy(io.Discard, body)
		return err
	}

	for i := 0; i < 5; i++ { // 预热：把上游/客户端的常驻 goroutine 拉起来
		if err := run(); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("预热第 %d 次错误 = %v，想要 DeadlineExceeded", i, err)
		}
	}
	base := runtime.NumGoroutine()
	if err := run(); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("错误 = %v，想要 DeadlineExceeded", err)
	}
	deadline := time.Now().Add(time.Second)
	for {
		if n := runtime.NumGoroutine(); n <= base+4 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("goroutine 未在 1s 内回落：base=%d now=%d", base, runtime.NumGoroutine())
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// ---- W8 ----

// TestWSStreamBodyCloseFrameIsEOF：上游 close(1000) → body 以 io.EOF 收尾
// （任意 close frame = 流结束，不是传输错误）。
func TestWSStreamBodyCloseFrameIsEOF(t *testing.T) {
	up := newContractUpstream(t, func(c *coderws.Conn, r *http.Request) {
		if _, _, err := c.Read(r.Context()); err != nil {
			return
		}
		if !writeFrames(c, r, wsCreatedEv) {
			return
		}
		_ = c.Close(coderws.StatusNormalClosure, "")
	})
	hc := contractClient(t, up.srv.URL)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	body := contractResponseBody(t, hc, ctx)
	defer func() { _ = body.Close() }()

	buf := make([]byte, 4096)
	var err error
	for {
		if _, err = body.Read(buf); err != nil {
			break
		}
	}
	if err != io.EOF {
		t.Fatalf("收尾错误 = %v，想要 io.EOF", err)
	}
}

// ---- W9 ----

// newTestConn 起一条真实升级的 pumped 连接（上游只做读循环），用于模型用例：
// 只经 push/pop/close/terminate 观察行为。
func newTestConn(t *testing.T, cfg connConfig) *conn {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := coderws.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer c.CloseNow()
		for {
			if _, _, rerr := c.Read(r.Context()); rerr != nil {
				return
			}
		}
	}))
	t.Cleanup(srv.Close)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	t.Cleanup(cancel)
	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http")
	c, _, err := coderws.Dial(ctx, wsURL, nil)
	if err != nil {
		t.Fatalf("拨号测试连接: %v", err)
	}
	cfg.mode = modePumped
	return newConn(c, cfg, ctx, cancel)
}

// TestWSTerminateDrainsQueue：入队 3 帧后终止 → 3 帧全读到，之后才返回 cause。
func TestWSTerminateDrainsQueue(t *testing.T) {
	cn := newTestConn(t, connConfig{readLimit: defaultReadLimit})
	frames := []string{deltaFrame(1), deltaFrame(2), deltaFrame(3)}
	for _, f := range frames {
		if err := cn.inbox.push([]byte(f)); err != nil {
			t.Fatalf("push: %v", err)
		}
	}
	// 用任意终止原因：本用例只关心"终止后先排空队列"，与具体原因无关。
	cause := errors.New("terminate-cause")
	cn.terminate(cause)

	ctx := context.Background()
	for i, want := range frames {
		got, err := cn.inbox.pop(ctx, 0, nil)
		if err != nil {
			t.Fatalf("第 %d 帧: %v（终止后应先排空队列）", i, err)
		}
		if string(got) != want {
			t.Fatalf("第 %d 帧 = %s，想要 %s", i, got, want)
		}
	}
	if _, err := cn.inbox.pop(ctx, 0, nil); !errors.Is(err, cause) {
		t.Fatalf("排空后错误 = %v，想要终止 cause", err)
	}
}

// ---- W10 ----

// TestWSQueueFramesBudget：帧数闸与字节闸（只经 push/pop 观察）。
func TestWSQueueFramesBudget(t *testing.T) {
	t.Run("帧数闸", func(t *testing.T) {
		q := newFrameInbox(4, 1<<20)
		for i := 0; i < 4; i++ {
			if err := q.push([]byte(deltaFrame(i))); err != nil {
				t.Fatalf("第 %d 帧应入队: %v", i, err)
			}
		}
		if err := q.push([]byte(deltaFrame(99))); !errors.Is(err, ErrWSBackpressure) {
			t.Fatalf("第 5 帧错误 = %v，想要 ErrWSBackpressure", err)
		}
	})
	t.Run("字节闸", func(t *testing.T) {
		q := newFrameInbox(100, 1000)
		if err := q.push([]byte(strings.Repeat("a", 600))); err != nil {
			t.Fatalf("空队列首帧应通过: %v", err)
		}
		if err := q.push([]byte(strings.Repeat("b", 600))); !errors.Is(err, ErrWSBackpressure) {
			t.Fatalf("超预算第二帧错误 = %v，想要 ErrWSBackpressure", err)
		}
	})
	t.Run("空队列允许单帧超预算", func(t *testing.T) {
		q := newFrameInbox(100, 1000)
		if err := q.push([]byte(strings.Repeat("c", 4096))); err != nil {
			t.Fatalf("空队列巨帧应通过: %v", err)
		}
	})
	t.Run("排空后腾出名额", func(t *testing.T) {
		q := newFrameInbox(2, 1<<20)
		for i := 0; i < 2; i++ {
			if err := q.push([]byte(deltaFrame(i))); err != nil {
				t.Fatalf("push: %v", err)
			}
		}
		if _, err := q.pop(context.Background(), 0, nil); err != nil {
			t.Fatalf("pop: %v", err)
		}
		if err := q.push([]byte(deltaFrame(7))); err != nil {
			t.Fatalf("消费后应可再入队: %v", err)
		}
	})
}

// ---- W11 ----

// TestWSFirstCauseWins：并发 terminate(a) / terminate(b) → 只有首个原因生效，
// 且后续观察（pop）永远看到同一个原因。
func TestWSFirstCauseWins(t *testing.T) {
	causeA := errors.New("first-cause-a")
	causeB := errors.New("first-cause-b")
	cn := newTestConn(t, connConfig{readLimit: defaultReadLimit})

	start := make(chan struct{})
	var wg sync.WaitGroup
	for _, cause := range []error{causeA, causeB} {
		wg.Add(1)
		go func(cause error) {
			defer wg.Done()
			<-start
			cn.terminate(cause)
		}(cause)
	}
	close(start)
	wg.Wait()

	ctx := context.Background()
	_, first := cn.inbox.pop(ctx, 0, nil)
	_, second := cn.inbox.pop(ctx, 0, nil)
	if !errors.Is(first, causeA) && !errors.Is(first, causeB) {
		t.Fatalf("终止原因 = %v，想要 a 或 b", first)
	}
	if first != second { //nolint:errorlint // 要的就是同一个 error 值（唯一起因）
		t.Fatalf("首因不稳定: 第一次 %v，第二次 %v", first, second)
	}
}

// ---- W12 ----

// TestWSCompletedSurvivesLateCancel：取消先到（未取到终态帧）→ ctx 错误；
// 终态先到 → 成功交付，事后取消不改写结果。
func TestWSCompletedSurvivesLateCancel(t *testing.T) {
	t.Run("取消先到", func(t *testing.T) {
		up := newContractUpstream(t, func(c *coderws.Conn, r *http.Request) {
			if _, _, err := c.Read(r.Context()); err != nil {
				return
			}
			serveAndStall(c, r, wsCreatedEv)
		})
		hc := contractClient(t, up.srv.URL)
		ctx, cancel := context.WithCancel(context.Background())
		body := contractResponseBody(t, hc, ctx)
		defer func() { _ = body.Close() }()
		cancel()
		if _, err := io.Copy(io.Discard, body); !errors.Is(err, context.Canceled) {
			t.Fatalf("错误 = %v，想要 context.Canceled", err)
		}
	})

	t.Run("终态先到", func(t *testing.T) {
		fin := make(chan struct{})
		up := newContractUpstream(t, func(c *coderws.Conn, r *http.Request) {
			if _, _, err := c.Read(r.Context()); err != nil {
				return
			}
			if !writeFrames(c, r, wsCreatedEv, wsCompletedEv) {
				return
			}
			close(fin)
			serveAndStall(c, r)
		})
		hc := contractClient(t, up.srv.URL)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		body := contractResponseBody(t, hc, ctx)
		defer func() { _ = body.Close() }()

		select {
		case <-fin:
		case <-time.After(10 * time.Second):
			t.Fatal("上游未写完终态帧")
		}
		time.Sleep(100 * time.Millisecond) // 让 pump 把终态帧入队
		cancel()

		got, err := io.ReadAll(body)
		if err != nil {
			t.Fatalf("事后取消不应改写已提交的结果: %v", err)
		}
		if !strings.Contains(string(got), "data: [DONE]") {
			t.Fatalf("终态帧未完整交付:\n%s", got)
		}
	})
}

// ---- W12b ----

// TestWSQueuedTerminalBeatsDeadline：终态帧已入队、消费侧 deadline 随即到点 →
// 仍交付终态帧并成功（已完成不因消费侧超时被判失败）。
func TestWSQueuedTerminalBeatsDeadline(t *testing.T) {
	fin := make(chan struct{})
	up := newContractUpstream(t, func(c *coderws.Conn, r *http.Request) {
		if _, _, err := c.Read(r.Context()); err != nil {
			return
		}
		if !writeFrames(c, r, wsCreatedEv, wsCompletedEv) {
			return
		}
		close(fin)
		serveAndStall(c, r)
	})
	hc := contractClient(t, up.srv.URL)
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	body := contractResponseBody(t, hc, ctx)
	defer func() { _ = body.Close() }()

	select {
	case <-fin:
	case <-time.After(10 * time.Second):
		t.Fatal("上游未写完终态帧")
	}
	time.Sleep(50 * time.Millisecond) // 终态帧入队先于 deadline
	<-ctx.Done()                      // deadline 到点：此刻帧已在队列里

	got, err := io.ReadAll(body)
	if err != nil {
		t.Fatalf("已入队的终态帧应优先于 deadline: %v", err)
	}
	if !strings.Contains(string(got), "data: [DONE]") {
		t.Fatalf("终态帧未完整交付:\n%s", got)
	}
}

// ---- W12c ----

// TestWSNextDeliversQueuedFrameOnCancelRace：消费者**已阻塞在 `pop` 里**、帧随后
// 入队、取消随即到点——已入队的帧仍须交付（不变式不能只在 `Next` 入口成立）。
//
// 构造上保证"帧先于 deadline 入队"：`push` 返回后才 `cancel`。此时 `pop` 的
// `select` 会同时看到 `notify` 与 `ctx.Done()`，走哪支是随机的——旧实现只要选中
// ctx 分支就把帧丢掉（`Next` 直接 `terminate` 返回 ctx 错误），50 轮必然复现。
func TestWSNextDeliversQueuedFrameOnCancelRace(t *testing.T) {
	for i := 0; i < 50; i++ {
		cn := newTestConn(t, connConfig{readLimit: defaultReadLimit})
		ex := &wsExchange{cn: cn}
		ctx, cancel := context.WithCancel(context.Background())
		type result struct {
			data []byte
			err  error
		}
		done := make(chan result, 1)
		go func() {
			data, err := ex.Next(ctx)
			done <- result{data, err}
		}()
		time.Sleep(200 * time.Microsecond) // 让 Next 先卡在 pop 的 select 上
		want := deltaFrame(i)
		if err := cn.inbox.push([]byte(want)); err != nil {
			t.Fatalf("第 %d 轮 push: %v", i, err)
		}
		cancel() // 帧已在队，取消到点
		select {
		case r := <-done:
			if r.err != nil {
				t.Fatalf("第 %d 轮：已入队的帧被取消吞掉: %v", i, r.err)
			}
			if string(r.data) != want {
				t.Fatalf("第 %d 轮：帧内容不符: %q", i, r.data)
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("第 %d 轮：Next 未返回", i)
		}
		cancel()
		cn.terminate(io.EOF) // 收尾，避免 50 条连接堆积
	}
}

// ---- W13 ----

// TestWSNoActivePing：**客户端不主动发 WS 层 ping**（对齐真实 codex 客户端）。
//
// 假上游在升级选项中挂 OnPingReceived/OnPongReceived 计数（coder 只在读循环在飞
// 时派发控制帧回调，所以假上游必须同时有常驻读）。客户端用 20ms 的
// WithPingInterval —— 旧实现下这段时间会打出十几条 ping；现在该选项是纯兼容面。
//
// 断言：整段观察窗内**一个** ping 都收不到，且响应照常完成（没有"因为没判活而
// 被误杀"的迹象）。
func TestWSNoActivePing(t *testing.T) {
	var pings, pongs atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := coderws.Accept(w, r, &coderws.AcceptOptions{
			OnPingReceived: func(context.Context, []byte) bool { pings.Add(1); return true },
			OnPongReceived: func(context.Context, []byte) { pongs.Add(1) },
		})
		if err != nil {
			return
		}
		defer c.CloseNow()
		go func() { // 常驻读：既是控制帧回调的前提，也在服务客户端可能的 ping
			for {
				if _, _, rerr := c.Read(r.Context()); rerr != nil {
					return
				}
			}
		}()
		// 观察窗 400ms（20ms 间隔的旧心跳会打 ~20 条 ping），再回一组正常帧。
		select {
		case <-time.After(400 * time.Millisecond):
		case <-r.Context().Done():
			return
		}
		_ = writeFrames(c, r, wsCreatedEv, wsCompletedEv)
		_ = c.Close(coderws.StatusNormalClosure, "")
	}))
	t.Cleanup(func() { srv.CloseClientConnections(); srv.Close() })

	hc := contractClient(t, srv.URL, WithPingInterval(20*time.Millisecond))
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	body := contractResponseBody(t, hc, ctx)
	defer func() { _ = body.Close() }()

	got, err := io.ReadAll(body)
	if err != nil {
		t.Fatalf("读 body: %v", err)
	}
	if !strings.Contains(string(got), "data: [DONE]") {
		t.Fatalf("响应未正常完成（无主动 ping 不该影响取帧）:\n%s", string(got))
	}
	if n := pings.Load(); n != 0 {
		t.Fatalf("客户端发出了 %d 个主动 ping，期望 0（WithPingInterval 已无效果）", n)
	}
	if n := pongs.Load(); n != 0 {
		t.Fatalf("客户端回了 %d 个 pong，但上游从未发 ping（不该有无端控制帧）", n)
	}
}

// ---- W14 ----

// TestWSFrameShapeUnchanged：帧 → SSE 的字节形状全量 golden。覆盖 event: 行取自
// data.type、帧字节 verbatim、[DONE] 位置、failed/incomplete 不补 [DONE]、
// type:error 透传、无 type 不出 event 行。
func TestWSFrameShapeUnchanged(t *testing.T) {
	noType := `{"hello":"world"}`
	errEv := `{"type":"error","error":{"type":"invalid_request_error","message":"nope"}}`
	incompleteEv := `{"type":"response.incomplete","response":{"id":"resp_ws","status":"incomplete"}}`

	block := func(frames ...string) string {
		var b strings.Builder
		for _, f := range frames {
			typ := gjson.Get(f, "type").String()
			if typ != "" {
				b.WriteString("event:" + typ + "\n")
			}
			b.WriteString("data:" + f + "\n\n")
		}
		return b.String()
	}

	cases := []struct {
		name   string
		frames []string
		want   string
	}{
		{
			name:   "completed 后补 [DONE]",
			frames: []string{wsCreatedEv, noType, errEv, wsCompletedEv},
			want: block(wsCreatedEv, noType, errEv, wsCompletedEv) +
				"data: [DONE]\n\n",
		},
		{
			name:   "failed 不补 [DONE]",
			frames: []string{wsCreatedEv, wsFailedEv},
			want:   block(wsCreatedEv, wsFailedEv),
		},
		{
			name:   "incomplete 不补 [DONE]",
			frames: []string{wsCreatedEv, incompleteEv},
			want:   block(wsCreatedEv, incompleteEv),
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			frames := tc.frames
			up := newContractUpstream(t, func(c *coderws.Conn, r *http.Request) {
				if _, _, err := c.Read(r.Context()); err != nil {
					return
				}
				serveAndStall(c, r, frames...)
			})
			hc := contractClient(t, up.srv.URL)
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			got, err := io.ReadAll(contractResponseBody(t, hc, ctx))
			if err != nil {
				t.Fatalf("读 body: %v", err)
			}
			if string(got) != tc.want {
				t.Fatalf("SSE 字节流不符\n实际 = %q\n想要 = %q", got, tc.want)
			}
			if n := strings.Count(string(got), "[DONE]"); n != strings.Count(tc.want, "[DONE]") {
				t.Fatalf("[DONE] 次数 = %d，想要 %d", n, strings.Count(tc.want, "[DONE]"))
			}
		})
	}
}

// ---- W15 ----

// TestWSCloseIdempotentNoPanic：Close 连续 3 次 + 2 goroutine 并发 Close：
// 不 panic，且所有调用拿到同一结果。
func TestWSCloseIdempotentNoPanic(t *testing.T) {
	url, _ := startEchoServer(t, "")
	c, err := Dial(context.Background(), PAT("pat-close"), Identity{},
		WithTransport(newFixedTransport(t, "https://chatgpt.com/backend-api/codex/responses", url)))

	if err != nil {
		t.Fatalf("Dial: %v", err)
	}

	results := make([]error, 0, 5)
	for i := 0; i < 3; i++ {
		results = append(results, c.Close(StatusNormalClosure, ""))
	}
	var mu sync.Mutex
	var wg sync.WaitGroup
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := c.Close(StatusNormalClosure, "")
			mu.Lock()
			results = append(results, err)
			mu.Unlock()
		}()
	}
	wg.Wait()

	first := results[0]
	for i, err := range results {
		if (err == nil) != (first == nil) { //nolint:errorlint // 比较"是否同一结果"，不比 identity
			t.Fatalf("第 %d 次 Close 结果 = %v，首个 = %v", i, err, first)
		}
		if err != nil && first != nil && err.Error() != first.Error() {
			t.Fatalf("第 %d 次 Close 结果 = %v，首个 = %v", i, err, first)
		}
	}
	if !c.Closed() {
		t.Fatal("Close 之后 Closed() 应为 true")
	}
}

// ---- W16 ----

// TestWSInboxCloseWakesWaiter：空队列上阻塞的 pop，close(cause) 后 1s 内返回
// cause（不得永久挂起）。
func TestWSInboxCloseWakesWaiter(t *testing.T) {
	q := newFrameInbox(8, 1<<20)
	cause := errors.New("inbox-closed-cause")

	done := make(chan error, 1)
	go func() {
		_, err := q.pop(context.Background(), 0, nil)
		done <- err
	}()
	time.Sleep(50 * time.Millisecond) // 让 pop 真正进入等待
	q.close(cause)

	select {
	case err := <-done:
		if !errors.Is(err, cause) {
			t.Fatalf("pop 返回 %v，想要 %v", err, cause)
		}
	case <-time.After(time.Second):
		t.Fatal("close 未唤醒空队列上的 pop")
	}
}
