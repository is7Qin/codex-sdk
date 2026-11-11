package codexsdk

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"runtime"
	"sync"
	"testing"
	"time"

	coderws "github.com/coder/websocket"
)

// 本文件是 WS 读空闲上限（idle read timeout）的验收面：P1–P11 + T1–T6。
//
// 口径（与 spec §5 一致）：
//   - 计时单位是**消费侧一次取帧等待**（pumped：pop 真正阻塞的一段；pull：一次 Recv），
//     不是"socket 多久没字节"。因此慢消费者、已入队帧、终态排队都不消耗预算。
//   - 超时一律经 beginFinish + lease.CompareAndSwap 裁决后才 terminate；seam 只有一个：
//     idleCommitHook(e)。
//   - 同步全部用 channel / 既有 waitFor barrier；**没有**用 sleep 抢时序的断言
//     （time.After/Ticker 只作为"慢消费者 / 分段上游"的**激励**，不作为断言依据）。
const idleBudget = 40 * time.Millisecond

// idleThread 是参与复用的身份（threadKey 非空）。
var idleThread = Identity{Session: Session{ThreadID: "idle-thread"}}

// ---- 夹具 ----

// idleUpstream 是 idle 用例的假 Responses WS 上游：每次升级执行一次 script。
//
// 计数刻意分开：dials（handler 进入次数）/ upgrades（升级成功次数）/ requests（收到的
// 客户端帧数）——P1 要能回答"是重拨了，还是重发了 create"。
type idleUpstream struct {
	srv *httptest.Server

	mu        sync.Mutex
	dials     int
	upgrades  int
	requests  int
	pongSeen  int
	closeSeen int

	gone     chan struct{} // 任一条连接被客户端断开时关闭（只有第一次）
	goneOnce sync.Once
}

func newIdleUpstream(t *testing.T, script func(u *idleUpstream, c *coderws.Conn, r *http.Request)) *idleUpstream {
	t.Helper()
	u := &idleUpstream{gone: make(chan struct{})}
	u.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		u.mu.Lock()
		u.dials++
		u.mu.Unlock()
		c, err := coderws.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer c.CloseNow()
		u.mu.Lock()
		u.upgrades++
		u.mu.Unlock()
		script(u, c, r)
	}))
	t.Cleanup(func() {
		u.srv.CloseClientConnections()
		u.srv.Close()
	})
	return u
}

func (u *idleUpstream) dialCount() int {
	u.mu.Lock()
	defer u.mu.Unlock()
	return u.dials
}

func (u *idleUpstream) upgradeCount() int {
	u.mu.Lock()
	defer u.mu.Unlock()
	return u.upgrades
}

func (u *idleUpstream) requestCount() int {
	u.mu.Lock()
	defer u.mu.Unlock()
	return u.requests
}

func (u *idleUpstream) pongCount() int {
	u.mu.Lock()
	defer u.mu.Unlock()
	return u.pongSeen
}

func (u *idleUpstream) closeCount() int {
	u.mu.Lock()
	defer u.mu.Unlock()
	return u.closeSeen
}

func (u *idleUpstream) markGone() { u.goneOnce.Do(func() { close(u.gone) }) }

// waitRequests 等上游收到第 n 帧（barrier：拨号 + 写 create 都已完成）。
func (u *idleUpstream) waitRequests(t *testing.T, n int) {
	t.Helper()
	waitFor(t, func() bool { return u.requestCount() >= n })
}

// waitGone 等"服务端观察到断连"（读返回错误）。
func (u *idleUpstream) waitGone(t *testing.T, within time.Duration) bool {
	t.Helper()
	select {
	case <-u.gone:
		return true
	case <-time.After(within):
		return false
	}
}

// readRequest 读一帧并计数（客户端每轮只发一帧）。失败 = 对端已断。
func (u *idleUpstream) readRequest(c *coderws.Conn, r *http.Request) bool {
	if _, _, err := c.Read(r.Context()); err != nil {
		u.markGone()
		return false
	}
	u.mu.Lock()
	u.requests++
	u.mu.Unlock()
	return true
}

// drain 停在对端读循环上：连接保持打开（服务 pong），并登记断连/关闭帧。
func (u *idleUpstream) drain(c *coderws.Conn, r *http.Request) {
	for {
		_, _, err := c.Read(r.Context())
		if err != nil {
			u.markGone()
			if coderws.CloseStatus(err) >= 0 {
				u.mu.Lock()
				u.closeSeen++
				u.mu.Unlock()
			}
			return
		}
	}
}

// idleClient 构造 WS 臂（pumped）客户端：固定 idle 预算 + 调用方追加的 Option。
func idleClient(t *testing.T, srvURL string, opts ...Option) *HTTPClient {
	t.Helper()
	return contractClient(t, srvURL, append([]Option{WithWSIdleTimeout(idleBudget)}, opts...)...)
}

// openIdleExchange 建一条 exchange 并等 create 帧到上游（barrier）。
func openIdleExchange(t *testing.T, hc *HTTPClient, ctx context.Context, up *idleUpstream) *wsExchange {
	t.Helper()
	ex, err := hc.openWSExchange(ctx, []byte(contractPayload), idleThread, wsStreamReadLimit)
	if err != nil {
		t.Fatalf("openWSExchange: %v", err)
	}
	if up != nil {
		up.waitRequests(t, 1)
	}
	return ex
}

// dialIdlePull 构造 pull 客户端（Dial → Client）。
func dialIdlePull(t *testing.T, srvURL string, opts ...Option) *Client {
	t.Helper()
	base := []Option{
		WithTransport(newFixedTransport(t, "https://chatgpt.com/backend-api/codex/responses", srvURL)),
		WithTurnAuto(false),
		WithTraceAuto(false),
	}
	cl, err := Dial(context.Background(), PAT("pat-idle"), Identity{}, append(base, opts...)...)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	t.Cleanup(func() { _ = cl.CloseNow() })
	return cl
}

// pauseFor 用 channel 表达"这段时间里消费侧什么都没做"（激励，不是断言依据）。
func pauseFor(d time.Duration) {
	done := make(chan struct{})
	time.AfterFunc(d, func() { close(done) })
	<-done
}

// assertIdleWindow 断言一次等待的时延落在 [0.9×budget, budget+500ms]。
func assertIdleWindow(t *testing.T, elapsed time.Duration) {
	t.Helper()
	if elapsed < idleBudget*9/10 {
		t.Fatalf("等待只用了 %s，短于 0.9×budget（%s）——计时起点不对", elapsed, idleBudget)
	}
	if elapsed > idleBudget+500*time.Millisecond {
		t.Fatalf("等待用了 %s，超过 budget+500ms（%s）——超时路径没生效", elapsed, idleBudget+500*time.Millisecond)
	}
}

// ---- P1：pumped 首帧（上游收 create 后静默）----

func TestWSIdlePumpedFirstFrame(t *testing.T) {
	up := newIdleUpstream(t, func(u *idleUpstream, c *coderws.Conn, r *http.Request) {
		if !u.readRequest(c, r) {
			return
		}
		u.drain(c, r) // 静默：一个字节都不回
	})
	hc := idleClient(t, up.srv.URL, WithWSConnCache(2))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	ex := openIdleExchange(t, hc, ctx, up)

	start := time.Now()
	_, nerr := ex.Next(ctx)
	elapsed := time.Since(start)

	if !errors.Is(nerr, ErrWSIdleTimeout) {
		t.Fatalf("Next = %v，期望 ErrWSIdleTimeout", nerr)
	}
	assertIdleWindow(t, elapsed)
	if ex.Release() {
		t.Fatal("空闲超时的连接 Release() 报成功（不可归还）")
	}
	if got := up.requestCount(); got != 1 {
		t.Fatalf("上游收到 %d 帧，期望 1（不得重发 create）", got)
	}
	if got := up.dialCount(); got != 1 {
		t.Fatalf("拨号 %d 次，期望 1（不得重拨）", got)
	}
	if got := up.upgradeCount(); got != 1 {
		t.Fatalf("升级 %d 次，期望 1（不得重拨）", got)
	}
	// 终止原因必须是哨兵：不得被取消守卫洗成 context.Canceled。
	if got := ex.cn.err(); !errors.Is(got, ErrWSIdleTimeout) {
		t.Fatalf("终止原因 = %v，期望 ErrWSIdleTimeout（被洗成 ctx 错误？）", got)
	}
}

// ---- P2a：慢消费者不会被杀（队列有货不计时）----

func TestWSIdleSlowConsumerNotKilled(t *testing.T) {
	nextFrame := make(chan struct{})
	up := newIdleUpstream(t, func(u *idleUpstream, c *coderws.Conn, r *http.Request) {
		if !u.readRequest(c, r) {
			return
		}
		if !writeFrames(c, r, wsCreatedEv, wsDeltaEv, wsItemDoneEv) {
			u.markGone()
			return
		}
		select {
		case <-nextFrame: // 消费者慢到超过预算之后，上游才写第 4 帧
		case <-time.After(5 * time.Second):
			return
		}
		if !writeFrames(c, r, wsCompletedEv) {
			u.markGone()
			return
		}
		u.drain(c, r)
	})
	hc := idleClient(t, up.srv.URL)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	ex := openIdleExchange(t, hc, ctx, up)
	defer ex.cn.terminate(io.EOF)

	for i, want := range []string{wsCreatedEv, wsDeltaEv, wsItemDoneEv} {
		got, err := ex.Next(ctx)
		if err != nil {
			t.Fatalf("第 %d 帧: %v", i, err)
		}
		if string(got) != want {
			t.Fatalf("第 %d 帧 = %s，期望 %s", i, got, want)
		}
	}
	// 消费者暂停 3×budget：期间没有 Next 在飞 ⇒ 没有计时器，连接不能被动退役。
	pauseFor(3 * idleBudget)
	if cerr := ex.cn.err(); cerr != nil {
		t.Fatalf("慢消费者把连接杀了: %v（时钟在 socket 上？）", cerr)
	}
	close(nextFrame)
	got, err := ex.Next(ctx)
	if err != nil {
		t.Fatalf("暂停 %s 后取下一帧: %v（慢消费者被误杀）", 3*idleBudget, err)
	}
	if string(got) != wsCompletedEv {
		t.Fatalf("第 4 帧 = %s", got)
	}
	if ex.cn.err() != nil {
		t.Fatalf("整轮无错误却出现终止原因: %v", ex.cn.err())
	}
}

// ---- P2b：帧间（队列空）超时 ----

func TestWSIdleBetweenFrames(t *testing.T) {
	up := newIdleUpstream(t, func(u *idleUpstream, c *coderws.Conn, r *http.Request) {
		if !u.readRequest(c, r) {
			return
		}
		if !writeFrames(c, r, wsCreatedEv, wsDeltaEv, wsItemDoneEv) {
			u.markGone()
			return
		}
		u.drain(c, r)
	})
	hc := idleClient(t, up.srv.URL)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	ex := openIdleExchange(t, hc, ctx, up)

	for i := 0; i < 3; i++ {
		if _, err := ex.Next(ctx); err != nil {
			t.Fatalf("第 %d 帧: %v", i, err)
		}
	}
	start := time.Now()
	_, nerr := ex.Next(ctx)
	elapsed := time.Since(start)
	if !errors.Is(nerr, ErrWSIdleTimeout) {
		t.Fatalf("帧间等待 = %v，期望 ErrWSIdleTimeout", nerr)
	}
	assertIdleWindow(t, elapsed)
}

// ---- P2c：终态排队（消费者慢，但连接仍可归还）----

func TestWSIdleTerminalQueuedStillReleasable(t *testing.T) {
	up := newIdleUpstream(t, func(u *idleUpstream, c *coderws.Conn, r *http.Request) {
		if !u.readRequest(c, r) {
			return
		}
		if !writeFrames(c, r, wsCreatedEv, wsCompletedEv) {
			u.markGone()
			return
		}
		u.drain(c, r)
	})
	hc := idleClient(t, up.srv.URL, WithWSConnCache(1))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	ex := openIdleExchange(t, hc, ctx, up)

	if _, err := ex.Next(ctx); err != nil {
		t.Fatalf("Next(created): %v", err)
	}
	// 终态已排队、消费者拖过 3×budget 才回来读。
	pauseFor(3 * idleBudget)
	got, err := ex.Next(ctx)
	if err != nil {
		t.Fatalf("终态帧: %v（排队终态被计时器写坏了）", err)
	}
	if string(got) != wsCompletedEv {
		t.Fatalf("终态帧 = %s", got)
	}
	if !ex.cn.releasable() {
		t.Fatalf("读完终态后连接不可归还: %v", ex.cn.err())
	}
	if !ex.Release() {
		t.Fatal("正常读完终态的连接 Release() 报失败（时钟在 socket 侧？）")
	}
}

// ---- P2d：帧优先于超时（白盒，无竞态）----

func TestWSIdleFrameBeatsTimeout(t *testing.T) {
	up := newContractUpstream(t, func(c *coderws.Conn, r *http.Request) {
		serveAndStall(c, r)
	})
	hc := contractClient(t, up.srv.URL, WithWSIdleTimeout(idleBudget))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	ex, err := hc.openWSExchange(ctx, []byte(contractPayload), Identity{}, wsStreamReadLimit)
	if err != nil {
		t.Fatalf("openWSExchange: %v", err)
	}
	defer ex.cn.terminate(io.EOF)

	// 模拟"超时判决的同一刻，帧已经到队"：先入队，再走超时裁决。
	if perr := ex.cn.inbox.push([]byte(wsCreatedEv)); perr != nil {
		t.Fatalf("push: %v", perr)
	}
	// 直接驱动 pop 返回 errIdleWait 之后的那段裁决（Next 里只是调用它）。
	got, nerr := ex.nextOnIdle(context.Background())
	if nerr != nil {
		t.Fatalf("nextOnIdle = %v，期望交付已入队帧（① 帧优先在 ③ 退役之前）", nerr)
	}
	if string(got) != wsCreatedEv {
		t.Fatalf("交付帧 = %s", got)
	}
	if cerr := ex.cn.err(); cerr != nil {
		t.Fatalf("已到帧被读成超时并退役了连接: %v", cerr)
	}
}

// ---- P3：长流不误杀（同夹具的负例把静默拉到 1.5×budget）----

func TestWSIdleLongStreamNotKilled(t *testing.T) {
	cases := []struct {
		name     string
		gap      time.Duration
		wantIdle bool
	}{
		{"每0.6×budget一帧", idleBudget * 6 / 10, false},
		{"静默1.5×budget", idleBudget * 3 / 2, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			up := newIdleUpstream(t, func(u *idleUpstream, c *coderws.Conn, r *http.Request) {
				if !u.readRequest(c, r) {
					return
				}
				for i := 0; i < 10; i++ {
					if i > 0 && tc.gap > 0 {
						select {
						case <-time.After(tc.gap):
						case <-time.After(5 * time.Second):
							return
						}
					}
					if !writeFrames(c, r, deltaFrame(i)) {
						u.markGone()
						return
					}
				}
				u.drain(c, r)
			})
			hc := idleClient(t, up.srv.URL)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			ex := openIdleExchange(t, hc, ctx, up)

			delivered := 0
			var lastErr error
			for i := 0; i < 10; i++ {
				if _, err := ex.Next(ctx); err != nil {
					lastErr = err
					break
				}
				delivered++
			}
			if tc.wantIdle {
				if !errors.Is(lastErr, ErrWSIdleTimeout) {
					t.Fatalf("静默 %s 后 Next = %v，期望 ErrWSIdleTimeout（夹具没判别力）", tc.gap, lastErr)
				}
				if delivered == 10 {
					t.Fatal("静默段没有被检测到")
				}
				return
			}
			if lastErr != nil {
				t.Fatalf("第 %d 帧后失败: %v（正常节奏的长流被误杀）", delivered, lastErr)
			}
			if delivered != 10 {
				t.Fatalf("交付 %d 帧，期望 10", delivered)
			}
		})
	}
}

// ---- P4：pull 首帧 ----

func TestWSIdlePullFirstFrame(t *testing.T) {
	up := newIdleUpstream(t, func(u *idleUpstream, c *coderws.Conn, r *http.Request) {
		if !u.readRequest(c, r) {
			return
		}
		u.drain(c, r)
	})
	cl := dialIdlePull(t, up.srv.URL, WithWSIdleTimeout(idleBudget))
	if err := cl.Send(context.Background(), []byte(contractPayload)); err != nil {
		t.Fatalf("Send: %v", err)
	}
	up.waitRequests(t, 1)

	start := time.Now()
	_, err := cl.Recv(context.Background())
	elapsed := time.Since(start)
	if !errors.Is(err, ErrWSIdleTimeout) {
		t.Fatalf("Recv = %v，期望 ErrWSIdleTimeout", err)
	}
	assertIdleWindow(t, elapsed)

	// 连接已退役：第二次 Recv 必须在 1s 内返回非 nil 错误（不挂住）。
	second := make(chan error, 1)
	go func() {
		_, rerr := cl.Recv(context.Background())
		second <- rerr
	}()
	select {
	case rerr := <-second:
		if rerr == nil {
			t.Fatal("连接已退役，第二次 Recv 却成功了")
		}
	case <-time.After(time.Second):
		t.Fatal("连接已退役，第二次 Recv 挂住了（M5：只返错不 terminate）")
	}
	if got := cl.cn.err(); !errors.Is(got, ErrWSIdleTimeout) {
		t.Fatalf("终止原因 = %v，期望 ErrWSIdleTimeout", got)
	}
	if !up.waitGone(t, 2*time.Second) {
		t.Fatal("服务端没有观察到断连")
	}
}

// ---- P4b：pull 轮间静默（契约变更的显式用例）----

func TestWSIdlePullInterRoundSilence(t *testing.T) {
	up := newIdleUpstream(t, func(u *idleUpstream, c *coderws.Conn, r *http.Request) {
		if !u.readRequest(c, r) {
			return
		}
		if !writeFrames(c, r, wsCreatedEv) {
			u.markGone()
			return
		}
		u.drain(c, r) // 之后永久静默
	})
	cl := dialIdlePull(t, up.srv.URL, WithWSIdleTimeout(idleBudget))
	if err := cl.Send(context.Background(), []byte(contractPayload)); err != nil {
		t.Fatalf("Send: %v", err)
	}
	up.waitRequests(t, 1)
	if got, err := cl.Recv(context.Background()); err != nil {
		t.Fatalf("首帧 Recv: %v", err)
	} else if string(got) != wsCreatedEv {
		t.Fatalf("首帧 = %s", got)
	}

	// 轮间静默：没有 Recv 在飞 ⇒ 没有计时器在跑，连接不该在这期间被退役。
	pauseFor(3 * idleBudget)
	if cerr := cl.cn.err(); cerr != nil {
		t.Fatalf("没有 Recv 在飞时连接被退役了: %v（计时器不跨等待存活）", cerr)
	}

	// 轮间静默 > 预算：下一次 Recv 重新起预算，仍拿不到帧 ⇒ 会话被关。
	start := time.Now()
	_, err := cl.Recv(context.Background())
	elapsed := time.Since(start)
	if !errors.Is(err, ErrWSIdleTimeout) {
		t.Fatalf("轮间静默后的 Recv = %v，期望 ErrWSIdleTimeout", err)
	}
	assertIdleWindow(t, elapsed)
	if got := cl.cn.err(); !errors.Is(got, ErrWSIdleTimeout) {
		t.Fatalf("终止原因 = %v，期望 ErrWSIdleTimeout（契约变更：会话被关）", got)
	}
	if !up.waitGone(t, 2*time.Second) {
		t.Fatal("服务端没有观察到断连")
	}
	// B11：不断言 Closed()（它只表示本地 Close/CloseNow）。
}

// ---- P5：Ping / Pong 不续命 ----

func TestWSIdlePingDoesNotExtendPumped(t *testing.T) {
	stop := make(chan struct{})
	var stopOnce sync.Once
	defer stopOnce.Do(func() { close(stop) })

	up := newIdleUpstream(t, func(u *idleUpstream, c *coderws.Conn, r *http.Request) {
		if !u.readRequest(c, r) {
			return
		}
		// 静默期每 5ms 一个 Ping：客户端必须就地回 Pong（服务端在 drain 里读到），
		// 但 Pong **不**续命。
		go func() {
			tk := time.NewTicker(5 * time.Millisecond)
			defer tk.Stop()
			for {
				select {
				case <-stop:
					return
				case <-tk.C:
					pctx, cancel := context.WithTimeout(context.Background(), time.Second)
					if c.Ping(pctx) == nil {
						u.mu.Lock()
						u.pongSeen++
						u.mu.Unlock()
					}
					cancel()
				}
			}
		}()
		u.drain(c, r)
	})
	hc := idleClient(t, up.srv.URL)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	ex := openIdleExchange(t, hc, ctx, up)

	start := time.Now()
	_, nerr := ex.Next(ctx)
	elapsed := time.Since(start)
	if !errors.Is(nerr, ErrWSIdleTimeout) {
		t.Fatalf("Ping 期间 Next = %v，期望 ErrWSIdleTimeout（pong 续命了？）", nerr)
	}
	assertIdleWindow(t, elapsed)
	if up.pongCount() == 0 {
		t.Fatal("服务端没观察到任何 pong（读路径卡住了，不是 Ping 不续命）")
	}
}

func TestWSIdlePingDoesNotExtendPull(t *testing.T) {
	stop := make(chan struct{})
	var stopOnce sync.Once
	defer stopOnce.Do(func() { close(stop) })

	up := newIdleUpstream(t, func(u *idleUpstream, c *coderws.Conn, r *http.Request) {
		if !u.readRequest(c, r) {
			return
		}
		go func() {
			tk := time.NewTicker(5 * time.Millisecond)
			defer tk.Stop()
			for {
				select {
				case <-stop:
					return
				case <-tk.C:
					pctx, cancel := context.WithTimeout(context.Background(), time.Second)
					if c.Ping(pctx) == nil {
						u.mu.Lock()
						u.pongSeen++
						u.mu.Unlock()
					}
					cancel()
				}
			}
		}()
		u.drain(c, r)
	})
	cl := dialIdlePull(t, up.srv.URL, WithWSIdleTimeout(idleBudget))
	if err := cl.Send(context.Background(), []byte(contractPayload)); err != nil {
		t.Fatalf("Send: %v", err)
	}
	up.waitRequests(t, 1)

	start := time.Now()
	_, err := cl.Recv(context.Background())
	elapsed := time.Since(start)
	if !errors.Is(err, ErrWSIdleTimeout) {
		t.Fatalf("Ping 期间 Recv = %v，期望 ErrWSIdleTimeout（pong 续命了？）", err)
	}
	assertIdleWindow(t, elapsed)
	if up.pongCount() == 0 {
		t.Fatal("服务端没观察到任何 pong（读路径卡住了，不是 Ping 不续命）")
	}
}

// TestWSIdleReceivedPongDoesNotExtend：等待窗口内**收到** pong 也不续命。
//
// coder/websocket 不暴露"发一个未请求的 Pong"的 API（控制帧写入是包内实现），
// 因此这里用"客户端发 Ping ⇒ 服务端回 Pong ⇒ 客户端在等待窗口里收到 Pong"构造
// 同一个读路径事件：控制帧被 Read 就地消化，不产生队列投递，也就不能续命。
func TestWSIdleReceivedPongDoesNotExtend(t *testing.T) {
	up := newIdleUpstream(t, func(u *idleUpstream, c *coderws.Conn, r *http.Request) {
		if !u.readRequest(c, r) {
			return
		}
		u.drain(c, r) // 读循环：服务客户端 Ping（自动回 Pong）
	})
	hc := idleClient(t, up.srv.URL)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	ex := openIdleExchange(t, hc, ctx, up)

	probes := make(chan error, 4)
	go func() {
		defer close(probes)
		for i := 0; i < 3; i++ {
			probes <- ex.cn.probe(ctx)
		}
	}()
	start := time.Now()
	_, nerr := ex.Next(ctx)
	elapsed := time.Since(start)

	okProbes := 0
	for perr := range probes {
		if perr == nil {
			okProbes++
		}
	}
	if okProbes == 0 {
		t.Fatal("等待期间的 Ping 全部失败——窗口里根本没有收到 pong")
	}
	if !errors.Is(nerr, ErrWSIdleTimeout) {
		t.Fatalf("窗口内收到 pong 后 Next = %v，期望 ErrWSIdleTimeout", nerr)
	}
	assertIdleWindow(t, elapsed)
}

// ---- P6：缓存里的空闲连接没有计时器 ----

func TestWSIdleCachedConnHasNoTimer(t *testing.T) {
	up := newIdleUpstream(t, func(u *idleUpstream, c *coderws.Conn, r *http.Request) {
		for round := 0; round < 2; round++ {
			if !u.readRequest(c, r) {
				return
			}
			if round == 0 {
				if !writeFrames(c, r, wsCreatedEv, wsCompletedEv) {
					u.markGone()
					return
				}
				continue
			}
			if !writeFrames(c, r, wsDeltaEv) {
				u.markGone()
				return
			}
		}
		u.drain(c, r)
	})
	hc := idleClient(t, up.srv.URL, WithWSConnCache(1))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	ex1 := openIdleExchange(t, hc, ctx, up)
	for i := 0; i < 2; i++ {
		if _, err := ex1.Next(ctx); err != nil {
			t.Fatalf("第 %d 帧: %v", i, err)
		}
	}
	if !ex1.Release() {
		t.Fatal("干净结束的连接 Release() 报失败")
	}

	// 归还后静默 3×budget：连接躺在缓存里，不该有任何计时器把它退役。
	pauseFor(3 * idleBudget)
	if !ex1.cn.releasable() {
		t.Fatalf("缓存里的空闲连接被退役了: %v", ex1.cn.err())
	}

	// 同 key 取用：必须命中复用（不重拨），帧正常交付、无 idle 错误。
	ex2 := openIdleExchange(t, hc, ctx, up)
	got, err := ex2.Next(ctx)
	if err != nil {
		t.Fatalf("复用连接取帧: %v", err)
	}
	if string(got) != wsDeltaEv {
		t.Fatalf("复用连接交付 = %s", got)
	}
	if hits := hc.wsCache.hits.Load(); hits != 1 {
		t.Fatalf("缓存命中 %d 次，期望 1", hits)
	}
	if got := up.dialCount(); got != 1 {
		t.Fatalf("拨号 %d 次，期望 1（应当命中缓存）", got)
	}
}

// TestWSIdleReleaseAdmission：归还准入（`releasable` + `fence`）不得缺。
//
// 上游发一帧就强断 ⇒ 读到该帧后连接已死（任何后续取帧都是错误）。此时 Release
// 必须拒绝归还：死连接进缓存会被下一个请求当活连接用（`create` 写不出去或读不到
// 帧）。这条是 M8 的判别点——注意单删 `releasable()` 时 `fence()` 仍会拦住
// （`finish` 已经 close 了 inbox），所以真正有判别力的是"归还准入整段缺失"。
func TestWSIdleReleaseAdmission(t *testing.T) {
	up := newIdleUpstream(t, func(u *idleUpstream, c *coderws.Conn, r *http.Request) {
		if !u.readRequest(c, r) {
			return
		}
		if !writeFrames(c, r, wsCreatedEv) {
			u.markGone()
			return
		}
		u.markGone()
		_ = c.CloseNow() // 强断：不发 close frame
	})
	hc := idleClient(t, up.srv.URL, WithWSConnCache(1), WithWSIdleTimeout(time.Hour))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	ex := openIdleExchange(t, hc, ctx, up)

	if _, err := ex.Next(ctx); err != nil {
		t.Fatalf("Next(created): %v", err)
	}
	// 连接已死：再取一帧拿到的是终止原因（不是帧）。
	if _, err := ex.Next(ctx); err == nil {
		t.Fatal("连接已死，Next 却交付了帧")
	}
	if ex.Release() {
		t.Fatal("死连接 Release() 报成功（归还准入缺失）")
	}
	hc.wsCache.mu.Lock()
	entries := len(hc.wsCache.idles)
	hc.wsCache.mu.Unlock()
	if entries != 0 {
		t.Fatalf("死连接进了缓存（%d 条）", entries)
	}

	// 同 key 再请求：必须重拨。
	ex2, err := hc.openWSExchange(ctx, []byte(contractPayload), idleThread, wsStreamReadLimit)
	if err != nil {
		t.Fatalf("第二个请求: %v", err)
	}
	defer ex2.cn.terminate(io.EOF)
	if got := up.dialCount(); got != 2 {
		t.Fatalf("拨号 %d 次，期望 2（死连接不得被复用）", got)
	}
}

// ---- P7：超时连接退役后同 key 重拨 ----
func TestWSIdleRetiresConn(t *testing.T) {
	up := newIdleUpstream(t, func(u *idleUpstream, c *coderws.Conn, r *http.Request) {
		if !u.readRequest(c, r) {
			return
		}
		u.drain(c, r) // 两轮都静默
	})
	hc := idleClient(t, up.srv.URL, WithWSConnCache(1))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	ex1 := openIdleExchange(t, hc, ctx, up)
	if _, err := ex1.Next(ctx); !errors.Is(err, ErrWSIdleTimeout) {
		t.Fatalf("首轮 Next = %v，期望 ErrWSIdleTimeout", err)
	}
	if got := ex1.cn.err(); !errors.Is(got, ErrWSIdleTimeout) {
		t.Fatalf("首轮终止原因 = %v，期望 ErrWSIdleTimeout", got)
	}
	if ex1.Release() {
		t.Fatal("超时连接 Release() 报成功")
	}
	hc.wsCache.mu.Lock()
	entries := len(hc.wsCache.idles)
	hc.wsCache.mu.Unlock()
	if entries != 0 {
		t.Fatalf("缓存里有 %d 条条目，期望 0（超时连接不得入表）", entries)
	}

	// 同 key 再请求：必须重拨（旧连接已退役）。
	ex2, err := hc.openWSExchange(ctx, []byte(contractPayload), idleThread, wsStreamReadLimit)
	if err != nil {
		t.Fatalf("第二个请求: %v", err)
	}
	defer ex2.cn.terminate(io.EOF)
	if got := up.dialCount(); got != 2 {
		t.Fatalf("拨号 %d 次，期望 2（退役后必须换新连接）", got)
	}
}

// ---- P8a：WithTimeout 与 idle 是两条独立预算（调用方优先）----

func TestWSIdleVsTotalTimeout(t *testing.T) {
	up := newIdleUpstream(t, func(u *idleUpstream, c *coderws.Conn, r *http.Request) {
		if !u.readRequest(c, r) {
			return
		}
		u.drain(c, r)
	})
	hc := contractClient(t, up.srv.URL,
		WithTimeout(80*time.Millisecond),
		WithWSIdleTimeout(5*time.Minute))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	ex, err := hc.openWSExchange(ctx, []byte(contractPayload), Identity{}, wsStreamReadLimit)
	if err != nil {
		t.Fatalf("openWSExchange: %v", err)
	}
	up.waitRequests(t, 1)

	if _, nerr := ex.Next(ctx); !errors.Is(nerr, context.DeadlineExceeded) {
		t.Fatalf("Next = %v，期望 context.DeadlineExceeded（总超时先到）", nerr)
	} else if errors.Is(nerr, ErrWSIdleTimeout) {
		t.Fatal("总超时被报成了 idle 哨兵（两条预算混了）")
	}
}

// ---- P8b：pull 的父 ctx 期限/取消优先 ----

func TestWSIdlePullParentContextWins(t *testing.T) {
	t.Run("父期限", func(t *testing.T) {
		up := newIdleUpstream(t, func(u *idleUpstream, c *coderws.Conn, r *http.Request) {
			if !u.readRequest(c, r) {
				return
			}
			u.drain(c, r)
		})
		cl := dialIdlePull(t, up.srv.URL, WithWSIdleTimeout(5*time.Minute))
		if err := cl.Send(context.Background(), []byte(contractPayload)); err != nil {
			t.Fatalf("Send: %v", err)
		}
		up.waitRequests(t, 1)

		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Millisecond)
		defer cancel()
		_, err := cl.Recv(ctx)
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("Recv = %v，期望 context.DeadlineExceeded", err)
		}
		if errors.Is(err, ErrWSIdleTimeout) {
			t.Fatal("父 deadline 被报成了 idle 哨兵（M3：提交前不裁决 ctx）")
		}
	})
	t.Run("父取消", func(t *testing.T) {
		up := newIdleUpstream(t, func(u *idleUpstream, c *coderws.Conn, r *http.Request) {
			if !u.readRequest(c, r) {
				return
			}
			u.drain(c, r)
		})
		cl := dialIdlePull(t, up.srv.URL, WithWSIdleTimeout(5*time.Minute))
		if err := cl.Send(context.Background(), []byte(contractPayload)); err != nil {
			t.Fatalf("Send: %v", err)
		}
		up.waitRequests(t, 1)

		ctx, cancel := context.WithCancel(context.Background())
		time.AfterFunc(30*time.Millisecond, cancel)
		defer cancel()
		_, err := cl.Recv(ctx)
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Recv = %v，期望 context.Canceled", err)
		}
		if errors.Is(err, ErrWSIdleTimeout) {
			t.Fatal("父取消被报成了 idle 哨兵")
		}
	})
}

// ---- P9：默认值与两条构造路径的透传 ----

func TestWSIdleDefaultsAndPlumbing(t *testing.T) {
	if got := defaultOptions().wsIdleTimeout; got != 300*time.Second {
		t.Fatalf("默认 wsIdleTimeout = %s，期望 300s（对齐 DEFAULT_STREAM_IDLE_TIMEOUT_MS）", got)
	}

	up := newIdleUpstream(t, func(u *idleUpstream, c *coderws.Conn, r *http.Request) {
		if !u.readRequest(c, r) {
			return
		}
		u.drain(c, r)
	})

	// NewHTTPClient 路径。
	hc := contractClient(t, up.srv.URL, WithWSIdleTimeout(7*time.Second))
	if got := hc.opts.wsIdleTimeout; got != 7*time.Second {
		t.Fatalf("HTTPClient 的 wsIdleTimeout = %s，期望 7s", got)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	ex, err := hc.openWSExchange(ctx, []byte(contractPayload), Identity{}, wsStreamReadLimit)
	if err != nil {
		t.Fatalf("openWSExchange: %v", err)
	}
	defer ex.cn.terminate(io.EOF)
	if got := ex.idle; got != 7*time.Second {
		t.Fatalf("exchange 的 idle = %s，期望 7s（透传断了）", got)
	}
	up.waitRequests(t, 1)

	// Dial 路径（含负值 = 关闭）。
	up2 := newIdleUpstream(t, func(u *idleUpstream, c *coderws.Conn, r *http.Request) {
		u.readRequest(c, r)
		u.drain(c, r)
	})
	clNeg := dialIdlePull(t, up2.srv.URL, WithWSIdleTimeout(-time.Second))
	if got := clNeg.cn.cfg.idleTimeout; got != -time.Second {
		t.Fatalf("Dial 路径的 idleTimeout = %s，期望 -1s", got)
	}

	// 不传 Option ⇒ Dial 也拿默认值。
	up3 := newIdleUpstream(t, func(u *idleUpstream, c *coderws.Conn, r *http.Request) {
		u.readRequest(c, r)
		u.drain(c, r)
	})
	clDefault := dialIdlePull(t, up3.srv.URL)
	if got := clDefault.cn.cfg.idleTimeout; got != 300*time.Second {
		t.Fatalf("Dial 默认 idleTimeout = %s，期望 300s", got)
	}
}

// ---- P10：关闭开关（0 / 负值），同夹具的正预算必红 ----

func TestWSIdleDisabled(t *testing.T) {
	cases := []struct {
		name     string
		budget   time.Duration
		wantIdle bool
	}{
		{"关闭-0", 0, false},
		{"关闭-负值", -time.Second, false},
		{"正预算必红", idleBudget, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			nextFrame := make(chan struct{})
			up := newIdleUpstream(t, func(u *idleUpstream, c *coderws.Conn, r *http.Request) {
				if !u.readRequest(c, r) {
					return
				}
				<-nextFrame // 等测试放行：期间完全静默
				if !writeFrames(c, r, wsCreatedEv) {
					u.markGone()
					return
				}
				u.drain(c, r)
			})
			hc := idleClient(t, up.srv.URL, WithWSIdleTimeout(tc.budget))
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			ex := openIdleExchange(t, hc, ctx, up)

			type result struct {
				frame []byte
				err   error
			}
			res := make(chan result, 1)
			go func() {
				d, err := ex.Next(ctx)
				res <- result{frame: d, err: err}
			}()
			// barrier：等到"读已挂起"——队列空且 Next 在飞，只能靠时间窗 + 之后的数据证明。
			pauseFor(5 * idleBudget)
			close(nextFrame)

			select {
			case got := <-res:
				if tc.wantIdle {
					if !errors.Is(got.err, ErrWSIdleTimeout) {
						t.Fatalf("Next = %v，期望 ErrWSIdleTimeout（关闭开关失效？）", got.err)
					}
					return
				}
				if got.err != nil {
					t.Fatalf("idle 已关闭，Next 仍失败: %v", got.err)
				}
				if string(got.frame) != wsCreatedEv {
					t.Fatalf("帧 = %s", got.frame)
				}
			case <-time.After(3 * time.Second):
				t.Fatal("Next 挂住了")
			}
			if tc.wantIdle {
				return
			}
			if cerr := ex.cn.err(); cerr != nil {
				t.Fatalf("idle 关闭时连接被退役了: %v", cerr)
			}
		})
	}
}

// ---- P11：分配斜率与常量化 ----

// TestWSIdleNoPerFrameAlloc：已在队帧的交付随帧数**不增长**分配（斜率 ≈ 0）。
func TestWSIdleNoPerFrameAlloc(t *testing.T) {
	cycle := func(n int) float64 {
		frames := make([][]byte, n)
		for i := range frames {
			frames[i] = []byte(deltaFrame(i))
		}
		q := newFrameInbox(n, 1<<20)
		ctx := context.Background()
		return testing.AllocsPerRun(10, func() {
			for _, f := range frames {
				if err := q.push(f); err != nil {
					panic(err)
				}
			}
			for range frames {
				if _, err := q.pop(ctx, 0, nil); err != nil {
					panic(err)
				}
			}
		})
	}
	a50, a100 := cycle(50), cycle(100)
	if a100 > a50+1 {
		t.Fatalf("分配随帧数增长：N=50 → %.2f，2N=100 → %.2f（每帧分配？）", a50, a100)
	}
	if a50 > 1 {
		t.Fatalf("已在队帧交付有分配：N=50 → %.2f allocs/cycle", a50)
	}
}

// TestWSIdleWaitReusesOneTimer：阻塞等待 0 新增分配（复用同一 *time.Timer）。
func TestWSIdleWaitReusesOneTimer(t *testing.T) {
	q := newFrameInbox(4, 1<<20)
	ctx := context.Background()
	timer := time.NewTimer(time.Hour)
	defer timer.Stop()

	allocs := testing.AllocsPerRun(10, func() {
		// 一次完整等待：队列空，预算到点 ⇒ errIdleWait。计时器由调用方复用。
		_, err := q.pop(ctx, 5*time.Millisecond, timer)
		if !errors.Is(err, errIdleWait) {
			panic("pop 未按时返回 errIdleWait: " + err.Error())
		}
	})
	if allocs > 0.5 {
		t.Fatalf("每次阻塞等待分配 %.2f 次，期望 0（每等待 time.After？）", allocs)
	}

	// 同一 exchange 的多轮等待复用同一个计时器（指针不变）。
	cn := &conn{cfg: connConfig{mode: modePumped}, inbox: q}
	e := &wsExchange{cn: cn, idle: 5 * time.Millisecond, ctx: ctx, cancel: func() {}}
	first := e.ensureIdleTimer()
	if first == nil {
		t.Fatal("idle>0 时应当创建计时器")
	}
	if second := e.ensureIdleTimer(); second != first {
		t.Fatal("第二次等待新建了计时器（应当复用）")
	}
	// idle<=0 时不建计时器。
	off := &wsExchange{cn: cn, ctx: ctx, cancel: func() {}}
	if off.ensureIdleTimer() != nil {
		t.Fatal("idle<=0 不应创建计时器")
	}
}

// TestWSIdleNoWatcherGoroutines：打开/关闭各 10 条连接后 goroutine 回落基线
// （idle 只消费等待，不新增"每连接看门狗"）。
func TestWSIdleNoWatcherGoroutines(t *testing.T) {
	up := newIdleUpstream(t, func(u *idleUpstream, c *coderws.Conn, r *http.Request) {
		if !u.readRequest(c, r) {
			return
		}
		u.drain(c, r)
	})
	hc := idleClient(t, up.srv.URL)

	base := runtime.NumGoroutine()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	for i := 0; i < 10; i++ {
		ex, err := hc.openWSExchange(ctx, []byte(contractPayload), Identity{}, wsStreamReadLimit)
		if err != nil {
			t.Fatalf("第 %d 条连接: %v", i, err)
		}
		if _, nerr := ex.Next(ctx); !errors.Is(nerr, ErrWSIdleTimeout) {
			t.Fatalf("第 %d 条连接 Next = %v，期望 ErrWSIdleTimeout", i, nerr)
		}
	}
	waitFor(t, func() bool { return runtime.NumGoroutine() <= base+2 })
}

// ---- T1：在飞 Next 遇 Release（既有语义锁死）----

func TestWSIdleInFlightNextVsRelease(t *testing.T) {
	up := newIdleUpstream(t, func(u *idleUpstream, c *coderws.Conn, r *http.Request) {
		if !u.readRequest(c, r) {
			return
		}
		u.drain(c, r)
	})
	// 预算故意拉长：本用例只钉"Release 与在飞 Next"的既有语义。
	hc := contractClient(t, up.srv.URL, WithWSIdleTimeout(time.Hour), WithWSConnCache(1))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	ex, err := hc.openWSExchange(ctx, []byte(contractPayload), idleThread, wsStreamReadLimit)
	if err != nil {
		t.Fatalf("openWSExchange: %v", err)
	}
	up.waitRequests(t, 1)

	nextErr := make(chan error, 1)
	go func() {
		_, nerr := ex.Next(ex.ctx) // 与既有内核用例一致：用 exchange 自己的 ctx 取帧
		nextErr <- nerr
	}()

	// barrier：确认 Next 已阻塞在 pop 的 select 上。notify 是 cap=1 的通道且只有 pop
	// 消费它——投递后若被立刻取走（len==0），就证明有等待者在 select 里。
	waitFor(t, func() bool {
		ex.cn.inbox.signal()
		return len(ex.cn.inbox.notify) == 0
	})

	released := make(chan bool, 1)
	go func() { released <- ex.Release() }()
	nerr := <-nextErr

	if !errors.Is(nerr, context.Canceled) {
		t.Fatalf("Next = %v，期望 context.Canceled（既有 309-315 分支）", nerr)
	}
	if errors.Is(nerr, ErrWSIdleTimeout) {
		t.Fatal("取消被报成了 idle 哨兵（超时路径多了副作用）")
	}
	if <-released {
		t.Fatal("在飞 Next 被取消后 Release() 报成功（连接已被 terminateOwned 退役）")
	}
	if got := ex.cn.err(); !errors.Is(got, context.Canceled) {
		t.Fatalf("终止原因 = %v，期望 context.Canceled", got)
	}
	hc.wsCache.mu.Lock()
	entries := len(hc.wsCache.idles)
	hc.wsCache.mu.Unlock()
	if entries != 0 {
		t.Fatalf("缓存收下了被取消的连接（%d 条）", entries)
	}
}

// ---- T2：超时方已抢到 beginFinish、尚未裁决租约 ----

func TestWSIdleCommitBeforeLeaseCAS(t *testing.T) {
	entered := make(chan struct{})
	releaseIdle := make(chan struct{})
	idleCommitHook = func(e *wsExchange) {
		close(entered)
		<-releaseIdle
	}
	defer func() { idleCommitHook = nil }()

	up := newIdleUpstream(t, func(u *idleUpstream, c *coderws.Conn, r *http.Request) {
		if !u.readRequest(c, r) {
			return
		}
		u.drain(c, r)
	})
	hc := idleClient(t, up.srv.URL, WithWSConnCache(1))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	ex := openIdleExchange(t, hc, ctx, up)

	nextErr := make(chan error, 1)
	go func() {
		_, nerr := ex.Next(ctx)
		nextErr <- nerr
	}()

	select {
	case <-entered: // 超时方已抢到收尾权，停在 lease CAS 之前
	case <-time.After(3 * time.Second):
		t.Fatal("没有走到超时提交（hook 未触发）")
	}
	// Release 的 beginFinish 会失败 ⇒ 它**立即**返回 false，走不到 nextMu。
	released := ex.Release()
	if released {
		t.Fatal("超时方已抢到收尾权，Release() 却报成功")
	}

	close(releaseIdle)
	nerr := <-nextErr
	if !errors.Is(nerr, ErrWSIdleTimeout) {
		t.Fatalf("Next = %v，期望 ErrWSIdleTimeout", nerr)
	}
	if got := ex.cn.err(); !errors.Is(got, ErrWSIdleTimeout) {
		t.Fatalf("终止原因 = %v，期望 ErrWSIdleTimeout", got)
	}
	hc.wsCache.mu.Lock()
	entries := len(hc.wsCache.idles)
	hc.wsCache.mu.Unlock()
	if entries != 0 {
		t.Fatalf("超时连接进了缓存（%d 条）", entries)
	}
}

// ---- T2b：Release 先赢 beginFinish（相反的次序）----

func TestWSIdleStaleCommitAfterRelease(t *testing.T) {
	up := newIdleUpstream(t, func(u *idleUpstream, c *coderws.Conn, r *http.Request) {
		if !u.readRequest(c, r) {
			return
		}
		u.drain(c, r)
	})
	hc := idleClient(t, up.srv.URL, WithWSConnCache(1), WithWSIdleTimeout(time.Hour))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	ex := openIdleExchange(t, hc, ctx, up)

	if !ex.Release() {
		t.Fatal("干净连接 Release() 应成功（进入缓存）")
	}
	hc.wsCache.mu.Lock()
	entries := len(hc.wsCache.idles)
	hc.wsCache.mu.Unlock()
	if entries != 1 {
		t.Fatalf("缓存条目 = %d，期望 1", entries)
	}

	// 陈旧提交：等待早已结束（连接已归还），但提交方还以为自己在飞。
	ex.closed.Store(false)
	if err := ex.idleExpire(); !errors.Is(err, io.EOF) {
		t.Fatalf("陈旧 idleExpire = %v，期望 io.EOF（租约已归 nil）", err)
	}
	if cerr := ex.cn.err(); cerr != nil {
		t.Fatalf("陈旧提交 terminate 了已归还的连接: %v", cerr)
	}
	if !ex.cn.releasable() {
		t.Fatal("陈旧提交之后连接不再 releasable")
	}
	hc.wsCache.mu.Lock()
	entries = len(hc.wsCache.idles)
	hc.wsCache.mu.Unlock()
	if entries != 1 {
		t.Fatalf("陈旧提交把缓存条目弄丢了（%d 条）", entries)
	}

	// 已 closed 的 exchange 再 Next：走 e.closed 入口分支。
	if _, err := ex.Next(ctx); !errors.Is(err, io.EOF) {
		t.Fatalf("已归还 exchange 的 Next = %v，期望 io.EOF", err)
	}
}

// ---- T3a：CAS 失败（租约已被新 exchange 持有）----

func TestWSIdleLeaseTakenByNewExchange(t *testing.T) {
	up := newIdleUpstream(t, func(u *idleUpstream, c *coderws.Conn, r *http.Request) {
		if !u.readRequest(c, r) {
			return
		}
		u.drain(c, r)
	})
	hc := idleClient(t, up.srv.URL, WithWSIdleTimeout(idleBudget))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	ex := openIdleExchange(t, hc, ctx, up)

	// 同 conn 的第二条租约（下一次取用的持有者）。
	e2 := &wsExchange{cn: ex.cn, ctx: context.Background(), cancel: func() {}, idle: time.Hour}
	idleCommitHook = func(e *wsExchange) {
		ex.cn.lease.Store(e2) // 租约已被新持有者取用
	}
	defer func() { idleCommitHook = nil }()

	_, nerr := ex.Next(ctx)
	if !errors.Is(nerr, io.EOF) {
		t.Fatalf("Next = %v，期望 io.EOF（CAS 失败）", nerr)
	}
	if errors.Is(nerr, ErrWSIdleTimeout) {
		t.Fatal("CAS 失败仍报 idle 哨兵")
	}
	if cerr := ex.cn.err(); cerr != nil {
		t.Fatalf("新租约的连接被误杀: %v", cerr)
	}
	if !ex.cn.releasable() {
		t.Fatal("CAS 失败后连接不再 releasable")
	}
	if got := ex.cn.lease.Load(); got != e2 {
		t.Fatalf("lease = %v，期望新 exchange", got)
	}
	// e2 能正常读帧（假上游刚发来一帧，等价于 readLoop 入队）。
	if perr := ex.cn.inbox.push([]byte(wsDeltaEv)); perr != nil {
		t.Fatalf("push: %v", perr)
	}
	got, rerr := e2.Next(context.Background())
	if rerr != nil {
		t.Fatalf("新租约读帧: %v", rerr)
	}
	if string(got) != wsDeltaEv {
		t.Fatalf("新租约读到 %s", got)
	}
}

// ---- T3b：CAS 失败（租约已归 nil）----

func TestWSIdleLeaseAlreadyReleased(t *testing.T) {
	up := newIdleUpstream(t, func(u *idleUpstream, c *coderws.Conn, r *http.Request) {
		if !u.readRequest(c, r) {
			return
		}
		u.drain(c, r)
	})
	hc := idleClient(t, up.srv.URL, WithWSConnCache(1), WithWSIdleTimeout(time.Hour))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	ex := openIdleExchange(t, hc, ctx, up)

	if !ex.Release() {
		t.Fatal("Release() 应成功（连接入缓存、lease=nil）")
	}
	// 伪造"陈旧提交方仍以为自己在飞"。
	ex.closed.Store(false)
	if err := ex.idleExpire(); !errors.Is(err, io.EOF) {
		t.Fatalf("idleExpire = %v，期望 io.EOF", err)
	}
	if cerr := ex.cn.err(); cerr != nil {
		t.Fatalf("已归还的连接被 terminate: %v", cerr)
	}
	if !ex.cn.releasable() {
		t.Fatal("连接不再 releasable")
	}
	hc.wsCache.mu.Lock()
	entries := len(hc.wsCache.idles)
	hc.wsCache.mu.Unlock()
	if entries != 1 {
		t.Fatalf("缓存条目 = %d，期望 1", entries)
	}
	if cc, ok := hc.wsCache.get(ex.key, ex.token); !ok || cc.cn != ex.cn {
		t.Fatal("cache.get 没有命中原来那条连接")
	}
}

// ---- T4：旧 exchange 重复 Release ----

func TestWSIdleDoubleRelease(t *testing.T) {
	up := newIdleUpstream(t, func(u *idleUpstream, c *coderws.Conn, r *http.Request) {
		if !u.readRequest(c, r) {
			return
		}
		u.drain(c, r)
	})
	hc := idleClient(t, up.srv.URL, WithWSConnCache(1), WithWSIdleTimeout(time.Hour))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	ex := openIdleExchange(t, hc, ctx, up)

	if !ex.Release() {
		t.Fatal("首次 Release() 应成功")
	}
	if ex.Release() {
		t.Fatal("第二次 Release() 报成功（重复归还）")
	}
	hc.wsCache.mu.Lock()
	entries := len(hc.wsCache.idles)
	hc.wsCache.mu.Unlock()
	if entries != 1 {
		t.Fatalf("缓存条目 = %d，期望 1（二次 Release 不影响表）", entries)
	}
	if cerr := ex.cn.err(); cerr != nil {
		t.Fatalf("二次 Release 终止了缓存里的连接: %v", cerr)
	}
}

// ---- T5：Close 与超时"并发"（一次退役、无 panic、幂等）----

func TestWSIdleCloseRacesTimeout(t *testing.T) {
	entered := make(chan struct{})
	releaseIdle := make(chan struct{})
	idleCommitHook = func(e *wsExchange) {
		close(entered)
		<-releaseIdle
	}
	defer func() { idleCommitHook = nil }()

	up := newIdleUpstream(t, func(u *idleUpstream, c *coderws.Conn, r *http.Request) {
		if !u.readRequest(c, r) {
			return
		}
		u.drain(c, r)
	})
	hc := idleClient(t, up.srv.URL)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	ex := openIdleExchange(t, hc, ctx, up)

	nextErr := make(chan error, 1)
	go func() {
		_, nerr := ex.Next(ctx)
		nextErr <- nerr
	}()
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("没有走到超时提交")
	}
	// Close 与超时提交并发：Close 的 beginFinish 失败 ⇒ 空操作（不二次退役）。
	if err := ex.Close(); err != nil {
		t.Fatalf("并发的 Close 返回错误: %v", err)
	}
	close(releaseIdle)
	if nerr := <-nextErr; !errors.Is(nerr, ErrWSIdleTimeout) {
		t.Fatalf("Next = %v，期望 ErrWSIdleTimeout", nerr)
	}
	// 幂等：再来一次不 panic、不改结果。
	if err := ex.Close(); err != nil {
		t.Fatalf("第二次 Close 返回错误: %v", err)
	}
	if got := ex.cn.err(); !errors.Is(got, ErrWSIdleTimeout) {
		t.Fatalf("终止原因 = %v，期望 ErrWSIdleTimeout（一次退役）", got)
	}
}

// ---- T6：pull 的 graceful Close 与 idle 到期（B12 首因胜）----

func TestWSIdleDuringGracefulClose(t *testing.T) {
	closeFrameSeen := make(chan struct{}, 1)
	up := newIdleUpstream(t, func(u *idleUpstream, c *coderws.Conn, r *http.Request) {
		if !u.readRequest(c, r) {
			return
		}
		if !writeFrames(c, r, wsCreatedEv) {
			u.markGone()
			return
		}
		// 关闭握手期间先不读：让 idle 先到期（state=closing），再将关闭帧读走。
		select {
		case <-time.After(200 * time.Millisecond):
		case <-r.Context().Done():
			return
		}
		for {
			if _, _, err := c.Read(context.Background()); err != nil {
				u.markGone()
				if coderws.CloseStatus(err) >= 0 {
					select {
					case closeFrameSeen <- struct{}{}:
					default:
					}
				}
				return
			}
		}
	})
	cl := dialIdlePull(t, up.srv.URL, WithWSIdleTimeout(120*time.Millisecond))
	if err := cl.Send(context.Background(), []byte(contractPayload)); err != nil {
		t.Fatalf("Send: %v", err)
	}
	up.waitRequests(t, 1)
	// 先把上游写的这一帧读掉：下面要测的是"空队时的一次等待"。
	if _, err := cl.Recv(context.Background()); err != nil {
		t.Fatalf("首帧 Recv: %v", err)
	}

	recvErr := make(chan error, 1)
	go func() {
		_, err := cl.Recv(context.Background())
		recvErr <- err
	}()
	closeDone := make(chan error, 1)
	go func() { closeDone <- cl.Close(StatusNormalClosure, "") }()

	// barrier：关闭握手已发起（state=closing，关闭帧已写出）之后才允许 idle 到期。
	waitFor(t, func() bool { return cl.cn.isClosing() })

	select {
	case err := <-recvErr:
		if !errors.Is(err, ErrWSIdleTimeout) {
			t.Fatalf("Recv = %v，期望 ErrWSIdleTimeout", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("idle 到期后 Recv 仍未返回")
	}
	select {
	case <-closeDone:
	case <-time.After(6 * time.Second):
		t.Fatal("Close 超出了既有上界（~10s）")
	}
	if got := cl.cn.err(); !errors.Is(got, ErrWSIdleTimeout) {
		t.Fatalf("终止原因 = %v，期望 ErrWSIdleTimeout（首因胜）", got)
	}
	select {
	case <-closeFrameSeen:
	case <-time.After(time.Second):
		t.Fatal("服务端没有观察到关闭帧")
	}
}
