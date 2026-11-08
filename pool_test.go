package codexsdk

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"
)

// poolTestWSURL 是 coder/websocket 把 wss:// 归一成 https:// 之后的请求 URL
// （fixedTransport 比对的是归一后的串）。
const poolTestWSURL = "https://chatgpt.com/backend-api/codex/responses"

// ---------------------------------------------------------------------------
// 测试基建：假 Responses WS 服务（必须带读循环，否则 pong 不回、ping 计数不触发）
// ---------------------------------------------------------------------------

type fakeOpts struct {
	// replyPong=false 时 OnPingReceived 返回 false → 服务端不回 pong
	// （模拟半开/静默上游）。
	replyPong bool
	// onFrame 在读到客户端数据帧时调用（帧内容丢弃）。nil = 只读不回。
	// 回调跑在服务端读循环 goroutine 里，只允许写帧（coder/websocket 单写者）。
	onFrame func(fc *fakeConn)
}

type fakeWS struct {
	t        *testing.T
	srv      *httptest.Server
	tr       *fixedTransport
	opts     fakeOpts
	accepted chan *fakeConn

	mu  sync.Mutex
	all []*fakeConn
}

type fakeConn struct {
	srv *fakeWS
	ws  *websocket.Conn

	mu     sync.Mutex
	pings  int
	wrote  int
	frames int // 本条连接上收到的客户端数据帧计数（1 起，供 onFrame 编排应答）
}

func newFakeWS(t *testing.T, opts fakeOpts) *fakeWS {
	t.Helper()
	f := &fakeWS{t: t, opts: opts, accepted: make(chan *fakeConn, 1024)}
	f.srv = httptest.NewServer(http.HandlerFunc(f.handle))
	f.tr = newFixedTransport(t, poolTestWSURL, f.srv.URL)
	t.Cleanup(f.srv.Close)
	return f
}

func (f *fakeWS) handle(w http.ResponseWriter, r *http.Request) {
	fc := &fakeConn{srv: f}
	c, err := websocket.Accept(w, r, &websocket.AcceptOptions{
		OnPingReceived: func(ctx context.Context, payload []byte) bool {
			fc.mu.Lock()
			fc.pings++
			fc.mu.Unlock()
			return f.opts.replyPong
		},
	})
	if err != nil {
		return
	}
	fc.ws = c
	f.mu.Lock()
	f.all = append(f.all, fc)
	f.mu.Unlock()
	f.accepted <- fc

	defer c.CloseNow()
	// 读循环：pong/ping/close 控制帧只有读才处理（F1）。数据帧交给 onFrame（nil 则丢弃）。
	for {
		_, _, err := c.Read(r.Context())
		if err != nil {
			return
		}
		fc.mu.Lock()
		fc.frames++
		fc.mu.Unlock()
		if f.opts.onFrame != nil {
			f.opts.onFrame(fc)
		}
	}
}

// dialFunc 返回池用的 DialFunc：本地 URL 重定向 + WithPingInterval(0)（C2）。
func (f *fakeWS) dialFunc(extra ...Option) DialFunc {
	return func(ctx context.Context, key PoolKey) (*Client, error) {
		opts := []Option{
			WithTransport(f.tr),
			WithPingInterval(0),
			WithTraceAuto(false),
			WithTurnAuto(false),
		}
		opts = append(opts, extra...)
		return Dial(ctx, PAT("pat-pool"), opts...)
	}
}

func (f *fakeWS) next(t *testing.T) *fakeConn {
	t.Helper()
	select {
	case fc := <-f.accepted:
		return fc
	case <-time.After(3 * time.Second):
		t.Fatal("等待服务端接受连接超时")
		return nil
	}
}

func (fc *fakeConn) write(data string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := fc.ws.Write(ctx, websocket.MessageText, []byte(data)); err != nil {
		return err
	}
	fc.mu.Lock()
	fc.wrote++
	fc.mu.Unlock()
	return nil
}

func (fc *fakeConn) send(t *testing.T, data string) {
	t.Helper()
	if err := fc.write(data); err != nil {
		t.Fatalf("服务端写帧失败: %v", err)
	}
}

// recvFrames 返回本条连接上收到的客户端数据帧计数（1 起）。
func (fc *fakeConn) recvFrames() int {
	fc.mu.Lock()
	defer fc.mu.Unlock()
	return fc.frames
}

// replyRounds 是"一问一答"假上游：每收到一帧就回一帧
// {"type":"response.created","round":<n>}，n = 该连接上收到的第几帧（1 起）。
// 用它把"租约 Send → 服务端回帧 → 租约 Recv"闭环，从而能逐轮核对帧归属。
func replyRounds(fc *fakeConn) {
	n := fc.recvFrames()
	_ = fc.write(fmt.Sprintf(`{"type":"response.created","round":%d}`, n))
}

func (fc *fakeConn) pingCount() int {
	fc.mu.Lock()
	defer fc.mu.Unlock()
	return fc.pings
}

func (fc *fakeConn) wroteCount() int {
	fc.mu.Lock()
	defer fc.mu.Unlock()
	return fc.wrote
}

func (fc *fakeConn) close() {
	_ = fc.ws.Close(websocket.StatusGoingAway, "fake-close")
}

// ---------------------------------------------------------------------------
// 测试基建：时钟 / 屏障
// ---------------------------------------------------------------------------

type mockClock struct {
	mu sync.Mutex
	t  time.Time
}

func newMockClock() *mockClock {
	return &mockClock{t: time.Date(2026, 11, 8, 12, 0, 0, 0, time.UTC)}
}

func (c *mockClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *mockClock) Advance(d time.Duration) {
	c.mu.Lock()
	c.t = c.t.Add(d)
	c.mu.Unlock()
}

// within 是异步断言的唯一手段：channel 屏障 + time.After 上界（禁 time.Sleep）。
func within(t *testing.T, timeout time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		if cond() {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("等待超时（%v）：%s", timeout, what)
		}
		<-time.After(time.Millisecond)
	}
}

func waitStats(t *testing.T, p *Pool, what string, cond func(PoolStats) bool) {
	t.Helper()
	within(t, 2*time.Second, "Stats: "+what, func() bool { return cond(p.Stats()) })
}

func waitPingCount(t *testing.T, fc *fakeConn, n int) {
	t.Helper()
	within(t, 2*time.Second, fmt.Sprintf("服务端 ping 计数 >= %d", n), func() bool {
		return fc.pingCount() >= n
	})
}

func mustAcquire(t *testing.T, p *Pool, key PoolKey) Lease {
	t.Helper()
	l, err := p.Acquire(context.Background(), key)
	if err != nil {
		t.Fatalf("Acquire(%q): %v", key, err)
	}
	return l
}

// poolConnsOf 取出某键下的连接（调用方自行保证生命周期内有效）。
func poolConnsOf(t *testing.T, p *Pool, key PoolKey) []*poolConn {
	t.Helper()
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]*poolConn(nil), p.keys[key]...)
}

// ---------------------------------------------------------------------------
// §4.2 测试矩阵
// ---------------------------------------------------------------------------

// TestPoolReusesConnection：同键两次 Acquire/Release → 升级只发生 1 次；
// 第二次 Reused()，且第二次必须真的 Send + Recv 一帧（只断言计数的话空实现也能过）。
func TestPoolReusesConnection(t *testing.T) {
	f := newFakeWS(t, fakeOpts{replyPong: true, onFrame: replyRounds})
	p := NewPool(PoolConfig{Dial: f.dialFunc()})
	defer p.CloseAll()

	var reused [2]bool
	var gens [2]uint64
	for i := 0; i < 2; i++ {
		idx := i
		l, err := p.Acquire(context.Background(), "k1")
		if err != nil {
			t.Fatalf("第 %d 次 Acquire: %v", idx+1, err)
		}
		reused[idx] = l.Reused()
		gens[idx] = l.Gen()
		if idx == 1 {
			// 复用轮必须真跑通一次往返：否则"复用"只是计数器好看。
			if err := l.Send(context.Background(), []byte(`{"type":"response.create","round":1}`)); err != nil {
				t.Fatalf("复用轮 Send: %v", err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			data, err := l.Recv(ctx)
			cancel()
			if err != nil {
				t.Fatalf("复用轮 Recv: %v（新租约的第一帧被丢了？）", err)
			}
			if !strings.Contains(string(data), `"round":1`) {
				t.Fatalf("复用轮 Recv = %s，want round:1", data)
			}
		}
		l.Release()
	}
	if reused[0] {
		t.Fatal("第 1 次 Acquire 必须是新拨连接（Reused=false）")
	}
	if !reused[1] {
		t.Fatal("第 2 次 Acquire 必须复用同键空闲连接（Reused=true）")
	}
	if gens[1] != gens[0]+1 {
		t.Fatalf("换代应恰好 +1：got %d → %d", gens[0], gens[1])
	}
	s := p.Stats()
	if s.Dials != 1 {
		t.Fatalf("WebSocket 升级次数 = %d，want 1", s.Dials)
	}
	if s.Hits != 1 || s.Misses != 1 {
		t.Fatalf("Hits/Misses = %d/%d，want 1/1", s.Hits, s.Misses)
	}
	if s.Released != 2 || s.Discarded != 0 {
		t.Fatalf("Released/Discarded = %d/%d，want 2/0", s.Released, s.Discarded)
	}
}

// TestPoolReuseDeliversFirstFrameAfterHandover 是 A2(v10) 专项：帧归属必须以
// **读出时刻**的连接代为准（不是读起点）。
//
// 每轮的 pump 都阻塞在 Read 里跨越了上一轮的 Release（这是常态：连接空闲时读者
// 仍然在读）。若按读起点代号归属，第二轮起的第一帧带着上一代代号，会被判成
// 跨代帧丢掉 ⇒ 每轮复用都丢 response.created，复用事实上不可用。
func TestPoolReuseDeliversFirstFrameAfterHandover(t *testing.T) {
	f := newFakeWS(t, fakeOpts{replyPong: true, onFrame: replyRounds})
	p := NewPool(PoolConfig{Dial: f.dialFunc()})
	defer p.CloseAll()

	const rounds = 32
	key := PoolKey("reuse-handover")
	for n := 1; n <= rounds; n++ {
		l, err := p.Acquire(context.Background(), key)
		if err != nil {
			t.Fatalf("第 %d 轮 Acquire: %v", n, err)
		}
		if n > 1 && !l.Reused() {
			t.Fatalf("第 %d 轮未复用连接（Dials=%d）", n, p.Stats().Dials)
		}
		if err := l.Send(context.Background(), []byte(`{"type":"response.create","round":1}`)); err != nil {
			t.Fatalf("第 %d 轮 Send: %v", n, err)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		data, err := l.Recv(ctx)
		cancel()
		if err != nil {
			t.Fatalf("第 %d 轮 Recv: %v（本轮首帧被当成跨代帧丢了？）", n, err)
		}
		if want := fmt.Sprintf(`"round":%d`, n); !strings.Contains(string(data), want) {
			t.Fatalf("第 %d 轮 Recv = %s，want %s", n, data, want)
		}
		l.Release()
	}

	s := p.Stats()
	if s.Dials != 1 {
		t.Fatalf("Dials = %d，want 1（全程必须复用同一条连接）", s.Dials)
	}
	if s.Dropped != 0 {
		t.Fatalf("Dropped = %d，want 0（复用不得丢任何一帧）", s.Dropped)
	}
}

// TestPoolIdleKeepsAlive：空闲期池拥有心跳（服务端 ping 计数增长），随后仍可租用。
func TestPoolIdleKeepsAlive(t *testing.T) {
	f := newFakeWS(t, fakeOpts{replyPong: true})
	p := NewPool(PoolConfig{
		Dial:         f.dialFunc(),
		PingInterval: 50 * time.Millisecond,
	})
	defer p.CloseAll()

	l := mustAcquire(t, p, "k")
	l.Release()
	fc := f.next(t)

	waitPingCount(t, fc, 2) // 空闲期至少两轮心跳
	if s := p.Stats(); s.PingSuppressed != 0 {
		t.Fatalf("空闲态不该抑制 ping，PingSuppressed=%d", s.PingSuppressed)
	}
	if p.Stats().Dead != 0 {
		t.Fatal("回 pong 的空闲连接不得判死")
	}

	// A3：ping 在飞时这条连接**不可出租**（Acquire 跳过它，无其它空闲则拨号）。
	// 所以"紧接着的一次 Acquire 必须复用"是过强假设——高负载 `-race` 下确实撞到过。
	// 这里改为有界尝试：每轮先**只读**等到可出租窗口（state==idle && !pinging），
	// 再 Acquire，最多 10 轮，只要曾经复用成功即通过（A3 允许偶发跳过，不是产品缺陷）。
	//
	// 注意：`Dials==1` 仍保留为硬断言——真拨了新连接两条断言都会红；这里不放宽它，
	// 只是不在"ping 正在飞"的窗口里贸然 Acquire。
	st := poolConnsOf(t, p, "k")[0]
	rentable := func() bool {
		st.mu.Lock()
		defer st.mu.Unlock()
		return st.state == stateIdle && !st.pinging
	}
	const attempts = 10
	reused := false
	for i := 0; i < attempts && !reused; i++ {
		within(t, 3*time.Second, "空闲连接回到可出租窗口（无在飞 ping）", rentable)
		l2 := mustAcquire(t, p, "k")
		reused = l2.Reused()
		l2.Release()
	}
	if !reused {
		t.Fatalf("有界 %d 轮内未复用空闲连接（A3 的 ping 互斥不该持续这么久）", attempts)
	}
	if s := p.Stats(); s.Dials != 1 {
		t.Fatalf("升级次数 = %d，want 1（心跳不得导致重拨）", s.Dials)
	}
}

// TestPoolNoHeartbeatIsRejected：DialFunc 返回 PingInterval!=0 的连接 → 拒绝并关闭。
func TestPoolNoHeartbeatIsRejected(t *testing.T) {
	f := newFakeWS(t, fakeOpts{replyPong: true})
	var kept *Client
	p := NewPool(PoolConfig{
		Dial: func(ctx context.Context, key PoolKey) (*Client, error) {
			c, err := Dial(ctx, PAT("pat-pool"), WithTransport(f.tr), WithPingInterval(30*time.Second))
			kept = c
			return c, err
		},
	})
	defer p.CloseAll()

	_, err := p.Acquire(context.Background(), "k")
	if !errors.Is(err, ErrHeartbeatConflict) {
		t.Fatalf("Acquire 错误 = %v，want ErrHeartbeatConflict", err)
	}
	if kept == nil {
		t.Fatal("DialFunc 未被调用")
	}
	if !kept.Closed() {
		t.Fatal("心跳冲突的连接必须被关闭（池是唯一 ping 所有者）")
	}
	s := p.Stats()
	if s.Misses != 1 || s.Dials != 1 || s.Hits != 0 || s.Idle != 0 || s.Leased != 0 {
		t.Fatalf("计数异常（不得登记连接）：%+v", s)
	}
}

// TestPoolPingSkipsWhileFramesInFlight：有帧在飞（租用中且队列非空）→ 不发 ping；
// **同一租约内**队列排空（静默）→ ping 恢复（不得只在 Release→idle 后才恢复）。
func TestPoolPingSkipsWhileFramesInFlight(t *testing.T) {
	f := newFakeWS(t, fakeOpts{replyPong: true})
	p := NewPool(PoolConfig{
		Dial:         f.dialFunc(),
		PingInterval: 10 * time.Millisecond,
		PongTimeout:  2 * time.Second,
		FrameQueue:   4,
	})
	defer p.CloseAll()

	l := mustAcquire(t, p, "k")
	fc := f.next(t)
	fc.send(t, `{"type":"response.created","seq":1}`)
	fc.send(t, `{"type":"response.output_text.delta","seq":2}`)
	waitStats(t, p, "帧入队", func(s PoolStats) bool { return s.QueuedBytes > 0 })

	<-time.After(120 * time.Millisecond)
	if n := fc.pingCount(); n != 0 {
		t.Fatalf("租用中数据在飞时服务端收到 %d 次 ping，want 0", n)
	}
	if p.Stats().PingSuppressed != 0 {
		t.Fatal("未被停读抑制的 ping 不得计入 PingSuppressed（口径：停读期间）")
	}

	// 同一租约内把队列排空（上游静默）：活性不再由数据证明 ⇒ ping 必须恢复。
	// 这一步才真正区分"只在 idle 发"与"静默就发"——后者才能发现租用态半开。
	if _, err := l.Recv(context.Background()); err != nil {
		t.Fatalf("第 1 帧 Recv: %v", err)
	}
	if _, err := l.Recv(context.Background()); err != nil {
		t.Fatalf("第 2 帧 Recv: %v", err)
	}
	if s := p.Stats(); s.QueuedBytes != 0 {
		t.Fatalf("队列应已排空：QueuedBytes=%d", s.QueuedBytes)
	}
	waitPingCount(t, fc, 1) // 仍在租用中（未 Release）

	l.Release()
	waitPingCount(t, fc, 2) // 回到 idle：ping 依旧常态生效
}

// TestPoolAcquireSkipsPinging：pinging 的连接不得被出租（A3）。
func TestPoolAcquireSkipsPinging(t *testing.T) {
	f := newFakeWS(t, fakeOpts{replyPong: true})
	p := NewPool(PoolConfig{Dial: f.dialFunc()})
	defer p.CloseAll()

	l := mustAcquire(t, p, "k")
	st := poolConnsOf(t, p, "k")[0]
	l.Release()

	st.mu.Lock()
	st.pinging = true // 人为制造"ping 在飞"
	st.mu.Unlock()
	defer func() {
		st.mu.Lock()
		st.pinging = false
		st.mu.Unlock()
	}()

	l2 := mustAcquire(t, p, "k")
	defer l2.Release()
	if l2.Reused() {
		t.Fatal("pinging 的连接被出租了（A3 违约）")
	}
	if s := p.Stats(); s.Dials != 2 {
		t.Fatalf("升级次数 = %d，want 2（跳过后必须重拨）", s.Dials)
	}
}

// TestPoolLeasePingDetectsHalfOpen：租用态上游静默 → ping 在 PongTimeout 内判死，
// Recv 返回错误（半开判死 ≤ PingInterval+PongTimeout，与现役网关同量级）。
func TestPoolLeasePingDetectsHalfOpen(t *testing.T) {
	f := newFakeWS(t, fakeOpts{replyPong: false}) // 假服务不回 pong
	p := NewPool(PoolConfig{
		Dial:         f.dialFunc(),
		PingInterval: 20 * time.Millisecond,
		PongTimeout:  60 * time.Millisecond,
	})
	defer p.CloseAll()

	l := mustAcquire(t, p, "k")
	fc := f.next(t)

	var recvErr error
	within(t, 2*time.Second, "租用态半开判死（Recv 返回错误）", func() bool {
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
		defer cancel()
		_, recvErr = l.Recv(ctx)
		return recvErr != nil && !errors.Is(recvErr, context.DeadlineExceeded)
	})
	if !errors.Is(recvErr, ErrPingTimeout) {
		t.Fatalf("Recv 错误 = %v，want ErrPingTimeout", recvErr)
	}
	if fc.pingCount() < 1 {
		t.Fatal("租用态必须发过 ping（否则半开只会在 HardLimit 兜底时才被发现）")
	}
	if s := p.Stats(); s.ReapedInFlight != 0 || s.PingSuppressed != 0 {
		t.Fatalf("租约未超龄、未停读，计数不该增长：%+v", s)
	}
}

// TestPoolPingTimeoutInLeaseNotDead：在途 ping 期间发生过停读 ⇒ 超时不计死（A1-③）。
//
// 关键序列正好是 spec §3.6 点名的那条：
//
//	ping 发出 → 停读（队列满）→ 排空恢复（blocked=false）→ ping 超时
//
// 用 blocked 布尔快照会在超时那一刻读到 false，把健康连接误杀；
// 用 blockedSeq 单调计数能识别"本次飞行期间确实停读过"，从而不计死。
func TestPoolPingTimeoutInLeaseNotDead(t *testing.T) {
	f := newFakeWS(t, fakeOpts{replyPong: false})
	p := NewPool(PoolConfig{
		Dial:         f.dialFunc(),
		PingInterval: 10 * time.Millisecond,
		PongTimeout:  300 * time.Millisecond,
		FrameQueue:   2,
	})
	defer p.CloseAll()

	l := mustAcquire(t, p, "k")
	fc := f.next(t)
	waitPingCount(t, fc, 1) // ping 已在飞（本连接不回 pong）

	for i := 1; i <= 4; i++ {
		fc.send(t, fmt.Sprintf(`{"seq":%d}`, i))
	}
	waitStats(t, p, "pump 因队列满停读", func(s PoolStats) bool { return s.QueueBlocked > 0 })

	// 取走 2 帧：pump 排空恢复读（blocked 变 false），队列里仍有 2 帧在飞
	// ⇒ 新 ping 被"有帧在飞"抑制，超时的那次仍是停读期间发出的。
	for i := 1; i <= 2; i++ {
		data, err := l.Recv(context.Background())
		if err != nil {
			t.Fatalf("第 %d 帧 Recv: %v", i, err)
		}
		if want := fmt.Sprintf(`{"seq":%d}`, i); string(data) != want {
			t.Fatalf("第 %d 帧 = %s，want %s", i, data, want)
		}
	}
	waitStats(t, p, "pump 恢复读并把剩余帧投递进来", func(s PoolStats) bool { return s.Delivered == 4 })

	<-time.After(400 * time.Millisecond) // 覆盖在飞 ping 的超时点（blocked 已恢复 false）
	if s := p.Stats(); s.Dead != 0 {
		t.Fatalf("停读期间发出的 ping 超时被判死（A1-③ 违约，疑似用了 blocked 布尔快照）：%+v", s)
	}
	for i := 3; i <= 4; i++ {
		data, err := l.Recv(context.Background())
		if err != nil {
			t.Fatalf("第 %d 帧 Recv: %v（连接不得被杀）", i, err)
		}
		if want := fmt.Sprintf(`{"seq":%d}`, i); string(data) != want {
			t.Fatalf("第 %d 帧 = %s，want %s（不得丢帧）", i, data, want)
		}
	}
	l.Release()
}

// TestPoolPingSuppressedWhileBlocked：queue 满 → ping 抑制（服务端计数不增）；
// 排空后 ping 恢复。
func TestPoolPingSuppressedWhileBlocked(t *testing.T) {
	f := newFakeWS(t, fakeOpts{replyPong: true})
	p := NewPool(PoolConfig{
		Dial:         f.dialFunc(),
		PingInterval: 10 * time.Millisecond,
		PongTimeout:  2 * time.Second,
		FrameQueue:   1,
	})
	defer p.CloseAll()

	l := mustAcquire(t, p, "k")
	fc := f.next(t)
	fc.send(t, `{"seq":1}`)
	waitStats(t, p, "帧入队", func(s PoolStats) bool { return s.Delivered == 1 })
	fc.send(t, `{"seq":2}`)
	waitStats(t, p, "pump 停读", func(s PoolStats) bool { return s.QueueBlocked > 0 })

	base := fc.pingCount()
	within(t, 2*time.Second, "停读期间 ping 被抑制并计数", func() bool {
		return p.Stats().PingSuppressed > 0
	})
	if n := fc.pingCount(); n != base {
		t.Fatalf("停读期间服务端 ping 计数 %d → %d，want 不增", base, n)
	}

	if _, err := l.Recv(context.Background()); err != nil {
		t.Fatalf("Recv: %v", err)
	}
	if _, err := l.Recv(context.Background()); err != nil {
		t.Fatalf("Recv: %v", err)
	}
	within(t, 2*time.Second, "排空后 ping 恢复", func() bool { return fc.pingCount() > base })
	l.Release()
}

// TestPoolIdleDropsStrayFrames：空闲期 3 帧 → Dropped==3；下一租约首帧是自己的。
func TestPoolIdleDropsStrayFrames(t *testing.T) {
	f := newFakeWS(t, fakeOpts{replyPong: true})
	p := NewPool(PoolConfig{Dial: f.dialFunc()})
	defer p.CloseAll()

	l := mustAcquire(t, p, "k")
	fc := f.next(t)
	l.Release()

	for i := 1; i <= 3; i++ {
		fc.send(t, fmt.Sprintf(`{"type":"codex.rate_limits","stray":%d}`, i))
	}
	waitStats(t, p, "空闲期丢弃 3 帧", func(s PoolStats) bool { return s.Dropped == 3 })
	if s := p.Stats(); s.Delivered != 0 {
		t.Fatalf("空闲帧不得投递：Delivered=%d", s.Delivered)
	}

	l2 := mustAcquire(t, p, "k")
	defer l2.Release()
	fc.send(t, `{"type":"response.created","mine":true}`)
	data, err := l2.Recv(context.Background())
	if err != nil {
		t.Fatalf("Recv: %v", err)
	}
	if !strings.Contains(string(data), `"mine":true`) {
		t.Fatalf("新租约首帧 = %s，want 自己的 response.created", data)
	}
}

// TestPoolNoCrossLeaseFrame（A2）：pump 已读帧、正待投递时 Release → 该帧不出现在下一租约。
func TestPoolNoCrossLeaseFrame(t *testing.T) {
	f := newFakeWS(t, fakeOpts{replyPong: true})
	p := NewPool(PoolConfig{Dial: f.dialFunc(), FrameQueue: 1})
	defer p.CloseAll()

	l1 := mustAcquire(t, p, "k")
	fc := f.next(t)
	fc.send(t, `{"seq":1}`)
	waitStats(t, p, "首帧入队", func(s PoolStats) bool { return s.Delivered == 1 })
	fc.send(t, `{"seq":2}`) // 队列已满 ⇒ pump 读到但停读等待（已读、待投递）
	waitStats(t, p, "pump 停读", func(s PoolStats) bool { return s.QueueBlocked > 0 })

	l1.Release() // 换代 + 清队列：待投递的 {"seq":2} 不得进入下一租约

	l2 := mustAcquire(t, p, "k")
	defer l2.Release()
	fc.send(t, `{"seq":3}`)
	data, err := l2.Recv(context.Background())
	if err != nil {
		t.Fatalf("Recv: %v", err)
	}
	if string(data) != `{"seq":3}` {
		t.Fatalf("新租约首帧 = %s，want {\"seq\":3}（旧代帧泄漏）", data)
	}
	if s := p.Stats(); s.Dropped < 1 {
		t.Fatalf("跨代帧必须计入 Dropped，got %d", s.Dropped)
	}
}

// TestPoolBackpressureBlocksAndResumes：队列满 ⇒ pump 停读（不杀连接、不丢帧）；
// 只要 Recv 取走 1 帧，pump 立刻恢复读（不靠 Release）。
func TestPoolBackpressureBlocksAndResumes(t *testing.T) {
	f := newFakeWS(t, fakeOpts{replyPong: true})
	p := NewPool(PoolConfig{Dial: f.dialFunc(), FrameQueue: 2})
	defer p.CloseAll()

	l := mustAcquire(t, p, "k")
	fc := f.next(t)
	const n = 6
	for i := 1; i <= n; i++ {
		fc.send(t, fmt.Sprintf(`{"seq":%d}`, i))
	}
	waitStats(t, p, "pump 因队列满停读", func(s PoolStats) bool {
		return s.QueueBlocked > 0 && s.Delivered == 2
	})
	if s := p.Stats(); s.Dropped != 0 || s.Dead != 0 {
		t.Fatalf("背压不得丢帧/杀连接：%+v", s)
	}

	// 取走 1 帧 → pump 必须立刻读下一帧（Delivered 3），整个过程不 Release。
	if data, err := l.Recv(context.Background()); err != nil || string(data) != `{"seq":1}` {
		t.Fatalf("首帧 = %s err=%v，want {\"seq\":1}", data, err)
	}
	waitStats(t, p, "取走 1 帧后 pump 恢复读（Delivered=3）", func(s PoolStats) bool {
		return s.Delivered == 3
	})

	for i := 2; i <= n; i++ {
		data, err := l.Recv(context.Background())
		if err != nil {
			t.Fatalf("第 %d 帧 Recv: %v", i, err)
		}
		if want := fmt.Sprintf(`{"seq":%d}`, i); string(data) != want {
			t.Fatalf("第 %d 帧 = %s，want %s（帧序/完整性）", i, data, want)
		}
	}
	if got := fc.wroteCount(); got != n {
		t.Fatalf("服务端写入帧数 = %d，want %d", got, n)
	}
	l.Release()
	if s := p.Stats(); s.Dropped != 0 || s.Dead != 0 {
		t.Fatalf("全链路完成后背压既不该丢帧也不该杀连接：%+v", s)
	}
}

// TestPoolQueueByteBudget：字节预算先到上限 → pump 停读（不杀连接），
// QueuedBytes ≤ 预算（单帧例外）。
func TestPoolQueueByteBudget(t *testing.T) {
	const budget = 10
	f := newFakeWS(t, fakeOpts{replyPong: true})
	p := NewPool(PoolConfig{
		Dial:       f.dialFunc(),
		FrameQueue: 64,
		QueueBytes: budget,
	})
	defer p.CloseAll()

	l := mustAcquire(t, p, "k")
	fc := f.next(t)
	fc.send(t, `{"seq":1}`)
	waitStats(t, p, "首帧入队", func(s PoolStats) bool { return s.Delivered == 1 })
	fc.send(t, `{"seq":22}`) // 8 字节帧：预算已尽（8+8 > 10）⇒ 停读
	waitStats(t, p, "字节预算触发停读", func(s PoolStats) bool { return s.QueueBlocked > 0 })

	if s := p.Stats(); s.QueuedBytes > budget {
		t.Fatalf("QueuedBytes = %d，want ≤ %d", s.QueuedBytes, budget)
	}
	if s := p.Stats(); s.Dead != 0 || s.Dropped != 0 {
		t.Fatalf("字节预算触底不得杀连接/丢帧：%+v", s)
	}

	if data, err := l.Recv(context.Background()); err != nil || string(data) != `{"seq":1}` {
		t.Fatalf("首帧 = %s err=%v", data, err)
	}
	waitStats(t, p, "取走 1 帧后 pump 恢复读", func(s PoolStats) bool { return s.Delivered == 2 })
	l.Release()
}

// TestLeaseRecvCtxCancelKeepsConn：Recv 的 ctx 取消不触碰 socket。
func TestLeaseRecvCtxCancelKeepsConn(t *testing.T) {
	f := newFakeWS(t, fakeOpts{replyPong: true})
	p := NewPool(PoolConfig{Dial: f.dialFunc()})
	defer p.CloseAll()

	l := mustAcquire(t, p, "k")
	defer l.Release()
	fc := f.next(t)

	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()
	if _, err := l.Recv(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Recv 错误 = %v，want context.DeadlineExceeded", err)
	}
	if p.Stats().Dead != 0 {
		t.Fatal("ctx 取消不得判死连接")
	}

	fc.send(t, `{"seq":1}`)
	data, err := l.Recv(context.Background())
	if err != nil || string(data) != `{"seq":1}` {
		t.Fatalf("取消后仍应能收帧：data=%s err=%v", data, err)
	}
}

// TestLeaseOpsIdempotent：Release/Discard/CloseAll 任意顺序重复 no-op；dead 后 Send/Recv 报错。
func TestLeaseOpsIdempotent(t *testing.T) {
	f := newFakeWS(t, fakeOpts{replyPong: true})
	p := NewPool(PoolConfig{Dial: f.dialFunc()})

	l := mustAcquire(t, p, "k")
	l.Release()
	l.Release()
	l.Discard()
	l.Discard()

	l2 := mustAcquire(t, p, "k")
	l2.Discard()
	l2.Discard()
	l2.Release()
	if _, err := l2.Recv(context.Background()); !errors.Is(err, ErrLeaseClosed) {
		t.Fatalf("dead 后 Recv 错误 = %v，want ErrLeaseClosed", err)
	}
	if err := l2.Send(context.Background(), []byte(`{"type":"response.create"}`)); !errors.Is(err, ErrLeaseClosed) {
		t.Fatalf("dead 后 Send 错误 = %v，want ErrLeaseClosed", err)
	}

	p.CloseAll()
	p.CloseAll()
	if _, err := p.Acquire(context.Background(), "k"); !errors.Is(err, ErrPoolClosed) {
		t.Fatalf("CloseAll 后 Acquire 错误 = %v，want ErrPoolClosed", err)
	}
	if s := p.Stats(); s.Released != 1 || s.Discarded != 1 {
		t.Fatalf("重复调用必须 no-op：Released/Discarded = %d/%d", s.Released, s.Discarded)
	}
}

// TestPoolReleaseClearsTurnState：stale turn-state 不得回传给下一个租约；
// 并且注入入口只有 Lease.SetTurnState（走 A4 的池内变体）。
func TestPoolReleaseClearsTurnState(t *testing.T) {
	f := newFakeWS(t, fakeOpts{replyPong: true})
	p := NewPool(PoolConfig{Dial: f.dialFunc()})
	defer p.CloseAll()

	l := mustAcquire(t, p, "k")
	c := poolConnsOf(t, p, "k")[0].c
	if err := l.SetTurnState("ts-lease-1"); err != nil {
		t.Fatalf("Lease.SetTurnState: %v", err)
	}
	if got := c.TurnState(); got != "ts-lease-1" {
		t.Fatalf("TurnState = %q，want 注入生效", got)
	}
	l.Release()
	if got := c.TurnState(); got != "" {
		t.Fatalf("Release 后 TurnState = %q，want 空串（清除必须在锁内完成）", got)
	}
	if err := l.SetTurnState("ts-after-release"); !errors.Is(err, ErrLeaseClosed) {
		t.Fatalf("已结束租约上 SetTurnState 错误 = %v，want ErrLeaseClosed", err)
	}

	l2 := mustAcquire(t, p, "k")
	defer l2.Release()
	if !l2.Reused() {
		t.Fatal("应复用同一连接")
	}
	if got := c.TurnState(); got != "" {
		t.Fatalf("TurnState = %q，want 空串（跨轮不得回传）", got)
	}
}

// TestPoolBreaksOnRemoteClose：空闲期对端关闭 → conn dead，下次 Acquire 重拨。
func TestPoolBreaksOnRemoteClose(t *testing.T) {
	f := newFakeWS(t, fakeOpts{replyPong: true})
	p := NewPool(PoolConfig{Dial: f.dialFunc()})
	defer p.CloseAll()

	l := mustAcquire(t, p, "k")
	fc := f.next(t)
	l.Release()
	fc.close()

	waitStats(t, p, "pump 感知对端关闭并判死", func(s PoolStats) bool { return s.Dead == 1 })

	l2 := mustAcquire(t, p, "k")
	defer l2.Release()
	if l2.Reused() {
		t.Fatal("已死连接不得被复用")
	}
	if s := p.Stats(); s.Dials != 2 {
		t.Fatalf("升级次数 = %d，want 2", s.Dials)
	}
}

// TestPoolCloseAllClosesLiveAndPump：持租约 CloseAll → 连接关闭且 pump/pinger 都退出。
func TestPoolCloseAllClosesLiveAndPump(t *testing.T) {
	f := newFakeWS(t, fakeOpts{replyPong: true})
	p := NewPool(PoolConfig{Dial: f.dialFunc()})

	l := mustAcquire(t, p, "k")
	st := poolConnsOf(t, p, "k")[0]
	client := st.c

	p.CloseAll()
	if !client.Closed() {
		t.Fatal("CloseAll 必须关闭在途连接的 socket")
	}
	select {
	case <-st.pumpDone:
	case <-time.After(3 * time.Second):
		t.Fatal("CloseAll 后 pump goroutine 未退出")
	}
	select {
	case <-st.pingDone:
	case <-time.After(3 * time.Second):
		t.Fatal("CloseAll 后 pinger goroutine 未退出")
	}
	if _, err := l.Recv(context.Background()); err == nil {
		t.Fatal("在途租约必须立刻收到错误")
	}
	if s := p.Stats(); s.Idle != 0 || s.Leased != 0 {
		t.Fatalf("CloseAll 后池内不得残留连接：%+v", s)
	}
}

// TestPoolCloseAllDuringDial：Dial 阻塞时 CloseAll → ErrPoolClosed 且拨出连接被关。
func TestPoolCloseAllDuringDial(t *testing.T) {
	f := newFakeWS(t, fakeOpts{replyPong: true})
	started := make(chan struct{})
	release := make(chan struct{})
	var (
		dialed *Client
		once   sync.Once
	)
	p := NewPool(PoolConfig{
		Dial: func(ctx context.Context, key PoolKey) (*Client, error) {
			once.Do(func() { close(started) })
			<-release
			c, err := Dial(ctx, PAT("pat-pool"), WithTransport(f.tr), WithPingInterval(0))
			dialed = c
			return c, err
		},
	})

	errCh := make(chan error, 1)
	go func() {
		_, err := p.Acquire(context.Background(), "k")
		errCh <- err
	}()
	select {
	case <-started:
	case <-time.After(3 * time.Second):
		t.Fatal("DialFunc 未被调用")
	}
	p.CloseAll()
	close(release)

	select {
	case err := <-errCh:
		if !errors.Is(err, ErrPoolClosed) {
			t.Fatalf("Acquire 错误 = %v，want ErrPoolClosed", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Dial 返回后 Acquire 未结束")
	}
	within(t, 3*time.Second, "拨出连接被关闭", func() bool { return dialed != nil && dialed.Closed() })
}

// TestPoolIdleConnAgedNotLeased：出租时刻的年龄闸——空闲连接 age ≥ HardLimit 不再出租
// （acquireIdle 即刻按 ErrConnExpired 回收，计 ReapedAge），本次请求改为新拨。
func TestPoolIdleConnAgedNotLeased(t *testing.T) {
	clk := newMockClock()
	f := newFakeWS(t, fakeOpts{replyPong: true})
	p := NewPool(PoolConfig{
		Dial:      f.dialFunc(),
		IdleTTL:   -1, // 关掉空闲 TTL，只测年龄闸
		HardLimit: 30 * time.Minute,
		Now:       clk.Now,
	})
	defer p.CloseAll()

	l := mustAcquire(t, p, "k")
	st := poolConnsOf(t, p, "k")[0]
	l.Release()
	clk.Advance(30 * time.Minute) // 恰好到达 HardLimit（>= 判据）

	l2 := mustAcquire(t, p, "k")
	defer l2.Release()
	if l2.Reused() {
		t.Fatal("age ≥ HardLimit 的空闲连接被出租了")
	}
	if s := p.Stats(); s.Dials != 2 {
		t.Fatalf("Dials = %d，want 2", s.Dials)
	}
	select {
	case <-st.done:
	default:
		t.Fatal("超龄连接必须被关闭")
	}
	waitStats(t, p, "按年龄回收计数", func(s PoolStats) bool { return s.ReapedAge == 1 })
}

// TestPoolLeasedConnReapedAtHardLimit：在租连接 age ≥ HardLimit → janitor 回收，
// Recv 返回 ErrConnExpired（errors.Is 可判），计 ReapedInFlight 而非 ReapedAge。
// 这是"请求时长归调用方、连接寿命归 HardLimit"下病态消费方的兜底判据。
func TestPoolLeasedConnReapedAtHardLimit(t *testing.T) {
	clk := newMockClock()
	f := newFakeWS(t, fakeOpts{replyPong: true})
	p := NewPool(PoolConfig{
		Dial:      f.dialFunc(),
		IdleTTL:   -1, // 关掉空闲 TTL，只测 HardLimit
		HardLimit: 30 * time.Minute,
		Now:       clk.Now,
	})
	defer p.CloseAll()

	l := mustAcquire(t, p, "k")
	st := poolConnsOf(t, p, "k")[0]
	clk.Advance(30 * time.Minute) // 恰好到达 HardLimit（>= 判据）
	p.sweep()

	if s := p.Stats(); s.ReapedInFlight != 1 || s.ReapedAge != 0 {
		t.Fatalf("ReapedInFlight/ReapedAge = %d/%d，want 1/0", s.ReapedInFlight, s.ReapedAge)
	}
	select {
	case <-st.done:
	default:
		t.Fatal("在租超龄连接必须被关闭（done 未关）")
	}
	if _, err := l.Recv(context.Background()); !errors.Is(err, ErrConnExpired) {
		t.Fatalf("Recv 错误 = %v，want ErrConnExpired", err)
	}
}

// TestPoolEmptyKeysCountsBucketRemoval：某键桶内的连接全部被判死后，sweep 整桶摘除
// ⇒ EmptyKeys 计 1；再 sweep 一次必须仍是 1（桶已不存在，不得重复计数）。
func TestPoolEmptyKeysCountsBucketRemoval(t *testing.T) {
	clk := newMockClock()
	f := newFakeWS(t, fakeOpts{replyPong: true})
	p := NewPool(PoolConfig{
		Dial:      f.dialFunc(),
		IdleTTL:   -1, // 关掉空闲 TTL：只走年龄回收，不掺 TTL 分支
		HardLimit: 30 * time.Minute,
		Now:       clk.Now,
	})
	defer p.CloseAll()

	l := mustAcquire(t, p, "k")
	if got := len(poolConnsOf(t, p, "k")); got != 1 {
		t.Fatalf("键桶内连接数 = %d，want 1", got)
	}
	l.Release()
	clk.Advance(30 * time.Minute) // 越过 HardLimit ⇒ 桶内唯一连接被判死
	p.sweep()

	s := p.Stats()
	if s.EmptyKeys != 1 || s.Keys != 0 {
		t.Fatalf("EmptyKeys/Keys = %d/%d，want 1/0（整桶摘除才计 1）", s.EmptyKeys, s.Keys)
	}
	if got := len(poolConnsOf(t, p, "k")); got != 0 {
		t.Fatalf("空桶仍留在 p.keys 里：%d 条", got)
	}
	p.sweep()
	if s := p.Stats(); s.EmptyKeys != 1 {
		t.Fatalf("重复 sweep 重复计数：EmptyKeys = %d，want 1", s.EmptyKeys)
	}
}

// TestPoolIdleTTL：注入时钟推进 → 空闲连接被 TTL 回收，重新拨号。
func TestPoolIdleTTL(t *testing.T) {
	clk := newMockClock()
	f := newFakeWS(t, fakeOpts{replyPong: true})
	p := NewPool(PoolConfig{
		Dial:    f.dialFunc(),
		IdleTTL: time.Minute,
		Now:     clk.Now,
	})
	defer p.CloseAll()

	l := mustAcquire(t, p, "k")
	st := poolConnsOf(t, p, "k")[0]
	l.Release()
	if s := p.Stats(); s.Idle != 1 {
		t.Fatalf("Idle = %d，want 1", s.Idle)
	}

	clk.Advance(2 * time.Minute)
	p.sweep()
	if s := p.Stats(); s.ReapedIdle != 1 || s.Idle != 0 {
		t.Fatalf("ReapedIdle/Idle = %d/%d，want 1/0", s.ReapedIdle, s.Idle)
	}
	select {
	case <-st.done:
	default:
		t.Fatal("TTL 到期的空闲连接必须被关闭")
	}

	l2 := mustAcquire(t, p, "k")
	defer l2.Release()
	if l2.Reused() || p.Stats().Dials != 2 {
		t.Fatalf("回收后必须重拨：Reused=%v Dials=%d", l2.Reused(), p.Stats().Dials)
	}
}

// TestPoolMaxIdlePerKey：单键空闲上限生效（多出来的按最久未用关闭）。
func TestPoolMaxIdlePerKey(t *testing.T) {
	f := newFakeWS(t, fakeOpts{replyPong: true})
	p := NewPool(PoolConfig{Dial: f.dialFunc(), MaxIdlePerKey: 1})
	defer p.CloseAll()

	l1 := mustAcquire(t, p, "k")
	l2 := mustAcquire(t, p, "k")
	if s := p.Stats(); s.Dials != 2 || s.Leased != 2 {
		t.Fatalf("准备两条在租连接失败：%+v", s)
	}
	clients := []*Client{poolConnsOf(t, p, "k")[0].c, poolConnsOf(t, p, "k")[1].c}

	l1.Release()
	if s := p.Stats(); s.Idle != 1 {
		t.Fatalf("Idle = %d，want 1", s.Idle)
	}
	l2.Release()

	// 关键：同键只保留 1 条空闲连接 —— 但刚归还的那条不能被自己淘汰掉。
	// 判定"至少一条被关闭、且 Idle 收敛到 1"是上限语义的可观测面。
	waitStats(t, p, "同键空闲收敛到 1", func(s PoolStats) bool { return s.Idle == 1 })
	closed := 0
	for _, c := range clients {
		if c.Closed() {
			closed++
		}
	}
	if closed != 1 {
		t.Fatalf("被淘汰的连接数 = %d，want 1", closed)
	}
	if s := p.Stats(); s.ReapedIdle != 1 {
		t.Fatalf("ReapedIdle = %d，want 1", s.ReapedIdle)
	}
}

// TestPoolMaxIdleTotalDefaultUnlimited：MaxIdleTotal 的 0（默认）与 <0 都是"不限"——
// 各灌 100 条空闲连接，一条都不能丢（容量由调用方的键空间形状决定）。
func TestPoolMaxIdleTotalDefaultUnlimited(t *testing.T) {
	if DefaultMaxIdleTotal != 0 {
		t.Fatalf("DefaultMaxIdleTotal = %d，want 0（不限）", DefaultMaxIdleTotal)
	}
	for _, total := range []int{0, -1} {
		t.Run(fmt.Sprintf("MaxIdleTotal=%d", total), func(t *testing.T) {
			f := newFakeWS(t, fakeOpts{replyPong: true})
			p := NewPool(PoolConfig{Dial: f.dialFunc(), MaxIdleTotal: total})
			defer p.CloseAll()
			// ≤0 不得被归一成任何常数（否则"容量归调用方"不成立）。
			if got := p.cfg.MaxIdleTotal; got != total {
				t.Fatalf("归一后 MaxIdleTotal = %d，want %d", got, total)
			}

			const n = 100
			clients := make([]*Client, 0, n)
			for i := 0; i < n; i++ {
				key := PoolKey(fmt.Sprintf("k-%03d", i))
				l := mustAcquire(t, p, key)
				clients = append(clients, poolConnsOf(t, p, key)[0].c)
				l.Release()
			}
			p.sweep()

			s := p.Stats()
			if s.Idle != n {
				t.Fatalf("Idle = %d，want %d（≤0 必须不限）", s.Idle, n)
			}
			if s.ReapedIdle != 0 {
				t.Fatalf("ReapedIdle = %d，want 0", s.ReapedIdle)
			}
			closed := 0
			for _, c := range clients {
				if c.Closed() {
					closed++
				}
			}
			if closed != 0 {
				t.Fatalf("被关闭的连接数 = %d，want 0", closed)
			}
		})
	}
}

// TestPoolMaxIdleTotal：显式给出 MaxIdleTotal 时总量闸生效——灌 300 条空闲 →
// 池内 idle 收敛到该上限，被淘汰者已关闭（默认 0=不限，见上一条测试）。
func TestPoolMaxIdleTotal(t *testing.T) {
	const limit = 256
	f := newFakeWS(t, fakeOpts{replyPong: true})
	p := NewPool(PoolConfig{Dial: f.dialFunc(), MaxIdleTotal: limit})
	defer p.CloseAll()

	const n = 300
	clients := make([]*Client, 0, n)
	for i := 0; i < n; i++ {
		key := PoolKey(fmt.Sprintf("k-%03d", i))
		l := mustAcquire(t, p, key)
		clients = append(clients, poolConnsOf(t, p, key)[0].c)
		l.Release()
	}
	p.sweep()

	s := p.Stats()
	if s.Idle != limit {
		t.Fatalf("Idle = %d，want %d（MaxIdleTotal 上限）", s.Idle, limit)
	}
	closed := 0
	for _, c := range clients {
		if c.Closed() {
			closed++
		}
	}
	if closed != n-limit {
		t.Fatalf("被淘汰并关闭的连接数 = %d，want %d", closed, n-limit)
	}
	if s.Dials != n {
		t.Fatalf("Dials = %d，want %d", s.Dials, n)
	}
}

// TestClientOwnership：池 claim 后外部 Send/Recv/Ping/Close/CloseNow 全部 ErrNotOwner；
// Lease.Send/Recv 正常。
func TestClientOwnership(t *testing.T) {
	f := newFakeWS(t, fakeOpts{replyPong: true})
	var kept *Client
	p := NewPool(PoolConfig{
		Dial: func(ctx context.Context, key PoolKey) (*Client, error) {
			c, err := Dial(ctx, PAT("pat-pool"), WithTransport(f.tr), WithPingInterval(0),
				WithTraceAuto(false), WithTurnAuto(false))
			kept = c
			return c, err
		},
	})
	defer p.CloseAll()

	l := mustAcquire(t, p, "k")
	defer l.Release()
	fc := f.next(t)
	if kept == nil {
		t.Fatal("DialFunc 未被调用")
	}

	bg := context.Background()
	if err := kept.Send(bg, []byte(`{"type":"response.create"}`)); !errors.Is(err, ErrNotOwner) {
		t.Fatalf("外部 Send 错误 = %v，want ErrNotOwner", err)
	}
	if _, err := kept.Recv(bg); !errors.Is(err, ErrNotOwner) {
		t.Fatalf("外部 Recv 错误 = %v，want ErrNotOwner", err)
	}
	if err := kept.Ping(bg); !errors.Is(err, ErrNotOwner) {
		t.Fatalf("外部 Ping 错误 = %v，want ErrNotOwner", err)
	}
	if err := kept.Close(StatusGoingAway, ""); !errors.Is(err, ErrNotOwner) {
		t.Fatalf("外部 Close 错误 = %v，want ErrNotOwner", err)
	}
	if err := kept.CloseNow(); !errors.Is(err, ErrNotOwner) {
		t.Fatalf("外部 CloseNow 错误 = %v，want ErrNotOwner", err)
	}
	if err := kept.SetTurnState("x"); !errors.Is(err, ErrNotOwner) {
		t.Fatalf("外部 SetTurnState 错误 = %v，want ErrNotOwner", err)
	}
	if got := kept.TurnState(); got != "" {
		t.Fatalf("TurnState = %q，want 空串（ErrNotOwner 的写入不得生效）", got)
	}
	if kept.Closed() {
		t.Fatal("ErrNotOwner 的 Close/CloseNow 不得真的关闭连接")
	}

	// 租约路径必须畅通：Lease.SetTurnState 注入 → Send 到服务端 + Recv 服务端帧。
	if err := l.SetTurnState("ts-via-lease"); err != nil {
		t.Fatalf("Lease.SetTurnState: %v", err)
	}
	if got := kept.TurnState(); got != "ts-via-lease" {
		t.Fatalf("TurnState = %q，want ts-via-lease（池内入口必须写得进去）", got)
	}
	if err := l.Send(bg, []byte(`{"type":"response.create"}`)); err != nil {
		t.Fatalf("Lease.Send: %v", err)
	}
	fc.send(t, `{"type":"response.created"}`)
	data, err := l.Recv(bg)
	if err != nil {
		t.Fatalf("Lease.Recv: %v", err)
	}
	if !strings.Contains(string(data), "response.created") {
		t.Fatalf("Lease.Recv 数据 = %s", data)
	}
}

// ---------------------------------------------------------------------------
// §4.3 bench（只记基线）
// ---------------------------------------------------------------------------

// benchTransport 把请求重写到本地假服务（bench 拿不到 *testing.T，故不复用
// newFixedTransport；语义与 fixedTransport 相同）。
type benchTransport struct{ target *url.URL }

func (tr benchTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	clone := req.Clone(req.Context())
	u := *req.URL
	u.Scheme = tr.target.Scheme
	u.Host = tr.target.Host
	clone.URL = &u
	return http.DefaultTransport.RoundTrip(clone)
}

func benchDialFunc(tb testing.TB, srvURL string) DialFunc {
	target, err := url.Parse(srvURL)
	if err != nil {
		tb.Fatalf("parse %q: %v", srvURL, err)
	}
	tr := benchTransport{target: target}
	return func(ctx context.Context, key PoolKey) (*Client, error) {
		return Dial(ctx, PAT("pat-bench"), WithTransport(tr), WithPingInterval(0),
			WithTraceAuto(false), WithTurnAuto(false))
	}
}

func BenchmarkLeaseRecv(b *testing.B) {
	var connected sync.WaitGroup
	connected.Add(1)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer c.CloseNow()
		connected.Done()
		payload := []byte(`{"type":"response.output_text.delta"}`)
		for { // 服务端只管推流；客户端 pump 是唯一读者
			ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
			err := c.Write(ctx, websocket.MessageText, payload)
			cancel()
			if err != nil {
				return
			}
		}
	}))
	defer srv.Close()
	p := NewPool(PoolConfig{Dial: benchDialFunc(b, srv.URL)})
	defer p.CloseAll()
	l, err := p.Acquire(context.Background(), "bench")
	if err != nil {
		b.Fatalf("Acquire: %v", err)
	}
	defer l.Release()
	ctx := context.Background()
	if _, err := l.Recv(ctx); err != nil { // 预热
		b.Fatalf("预热 Recv: %v", err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := l.Recv(ctx); err != nil {
			b.Fatalf("Recv: %v", err)
		}
	}
}

func BenchmarkPoolAcquireRelease(b *testing.B) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer c.CloseNow()
		for {
			if _, _, err := c.Read(r.Context()); err != nil {
				return
			}
		}
	}))
	defer srv.Close()
	p := NewPool(PoolConfig{Dial: benchDialFunc(b, srv.URL)})
	defer p.CloseAll()
	ctx := context.Background()
	if l, err := p.Acquire(ctx, "bench"); err == nil {
		l.Release()
	} else {
		b.Fatalf("预热 Acquire: %v", err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		l, err := p.Acquire(ctx, "bench")
		if err != nil {
			b.Fatalf("Acquire: %v", err)
		}
		l.Release()
	}
}
