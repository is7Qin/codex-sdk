package codexsdk

import (
	"bytes"
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

// 本文件是 T5.1（连接复用内核）的验收用例：R2/R3/R4b/R14/R15/R16/R17/R20/R23。
//
// T5.1 里没有缓存（`wsExchange.sink == nil`），所以"归还"这条路径用桩 sink 观测；
// 端到端的"真的进了表、下一个请求 accept 不增"归 T5.2 的 R1/R4。

// ---- R2 / R23：fence / unfence 的语义（inbox 层，确定性） ----

// TestWSInboxFenceRequiresEmptyQueue（R2）：队列非空时 fence 必须失败。
//
// 这是"读到终态 + 队列空"里的后半条：上游终态之后又发了一帧，就说明还有东西在
// 路上，这条连接不能交出去（否则下一请求会把残帧当自己的事件）。
func TestWSInboxFenceRequiresEmptyQueue(t *testing.T) {
	q := newFrameInbox(4, 1<<10)
	if err := q.push([]byte(`{"type":"response.created"}`)); err != nil {
		t.Fatalf("push: %v", err)
	}
	if q.fence() {
		t.Fatal("队列非空时 fence 成功了（残帧会被下一个请求读到）")
	}
	if _, ok := q.tryPop(); !ok {
		t.Fatal("tryPop 应当取到那一帧")
	}
	if !q.fence() {
		t.Fatal("队列已空，fence 应当成功")
	}
}

// TestWSInboxFencePoisonsOnLateFrame（R23）：置栏之后的帧必须把连接毒化，
// 且此后 unfence 永远失败（这条连接不能再被任何请求取用）。
func TestWSInboxFencePoisonsOnLateFrame(t *testing.T) {
	q := newFrameInbox(4, 1<<10)
	if !q.fence() {
		t.Fatal("空队列 fence 应当成功")
	}
	err := q.push([]byte(`{"type":"response.created"}`))
	if !errors.Is(err, errIdleFrame) {
		t.Fatalf("空闲期收到帧的 push 错误 = %v，期望 errIdleFrame", err)
	}
	if q.unfence() {
		t.Fatal("poisoned 之后 unfence 成功了（残帧会被当新请求的首帧）")
	}
	// 毒化是永久的：再来一次 unfence 也不能成功。
	if q.unfence() {
		t.Fatal("第二次 unfence 仍然成功")
	}
}

// ---- R3 / R17：空闲期收到帧 ⇒ readLoop 既有分支终止连接 ----

// TestWSIdleFrameTerminatesConn（R3 + R17）：把一条真连接的 inbox 置栏，再让上游
// 发一帧 ⇒ push 返回 errIdleFrame，readLoop 的**既有**分支（`terminate(perr)`）
// 终止连接，且终止原因就是 errIdleFrame（没有新分支、没有吞错）。
func TestWSIdleFrameTerminatesConn(t *testing.T) {
	// 上游先读到我们的第一帧，再发它自己的帧 ⇒ "置栏"与"回帧"的先后完全确定。
	up := newContractUpstream(t, func(c *coderws.Conn, r *http.Request) {
		if _, _, err := c.Read(r.Context()); err != nil {
			return
		}
		serveAndStall(c, r, wsCreatedEv)
	})

	hc := contractClient(t, up.srv.URL)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	cn, _, err := dialConn(ctx, dialSpec{
		auth:      hc.auth,
		opts:      hc.opts,
		readLimit: wsStreamReadLimit,
		mode:      modePumped,
		headers:   hc.opts.headers,
	})
	if err != nil {
		t.Fatalf("dialConn: %v", err)
	}
	defer cn.terminate(io.EOF)

	// 队列空 ⇒ 置栏成功（模拟"刚归还、还没被取用"）。
	if !cn.inbox.fence() {
		t.Fatal("空队列 fence 应当成功")
	}
	// 上游此刻正等我们的第一帧；写出之后它才会回脚本帧。
	if werr := cn.write(ctx, []byte(`{"type":"response.create"}`)); werr != nil {
		t.Fatalf("write: %v", werr)
	}

	waitFor(t, func() bool { return cn.err() != nil })
	if got := cn.err(); !errors.Is(got, errIdleFrame) {
		t.Fatalf("终止原因 = %v，期望 errIdleFrame（走 readLoop 既有分支）", got)
	}
	cn.inbox.mu.Lock()
	poisoned := cn.inbox.poisoned
	cn.inbox.mu.Unlock()
	if !poisoned {
		t.Fatal("poisoned 未置位")
	}
}

// ---- R14：WithTimeout 覆盖拨号/升级 ----

// TestWSTimeoutCoversDial（R14）：上游 accept 之后挂起、永不完成升级时，
// WithTimeout 必须把拨号也掐掉（T5 之前它只覆盖取帧）。
func TestWSTimeoutCoversDial(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-r.Context().Done() // 永不升级
	}))
	t.Cleanup(srv.Close)

	hc := NewHTTPClient(PAT("pat-timeout"),
		WithResponsesTransport(TransportWS),
		WithTransport(newFixedTransport(t, "https://chatgpt.com/backend-api/codex/responses", srv.URL)),
		WithTimeout(80*time.Millisecond),
	)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	start := time.Now()
	_, err := hc.StreamBody(ctx, []byte(`{"model":"gpt-5.6","input":[]}`), Identity{})
	if err == nil {
		t.Fatal("拨号挂起时 StreamBody 应当失败")
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("错误 = %v，期望 DeadlineExceeded（WithTimeout 必须覆盖拨号）", err)
	}
	if el := time.Since(start); el > 2*time.Second {
		t.Fatalf("超时耗时 %v，说明没有按 WithTimeout 生效", el)
	}
}

// ---- R15：上游终态后立即关连接 ⇒ 归还失败但不 panic ----

// TestWSReleaseAfterUpstreamCloseIsSafe（R15）：上游回完终态就强断。此时连接已经
// 终止（closeDone 已关），归还必须**被拒绝**——既不能交给 sink，也不能报成功。
func TestWSReleaseAfterUpstreamCloseIsSafe(t *testing.T) {
	up := newWSUpstream(t)
	up.setScripts(wsScript{frames: []string{wsCreatedEv, wsCompletedEv}, abrupt: true})

	hc := NewHTTPClient(PAT("pat-abrupt"),
		WithResponsesTransport(TransportWS),
		WithTransport(newFixedTransport(t, "https://chatgpt.com/backend-api/codex/responses", up.srv.URL)),
	)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	ex, err := hc.openWSExchange(ctx, []byte(`{"model":"gpt-5.6","input":[]}`), Identity{}, wsStreamReadLimit)
	if err != nil {
		t.Fatalf("openWSExchange: %v", err)
	}
	// 读到终态帧（两帧之后上游就强断了）。
	if _, nerr := ex.Next(ex.ctx); nerr != nil {
		t.Fatalf("Next(created): %v", nerr)
	}
	if _, nerr := ex.Next(ex.ctx); nerr != nil {
		t.Fatalf("Next(completed): %v", nerr)
	}
	// 等连接真的死透（readLoop 看到 close/EOF 后 terminate）。
	select {
	case <-ex.cn.closeDone:
	case <-time.After(3 * time.Second):
		t.Fatal("上游强断后连接未终止")
	}

	sink := &recordingSink{}
	ex.sink = sink
	if ex.Release() {
		t.Fatal("连接已终止，Release 却报成功")
	}
	if sink.got != nil {
		t.Fatal("已终止的连接被交给了缓存")
	}
}

// TestStreamBodyAfterUpstreamCloseIsSafe 是同一场景的公开路径安全网（不 panic、
// 已读到的内容不受影响）。
func TestStreamBodyAfterUpstreamCloseIsSafe(t *testing.T) {
	up := newWSUpstream(t)
	up.setScripts(wsScript{frames: []string{wsCreatedEv, wsCompletedEv}, abrupt: true})

	hc := NewHTTPClient(PAT("pat-abrupt2"),
		WithResponsesTransport(TransportWS),
		WithTransport(newFixedTransport(t, "https://chatgpt.com/backend-api/codex/responses", up.srv.URL)),
	)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resp, err := hc.StreamBody(ctx, []byte(`{"model":"gpt-5.6","input":[]}`), Identity{})
	if err != nil {
		t.Fatalf("StreamBody: %v", err)
	}
	if body := readAllBody(t, resp.Body); body == "" {
		t.Fatal("终态之后的内容不该为空")
	}
}

// ---- R16：调用方取消、不关 body ⇒ 连接必须被终止（不泄漏） ----

// TestWSCancelWithoutCloseTerminatesConn（R16）：取消 ctx 之后**既不读、也不关 body**
// ——只有取消守卫（AfterFunc + 租约 CAS）能把连接终止，否则 readLoop 会永久挂在
// socket 上（goroutine 泄漏）。所以这条用例用"goroutine 回落到基线"来断言。
func TestWSCancelWithoutCloseTerminatesConn(t *testing.T) {
	// 没有终态帧：流会一直停在那里等下一帧。
	up := newContractUpstream(t, func(c *coderws.Conn, r *http.Request) {
		serveAndStall(c, r, wsCreatedEv, wsDeltaEv)
	})
	hc := contractClient(t, up.srv.URL)

	base := runtime.NumGoroutine()
	ctx, cancel := context.WithCancel(context.Background())
	resp, err := hc.StreamBody(ctx, []byte(`{"model":"gpt-5.6","input":[]}`), Identity{})
	if err != nil {
		cancel()
		t.Fatalf("StreamBody: %v", err)
	}
	buf := make([]byte, 64)
	if _, rerr := resp.Body.Read(buf); rerr != nil {
		cancel()
		t.Fatalf("首帧读取应当成功: %v", rerr)
	}

	cancel() // 关键：**不**再读，也**不**调用 resp.Body.Close()

	waitFor(t, func() bool { return runtime.NumGoroutine() <= base+1 })
}

// ---- R20：归还之后，调用方的 defer cancel() 不得杀掉连接 ----

// recordingSink 是"缓存收下"的桩：只记录，不持有。
type recordingSink struct {
	mu   chan struct{} // 未使用；仅为让结构体不为空
	got  *wsExchange
	deny bool
}

func (s *recordingSink) put(e *wsExchange) bool {
	if s.deny {
		return false
	}
	s.got = e
	return true
}

// TestWSReleaseSurvivesCallerCancel（R20）：Release 已经把连接交出去（租约已清、
// 取消回调已注销），此后调用方的 defer cancel() 不能把这条连接杀掉——否则复用
// 永远无法生效（每个请求结束时调用方都会 cancel）。
func TestWSReleaseSurvivesCallerCancel(t *testing.T) {
	// 上游发一帧后保持连接打开（不关、不终态）：这样"连接还活着"才有意义。
	up := newContractUpstream(t, func(c *coderws.Conn, r *http.Request) {
		serveAndStall(c, r, wsCreatedEv)
	})

	hc := contractClient(t, up.srv.URL)
	parent, cancelParent := context.WithCancel(context.Background())
	defer cancelParent()

	ex, err := hc.openWSExchange(parent, []byte(`{"model":"gpt-5.6","input":[]}`), Identity{}, wsStreamReadLimit)
	if err != nil {
		t.Fatalf("openWSExchange: %v", err)
	}
	defer ex.cn.terminate(io.EOF)

	// 先把已入队的那一帧读掉，队列才会是空的（fence 的前提）。
	if _, nerr := ex.Next(ex.ctx); nerr != nil {
		t.Fatalf("Next: %v", nerr)
	}

	sink := &recordingSink{}
	ex.sink = sink
	if !ex.Release() {
		t.Fatal("Release 应当成功（队列空 + 连接可用 + sink 收下）")
	}
	if sink.got != ex {
		t.Fatal("sink 没有收到这条连接")
	}

	cancelParent() // 调用方的 defer cancel()
	time.Sleep(30 * time.Millisecond)
	if got := ex.cn.err(); got != nil {
		t.Fatalf("归还之后连接被旧 ctx 杀掉了：%v（租约 CAS / stopWatch 失效）", got)
	}
}

// ---- C1：Next 与 Release 必须互斥（评审 oracle 的屏障反例） ----

// pausedErrContext 在第一次 Err() 上设屏障：用来把"取帧正在进行"这一刻定住，
// 从而把 Next 与 Release 的交错变成确定性的。
type pausedErrContext struct {
	context.Context
	entered chan struct{}
	resume  chan struct{}
	once    sync.Once
}

func (c *pausedErrContext) Err() error {
	c.once.Do(func() { close(c.entered); <-c.resume })
	return c.Context.Err()
}

// TestWSNextAfterReleaseIsInert：Release 成功之后，任何（迟到的）Next 都必须
// 立刻返回 io.EOF，既不能 terminate 这条连接，也不能从队列里取帧。
func TestWSNextAfterReleaseIsInert(t *testing.T) {
	up := newContractUpstream(t, func(c *coderws.Conn, r *http.Request) {
		serveAndStall(c, r, wsCompletedEv)
	})
	hc := contractClient(t, up.srv.URL)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	ex, err := hc.openWSExchange(ctx, []byte(`{"model":"gpt-5.6","input":[]}`), Identity{}, wsStreamReadLimit)
	if err != nil {
		t.Fatalf("openWSExchange: %v", err)
	}
	defer ex.cn.terminate(io.EOF)
	if _, nerr := ex.Next(ex.ctx); nerr != nil {
		t.Fatalf("Next: %v", nerr)
	}
	sink := &recordingSink{}
	ex.sink = sink
	if !ex.Release() {
		t.Fatal("Release 应当成功")
	}

	// 模拟"下一个请求取走了这条连接"：先解栏，再把 E2 的首帧放进队列。
	next := &wsExchange{cn: ex.cn}
	ex.cn.lease.Store(next)
	if !ex.cn.inbox.unfence() {
		t.Fatal("解开 E2 的栏失败（连接不该被毒化）")
	}
	if perr := ex.cn.inbox.push([]byte(wsCreatedEv)); perr != nil {
		t.Fatalf("给 E2 放首帧失败: %v", perr)
	}

	if _, nerr := ex.Next(ex.ctx); !errors.Is(nerr, io.EOF) {
		t.Fatalf("Release 之后的 Next 返回 %v，期望 io.EOF", nerr)
	}
	if got := ex.cn.err(); got != nil {
		t.Fatalf("迟到的 Next 杀掉了已移交的连接：%v", got)
	}
	if ex.cn.lease.Load() != next {
		t.Fatal("租约被迟到的 Next 抢走了")
	}
	// 最关键的一条：E2 的首帧必须还在队列里（没有被旧 Next 抢走）。
	frame, ok := ex.cn.inbox.tryPop()
	if !ok {
		t.Fatal("E2 的首帧被旧 Next 抢走了")
	}
	if !bytes.Equal(frame, []byte(wsCreatedEv)) {
		t.Fatalf("E2 的首帧内容被改动: %q", frame)
	}
}

// TestWSReleaseWaitsForInFlightNext：有取帧在飞时，Release 必须等它退出，
// 且**不得**把连接交出去（那次取帧看到的是取消，本次请求已被放弃）。
func TestWSReleaseWaitsForInFlightNext(t *testing.T) {
	up := newContractUpstream(t, func(c *coderws.Conn, r *http.Request) {
		serveAndStall(c, r, wsCompletedEv)
	})
	hc := contractClient(t, up.srv.URL)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	ex, err := hc.openWSExchange(ctx, []byte(`{"model":"gpt-5.6","input":[]}`), Identity{}, wsStreamReadLimit)
	if err != nil {
		t.Fatalf("openWSExchange: %v", err)
	}
	defer ex.cn.terminate(io.EOF)
	if _, nerr := ex.Next(ex.ctx); nerr != nil {
		t.Fatalf("Next: %v", nerr)
	}
	sink := &recordingSink{}
	ex.sink = sink

	paused := &pausedErrContext{Context: ex.ctx, entered: make(chan struct{}), resume: make(chan struct{})}
	nextDone := make(chan struct{})
	go func() {
		defer close(nextDone)
		_, _ = ex.Next(paused)
	}()
	<-paused.entered // 取帧已进入，且卡在第一次 ctx.Err()

	released := make(chan bool, 1)
	go func() { released <- ex.Release() }()

	// 等 Release 真的走到 cancel（它一定在等 nextMu 之前 cancel），再放行取帧。
	// 没有这个屏障，"删掉 nextMu" 的变体可能因旧 Next 先拿到 CAS 而偶然变绿。
	select {
	case <-ex.ctx.Done():
	case <-time.After(3 * time.Second):
		t.Fatal("Release 没有 cancel exchangeCtx")
	}
	select {
	case got := <-released:
		t.Fatalf("有取帧在飞时 Release 提前返回了 %v", got)
	default:
	}
	close(paused.resume)
	<-nextDone
	if got := <-released; got {
		t.Fatal("有取帧在飞时 Release 报成功（连接被交出去了）")
	}
	if sink.got != nil {
		t.Fatal("有取帧在飞时连接仍被交给了缓存")
	}
	if ex.cn.err() == nil {
		t.Fatal("被放弃的请求没有终止连接")
	}
}

// recordingStream 是 frameStream 桩：记录 Release/Close 各被调了几次。
type recordingStream struct {
	frames   [][]byte
	releases int
	closes   int
}

func (s *recordingStream) Next(context.Context) ([]byte, error) {
	if len(s.frames) == 0 {
		return nil, io.EOF
	}
	f := s.frames[0]
	s.frames = s.frames[1:]
	return f, nil
}

func (s *recordingStream) Release() bool { s.releases++; return true }
func (s *recordingStream) Close() error  { s.closes++; return nil }

// TestSSEViewCloseReleasesOnTerminal（R4b）：已经取出终态帧（terminal=true），
// 哪怕只读了一部分就 Close，也必须走 Release（连接停在干净帧边界）。
func TestSSEViewCloseReleasesOnTerminal(t *testing.T) {
	st := &recordingStream{frames: [][]byte{[]byte(wsCreatedEv), []byte(wsCompletedEv)}}
	v := newSSEView(context.Background(), st, 0)

	buf := make([]byte, 16) // 故意小于一帧的渲染长度：只读一部分
	if _, err := v.Read(buf); err != nil {
		t.Fatalf("首帧读取: %v", err)
	}
	// 继续读到终态帧被 load 出来（terminal 置位），但不读尽它。
	for !v.terminal {
		if _, err := v.Read(buf); err != nil && err != io.EOF {
			t.Fatalf("读取到终态前失败: %v", err)
		}
	}
	if err := v.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if st.releases != 1 {
		t.Fatalf("Release 调用了 %d 次，期望 1 次", st.releases)
	}
	if st.closes != 0 {
		t.Fatalf("Close 调用了 %d 次，期望 0 次（终态路径必须归还）", st.closes)
	}
}

// TestSSEViewCloseAbandonsWithoutTerminal：没读到终态就 Close ⇒ 必须走 Close
// （本次请求被放弃，连接不能留给下一个请求）。
func TestSSEViewCloseAbandonsWithoutTerminal(t *testing.T) {
	st := &recordingStream{frames: [][]byte{[]byte(wsCreatedEv), []byte(wsDeltaEv)}}
	v := newSSEView(context.Background(), st, 0)
	buf := make([]byte, 16)
	if _, err := v.Read(buf); err != nil {
		t.Fatalf("首帧读取: %v", err)
	}
	if err := v.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if st.closes != 1 || st.releases != 0 {
		t.Fatalf("releases=%d closes=%d，期望 0/1（非终态必须放弃）", st.releases, st.closes)
	}
}
