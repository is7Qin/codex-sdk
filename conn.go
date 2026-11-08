package codexsdk

import (
	"context"
	"errors"
	"io"
	"sync"
	"sync/atomic"
	"time"

	coderws "github.com/coder/websocket"
)

// 连接内核的固定参数。
const (
	// defaultQueueFrames 是 pumped 队列的帧数闸：满即 ErrWSBackpressure
	// （读者不在入队上停留）。
	defaultQueueFrames = 256
	// defaultQueueBytes 是 pumped 队列的字节闸：队列非空且加入本帧超预算即
	// ErrWSBackpressure；空队列允许单帧超预算（合法巨帧不能因为预算小就被判死）。
	defaultQueueBytes = 8 << 20
	// defaultPongTimeout 是心跳探测预算。coder 的 Ping 用同一个 ctx 完成
	// "写控制帧（可能等底层写锁）"与"等 pong"两段，所以它约束的是整次探测。
	defaultPongTimeout = 2 * time.Second
	// inboxInitialFrames 是 pumped 队列的初始容量：常见流一次分配即可装下，
	// 长流再按需增长（帧数闸仍是硬上界）。
	inboxInitialFrames = 16
)

// ErrWSBackpressure 表示入站队列饱和：下游消费慢于上游产出。
//
// 它是**传输层错误**而不是协议终态：调用方据此判失败/重试，不能当正常 EOF
// （否则截断会被读成"响应正常结束"，成功率统计失真）。触发即终止连接。
var ErrWSBackpressure = errors.New("codexsdk: ws 入站队列饱和（消费者过慢）")

// ErrKeepaliveTimeout 表示心跳探测没能在预算内完成（写已发出、等不到 pong）。
// 仅 pumped 模式（传输臂）产生；可 errors.Is 判定。
var ErrKeepaliveTimeout = errors.New("codexsdk: ws 心跳探测超预算")

// 内核内部错误。
var (
	// errConnClosing 关闭进行中拒绝写/探测。
	errConnClosing = errors.New("codexsdk: ws 连接正在关闭")
	// errProbeSkipped 心跳抢不到写闸（有写在飞 = 活性已证），本轮跳过。
	errProbeSkipped = errors.New("codexsdk: ws 心跳探测跳过")
	// errGracefulCloseUnsupported pumped 模式调 closeGracefully：属实现错误。
	errGracefulCloseUnsupported = errors.New("codexsdk: ws 传输臂不支持关闭握手")
)

// connMode 是连接模式，构造期固定、互斥。
//
// 为什么要两种模式：coder/websocket 的 Read(ctx) 被取消时会**关闭底层 socket**，
// 因此"先取消读、再做关闭握手"不可能（关闭帧永远发不出去）。pull 模式保留
// 公开 Dial → Client 的今日语义（调用方常驻 Recv 服务 pong + 关闭握手）；
// pumped 模式由常驻读者服务 pong、收尾一律强断。
type connMode uint8

const (
	// modePull 公开 Dial → Client：Recv 直读，无 pump、无队列，Close 走关闭握手。
	modePull connMode = iota
	// modePumped 传输臂（wsExchange）：常驻读 + 有界队列，只有 terminate。
	modePumped
)

// connState 是连接状态机。open → closing（仅 pull 的关闭握手）→ terminated。
type connState uint8

const (
	connOpen connState = iota
	connClosing
	connTerminated
)

// connConfig 是连接内核的配置（值语义，构造后不变）。
type connConfig struct {
	mode         connMode
	readLimit    int64         // 单帧上限；-1 = 不限
	pingInterval time.Duration // 心跳间隔；≤0 关闭心跳
	pongTimeout  time.Duration // 探测预算（固定 2s，无 Option）
	queueFrames  int           // pumped 帧数闸
	queueBytes   int           // pumped 字节闸
}

// conn 是"一条连接 = 一个 socket 读点 + 一个写闸 + 一个终止入口"的内核。
//
// 读点唯一（readFrame 是全仓唯一 ws.Read 调用点）；写与探测共用 gate；
// 终止与结果发布只在 finish 里发生一次。
type conn struct {
	ws    *coderws.Conn
	cfg   connConfig
	inbox *frameInbox // 仅 pumped

	ctx  context.Context // 连接寿命 ctx（pumped = 请求 ctx；pull = Background）
	stop context.CancelFunc

	pingOnce sync.Once
	pingStop chan struct{} // 停心跳；只能经 stopPings 关闭一次

	closeDone    chan struct{} // 终止/关闭完成屏障（由 finish 唯一关闭）
	pendingCause *error        // 握手在飞时登记的终止原因（mu 保护）
	closeErr     error         // 关闭握手结果（mu 保护）

	gate chan struct{} // cap=1 信号量：写与探测的唯一准入

	mu    sync.Mutex
	state connState

	cause    atomic.Pointer[error]
	readDone chan struct{} // pumped：pump 退出信号
}

// newConn 构造连接内核并按模式启动常驻 goroutine（心跳 / pump）。
//
// ctx 与 stop 是连接寿命：pumped 传请求 ctx（含 WithTimeout），pull 传
// Background + 自己的 cancel。所有通道在此初始化，零值不可用。
func newConn(ws *coderws.Conn, cfg connConfig, ctx context.Context, stop context.CancelFunc) *conn {
	if cfg.pongTimeout <= 0 {
		cfg.pongTimeout = defaultPongTimeout
	}
	if cfg.mode == modePumped {
		if cfg.queueFrames <= 0 {
			cfg.queueFrames = defaultQueueFrames
		}
		if cfg.queueBytes <= 0 {
			cfg.queueBytes = defaultQueueBytes
		}
	}
	c := &conn{
		ws:        ws,
		cfg:       cfg,
		ctx:       ctx,
		stop:      stop,
		pingStop:  make(chan struct{}),
		closeDone: make(chan struct{}),
		gate:      make(chan struct{}, 1),
	}
	if cfg.mode == modePumped {
		c.inbox = newFrameInbox(cfg.queueFrames, cfg.queueBytes)
		c.readDone = make(chan struct{})
		go c.readLoop()
	}
	if cfg.pingInterval > 0 {
		go c.keepalive()
	}
	return c
}

// readFrame 是全仓唯一的 socket 读点。pumped 的 pump 与 pull 的 Recv 都经它。
func (c *conn) readFrame(ctx context.Context) ([]byte, error) {
	_, data, err := c.ws.Read(ctx)
	return data, err
}

// stopPings 停心跳（幂等）。
func (c *conn) stopPings() {
	c.pingOnce.Do(func() { close(c.pingStop) })
}

// finish 是唯一的状态与结果发布点：裁决 + 首因选择 + 发布在同一个 mu 临界区内
// 完成（无 TOCTOU）。graceful=true 表示已做完关闭握手，不再 CloseNow。
func (c *conn) finish(cause, closeErr error, graceful bool) {
	if cause == nil {
		cause = io.EOF
	}
	c.mu.Lock()
	switch {
	case c.state == connTerminated: // 已发布：幂等返回
		c.mu.Unlock()
		return
	case !graceful && c.state == connClosing: // 收尾权归握手者：只登记首因，不发布
		if c.pendingCause == nil {
			p := cause
			c.pendingCause = &p
		}
		c.mu.Unlock()
		return
	}
	if c.pendingCause != nil { // 握手期间到达的原因优先（首因胜）
		cause = *c.pendingCause
	}
	c.closeErr = closeErr    // ① 结果先写
	c.state = connTerminated // ② 同一临界区内发布状态
	c.mu.Unlock()            // ← 只有本 goroutine 能完成这次迁移，后续动作天然只执行一次

	c.cause.Store(&cause)
	c.stopPings()
	c.stop()
	if c.inbox != nil {
		c.inbox.close(cause) // 登记 + 唤醒等待者
	}
	if !graceful {
		_ = c.ws.CloseNow()
	}
	close(c.closeDone) // ③ 最后放开所有等待者
}

// terminate 以 cause 终止连接（幂等；首个 cause 生效）。
func (c *conn) terminate(cause error) { c.finish(cause, nil, false) }

// closeGracefully 发起关闭握手（仅 pull）：只停心跳，**不动读 ctx**
// （动了 coder 就会关 socket，关闭帧永远发不出去）。
//
// 重复/并发 Close 等到同一结果：closing 等 closeDone 再读 closeErr；
// terminated 直接读 closeErr。时延上界 = coder 的写 5s + 等 5s。
func (c *conn) closeGracefully(status StatusCode, reason string) (err error) {
	if c.cfg.mode != modePull {
		return errGracefulCloseUnsupported
	}
	c.mu.Lock()
	switch {
	case c.state == connClosing: // 握手在飞：等它完成
		c.mu.Unlock()
		<-c.closeDone
		c.mu.Lock()
		err = c.closeErr
		c.mu.Unlock()
		return err
	case c.state == connTerminated: // 已完成（含 CloseNow 先到）：沿用既有结果
		err = c.closeErr
		c.mu.Unlock()
		return err
	}
	c.state = connClosing
	c.mu.Unlock()

	c.stopPings() // 只停心跳，不动读 ctx
	err = c.ws.Close(status, reason)
	c.finish(io.EOF, err, true) // 首因选择在 finish 的临界区内完成
	return err
}

// acquireGate 取写闸（可被 ctx 取消）。
func (c *conn) acquireGate(ctx context.Context) error {
	select {
	case c.gate <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// releaseGate 释放写闸。
func (c *conn) releaseGate() { <-c.gate }

// err 返回终止原因（未终止为 nil）。所有"连接还能否用"的判定都走它。
func (c *conn) err() error {
	if p := c.cause.Load(); p != nil {
		return *p
	}
	return nil
}

// isClosing 报告连接是否已不在 open 态（closing 或 terminated）。
func (c *conn) isClosing() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.state != connOpen
}

// write 发送一帧（取门前/后各查一次终止原因）。
func (c *conn) write(ctx context.Context, wire []byte) error {
	if cause := c.err(); cause != nil {
		return cause
	}
	if c.isClosing() {
		return errConnClosing
	}
	if err := c.acquireGate(ctx); err != nil {
		return err
	}
	defer c.releaseGate()
	if cause := c.err(); cause != nil {
		return cause
	}
	return c.ws.Write(ctx, coderws.MessageText, wire)
}

// probe 发送 WS 层 ping 并等待 pong：公开 Ping 用，完全沿用调用方 ctx
// （与既有 Client.Ping 语义一致，不加任何封顶）。
func (c *conn) probe(ctx context.Context) error {
	if cause := c.err(); cause != nil {
		return cause
	}
	if err := c.acquireGate(ctx); err != nil {
		return err
	}
	defer c.releaseGate()
	if cause := c.err(); cause != nil {
		return cause
	}
	return c.ws.Ping(ctx)
}

// tryProbe 是心跳用的非阻塞探测：抢不到写闸 = 有写在飞（活性已证），跳过本轮。
// 预算派生自 Background（固定 pongTimeout）——心跳不该被连接 ctx 的取消
// 二次污染。
func (c *conn) tryProbe() error {
	select {
	case c.gate <- struct{}{}:
	default:
		return errProbeSkipped
	}
	defer c.releaseGate()
	if cause := c.err(); cause != nil {
		return cause
	}
	pctx, cancel := context.WithTimeout(context.Background(), c.cfg.pongTimeout)
	defer cancel()
	return c.ws.Ping(pctx)
}

// keepalive 是唯一的心跳循环：pong 由"谁在读 socket"服务（pull = 调用方的
// 常驻 Recv，pumped = pump）。
func (c *conn) keepalive() {
	t := time.NewTicker(c.cfg.pingInterval)
	defer t.Stop()
	for {
		select {
		case <-c.pingStop:
			return
		case <-c.ctx.Done():
			return
		case <-t.C:
		}
		if c.isClosing() { // 关闭握手进行中：不得打断
			return
		}
		err := c.tryProbe()
		switch {
		case err == nil, errors.Is(err, errProbeSkipped):
			continue
		case c.err() != nil, c.isClosing(): // 已被其它路径终止/正在关闭：保留首因
			return
		case !errors.Is(err, context.DeadlineExceeded): // 写失败/连接错：不判活死
			continue
		default:
			c.terminate(ErrKeepaliveTimeout)
			return
		}
	}
}

// readLoop 是 pumped 模式的常驻读者：除入队临界区外恒在 Read，因此 pong 与
// 对端 ping 都被服务（没有"消费者慢 ⇒ 心跳误判"的截断源）。没有 closing 分支
// ——pumped 从不优雅关闭。
func (c *conn) readLoop() {
	defer close(c.readDone)
	for {
		data, err := c.readFrame(c.ctx)
		if err != nil {
			c.terminate(readCause(err, c.ctx)) // ctx 错误优先，绝不吞成 EOF
			return
		}
		if perr := c.inbox.push(data); perr != nil { // 满 ⇒ ErrWSBackpressure
			c.terminate(perr)
			return
		}
	}
}

// readCause 把 socket 读错翻译成终止原因：ctx 错误优先（不吞成 EOF），
// 任意 close frame（含 1000/1001/1005/1011）= io.EOF，其余原样。
func readCause(err error, ctx context.Context) error {
	if cerr := ctx.Err(); cerr != nil {
		return cerr
	}
	if coderws.CloseStatus(err) >= 0 {
		return io.EOF
	}
	return err
}

// isContextError 报告 err 是否由 ctx 取消/超时引起（errors.Is 链）。
func isContextError(err error) bool {
	return errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
}

// frameInbox 是 pumped 模式的有界入站队列：pump 非阻塞入队，消费者 pop。
//
// 双闸（帧数 / 字节）；空队列允许单帧超预算。内存上界 = queueBytes + 1 帧
// （空队列例外）+ pump 正在投递的 1 帧 + 消费侧持有的 1 帧；readLimit=-1 时
// 无总上界。
type frameInbox struct {
	frames [][]byte
	head   int
	bytes  int

	maxFrames int
	maxBytes  int

	cause  error
	closed bool

	notify chan struct{} // cap 1：有帧可交付或已关闭
	mu     sync.Mutex
}

// newFrameInbox 构造队列（notify 在此初始化，零值不可用）。
//
// frames 预留一个小容量：典型流是"几帧到几十帧"，从零开始 append 会连做几次
// 扩容（每次一个分配）；预留到 min(maxFrames, inboxInitialFrames) 后常见形态
// 只需一次分配，长流再按需增长（帧数闸仍是硬上界）。
func newFrameInbox(maxFrames, maxBytes int) *frameInbox {
	initial := maxFrames
	if initial > inboxInitialFrames {
		initial = inboxInitialFrames
	}
	if initial < 0 {
		initial = 0
	}
	return &frameInbox{
		frames:    make([][]byte, 0, initial),
		maxFrames: maxFrames,
		maxBytes:  maxBytes,
		notify:    make(chan struct{}, 1),
	}
}

// signal 唤醒一个等待中的 pop（cap 1，重复投递无害）。
func (q *frameInbox) signal() {
	select {
	case q.notify <- struct{}{}:
	default:
	}
}

// push 入队一帧：满 ⇒ ErrWSBackpressure（不阻塞、不丢帧、不无限缓冲）。
func (q *frameInbox) push(frame []byte) error {
	q.mu.Lock()
	if q.closed {
		cause := q.cause
		q.mu.Unlock()
		if cause == nil {
			cause = io.EOF
		}
		return cause
	}
	if len(q.frames)-q.head >= q.maxFrames {
		q.mu.Unlock()
		return ErrWSBackpressure
	}
	// 队列里还有货时才查字节预算：空队列允许任意大的单帧通过。
	if len(q.frames)-q.head > 0 && q.bytes+len(frame) > q.maxBytes {
		q.mu.Unlock()
		return ErrWSBackpressure
	}
	q.frames = append(q.frames, frame)
	q.bytes += len(frame)
	q.mu.Unlock()
	q.signal()
	return nil
}

// tryPop 非阻塞取一帧（有帧才交付）。
func (q *frameInbox) tryPop() ([]byte, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.head >= len(q.frames) {
		return nil, false
	}
	return q.takeLocked(), true
}

// pop 取下一帧，顺序是契约：
//  1. ctx 已结束 → 立即返回 ctx 错误（本次不 pop）；
//  2. 有帧 → 交付（不再查 ctx）；
//  3. 已关闭且空 → 返回终止 cause；
//  4. 否则等 notify / ctx.Done()。
func (q *frameInbox) pop(ctx context.Context) ([]byte, error) {
	for {
		if cerr := ctx.Err(); cerr != nil { // ①
			return nil, cerr
		}
		q.mu.Lock()
		if q.head < len(q.frames) { // ②
			frame := q.takeLocked()
			q.mu.Unlock()
			return frame, nil
		}
		if q.closed { // ③
			cause := q.cause
			q.mu.Unlock()
			if cause == nil {
				cause = io.EOF
			}
			return nil, cause
		}
		q.mu.Unlock()
		select { // ④
		case <-q.notify:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

// takeLocked 弹出队头并维护 head/bytes（调用方持锁）。队头推进到边界时复位，
// 或按需压缩，避免持续丢容量。
func (q *frameInbox) takeLocked() []byte {
	frame := q.frames[q.head]
	q.frames[q.head] = nil
	q.head++
	q.bytes -= len(frame)
	switch {
	case q.head == len(q.frames):
		q.frames = q.frames[:0]
		q.head = 0
	case q.head >= 64 && q.head*2 >= len(q.frames):
		n := copy(q.frames, q.frames[q.head:])
		for i := n; i < len(q.frames); i++ {
			q.frames[i] = nil
		}
		q.frames = q.frames[:n]
		q.head = 0
	}
	return frame
}

// close 登记终止 cause 并唤醒等待者（必须 signal：否则空队列上的 pop 永久挂起）。
func (q *frameInbox) close(cause error) {
	if cause == nil {
		cause = io.EOF
	}
	q.mu.Lock()
	if !q.closed {
		q.closed = true
		q.cause = cause
	}
	q.mu.Unlock()
	q.signal()
}
