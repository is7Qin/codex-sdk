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
	// inboxInitialFrames 是 pumped 队列的初始容量：常见流一次分配即可装下，
	// 长流再按需增长（帧数闸仍是硬上界）。
	inboxInitialFrames = 16
)

// ErrWSBackpressure 表示入站队列饱和：下游消费慢于上游产出。
//
// 它是**传输层错误**而不是协议终态：调用方据此判失败/重试，不能当正常 EOF
// （否则截断会被读成"响应正常结束"，成功率统计失真）。触发即终止连接。
var ErrWSBackpressure = errors.New("codexsdk: ws 入站队列饱和（消费者过慢）")

// ErrKeepaliveTimeout 是**已退役**的主动心跳探测错误。
//
// 主动探测已从连接内核删除（对齐真实 codex 客户端：它只在收到 Ping 时回 Pong，
// 从不主动周期 Ping；空闲 EOF/Close 由常驻读者观测，复用前由 releasable 复查）。
// 保留这个变量只为不破坏调用方 `errors.Is(err, ErrKeepaliveTimeout)` 的编译面：
// **它不再有任何产生点**，该判定恒为 false。
//
// Deprecated: 不再产生；判活改用读写错误 / 调用方 ctx / releasable 复查。
var ErrKeepaliveTimeout = errors.New("codexsdk: ws 心跳探测超预算")

// ErrWSIdleTimeout 是 WS 读空闲超时：一次取帧等待的预算内没有收到任何完整数据
// 消息（Text/Binary 帧）。
//
// 它是**传输层错误**，不是协议终态：触发即经所有权裁决退役该连接（不归还缓存、
// 不可复用）、**不重试**（不重拨、不重发 create）。计时单位是"一次消费侧取帧等
// 待"（pumped：一次 `Next` 内的阻塞等待；pull：一次 `Recv`），不是"底层 socket
// 多久没字节"——队里已有帧时立即交付、不消耗预算。
//
// 错误链口径：**不**包装 context.DeadlineExceeded（调用方按 DeadlineExceeded 判
// 续接 ACK 窗口的地方不能误命中），也**不**实现 net.Error / Timeout()。
var ErrWSIdleTimeout = errors.New("codexsdk: ws 读空闲超时（预算内未收到任何帧）")

// 内核内部错误。
var (
	// errConnClosing 关闭进行中拒绝写/公开 Ping。
	errConnClosing = errors.New("codexsdk: ws 连接正在关闭")
	// errIdleWait 是 frameInbox.pop 的包内哨兵：本次等待已用尽预算。
	// **只表示"超了"**，不在这里退役连接——裁决在 wsExchange.idleExpire
	// （beginFinish + 租约 CAS）。不外泄给调用方。
	errIdleWait = errors.New("codexsdk: ws 读等待超时")
	// errGracefulCloseUnsupported pumped 模式调 closeGracefully：属实现错误。
	errGracefulCloseUnsupported = errors.New("codexsdk: ws 传输臂不支持关闭握手")
	// errIdleFrame 已归还、未被取用的连接收到帧：上游违约（空闲期不该有帧）。
	// push 返回它 ⇒ readLoop 既有分支 terminate(perr)。
	//
	// 这是与真实 codex 客户端**唯一有意的偏差**（理由见 frameInbox.fence 的注释）：
	// 网关从 response.completed 嗅探 usage 计费，把上一轮的终态记到新请求上是账目
	// 错误；客户端不承担计费职责，所以它能容忍空闲期残帧。
	errIdleFrame = errors.New("codexsdk: ws 空闲连接收到帧")
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
//
// 没有心跳间隔/探测预算字段：主动探测已删除（client.go 的 WithPingInterval 因此
// 只保留 API 兼容面，见其注释）。
type connConfig struct {
	mode        connMode
	readLimit   int64 // 单帧上限；-1 = 不限
	queueFrames int   // pumped 帧数闸
	queueBytes  int   // pumped 字节闸
	// idleTimeout 是一次"消费侧取帧等待"的预算（pumped：pop 真正阻塞时起算；
	// pull：一次 Recv）。<=0 = 关闭（显式 opt-out）。它**不**给 socket 读点任
	// 何期限：readLoop / readFrame 与它无关。
	idleTimeout time.Duration
}

// conn 是"一条连接 = 一个 socket 读点 + 一个写闸 + 一个终止入口"的内核。
//
// 读点唯一（readFrame 是全仓唯一 ws.Read 调用点）；写与公开 Ping 共用 gate；
// 终止与结果发布只在 finish 里发生一次。
type conn struct {
	ws    *coderws.Conn
	cfg   connConfig
	inbox *frameInbox // 仅 pumped

	ctx  context.Context // 连接寿命 ctx（恒为 Background + 自身 cancel，与请求解耦）
	stop context.CancelFunc

	closeDone    chan struct{} // 终止/关闭完成屏障（由 finish 唯一关闭）
	pendingCause *error        // 握手在飞时登记的终止原因（mu 保护）
	closeErr     error         // 关闭握手结果（mu 保护）

	gate chan struct{} // cap=1 信号量：写与公开 Ping 的唯一准入

	// lease 是这条连接的归属租约（仅 pumped，复用开启时才有意义）：
	// 谁 CAS 成功谁拥有收尾权——归还要 CAS 成功才 put，取消回调要 CAS 成功才
	// terminate。在表里的连接 lease == nil，下一次取出时再置为新 exchange。
	// 用原子指针而不是布尔门：布尔门"先查后做"在查询与动作之间有抢占窗口。
	lease atomic.Pointer[wsExchange]

	mu    sync.Mutex
	state connState

	cause atomic.Pointer[error]
}

// newConn 构造连接内核并按模式启动常驻 goroutine（仅 pump；没有心跳）。
//
// ctx 与 stop 是连接寿命：与请求 ctx 解耦（Background + 自身 cancel），由 dialConn
// 构造。所有通道在此初始化，零值不可用。
func newConn(ws *coderws.Conn, cfg connConfig, ctx context.Context, stop context.CancelFunc) *conn {
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
		closeDone: make(chan struct{}),
		gate:      make(chan struct{}, 1),
	}
	if cfg.mode == modePumped {
		c.inbox = newFrameInbox(cfg.queueFrames, cfg.queueBytes)
		go c.readLoop()
	}
	return c
}

// readFrame 是全仓唯一的 socket 读点。pumped 的 pump 与 pull 的 Recv 都经它。
func (c *conn) readFrame(ctx context.Context) ([]byte, error) {
	_, data, err := c.ws.Read(ctx)
	return data, err
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

// closeGracefully 发起关闭握手（仅 pull）：**不动读 ctx**
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

	// 这里**不动**连接寿命 ctx（也不动任何读路径）：关闭握手要先写关闭帧，
	// 一旦取消读，coder 会直接关掉 socket，帧永远发不出去。已无心跳要停。
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

// releasable 报告这条连接是否还能交回缓存：没有终止原因、而且不在关闭中。
// 它是归还判定的第一道闸（"终态 + 队列空"之外的另一半）。
func (c *conn) releasable() bool {
	return c.err() == nil && !c.isClosing()
}

// write 发送一帧（取门前/后各查一次终止原因）。
func (c *conn) write(ctx context.Context, wire []byte) error {
	return c.writeStamped(ctx, wire, -1)
}

// writeStamped 同 write，但在取得写闸之后、ws.Write 之前用当时毫秒原地覆写
// wire[off:off+13]（x-codex-ws-stream-request-start-ms 的数字；off<0 不覆写，
// 零分配）。在写闸上排队的时间因此不会算进"发送时刻"之前。
func (c *conn) writeStamped(ctx context.Context, wire []byte, off int) error {
	return c.writeGated(ctx, wire, off, nil)
}

// gatedWrite 是在写闸内、围绕 ws.Write 执行的登记（WS→WS 会话按线上顺序登记
// "本帧身份"）。两个方法都不得阻塞。
type gatedWrite interface {
	beforeWrite() // 取得写闸、确认连接未终止之后，ws.Write 之前
	writeFailed() // ws.Write 失败时（仍持写闸），撤销 beforeWrite 的登记
}

// writeGated 同 writeStamped；g 非 nil 时在写闸内、写帧之前调用 g.beforeWrite，
// 写失败时在写闸内调用 g.writeFailed。登记先于写出，上游再快的回帧也不会先于
// 登记到达；写闸串行化，登记顺序即帧在线上的顺序。
func (c *conn) writeGated(ctx context.Context, wire []byte, off int, g gatedWrite) error {
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
	if g != nil {
		g.beforeWrite()
	}
	if hook := writeStampHook.Load(); hook != nil {
		(*hook)()
	}
	stampStartMs(wire, off, time.Now())
	if err := c.ws.Write(ctx, coderws.MessageText, wire); err != nil {
		if g != nil {
			g.writeFailed()
		}
		return err
	}
	return nil
}

// writeStampHook 是测试挂点（生产恒为 nil）：取得写闸之后、覆写发送时刻之前调用。
// 原子指针：测试在连接存活期间设置/清除它，与写帧的 goroutine 无数据竞争。
var writeStampHook atomic.Pointer[func()]

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

// tryProbe / keepalive 已删除（对齐真实 codex 客户端）。
//
// 客户端证据：codex-api 的 Responses WS pump 只有"收到 Ping 就回 Pong"，没有
// ticker、没有 pong deadline；空闲 EOF/Close 由常驻读者观测，复用前由
// releasable 复查。SDK 因此**不做任何主动探测**——半开连接只能靠读写错误或调用方
// ctx 兜底（这是对齐客户端后接受的代价，不是遗漏）。
//
// 缓存里空闲条目的 TTL/maxAge 退役改由 connCache 自持的维护 ticker 负责（只清扫、
// 不探测）：连接不再知道缓存存在，更不会为它开定时器。

// readLoop 是 pumped 模式的常驻读者：除入队临界区外恒在 Read，因此对端 ping
// 与 pong 都被服务（没有任何"消费者慢 ⇒ 心跳误判"的截断源——已无心跳）。
// 没有 closing 分支——pumped 从不优雅关闭。
func (c *conn) readLoop() {
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

	// fenced 表示"这条连接已归还、还没被取用"：此期收到的任何帧都不属于下一个
	// 请求（fence 的注释里有保留它的理由）。它和 push 同锁 ⇒ "查空 + 置栏"与
	// "入队"天然互斥。
	fenced bool
	// poisoned 表示空闲期收到过帧：一旦置位，这条连接永不归还（也不解栏）。
	poisoned bool

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
// 已置栏（连接已归还、未被取用）⇒ 置 poisoned 并返回 errIdleFrame。
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
	if q.fenced {
		q.poisoned = true
		q.mu.Unlock()
		return errIdleFrame
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

// fence 置栏：只有队列为空时才能成功（"读到终态 + 队列空"是归还前提）。
// 与 push 同锁 ⇒ 置栏之后到达的帧一定走 poisoned 分支，不会被静默缓存。
//
// **与真实 codex 客户端的唯一有意偏差（保留决定，勿删）**：
// 客户端没有 fence/poison：它靠"请求独占 + 只在正常完成后归还 + 错误即退役"，
// 空闲期若真来一帧，下一次消费者会把它当普通事件读掉（R:818 起的普通循环）。
// 我们**不能**照抄这一点，因为**网关从 response.completed 嗅探 usage 计费**：
// 把上一轮的终态记到新请求头上是账目错误，而客户端不承担这个职责。
// 所以空闲期收到帧一律终止该连接、不交给下一个请求——这是**复用准入的保守**，
// 不是协议判定：它不看帧类型、不看 response_id、也**不**假设"终态之后不会再有帧"。
//
// 明确不做（客户端不这么做，加了就是自造失败面）：
//   - 不做首帧类型白名单 / created 门槛（admit / errConnDesynced 已删除）；
//   - 不做 response_id 归属过滤；
//   - 不做归还后静默窗（任何时间窗都不能证明后面没有帧）；
//   - 不做写后自动重试（response.create 可能已被上游执行 ⇒ 重复计费）。
func (q *frameInbox) fence() bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed || q.fenced || q.head < len(q.frames) {
		return false
	}
	q.fenced = true
	return true
}

// unfence 解栏：取用已归还的连接时调用。空闲期收到过帧（poisoned）或已关闭
// ⇒ 失败，调用方必须丢弃这条连接（那些帧属于上一个请求，不能当本请求的事件）。
//
// 顺序契约：解栏必须在写 create 帧**之前**。反过来的话，一个响应极快的上游
// 会在我们写完的那几微秒内回帧，撞上 fenced 被判废（误杀合法响应）。
func (q *frameInbox) unfence() bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	if !q.fenced || q.poisoned || q.closed {
		return false
	}
	q.fenced = false
	return true
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
//  4. 否则等 notify / ctx.Done()；budget>0 时再加一个"本次等待"的期限。
//
// budget 是**消费侧一次等待**的预算（<=0 = 不设限，既有行为）：deadline 只在
// "队列已空、真的要阻塞"时初始化 ⇒ 调用方慢、队列有货、终态排队都不消耗预算。
// 到点只返回包内哨兵 errIdleWait，**不在这里退役连接**（裁决在 idleExpire）。
// t 是调用方复用的一次性计时器（每 exchange 至多 1 个；budget<=0 时可为 nil）。
//
// 绝不给 socket 读点加期限：pump 的 Read 与本函数无关，"socket 静默"不等于空闲。
func (q *frameInbox) pop(ctx context.Context, budget time.Duration, t *time.Timer) ([]byte, error) {
	var deadline time.Time // 只在真正开始阻塞时初始化 ⇒ 起点 = 开始等帧
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
		if budget <= 0 { // ④ 无预算：既有行为
			select {
			case <-q.notify:
			case <-ctx.Done():
				return nil, ctx.Err()
			}
			continue
		}
		if deadline.IsZero() {
			deadline = time.Now().Add(budget)
		}
		wait := time.Until(deadline)
		if wait <= 0 { // ⑤ 只报"超了"，退役裁决不在这里
			return nil, errIdleWait
		}
		resetTimer(t, wait)
		select {
		case <-q.notify: // 有帧/关闭：回到循环重判
		case <-t.C: // 到点：回到循环（可能刚好来帧）
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

// resetTimer 复用一个已经 Stop 过（或已触发）的计时器。Go ≥1.23 下 Stop 后不会
// 残留陈旧值，但保守写法多排空一次也无害；计时器只在冷路径（真正阻塞时）被重置。
func resetTimer(t *time.Timer, d time.Duration) {
	if !t.Stop() {
		select {
		case <-t.C:
		default:
		}
	}
	t.Reset(d)
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
