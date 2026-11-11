package codexsdk

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"time"

	coderws "github.com/coder/websocket"
)

// frameStream 是"一次请求 = 一条入站帧流"的取帧入口。sseView 只认这个接口，
// 具体实现是 wsExchange（连接内核之上的一层生命周期）。
type frameStream interface {
	// Next 交付下一帧；流结束后返回 EOF 或终止原因。
	Next(ctx context.Context) ([]byte, error)
	// MarkFailed 声明"本轮以**失败终态**结束"（response.failed /
	// response.incomplete）：此后 Release **不再归还**连接，改为终止它。
	//
	// 为什么要这个显式标记：传输层不做帧类型判定（那已删掉），但"以错误结束的
	// 交换不把 socket 放回"是真实客户端的明确语义（错误即退役）。因此判定发生在
	// **已经解析事件的地方**（聚合器 / SSE 视图），由它们调本方法。
	//
	// 契约（给纯字节透传的调用方，例如网关 relay）：**看到失败终态必须先调
	// MarkFailed()（或直接 Close()）再收尾**，否则连接会被当成干净结束而复用。
	// 内置的 sseView 与 responsesStreamWS 已在看到 failed/incomplete 时自动标记。
	//
	// 另一条终止路径**不需要**本方法：带 4xx/5xx status 的 error 帧由上面两层直接
	// 以 *HTTPError 返回（调用方随即 Close），这条路径根本不走到 Release。
	MarkFailed()
	// Release 读到终态后尝试把连接交回缓存（T5 起）。true = 已交回，
	// false = 未交回（连接已终止或将被终止）。没有缓存时恒 false。
	// 本轮已 MarkFailed 的连接**永不**交回（返回 false 并终止）。
	Release() bool
	// Close 放弃本次请求（幂等；Release 之后为 no-op）。
	// 看到失败终态时调它同样安全：连接一律不以可复用状态返回。
	Close() error
}

// connSink 是"可复用连接的归处"。T5.1 里恒为 nil（复用关闭）；T5.2 由 connCache
// 实现。用接口而不是具体类型，是为了让内核不必知道缓存长什么样——归还的唯一
// 语义是"收下了没有"。
type connSink interface {
	put(*wsExchange) bool
}

// connKey 是一条可复用连接的身份。
//
// 身份 = (threadKey, readLimit)，不含 turn-state、也不含 window：真实 codex 的
// WS 连接是 thread 级、跨轮跨窗口缓存复用；ts 与 window 都是请求级数据。
// threadKey == "" 是匿名，不参与复用（openWSExchange 不取不存）。
//
// readLimit 必须进键：两条取数路径的单帧内存上界不同（64MiB / 16MiB），按路径
// 拨号时就把上限固定了，共享连接会把聚合臂的单帧上界放大 4 倍。
type connKey struct {
	threadKey string
	readLimit int64
}

// wsExchange 是一次 Responses WS 请求的生命周期：一条连接 + 取帧入口。conn
// 不知道"请求"，exchange 不知道"socket"。
//
// 收尾契约（Release/Close 先到者生效）：
//
//	1 stopWatch()（注销取消回调）→ 2 cancel()（唤醒阻塞的取帧）→
//	3 lease CAS 裁决：成功者才拥有收尾权（归还或终止），失败者说明取消回调已接手。
//	Release 在第 3 步之前还要拿 nextMu：先把在飞的取帧排干，再移交。
//
// stopWatch() 的返回值故意忽略：它只能"尽力"注销，真正互斥的是第 3 步的 CAS。
type wsExchange struct {
	cn     *conn
	sink   connSink // nil = 不复用（缓存关闭或匿名）
	key    connKey
	token  string    // 本次升级头使用的 Authorization（401 轮转后是新的一份）
	bornAt time.Time // 拨号起始时刻（maxAge 起点，命中不重置）

	ctx       context.Context
	cancel    context.CancelFunc
	stopWatch func() bool

	// reused 只用于诊断：这条连接取自缓存。它**不参与任何协议判定**（客户端没有
	// 首帧白名单，admit 已删除）；留着是为了写失败时能区分"新拨"与"复用已死连接"。
	reused bool
	// delivered 是本轮已交付给消费者的帧数：仅诊断用（复用连接写失败时要回答
	// "上游是否已经回过任何帧"）。Next 里更新、错误包装里跨路径读取，因此原子。
	delivered atomic.Int64

	// nextMu 串行化"取帧"与"移交"：Next 全程持它，Release 在 cancel 之后等它。
	// 没有它的话，一个在飞的 Next 会（a）拿到旧 ctx 的取消后 terminate 掉刚
	// 交出去的连接，（b）用 tryPop 抢走下一个请求的首帧。
	// 先 cancel 再等锁：否则阻塞在 pop 里的 Next 永远不会退出。
	nextMu sync.Mutex

	// idle 是一次"等帧"的预算（<=0 = 关闭）与 idleTimer 是它复用的计时器：
	// 懒建，每个 exchange 至多 1 个。计时器**不跨等待存活**（deadline 是 pop 的
	// 局部变量），所以连接在缓存里、或消费者没在等帧时都没有计时器在跑。
	idle      time.Duration
	idleTimer *time.Timer

	// closed 是 Release/Close 的先到者仲裁（CAS）：抢到的一方才做收尾。
	closed atomic.Bool
	// failed 标记"本轮以失败终态结束"（由已解析事件的那一层调 MarkFailed）。
	// 它是**归还准入**的一部分，不是协议判定：传输层从不自己置位。
	failed atomic.Bool
}

// beginFinish 抢本次 exchange 的收尾权。true = 抢到（调用方必须自己收尾）。
func (e *wsExchange) beginFinish() bool {
	return e.closed.CompareAndSwap(false, true)
}

// teardown 执行 1→2→3 收尾序并终止连接（Release 之外的放弃路径）。
func (e *wsExchange) teardown(cause error) {
	if !e.beginFinish() {
		return
	}
	if e.stopWatch != nil {
		e.stopWatch()
	}
	e.cancel()
	if e.cn.lease.CompareAndSwap(e, nil) {
		e.cn.terminate(cause)
	}
}

// openWSExchange 建立一次请求级 WS exchange。
//
// 创建次序是契约（写死）：取请求身份快照 → **编码**（本地校验在此，坏请求体
// 不烧握手）→ 拨号 → 把本次签发值交给 Sink → 发一帧。
//
// T5 起 exchangeCtx 承担三件事：拨号预算、取帧预算、取消守卫。connCtx 已经
// 与请求解耦（Background），所以请求结束不再杀 socket；但"调用方取消 ⇒ 本次
// 请求的连接必须终止"仍由 AfterFunc + 租约 CAS 保证。
func (c *HTTPClient) openWSExchange(ctx context.Context, wire []byte, id Identity, readLimit int64) (*wsExchange, error) {
	exchangeCtx, cancel := exchangeContext(ctx, c.opts.timeout)

	turn := uint64(0)
	if c.opts.turnProvider != nil {
		turn = 1 // 一次 exchange 只发一帧 ⇒ 恒为第 1 轮
	}
	frame, err := c.codec.encode(wire, encodeWSResponses, frameInput{
		Identity: id,
		Turn:     turn,
		Now:      time.Now(),
	})
	if err != nil {
		cancel()
		return nil, err
	}
	threadKey := id.threadKey()
	key := connKey{threadKey: threadKey, readLimit: readLimit}
	// sink 是归还口：**必须**在 nil 时保持真正的 nil 接口（typed-nil 陷阱：
	// 把 (*connCache)(nil) 装进接口后 e.sink == nil 为假，Release 会调进去）。
	// 匿名（threadKey == ""）不复用：不取缓存、读完即终止（不 put）。
	var sink connSink
	if c.wsCache != nil && threadKey != "" {
		sink = c.wsCache
	}

	// 取用：只有开启缓存且非匿名才可能命中。token 必须在锁外取（OAuth 可能
	// 取锁/刷新），报错就直接失败——不拿旧连接硬上。
	if cache := c.wsCache; cache != nil && threadKey != "" {
		if c.auth == nil { // 与 dialConn 同一道保护：缓存分支不能先崩在解引用上
			cancel()
			return nil, errors.New("codexsdk: auth 不能为 nil（用 PAT 或 OAuth）")
		}
		token, terr := c.auth.Authorization(exchangeCtx)
		if terr != nil {
			cancel()
			return nil, fmt.Errorf("codexsdk: 获取鉴权信息失败: %w", terr)
		}
		if cc, ok := cache.get(key, token); ok {
			e := c.activate(exchangeCtx, cancel, cc.cn, cc.key,
				handshake{token: cc.token, bornAt: cc.bornAt}, true, sink)
			// 复查 + 解栏。顺序是契约：解栏在写 create 帧**之前**——反过来的话，
			// 一个响应极快的上游会在我们写完的几微秒内回帧，撞上 fenced 被判废。
			if cc.cn.releasable() && cc.cn.inbox.unfence() {
				cache.hits.Add(1)
				if werr := cc.cn.write(exchangeCtx, frame); werr != nil {
					e.teardown(werr) // 写失败不重试、不重拨（create 可能已进 socket）
					return nil, e.writeFailure(werr)
				}
				return e, nil
			}
			// 取用窗口失败（空闲期来过帧 / 连接已不可用）：帧尚未发出，重拨安全。
			// 这条"扫到死连接"的路径不产生错误（本次请求会成功），可观测性靠
			// cache.misses 计数；真正需要定因的是下面的写失败，它带复用诊断。
			e.stopWatch()
			cc.cn.terminate(io.EOF)
			cache.misses.Add(1)
		} else {
			cache.misses.Add(1)
		}
	}

	cn, hs, derr := dialConn(exchangeCtx, dialSpec{
		auth:      c.auth,
		opts:      c.opts,
		readLimit: readLimit,
		mode:      modePumped,
		headers:   c.opts.headers,
		identity:  id,
	})
	if derr != nil {
		cancel()
		return nil, wsDialError(derr)
	}
	// 本次握手签发值只走 Sink（HTTP 臂读 resp.Header；WS 面只有这一个观测点）。
	// 帧已按 Identity 编码，回写不会污染本轮帧。命中路径没有握手，也不回写。
	id.deliverTurnState(hs.turnState)
	e := c.activate(exchangeCtx, cancel, cn, key, hs, false, sink)
	if werr := cn.write(exchangeCtx, frame); werr != nil {
		e.teardown(werr) // 写失败不重试、不重拨（response.create 可能已进 socket）
		return nil, werr
	}
	return e, nil
}

// activate 把一条连接交给新的 exchange：先落租约，再装取消守卫。
//
// 次序是契约：lease 必须先 Store 再注册 AfterFunc。若注册时 ctx 已经取消，回调会
// 立刻在新 goroutine 里跑；此时 lease 还没落地的话 CAS 必失败 ⇒ 没人终止这条连接，
// 而 write 只查 err()/isClosing()，会把 response.create 照常写出去。
func (c *HTTPClient) activate(ctx context.Context, cancel context.CancelFunc, cn *conn, key connKey, hs handshake, reused bool, sink connSink) *wsExchange {
	e := &wsExchange{
		cn:     cn,
		sink:   sink,
		key:    key,
		token:  hs.token,
		bornAt: hs.bornAt,
		ctx:    ctx,
		cancel: cancel,
		reused: reused,
		idle:   c.opts.wsIdleTimeout, // 每条连接一致；<=0 = 关闭
	}
	cn.lease.Store(e)
	e.stopWatch = context.AfterFunc(ctx, func() {
		if cn.lease.CompareAndSwap(e, nil) {
			cn.terminate(ctx.Err())
		}
	})
	return e
}

// exchangeContext 派生一次 exchange 的 ctx：timeout>0 用它封顶（含拨号/升级），
// 否则也包一层 WithCancel——cancel 恒非 nil 是收尾序第 2 步的前提。
func exchangeContext(ctx context.Context, timeout time.Duration) (context.Context, context.CancelFunc) {
	if timeout > 0 {
		return context.WithTimeout(ctx, timeout)
	}
	return context.WithCancel(ctx)
}

// terminateOwned 只在这条连接仍属于本 exchange 时才终止它。
//
// 为什么不能直接 terminate：Next 可能在"取帧中"被挂起，期间 Release 已经把连接
// 交回缓存（甚至已被下一个请求取走）；此时它再 terminate 就会杀掉一条别人的连接。
// 租约 CAS 是唯一裁决：CAS 失败说明我早已不是主人。
func (e *wsExchange) terminateOwned(cause error) {
	if e.cn.lease.CompareAndSwap(e, nil) {
		e.cn.terminate(cause)
	}
}

// writeFailure 给"写 create 帧失败"补上复用诊断：**只改错误文案，不改分类**
// （%w 保留原错误链，errors.Is/As 全部照旧），也不触发任何重试。
//
// 为什么需要：线上必须能把两种失败分开——
//   - 写前就死（write 入口看到 readLoop 早已登记的 io.EOF）：连接空闲期已经被
//     上游关掉，create 一定没发出去；
//   - 写后才 EOF（ws.Write 自己返回错误）：create 可能已进 socket、上游可能已
//     开始执行——这正是"绝不能自动重试"的情形（create 非幂等 ⇒ 重复计费）。
//
// 连接年龄（= 拨号到现在的时长，命中不重置）+ 本轮已交付帧数就够区分：新拨的
// 连接年龄≈0、交付数恒 0，对它们不加这段文案（只是噪音，且会动新拨路径的错误面）。
func (e *wsExchange) writeFailure(werr error) error {
	if !e.reused {
		return werr
	}
	return fmt.Errorf("codexsdk: ws 复用连接写入失败（连接年龄 %s，本轮已交付 %d 帧）: %w",
		time.Since(e.bornAt).Truncate(time.Millisecond), e.delivered.Load(), werr)
}

// Next 交付下一帧。
//
// 帧**不做任何类型判定**：首帧就是普通帧，不认识的事件由消费者（聚合器 / relay）
// 自己忽略。真实 codex 客户端一模一样——它没有 response_id 过滤、没有 created
// 门槛、没有首帧白名单，只靠"干净归还的连接 + 只认自己那一轮的语义"。
//
//   - 入口 ctx 已结束：先尝试交付**已入队**的帧（上游已完成的结果不能被消费侧
//     超时改写），队列空才把取消当作本次请求的放弃；
//   - 阻塞等待期间被取消：同样先 `tryPop` 一次再认输——否则"帧已在队、随后
//     deadline 到点"这个次序（消费者已卡在 pop 里）会把已完成的终态帧丢掉，
//     把上游成功的请求报成 DeadlineExceeded；
//   - 有帧立即返回（取帧即提交：已取出的帧不再受 ctx 影响）；
//   - 一次等待超过 idle 预算且队列仍空：帧优先 → ctx 优先 → 经所有权裁决退役
//     （见 nextOnIdle / idleExpire），返回 ErrWSIdleTimeout；
//   - 流结束：close frame 归一为 io.EOF，其它终止原因原样返回。
func (e *wsExchange) Next(ctx context.Context) ([]byte, error) {
	e.nextMu.Lock()
	defer e.nextMu.Unlock()
	if e.closed.Load() { // 已归还/已放弃：不能再碰这条连接（它可能已经在缓存里）
		return nil, io.EOF
	}
	if ctx.Err() != nil {
		if data, ok := e.cn.inbox.tryPop(); ok {
			e.delivered.Add(1)
			return data, nil
		}
		e.terminateOwned(ctx.Err())
		return nil, ctx.Err()
	}
	data, err := e.cn.inbox.pop(ctx, e.idle, e.ensureIdleTimer())
	if err == nil {
		e.delivered.Add(1)
		return data, nil
	}
	if errors.Is(err, errIdleWait) {
		return e.nextOnIdle(ctx)
	}
	if cerr := ctx.Err(); cerr != nil {
		if data, ok := e.cn.inbox.tryPop(); ok { // 取消与入队并发：已入队的帧优先
			e.delivered.Add(1)
			return data, nil
		}
		e.terminateOwned(cerr) // 队列确实空：请求已放弃
		return nil, cerr
	}
	if coderws.CloseStatus(err) >= 0 {
		return nil, io.EOF
	}
	return nil, err
}

// MarkFailed 标记本轮以失败终态结束（response.failed / response.incomplete）。
// 幂等；一旦置位，Release 必定拒绝归还并终止连接。
func (e *wsExchange) MarkFailed() { e.failed.Store(true) }

// ensureIdleTimer 懒建"等帧预算"复用的计时器（每个 exchange 至多 1 个）。
//
// 只在真正要阻塞等待时才创建：队列已有帧的路径（热路径）不碰它，因此没有每帧
// 分配；同一 exchange 后续的等待全部复用它（resetTimer），不做 time.After。
// idle <= 0（显式关闭）时返回 nil——pop 在该形态下不读计时器。
func (e *wsExchange) ensureIdleTimer() *time.Timer {
	if e.idle <= 0 {
		return nil
	}
	if e.idleTimer == nil {
		e.idleTimer = time.NewTimer(e.idle)
		if !e.idleTimer.Stop() {
			<-e.idleTimer.C
		}
	}
	return e.idleTimer
}

// nextOnIdle 是"一次等待用尽预算"之后的裁决，次序写死（三支互斥）：
//
//  1. **帧优先**：竞态窗口里刚入队的帧必须交付——永不把"已到帧"读成超时
//     （与 Next 里"取消与入队并发时帧优先"同一条既有语义）；
//  2. **调用方取消优先**：预算与 ctx 同刻就绪时按既有次序报 ctx 错误
//     （与 Next:296-316 一致），不把调用方的放弃洗成 idle；
//  3. 队列空 + 未关闭 + ctx 未结束（errIdleWait 只能在这个状态下产生）⇒ 提交退役：
//     beginFinish + 租约 CAS 裁决，绝不直接 terminate。
//
// 抽成独立函数是为了让"帧优先于超时"能被白盒用例**无竞态**地驱动（P2d）。
func (e *wsExchange) nextOnIdle(ctx context.Context) ([]byte, error) {
	if data, ok := e.cn.inbox.tryPop(); ok { // ①
		e.delivered.Add(1)
		return data, nil
	}
	if cerr := ctx.Err(); cerr != nil { // ②
		e.terminateOwned(cerr)
		return nil, cerr
	}
	return nil, e.idleExpire() // ③
}

// idleCommitHook 是超时提交的**测试挂点**（生产恒为 nil）。
//
// 位置唯一：`beginFinish` 成功之后、`lease.CompareAndSwap` 之前。放进这个窗口
// 才能被确定性交错压到 CAS 那一行（放在 beginFinish 之前的话，Release 会先赢
// beginFinish，通道变成"Release 先"的另一条分支）。测试同步调用：允许在 hook 里
// 阻塞（channel）或改状态（换租约），返回后实现继续执行 CAS。
//
// 重入契约：同一次 idleExpire 至多调用一次。
var idleCommitHook func(e *wsExchange)

// idleExpire 提交"本次等待超预算"的退役，返回给 Next 的错误。
//
// 与 Release/teardown 共用**同一原子裁决点**：beginFinish（closed CAS）决定谁
// 有收尾权，lease.CompareAndSwap 决定连接还属不属于我。少了任何一步，
// "读到超时"就会被当成"终止权"——那会杀掉已归还/已被新请求取走的连接。
//
// 收尾顺序与 teardown 有意不同：**先停取消守卫（AfterFunc），再抢租约、再
// cancel**。否则 AfterFunc 会在 cancel 当下抢走 CAS，把终止原因写成
// context.Canceled，把"空闲超时"洗成"调用方取消"（finish 首因胜）。
func (e *wsExchange) idleExpire() error {
	if !e.beginFinish() { // Release/Close 已抢到收尾权 ⇒ 我无权处置
		return io.EOF // 已交接：按既有"已关闭"语义返回（Next 的 e.closed 分支同义）
	}
	if e.stopWatch != nil {
		e.stopWatch()
	}
	// 测试挂点：生产恒为 nil ⇒ 冷路径上一次可预测的 nil 判断。idleExpire 只在
	// 超时提交时走（≤1 次/exchange），不在每帧热路径，故不引入 build-tag 双实现/
	// 默认空实现那套构建复杂度（spec r4 方案 A）。
	if idleCommitHook != nil {
		idleCommitHook(e)
	}
	if !e.cn.lease.CompareAndSwap(e, nil) {
		return io.EOF // 租约已不属我（新租约已持有 / 已归 nil）⇒ 不 terminate
	}
	e.cancel()
	e.cn.terminate(ErrWSIdleTimeout)
	return ErrWSIdleTimeout
}

// Release 尝试把连接交回缓存。true = 已交回（调用方随后什么都不用做）；
// false = 未交回（连接已终止，或取消回调已接手）。
//
// 归还判定是**五步合取**：租约 CAS 成功 → **本轮不是失败终态** → 连接可用 →
// 队列空（置栏）→ 缓存收下。任何一步失败都终止连接——绝不允许"报成功但没进表"。
//
// 为什么要"不是失败终态"这一步：真实客户端在错误路径上直接丢 socket（R:388–396
// 不放回），只有正常完成才归还。SDK 不在传输层判帧类型，而是由**已经解析事件的
// 那一层**（聚合器 / SSE 视图）在看到 failed / incomplete 时调 MarkFailed——
// 那个位置本来就知道事件类型，不违反"传输层不做协议判定"。
//
// 纯字节透传的调用方（如网关 relay）若自己消费帧而不经过上述两层，看到失败终态
// 必须调 MarkFailed() 或直接 Close()，否则连接会被当成干净结束而复用。带 4xx/5xx
// status 的 error 帧例外：它由那两层直接以 *HTTPError 结束本轮（随后 Close），所以
// 不经过 Release，也就不需要 MarkFailed。
//
// 这条"不干净不归还"是复用的全部安全性来源（客户端也一样：错误/早退的 socket
// 直接丢，不放回）。它不靠逐帧归属判定，所以不要为了"更安全"再往这里加逐帧检查。
func (e *wsExchange) Release() bool {
	if !e.beginFinish() {
		return false
	}
	if e.stopWatch != nil {
		e.stopWatch()
	}
	e.cancel()
	// 等已在飞的 Next 退出：它可能会 tryPop（拿走本属于下一个请求的首帧）或
	// 在取消分支里 terminate。cancel() 已经把阻塞的 pop 唤醒了，这里不会死等。
	e.nextMu.Lock()
	defer e.nextMu.Unlock()
	if !e.cn.lease.CompareAndSwap(e, nil) {
		return false // 取消回调已接手终止
	}
	if e.sink == nil || e.failed.Load() || !e.cn.releasable() || !e.cn.inbox.fence() || !e.sink.put(e) {
		e.cn.terminate(io.EOF)
		return false
	}
	return true
}

// Close 放弃本次 exchange（幂等）：收尾一律强断，pumped 模式不做关闭握手。
// Release 之后的 Close 是 no-op（连接已经交出去了，不能再杀）。
// 看到失败终态时调它同样安全（且无需先 MarkFailed）：连接一定不会被复用。
func (e *wsExchange) Close() error {
	e.teardown(io.EOF)
	return nil
}

var _ frameStream = (*wsExchange)(nil)
