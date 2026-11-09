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
	// Release 读到终态后尝试把连接交回缓存（T5 起）。true = 已交回，
	// false = 未交回（连接已终止或将被终止）。没有缓存时恒 false。
	Release() bool
	// Close 放弃本次请求（幂等；Release 之后为 no-op）。
	Close() error
}

// connSink 是"可复用连接的归处"。T5.1 里恒为 nil（复用关闭）；T5.2 由 connCache
// 实现。用接口而不是具体类型，是为了让内核不必知道缓存长什么样——归还的唯一
// 语义是"收下了没有"。
type connSink interface {
	put(*wsExchange) bool
}

// connKey 是一条可复用连接的身份（§3.2）。T5.1 只负责把它带在 exchange 上；
// T5.2 用它做缓存键。
//
// readLimit 必须进键：两条取数路径的单帧内存上界不同（64MiB / 16MiB），按路径
// 拨号时就把上限固定了，共享连接会把聚合臂的单帧上界放大 4 倍。
type connKey struct {
	turnState string // 本次请求生效的 x-codex-turn-state（"" = 无）
	readLimit int64
}

// wsExchange 是一次 Responses WS 请求的生命周期：一条连接 + 取帧入口 + 本次
// 握手签发值（合成响应的 x-codex-turn-state 头靠它对外可见）。conn 不知道
// "请求"，exchange 不知道"socket"。
//
// 生效 turn-state 只出现在 `key.turnState` 里（它同时是缓存键与"本次请求用了哪个
// ts"的唯一记录）；不再单开一个字段——那只会让人以为它还有别的读者。
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
	sink   connSink // nil = 复用关闭（T5.1 恒 nil）
	key    connKey
	issued string    // 本次握手签发值（空 = 上游未签发）
	token  string    // 本次升级头使用的 Authorization（401 轮转后是新的一份）
	bornAt time.Time // 拨号起始时刻（maxAge 起点，命中不重置）

	ctx       context.Context
	cancel    context.CancelFunc
	stopWatch func() bool

	reused bool // 连接取自缓存（T5.2 置位；T5.1 恒 false）
	first  bool // 尚未交付过帧（首帧白名单只查这一次）

	// nextMu 串行化"取帧"与"移交"：Next 全程持它，Release 在 cancel 之后等它。
	// 没有它的话，一个在飞的 Next 会（a）拿到旧 ctx 的取消后 terminate 掉刚
	// 交出去的连接，（b）用 tryPop 抢走下一个请求的首帧。
	// 先 cancel 再等锁：否则阻塞在 pop 里的 Next 永远不会退出。
	nextMu sync.Mutex

	// closed 是 Release/Close 的先到者仲裁（CAS）：抢到的一方才做收尾。
	closed atomic.Bool
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
// 创建次序是契约（写死）：取 turn-state 快照 → **编码**（本地校验在此，
// 坏请求体不烧握手）→ 拨号 → 回写本次签发值 → 发一帧。effective 必须在编码
// 前取：握手刚签发的值不出现在本轮请求里（与 HTTP 臂的请求/响应头时序一致）。
//
// T5 起 exchangeCtx 承担三件事：拨号预算、取帧预算、取消守卫。connCtx 已经
// 与请求解耦（Background），所以请求结束不再杀 socket；但"调用方取消 ⇒ 本次
// 请求的连接必须终止"仍由 AfterFunc + 租约 CAS 保证。
func (c *HTTPClient) openWSExchange(ctx context.Context, wire []byte, readLimit int64) (*wsExchange, error) {
	exchangeCtx, cancel := exchangeContext(ctx, c.opts.timeout)

	effective := c.effectiveTurnState()
	turn := uint64(0)
	if c.opts.turnProvider != nil {
		turn = 1 // 一次 exchange 只发一帧 ⇒ 恒为第 1 轮
	}
	frame, err := c.codec.encode(wire, encodeWSResponses, frameInput{
		TurnState: effective,
		Turn:      turn,
		Now:       time.Now(),
	})
	if err != nil {
		cancel()
		return nil, err
	}
	key := connKey{turnState: effective, readLimit: readLimit}
	// sink 是归还口：**必须**在 nil 时保持真正的 nil 接口（typed-nil 陷阱：
	// 把 (*connCache)(nil) 装进接口后 e.sink == nil 为假，Release 会调进去）。
	var sink connSink
	if c.wsCache != nil {
		sink = c.wsCache
	}

	// 取用：只有开启缓存才可能命中。token 必须在锁外取（OAuth 可能取锁/刷新），
	// 报错就直接失败——不拿旧连接硬上。
	if cache := c.wsCache; cache != nil {
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
				handshake{turnState: cc.issued, token: cc.token, bornAt: cc.bornAt}, true, sink)
			// 复查 + 解栏。顺序是契约：解栏在写 create 帧**之前**——反过来的话，
			// 一个响应极快的上游会在我们写完的几微秒内回帧，撞上 fenced 被判废。
			if cc.cn.releasable() && cc.cn.inbox.unfence() {
				cache.hits.Add(1)
				if werr := cc.cn.write(exchangeCtx, frame); werr != nil {
					e.teardown(werr) // 写失败不重试、不重拨（create 可能已进 socket）
					return nil, werr
				}
				return e, nil
			}
			// 取用窗口失败（空闲期来过帧 / 连接已不可用）：帧尚未发出，重拨安全。
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
		headers:   c.wsHeaders,
		cache:     c.wsCache,
	})
	if derr != nil {
		cancel()
		return nil, wsDialError(derr)
	}
	// 本次握手签发值回写（HTTP 臂在这里读 resp.Header；WS 面只有这一个观测点）。
	// 顺序不能反：帧已按 effective 编码，回写不会污染本轮帧。命中路径没有握手，
	// 所以不回写（合成的 x-codex-turn-state 头由 cache 条目里的 issued 提供）。
	c.captureTurnStateValue(hs.turnState)
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
		issued: hs.turnState,
		token:  hs.token,
		bornAt: hs.bornAt,
		ctx:    ctx,
		cancel: cancel,
		reused: reused,
		first:  true,
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

// admit 做复用连接的首帧检查（I8）：只有"另一个请求的响应起始事件"才能被当成本
// 次请求的第一帧。迟到的中间事件/终态事件一律判废（继续喂下去会静默算错 usage）。
//
// 成本：只在"复用到的第一帧"走一次 frameEventType（零分配）。
func (e *wsExchange) admit(raw []byte) error {
	if !e.first {
		return nil
	}
	e.first = false
	if !e.reused || frameStartsResponse(raw) {
		return nil
	}
	e.terminateOwned(errConnDesynced)
	return errConnDesynced
}

// Next 交付下一帧。
//
//   - 入口 ctx 已结束：先尝试交付**已入队**的帧（上游已完成的结果不能被消费侧
//     超时改写），队列空才把取消当作本次请求的放弃；
//   - 阻塞等待期间被取消：同样先 `tryPop` 一次再认输——否则"帧已在队、随后
//     deadline 到点"这个次序（消费者已卡在 pop 里）会把已完成的终态帧丢掉，
//     把上游成功的请求报成 DeadlineExceeded；
//   - 有帧立即返回（取帧即提交：已取出的帧不再受 ctx 影响）；
//   - 复用连接的首帧先过 I8 白名单（违约 ⇒ errConnDesynced + 终止）；
//   - 流结束：close frame 归一为 io.EOF，其它终止原因原样返回。
func (e *wsExchange) Next(ctx context.Context) ([]byte, error) {
	e.nextMu.Lock()
	defer e.nextMu.Unlock()
	if e.closed.Load() { // 已归还/已放弃：不能再碰这条连接（它可能已经在缓存里）
		return nil, io.EOF
	}
	if ctx.Err() != nil {
		if data, ok := e.cn.inbox.tryPop(); ok {
			if aerr := e.admit(data); aerr != nil {
				return nil, aerr
			}
			return data, nil
		}
		e.terminateOwned(ctx.Err())
		return nil, ctx.Err()
	}
	data, err := e.cn.inbox.pop(ctx)
	if err == nil {
		if aerr := e.admit(data); aerr != nil {
			return nil, aerr
		}
		return data, nil
	}
	if cerr := ctx.Err(); cerr != nil {
		if data, ok := e.cn.inbox.tryPop(); ok { // 取消与入队并发：已入队的帧优先
			if aerr := e.admit(data); aerr != nil {
				return nil, aerr
			}
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

// Release 尝试把连接交回缓存。true = 已交回（调用方随后什么都不用做）；
// false = 未交回（连接已终止，或取消回调已接手）。
//
// 归还判定是四步合取：租约 CAS 成功 → 连接可用 → 队列空（置栏）→ 缓存收下。
// 任何一步失败都终止连接——绝不允许"报成功但没进表"。
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
	if e.sink == nil || !e.cn.releasable() || !e.cn.inbox.fence() || !e.sink.put(e) {
		e.cn.terminate(io.EOF)
		return false
	}
	return true
}

// Close 放弃本次 exchange（幂等）：收尾一律强断，pumped 模式不做关闭握手。
// Release 之后的 Close 是 no-op（连接已经交出去了，不能再杀）。
func (e *wsExchange) Close() error {
	e.teardown(io.EOF)
	return nil
}

var _ frameStream = (*wsExchange)(nil)
