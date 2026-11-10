package codexsdk

import (
	"errors"
	"io"
	"sync"
	"sync/atomic"
	"time"
)

// 连接复用的寿命参数与维护节拍。
const (
	// defaultConnIdleTTL 是空闲多久后退役：超过它，连接不再值得复用（上游可能已经
	// 悄悄关掉，而我们还不知道）。
	defaultConnIdleTTL = 5 * time.Minute
	// defaultConnMaxAge 是连接寿命上限。服务端硬上限是 60 分钟，本地提前一半退役——
	// 与其等上游甩一个 websocket_connection_limit_reached 给真实请求，不如自己换新的。
	defaultConnMaxAge = 30 * time.Minute
	// defaultConnSweepInterval 是空闲表的维护节拍：**只摘 TTL/maxAge 到期条目，
	// 不探测任何连接**。它取代了已删除的"心跳 tick 顺带 sweep"——否则被丢弃的
	// HTTPClient（网关每轮重建）的空闲连接会永久占位到对端断开。
	defaultConnSweepInterval = 30 * time.Second
)

// errConnExpired 是 sweep 退役连接时登记的终止原因。
//
// 它从不出现在调用方的错误链里：被退役的连接已经不在表中，没有任何请求持有它。
// 保留它只是为了让连接内核的终止原因可读（调试与测试断言）。
var errConnExpired = errors.New("codexsdk: ws 连接已过期")

// cachedConn 是一条空闲连接及其身份。全部字段受 connCache.mu 保护。
type cachedConn struct {
	cn    *conn
	token string // dial 时使用的 Authorization（OAuth 轮转后旧连接必须丢弃）
	key   connKey
	// idleSince 是入表时刻（TTL 起点）；bornAt 是拨号时刻（maxAge 起点）。
	// bornAt 随连接在表里逐次传递，**不重置**——否则热连接永远撞不到 30min 上限。
	idleSince time.Time
	bornAt    time.Time
}

// connCache 是挂在 HTTPClient 上的空闲连接表（容量 n 条）。
//
// 它同时是 wsExchange 的归还口（实现 connSink）：Release 只需要知道"收下了没有"。
type connCache struct {
	mu         sync.Mutex
	cap        int
	ttl        time.Duration // 默认 defaultConnIdleTTL（测试可注入）
	maxAge     time.Duration // 默认 defaultConnMaxAge（测试可注入）
	sweepEvery time.Duration // 维护 ticker 节拍（构造期固定；测试可注入）
	closed     bool          // Close 之后 put 拒收（由 Release 终止），idle 已清空
	idles      []cachedConn

	// stop 关闭维护 ticker（仅 Close）；closeOnce 保证幂等。
	stop      chan struct{}
	closeOnce sync.Once

	// hits/misses 只服务 bench 与测试（不导出）：hit 仅在连接真的被移交
	// （unfence 通过）时计；misses 覆盖“候选被 drop 后重拨”与“根本没找到候选”。
	// 两者只统计**走到连接阶段**的请求（编码失败/auth 报错在更早的就返回了）。
	hits   atomic.Int64
	misses atomic.Int64
}

// newConnCache 构造空闲表并启动它自己的维护 ticker。
//
// sweepEvery ≤ 0 ⇒ defaultConnSweepInterval。ticker 由缓存持有（而不是挂在连接上）：
// 连接不该知道缓存存在，也不该为缓存开定时器。
func newConnCache(capacity int, sweepEvery time.Duration) *connCache {
	if capacity < 1 {
		// 内部构造器也不允许非正容量：put 的“满则驱逐”分支会去读 idles[0]。
		// 公开面（WithWSConnCache）本来就用 newConnCacheIf 过滤过，这里只是加一道防线。
		capacity = 1
	}
	if sweepEvery <= 0 {
		sweepEvery = defaultConnSweepInterval
	}
	c := &connCache{
		cap:        capacity,
		ttl:        defaultConnIdleTTL,
		maxAge:     defaultConnMaxAge,
		sweepEvery: sweepEvery,
		stop:       make(chan struct{}),
	}
	go c.maintain()
	return c
}

// newConnCacheIf 按容量决定是否开启复用：n≤0 返回 nil（零行为变化）。
func newConnCacheIf(n int, sweepEvery time.Duration) *connCache {
	if n <= 0 {
		return nil
	}
	return newConnCache(n, sweepEvery)
}

// maintain 是空闲表唯一的周期维护者：到点只做 sweep（摘 TTL/maxAge 到期条目），
// **不做任何探测**——主动心跳已删除（对齐真实 codex 客户端）。空闲连接的死活由
// 常驻读者（readLoop）与取用前的 releasable 复查发现，不靠本地 ping。
//
// 只有 Close 会停它；clear（CloseIdleConnections）不清 ticker——缓存仍可复用。
func (c *connCache) maintain() {
	t := time.NewTicker(c.sweepEvery)
	defer t.Stop()
	for {
		select {
		case <-c.stop:
			return
		case now := <-t.C:
			c.sweep(now)
		}
	}
}

// expired 报告条目是否已过 TTL 或超过寿命上限（调用方持锁）。
func (c *connCache) expired(cc cachedConn, now time.Time) bool {
	return now.Sub(cc.idleSince) > c.ttl || now.Sub(cc.bornAt) > c.maxAge
}

// sweep 摘除过期/超龄条目并在锁外 terminate。
//
// 这是**唯一**的退役入口：判定与摘除在同一把锁内完成，所以被 get 摘走的连接
// 它再也看不到——不可能误杀一个正在跑的请求。它只由 connCache.maintain 周期调
// （外加 get/put 的惰性清理与测试直调），幂等。
func (c *connCache) sweep(now time.Time) {
	var dead []*conn
	c.mu.Lock()
	kept := c.idles[:0]
	for _, cc := range c.idles {
		if c.expired(cc, now) {
			dead = append(dead, cc.cn)
			continue
		}
		kept = append(kept, cc)
	}
	c.idles = kept
	c.mu.Unlock()
	for _, cn := range dead {
		cn.terminate(errConnExpired)
	}
}

// get 取一条同 key、同 token 的空闲连接：**摘除即代表独占**。
//
// 返回 true 只表示"找到了候选并已从表里摘走"，还不代表可以交付——调用方必须在
// 持有 exchange（lease.Store）之后做 releasable 复查 + unfence（顺序是契约：
// 解栏要发生在写 create 帧之前）。
//
// token 比对在这里做（锁内只比字符串）：取 token 本身（可能触发 OAuth 刷新）由
// 调用方在锁外完成。
func (c *connCache) get(key connKey, token string) (*cachedConn, bool) {
	now := time.Now()
	var dead []*conn
	var hit *cachedConn
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil, false
	}
	kept := c.idles[:0]
	for _, cc := range c.idles {
		switch {
		case cc.token != token:
			// 凭据变了：这条连接对我们已经没用，顺手退役（否则它会一直占着容量）。
			dead = append(dead, cc.cn)
		case c.expired(cc, now):
			dead = append(dead, cc.cn)
		case cc.key != key:
			// 另一条取数路径（readLimit）的连接：留在表里等它自己的请求。
			kept = append(kept, cc)
		case hit == nil:
			cp := cc
			hit = &cp
		default:
			kept = append(kept, cc)
		}
	}
	c.idles = kept
	c.mu.Unlock()
	for _, cn := range dead {
		cn.terminate(io.EOF)
	}
	return hit, hit != nil
}

// put 收下一条可复用的连接（实现 connSink）。
//
// 满时**驱逐 idleSince 最旧的条目**再入表，绝不因满而拒收新连接：两条取数路径
// （两个 readLimit）并存时，旧条目不该把新归还的连接挤掉。
//
// Close 之后返回 false 且不自行 terminate：归还中的连接由 Release 按既有四步
// 合取终止（避免双重 terminate 与 Release 返回值语义失真）。
func (c *connCache) put(e *wsExchange) bool {
	now := time.Now()
	var dead []*conn
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return false
	}
	kept := c.idles[:0]
	for _, cc := range c.idles {
		if c.expired(cc, now) {
			dead = append(dead, cc.cn)
			continue
		}
		kept = append(kept, cc)
	}
	c.idles = kept
	if len(c.idles) >= c.cap {
		oldest := 0
		for i := 1; i < len(c.idles); i++ {
			if c.idles[i].idleSince.Before(c.idles[oldest].idleSince) {
				oldest = i
			}
		}
		dead = append(dead, c.idles[oldest].cn)
		c.idles = append(c.idles[:oldest], c.idles[oldest+1:]...)
	}
	c.idles = append(c.idles, cachedConn{
		cn:        e.cn,
		token:     e.token,
		key:       e.key,
		idleSince: now,
		bornAt:    e.bornAt, // 不重置
	})
	c.mu.Unlock()
	for _, cn := range dead {
		cn.terminate(io.EOF)
	}
	return true
}

// RetireThread 退役一个 threadKey 的全部 idle 连接：锁内摘表、锁外 terminate。
//
// 不设 retired 集合/墓碑：threadKey 唯一属于一个会话，会话被 busy 独占租借；
// 退休发生在该请求 Close（= 归还）之后、finish 之前，所以调用时该 key 不可能
// 有在飞 exchange。Close 之后 idles 已空，本方法自然为 no-op。
func (c *connCache) RetireThread(threadKey string) {
	if c == nil || threadKey == "" {
		return
	}
	var dead []*conn
	c.mu.Lock()
	kept := c.idles[:0]
	for _, cc := range c.idles {
		if cc.key.threadKey == threadKey {
			dead = append(dead, cc.cn)
			continue
		}
		kept = append(kept, cc)
	}
	c.idles = kept
	c.mu.Unlock()
	for _, cn := range dead {
		cn.terminate(io.EOF)
	}
}

// Close 关闭缓存：停维护 ticker + 置 closed（此后 put 返回 false，由 Release 终止
// 归还中的连接）+ 清空并终止全部 idle。幂等；Close 之后 RetireThread 为 no-op。
//
// 这是**唯一**停 ticker 的地方：clear（CloseIdleConnections）只清空 idle，缓存本身
// 仍可用，维护节拍必须跟着继续跑。
func (c *connCache) Close() {
	if c == nil {
		return
	}
	c.closeOnce.Do(func() { close(c.stop) })
	c.dropIdle(true)
}

// clear 只清空 idle、不置 closed（CloseIdleConnections 用）：随后新建的连接
// 仍可正常复用。
func (c *connCache) clear() { c.dropIdle(false) }

// dropIdle 是 Close / clear 的共同实现：锁内摘表、锁外 terminate。
func (c *connCache) dropIdle(markClosed bool) {
	if c == nil {
		return
	}
	c.mu.Lock()
	if markClosed {
		c.closed = true
	}
	idles := c.idles
	c.idles = nil
	c.mu.Unlock()
	for _, cc := range idles {
		cc.cn.terminate(io.EOF)
	}
}

// WithWSConnCache 开启 Responses WS 臂的连接复用：n = 同时保留的空闲连接上限。
//
// 默认 **0 = 关闭**（每请求一条连接，与历史行为完全一致）。开启后，同一个
// HTTPClient、同一条取数路径（流式 64MiB / 聚合 16MiB）的**连续**请求会复用
// 上一条连接、不再付握手成本。连接身份 = (threadKey, readLimit)：与真实 codex
// 一致，连接是 thread 级、跨轮跨窗口复用；ts 是轮级、每请求随 client_metadata
// 下发；匿名（threadKey == ""）不复用。
//
// 收益边界：从同一 thread 的**第 2 个请求**起就有收益（跨轮也命中）；
// 每实例、每条取数路径各留一条空闲连接，容量按 2 规划。
func WithWSConnCache(n int) Option {
	return func(o *options) {
		if n < 0 {
			n = 0
		}
		o.wsConnCache = n
	}
}
