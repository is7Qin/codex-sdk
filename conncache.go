package codexsdk

import (
	"errors"
	"io"
	"sync"
	"sync/atomic"
	"time"
)

// 连接复用的两个寿命参数。
const (
	// defaultConnIdleTTL 是空闲多久后退役：超过它，连接不再值得复用（上游可能已经
	// 悄悄关掉，而我们还不知道）。
	defaultConnIdleTTL = 5 * time.Minute
	// defaultConnMaxAge 是连接寿命上限。服务端硬上限是 60 分钟，本地提前一半退役——
	// 与其等上游甩一个 websocket_connection_limit_reached 给真实请求，不如自己换新的。
	defaultConnMaxAge = 30 * time.Minute
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
	// issued 是这条连接握手时上游签发的 turn-state：命中时回填给新的 exchange，
	// 让合成响应头与 TurnState() 与本轮握手一致（复用没有新的握手）。
	issued string
	// idleSince 是入表时刻（TTL 起点）；bornAt 是拨号时刻（maxAge 起点）。
	// bornAt 随连接在表里逐次传递，**不重置**——否则热连接永远撞不到 30min 上限。
	idleSince time.Time
	bornAt    time.Time
}

// connCache 是挂在 HTTPClient 上的空闲连接表（容量 n 条）。
//
// 它同时是 wsExchange 的归还口（实现 connSink）：Release 只需要知道"收下了没有"。
type connCache struct {
	mu     sync.Mutex
	cap    int
	ttl    time.Duration // 默认 defaultConnIdleTTL（测试可注入）
	maxAge time.Duration // 默认 defaultConnMaxAge（测试可注入）
	idles  []cachedConn

	// hits/misses 只服务 bench 与测试（不导出）：hit 仅在连接真的被移交
	// （unfence 通过）时计；misses 覆盖“候选被 drop 后重拨”与“根本没找到候选”。
	// 两者只统计**走到连接阶段**的请求（编码失败/auth 报错在更早的就返回了）。
	hits   atomic.Int64
	misses atomic.Int64
}

func newConnCache(capacity int) *connCache {
	if capacity < 1 {
		// 内部构造器也不允许非正容量：put 的“满则驱逐”分支会去读 idles[0]。
		// 公开面（WithWSConnCache）本来就用 newConnCacheIf 过滤过，这里只是加一道防线。
		capacity = 1
	}
	return &connCache{
		cap:    capacity,
		ttl:    defaultConnIdleTTL,
		maxAge: defaultConnMaxAge,
	}
}

// newConnCacheIf 按容量决定是否开启复用：n≤0 返回 nil（零行为变化）。
func newConnCacheIf(n int) *connCache {
	if n <= 0 {
		return nil
	}
	return newConnCache(n)
}

// expired 报告条目是否已过 TTL 或超过寿命上限（调用方持锁）。
func (c *connCache) expired(cc cachedConn, now time.Time) bool {
	return now.Sub(cc.idleSince) > c.ttl || now.Sub(cc.bornAt) > c.maxAge
}

// sweep 摘除过期/超龄条目并在锁外 terminate。
//
// 这是**唯一**的退役入口：判定与摘除在同一把锁内完成，所以被 get 摘走的连接
// 它再也看不到——不可能误杀一个正在跑的请求。多个连接的 keepalive 都会调它，幂等。
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
	kept := c.idles[:0]
	for _, cc := range c.idles {
		switch {
		case cc.token != token:
			// 凭据变了：这条连接对我们已经没用，顺手退役（否则它会一直占着容量）。
			dead = append(dead, cc.cn)
		case c.expired(cc, now):
			dead = append(dead, cc.cn)
		case cc.key != key:
			// 别的 turn-state / 另一条取数路径的连接：留在表里等它自己的请求。
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

// put 收下一条可复用的连接（实现 connSink）。恒返回 true。
//
// 满时**驱逐 idleSince 最旧的条目**再入表，绝不因满而拒收新连接：否则 ts 一变，
// cap=1 的缓存就永久占满、命中率归零。
func (c *connCache) put(e *wsExchange) bool {
	now := time.Now()
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
		issued:    e.issued,
		idleSince: now,
		bornAt:    e.bornAt, // 不重置
	})
	c.mu.Unlock()
	for _, cn := range dead {
		cn.terminate(io.EOF)
	}
	return true
}

// clear 清空缓存并终止其中的连接（CloseIdleConnections 用）。
func (c *connCache) clear() {
	c.mu.Lock()
	idles := c.idles
	c.idles = nil
	c.mu.Unlock()
	for _, cc := range idles {
		cc.cn.terminate(io.EOF)
	}
}

// WithWSConnCache 开启 Responses WS 臂的连接复用：n = 同时保留的空闲连接上限。
//
// 默认 **0 = 关闭**（每请求一条连接，与历史行为完全一致）。开启后，同一
// turn-state、同一条取数路径（流式 64MiB / 聚合 16MiB）的**连续**请求会复用
// 上一条连接、不再付握手成本；跨 turn-state 不复用（服务端用它做 sticky routing）。
//
// 收益边界：命中从同一轮的第 2 个请求起（调用方自带 turn-state）或第 3 个请求起
// （turn-state 靠上一轮响应捕获）——一轮只有 1–2 个请求时几乎没有收益。
func WithWSConnCache(n int) Option {
	return func(o *options) {
		if n < 0 {
			n = 0
		}
		o.wsConnCache = n
	}
}
