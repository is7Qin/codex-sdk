package codexsdk

import (
	"context"
	"errors"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// # 形状（spec §3.1）
//
//	Pool ──map[PoolKey][]*poolConn──> poolConn
//	                                    ├─ c   *Client       （socket + 归一/鉴权；已被池 claim）
//	                                    ├─ tok *ownerToken   （A4）
//	                                    ├─ pump   goroutine   （唯一读者，生命周期 = 连接）
//	                                    ├─ pinger goroutine   （唯一 ping 所有者，生命周期 = 连接）
//	                                    ├─ state idle|leased|dead
//	                                    ├─ pinging bool       （A3：ping 在飞时不可出租）
//	                                    ├─ gen uint64         （A2：租约代号，Release 时 ++）
//	                                    ├─ queue []poolFrame  （锁内队列；frame 带 gen）
//	                                    ├─ notify / room      （两条独立通知通道，见 §3.4）
//	                                    └─ done               （所有等待的终止条件）
//
// 四条公理（代码对应点）：
//
//	A1 读让位于投递，但 ping 让位于读。
//	   ① 队列满 ⇒ pump 停读等待（poolConn.pump 的 blocked 等待循环；Stats.QueueBlocked）
//	   ② 停读/有帧在飞期间 ping 抑制（poolConn.pingLoop 的 blocked / 队列非空分支；Stats.PingSuppressed）
//	   ③ 停读期间的 ping 超时不计死（pingLoop 用 blockedSeq 前后比较，不用 bool 快照）
//	   ④ 空闲态队列恒空 ⇒ ping 常态生效
// A2 帧交接原子：poolConn.mu 内完成 state/gen/queue 三件事；帧的归属以**读出时刻**
//   的连接代为准（pump 持锁时取 st.gen），投递等待（背压）期间换代 ⇒ 该帧丢弃；
//   Release 先 gen++ 再清空队列；Recv 的代次过滤不变。
// A3 交接与 ping 互斥：pinging 的连接不出租（Acquire 跳过），ping 结束按 blockedSeq 判死。
// A4 所有权在代码里：池 claim 后外部 Send/Recv/Ping/Close/CloseNow/SetTurnState
//   一律 ErrNotOwner（池内走 *Owned 变体，按请求注入 turn-state 走 Lease.SetTurnState）。
//
// 两条通知通道（§3.4）：notify 唤醒消费者（Recv），room 唤醒 pump；互不吞噬。
// 共用一条 cap=1 通道会互相吃掉唤醒，等价于漏信号——本实现刻意分成两条。
//
// goroutine 账：每连接 2 个（pump + pinger），都随 done 退出；空闲总量默认不限
// （MaxIdleTotal 的 0/<0 都是"不限"），最坏 goroutine 数由调用方的容量规划决定。

// 默认值（spec §3.9 / impl-brief §1，exact values）。
const (
	// DefaultIdleTTL 空闲连接存活上限。空闲有心跳维持，TTL 只是资源旋钮。
	DefaultIdleTTL = 3 * time.Minute
	// DefaultHardLimit 上游单连接 60min 硬限（websocket_connection_limit_reached）。
	// 这是连接寿命的**唯一**权威，不可禁用（配置 <0 归默认）。
	DefaultHardLimit = 60 * time.Minute
	// DefaultPingInterval 空闲心跳间隔（与现役网关 30s 一致，非 SDK 默认的 30s/2s 超时）。
	DefaultPingInterval = 30 * time.Second
	// DefaultPongTimeout 心跳 pong 超时（与现役网关 10s 一致）。
	DefaultPongTimeout = 10 * time.Second
	// DefaultMaxIdlePerKey 单键空闲连接上限（防同键堆积）。
	DefaultMaxIdlePerKey = 1
	// DefaultMaxIdleTotal **空闲**连接上限，0 = 不限。SDK 看不到账号维度，任何常数都会
	// 在某一侧错：小规模浪费额度，多账号部署则静默抖动（每个请求都 miss）。容量应由
	// 键空间的真实形状决定——空闲数 ≈ min(键数, IdleTTL 窗口内被触及的去重键数)，
	// IdleTTL 是时间维度的自愈闸，MaxIdlePerKey 是每键闸。它**不限在租 / 在拨**：
	// 同键可并发多条租约，拨号也不占名额——全池准入是调用方（网关）的责任。
	DefaultMaxIdleTotal = 0
	// DefaultFrameQueue 单连接帧队列深度上限。
	DefaultFrameQueue = 8
	// DefaultQueueBytes 单连接帧队列字节预算。单连接最坏占用 = QueueBytes + 2×ReadLimit
	// （队列空时允许一帧超预算，且 pump 手上还可能有一帧在飞；ReadLimit 默认 16MiB
	// ⇒ ≈36MiB），见 roomLocked。
	DefaultQueueBytes = 4 << 20

	// janitorDiv janitor 周期 = IdleTTL/janitorDiv，夹 [1s, 30s]。
	janitorDiv = 4
)

// 连接池错误值。全部用 errors.Is 判定。
var (
	// ErrPoolClosed 池已 CloseAll，不再拨号也不再出租。
	ErrPoolClosed = errors.New("codexsdk: 连接池已关闭")
	// ErrPoolNilDialer 未配置 DialFunc。
	ErrPoolNilDialer = errors.New("codexsdk: 连接池未配置 DialFunc")
	// ErrHeartbeatConflict DialFunc 返回的连接自带 SDK 心跳（PingInterval!=0）：
	// 池是唯一 ping 所有者（C2），冲突即拒绝并关闭该连接。
	ErrHeartbeatConflict = errors.New("codexsdk: 心跳所有权冲突（DialFunc 必须 WithPingInterval(0)）")
	// ErrLeaseClosed 租约已结束（Release/Discard/换代后再用）。
	ErrLeaseClosed = errors.New("codexsdk: 租约已结束")
	// ErrPingTimeout 心跳 ping 在 PongTimeout 内没等到 pong，且本次飞行期间没发生停读。
	ErrPingTimeout = errors.New("codexsdk: 心跳 ping 超时")
	// ErrConnExpired 连接年龄达到 HardLimit 被回收（idle 与 leased **一视同仁**；
	// 在租态回收计入 PoolStats.ReapedInFlight）。
	//
	// 公开错误值：调用方用 errors.Is(err, ErrConnExpired) 判定（Recv 取空队列后返回它）。
	ErrConnExpired = errors.New("codexsdk: 连接超过 HardLimit 年龄")

	// 以下为**内部**死因/哨兵：不导出，调用方不应依赖其身份。
	// errConnIdle 空闲连接被回收（TTL / MaxIdlePerKey / MaxIdleTotal）的内部死因。
	errConnIdle = errors.New("codexsdk: 空闲连接被回收")
	// errNilClient DialFunc 返回 nil。
	errNilClient = errors.New("codexsdk: DialFunc 返回 nil 客户端")
)

// PoolKey 是连接池的分桶键（opaque）：SDK 只做等值比较，不解析、不派生。
//
// **不变量：握手身份维度必须进键**（连接身份包含握手头，头变化则连接不可复用，
// F5）—— 同一键意味着"这两次请求可以重新使用同一条握手过的 socket"。
//
// 必含：账号、凭据、出口、安装 ID、sessionID、threadID、windowID、
// clientRequestID、modelSlug（上游 slug）。
// 必剔：turn_metadata / parent_turn_id / parent_thread_id / turn_id / traceparent /
// tracestate / x-codex-turn-state（每轮/每帧变化，进键会让池退化成"一次一连接"）。
//
// 编码由调用方自选（任意字符串，长度与内容不参与池的判定）；上面两条是**唯一的
// 可见约束**，漏掉任一维度都会把不同身份的请求复用到同一条连接上。
type PoolKey string

// DialFunc 拨一条新连接。返回前必须 WithPingInterval(0)（池拥有心跳，C2）。
//
// key 由调用方按 PoolKey 的不变量构造：**握手身份维度必须进键**（必含/必剔清单
// 见 PoolKey），池侧不做任何校验或归一。
//
// 返回后调用方（DialFunc 闭包）不得再碰该 *Client：池 claim 之后外部
// Send/Recv/Ping/Close/CloseNow/SetTurnState 全部返回 ErrNotOwner（A4）。
type DialFunc func(ctx context.Context, key PoolKey) (*Client, error)

// PoolConfig 是池的配置。所有 Duration 字段的 0 值取默认（见 Default* 常量）；
// IdleTTL 取负值表示"不回收空闲连接"；HardLimit 不可禁用（<0 归默认）。
type PoolConfig struct {
	Dial DialFunc

	IdleTTL       time.Duration    // 0 → 3min；<0 不回收空闲连接
	HardLimit     time.Duration    // 0 → 60min（上游单连接硬限；<0 归默认，不可禁用）
	PingInterval  time.Duration    // 0 → 30s；<0 关闭池心跳（仅测试/自管心跳时用）
	PongTimeout   time.Duration    // 0 → 10s
	MaxIdlePerKey int              // 0 → 1
	MaxIdleTotal  int              // 0/<0 → 不限（容量归调用方）
	FrameQueue    int              // 0 → 8（帧数上限）
	QueueBytes    int              // 0 → 4MiB（字节预算；单帧可超预算，见 roomLocked）
	Now           func() time.Time // 0 → time.Now（测试注入假时钟）
}

// Lease 是一次独占租约：一条连接在其生命周期内只属于一个租约。
//
// 契约：
//   - Send/Recv/SetTurnState 只能由租约持有者调用；Release/Discard 之后一律 ErrLeaseClosed。
//   - Recv 是**单消费者**：同一租约并发调用 Recv 未定义（notify 只保证一个等待者）。
//   - Recv 的 ctx 取消不触碰 socket（取消后队列里已有的帧仍可取）。
//   - Release 归还连接（gen++、清队列、清 turn-state），Discard 直接判死。
//   - Release/Discard 幂等：首次生效，其后 no-op。
type Lease interface {
	// Send 发送一帧。
	//
	// 定序：**同一租约内先 SetTurnState 再 Send**——Send 在 st.mu 外读 turnState，
	// 跨调用没有内存定序保证；同一 goroutine 顺序调用时新值对该帧生效。
	Send(ctx context.Context, frame []byte) error
	Recv(ctx context.Context) ([]byte, error)
	// SetTurnState 设置本租约后继帧的 x-codex-turn-state 值（空串 = 清除）；
	// 非本代 / 已死 → ErrLeaseClosed / 死因。
	//
	// 这是网关在池化路径上"按请求注入入站 x-codex-turn-state"的唯一入口：
	// HTTP 面靠每轮重建 client + WithHeader，池下不可用。
	// 定序见 Send：先 Set 再 Send，别指望跨调用的内存定序。
	SetTurnState(state string) error
	// Reused 报告本次租约是否复用了池内既有连接（false = 本次新拨）。
	Reused() bool
	// Age 连接年龄（born → 现在）。
	Age() time.Duration
	// Gen 租约代号（A2）。
	Gen() uint64
	Release()
	Discard()
}

// PoolStats 是池的**近似快照**：原子计数与逐连接汇总不在同一时刻读取，
// 仅用于观测/告警，不得用于断言跨字段恒等式。字段集是契约，口径固定：
// Hits/Misses 先判；Dials/DialErrors 后判；ErrPoolClosed/ErrPoolNilDialer 不计 Misses。
type PoolStats struct {
	Keys, Idle, Leased int // 键数 / 空闲连接 / 在租连接
	QueuedBytes        int // 当前队列占用字节（全部连接求和）

	Hits, Misses      int64
	Dials, DialErrors int64
	// ReapedIdle / ReapedAge / ReapedInFlight：空闲回收（TTL/超额）、超龄回收
	// （idle，按 HardLimit）、在租态超龄回收（leased，同样按 HardLimit）。
	ReapedIdle, ReapedAge, ReapedInFlight int64
	QueueBlocked                          int64 // A1-①：pump 因队列满而停读的次数
	PingSuppressed                        int64 // A1-②：停读期间被抑制的 ping 次数
	Dropped                               int64 // 无主/换代/空闲态丢弃帧数
	Delivered                             int64 // 投递给租约的帧数
	// EmptyKeys sweep 清空的键桶数（桶内连接均已被判死后整桶摘除；口径：一次清空计 1）。
	EmptyKeys           int64
	Discarded, Released int64
	// CloseErrors 关闭连接失败次数（fd 可能悬空——观测用）。
	CloseErrors int64
	// Dead 已死但尚未被 janitor 摘除的连接数（§4.2 TestPoolBreaksOnRemoteClose 断言）。
	Dead int
}

// Pool 是 Responses WS 连接池。
//
// 零值不可用：必须 NewPool 构造（启动 janitor）。并发安全；CloseAll 幂等。
type Pool struct {
	cfg PoolConfig

	mu     sync.Mutex
	closed bool
	keys   map[PoolKey][]*poolConn

	// idleTotal / idleCount 是"状态迁移时维护"的空闲计数（§3.11-2）：
	// 只有超限时才做 LRU 收敛，正常请求收尾不再全池扫描 + 排序。
	idleTotal int
	idleCount map[PoolKey]int

	janitorStop chan struct{}
	janitorDone chan struct{}

	hits, misses        atomic.Int64
	dials, dialErrors   atomic.Int64
	reapedIdle          atomic.Int64
	reapedAge           atomic.Int64
	reapedInFlight      atomic.Int64
	queueBlocked        atomic.Int64
	pingSuppressed      atomic.Int64
	dropped, delivered  atomic.Int64
	emptyKeys           atomic.Int64
	discarded, released atomic.Int64
	closeErrors         atomic.Int64
}

// NewPool 构造连接池并启动 janitor。
func NewPool(cfg PoolConfig) *Pool {
	p := &Pool{
		cfg:         normalizePoolConfig(cfg),
		keys:        make(map[PoolKey][]*poolConn),
		idleCount:   make(map[PoolKey]int),
		janitorStop: make(chan struct{}),
		janitorDone: make(chan struct{}),
	}
	go p.janitor()
	return p
}

func normalizePoolConfig(cfg PoolConfig) PoolConfig {
	switch {
	case cfg.IdleTTL == 0:
		cfg.IdleTTL = DefaultIdleTTL
	}
	if cfg.HardLimit <= 0 {
		cfg.HardLimit = DefaultHardLimit
	}
	switch {
	case cfg.PingInterval < 0:
		cfg.PingInterval = 0 // 显式关闭池心跳
	case cfg.PingInterval == 0:
		cfg.PingInterval = DefaultPingInterval
	}
	if cfg.PongTimeout <= 0 {
		cfg.PongTimeout = DefaultPongTimeout
	}
	if cfg.MaxIdlePerKey <= 0 {
		cfg.MaxIdlePerKey = DefaultMaxIdlePerKey
	}
	// MaxIdleTotal 不做归一：0 与 <0 都是"不限"（容量归调用方），见 DefaultMaxIdleTotal。
	if cfg.FrameQueue <= 0 {
		cfg.FrameQueue = DefaultFrameQueue
	}
	if cfg.QueueBytes <= 0 {
		cfg.QueueBytes = DefaultQueueBytes
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	return cfg
}

func (p *Pool) now() time.Time { return p.cfg.Now() }

// Acquire 取一条租约：优先复用同键空闲连接（A3：跳过 pinging / 超龄），
// 无可用则拨号。Hits/Misses 口径见 PoolStats。
//
// **调用方必须 `defer l.Discard()`**（`Release` 之后再 `Discard` 是 no-op，见
// `poolLease.Discard`：换代/已归还一律跳过）；**不要用「每条路径显式 Release」
// 替代它**——panic 路径漏掉的恰恰是 Release。池不包住调用方 panic（删掉 Do/safeCall
// 后不再有那层 recover），只有这个 defer 能在 panic 时把连接判死；漏了它，连接会以
// leased 状态泄漏到 HardLimit（最坏 60min）。panic 安全由调用方负责，HardLimit 是唯一兜底。
func (p *Pool) Acquire(ctx context.Context, key PoolKey) (Lease, error) {
	if p == nil {
		return nil, ErrPoolClosed
	}
	p.mu.Lock()
	closed := p.closed
	p.mu.Unlock()
	if closed {
		return nil, ErrPoolClosed
	}
	if p.cfg.Dial == nil {
		return nil, ErrPoolNilDialer
	}
	if l := p.acquireIdle(key); l != nil {
		return l, nil
	}
	p.misses.Add(1)
	st, err := p.dialConn(ctx, key)
	if err != nil {
		return nil, err
	}
	return &poolLease{pool: p, st: st, gen: st.gen}, nil
}

// acquireIdle 尝试把同键空闲连接转成租约（p.mu 内完成，st.mu 内换代配置）。
func (p *Pool) acquireIdle(key PoolKey) *poolLease {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil
	}
	now := p.now()
	var stale []*poolConn
	var lease *poolLease
	for _, st := range p.keys[key] {
		st.mu.Lock()
		state, pinging := st.state, st.pinging
		// 出租时刻的年龄闸：HardLimit 是唯一寿命权威（idle/leased 一致）。
		// ★ 与 classify 的 stateIdle/stateLeased 年龄判据是**同一不变式的两份表达式**，
		// 改这里必须同步改那里（否则出租时刻与 janitor 之间会产生超龄窗口）。
		aged := p.cfg.HardLimit > 0 && now.Sub(st.born) >= p.cfg.HardLimit
		if state == stateIdle && !pinging && !aged {
			st.state = stateLeased
			st.idleAt = time.Time{}
			p.noteIdleGoneLocked(key) // I-3：状态迁移处维护空闲计数
			lease = &poolLease{pool: p, st: st, gen: st.gen, reused: true}
			st.mu.Unlock()
			break
		}
		st.mu.Unlock()
		if state == stateIdle && aged {
			stale = append(stale, st)
		}
	}
	if lease != nil {
		p.hits.Add(1)
	}
	p.mu.Unlock()
	// 超龄空闲连接不再出租：即刻关闭（janitor 的同类扫描会跳过已死者）。
	for _, st := range stale {
		if p.killConn(st, ErrConnExpired) {
			p.reapedAge.Add(1)
		}
	}
	return lease
}

// dialConn 拨一条新连接并登记为 leased。闭包返回的 *Client 在此被 claim（A4）。
//
// 不设键数准入：键空间由调用方决定（PoolKey 的不变量），池只负责登记与回收。
func (p *Pool) dialConn(ctx context.Context, key PoolKey) (*poolConn, error) {
	p.dials.Add(1)
	c, err := p.cfg.Dial(ctx, key)
	if err != nil {
		p.dialErrors.Add(1)
		return nil, err
	}
	if c == nil {
		p.dialErrors.Add(1)
		return nil, errNilClient
	}
	if c.PingInterval() != 0 {
		// C2：池拥有心跳。此刻尚未 claim，外部调用合法。
		_ = c.CloseNow()
		return nil, ErrHeartbeatConflict
	}
	tok := c.claim()
	now := p.now()
	st := &poolConn{
		pool:     p,
		c:        c,
		tok:      tok,
		key:      key,
		state:    stateLeased,
		gen:      1,
		born:     now,
		done:     make(chan struct{}),
		notify:   make(chan struct{}, 1),
		room:     make(chan struct{}, 1),
		pumpDone: make(chan struct{}),
		pingDone: make(chan struct{}),
	}

	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		p.killConn(st, ErrPoolClosed)
		return nil, ErrPoolClosed
	}
	// 登记：键数不设上限（容量归调用方），已有键追加连接与同键并发租约都是常态。
	p.keys[key] = append(p.keys[key], st)
	p.mu.Unlock()

	// 每连接 2 goroutine：pump（唯一读者）+ pinger（唯一 ping 所有者），都随 done 退出。
	go st.pump()
	go st.pingLoop()
	return st, nil
}

// Stats 返回池的当前快照（p.mu + 各 conn.mu 顺序加锁，口径见 PoolStats）。
func (p *Pool) Stats() PoolStats {
	if p == nil {
		return PoolStats{}
	}
	p.mu.Lock()
	s := PoolStats{Keys: len(p.keys)}
	conns := make([]*poolConn, 0, len(p.keys))
	for _, list := range p.keys {
		conns = append(conns, list...)
	}
	p.mu.Unlock()

	s.Hits = p.hits.Load()
	s.Misses = p.misses.Load()
	s.Dials = p.dials.Load()
	s.DialErrors = p.dialErrors.Load()
	s.ReapedIdle = p.reapedIdle.Load()
	s.ReapedAge = p.reapedAge.Load()
	s.ReapedInFlight = p.reapedInFlight.Load()
	s.QueueBlocked = p.queueBlocked.Load()
	s.PingSuppressed = p.pingSuppressed.Load()
	s.Dropped = p.dropped.Load()
	s.Delivered = p.delivered.Load()
	s.EmptyKeys = p.emptyKeys.Load()
	s.Discarded = p.discarded.Load()
	s.Released = p.released.Load()
	s.CloseErrors = p.closeErrors.Load()

	for _, st := range conns {
		st.mu.Lock()
		switch st.state {
		case stateIdle:
			s.Idle++
		case stateLeased:
			s.Leased++
		default:
			s.Dead++
		}
		s.QueuedBytes += st.queueBytes
		st.mu.Unlock()
	}
	return s
}

// CloseAll 关闭池：全部连接（含在途租约）置 dead 并摘下，随后 close(done)+CloseNow。
// 幂等。关闭后 Acquire 返回 ErrPoolClosed；在途租约的 Recv 立刻返回错误。
func (p *Pool) CloseAll() {
	if p == nil {
		return
	}
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return
	}
	p.closed = true
	var toClose []*poolConn
	for key, list := range p.keys {
		for _, st := range list {
			if p.killLocked(st, ErrPoolClosed) {
				toClose = append(toClose, st)
			}
		}
		delete(p.keys, key)
	}
	p.idleTotal = 0
	p.idleCount = make(map[PoolKey]int)
	stop := p.janitorStop
	p.mu.Unlock()

	close(stop) // 只关一次（closed 已置位）
	for _, st := range toClose {
		p.finishDead(st)
	}
}

// janitor 周期回收（§3.5）：超龄（idle/leased 一致，HardLimit 是唯一寿命权威）、
// 空闲超 TTL、空闲超额（每键 / 总量）。
func (p *Pool) janitor() {
	defer close(p.janitorDone)
	t := time.NewTicker(p.janitorPeriod())
	defer t.Stop()
	for {
		select {
		case <-p.janitorStop:
			return
		case <-t.C:
			p.sweep()
		}
	}
}

func (p *Pool) janitorPeriod() time.Duration {
	if p.cfg.IdleTTL <= 0 {
		return 30 * time.Second // 不回收空闲连接时只需覆盖年龄回收，取夹取上界
	}
	d := p.cfg.IdleTTL / janitorDiv
	if d < time.Second {
		d = time.Second
	}
	if d > 30*time.Second {
		d = 30 * time.Second
	}
	return d
}

type reapAction uint8

const (
	reapKeep reapAction = iota
	reapDrop
	reapIdle
	reapAge
)

// sweep 执行一次回收。测试可直接调用（注入时钟后无需等真实 tick）。
//
// 锁纪律（§3.11-1）：p.mu 内只做"选中 + 摘下 + 置 dead"（killLocked 不碰 socket），
// close(done)/CloseNow 一律放到解锁后。
func (p *Pool) sweep() {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return
	}
	now := p.now()
	var idleV, ageV, inFlightV []*poolConn
	for key, list := range p.keys {
		alive := list[:0]
		for _, st := range list {
			switch act := p.classify(st, now); act {
			case reapDrop:
				// 已死：摘除即可（空闲计数在判死时已维护）
			case reapIdle:
				if p.killLocked(st, errConnIdle) {
					idleV = append(idleV, st)
				}
			case reapAge:
				// 同一个年龄判据覆盖 idle/leased；计数口径按回收瞬间的状态区分。
				// 状态迁移全部在 p.mu 内完成 ⇒ 先读状态再 kill 不会漂移。
				st.mu.Lock()
				inFlight := st.state == stateLeased
				st.mu.Unlock()
				if p.killLocked(st, ErrConnExpired) {
					if inFlight {
						inFlightV = append(inFlightV, st)
					} else {
						ageV = append(ageV, st)
					}
				}
			default:
				alive = append(alive, st)
			}
		}
		for i := len(alive); i < len(list); i++ {
			list[i] = nil
		}
		if len(alive) == 0 {
			delete(p.keys, key)
			p.emptyKeys.Add(1) // 口径：清空一个键桶计 1
			continue
		}
		p.keys[key] = alive
	}
	trimmed := p.trimIdleLocked()
	p.mu.Unlock()

	for _, st := range idleV {
		p.finishDead(st)
		p.reapedIdle.Add(1)
	}
	for _, st := range ageV {
		p.finishDead(st)
		p.reapedAge.Add(1)
	}
	for _, st := range inFlightV {
		p.finishDead(st)
		p.reapedInFlight.Add(1)
	}
	for _, st := range trimmed {
		p.finishDead(st)
		p.reapedIdle.Add(1)
	}
}

// classify 判定单连接本轮回收动作（st.mu 内读状态）。
//
// ★ 两处 `age ≥ HardLimit` 与 acquireIdle 的出租时刻年龄闸是**同一不变式的两份
// 表达式**：改这里必须同步改那里（否则出租闸与 janitor 之间会产生超龄窗口）。
func (p *Pool) classify(st *poolConn, now time.Time) reapAction {
	st.mu.Lock()
	defer st.mu.Unlock()
	switch st.state {
	case stateDead:
		return reapDrop
	case stateIdle:
		if p.cfg.IdleTTL > 0 && now.Sub(st.idleAt) >= p.cfg.IdleTTL {
			return reapIdle
		}
		if p.cfg.HardLimit > 0 && now.Sub(st.born) >= p.cfg.HardLimit {
			return reapAge
		}
	case stateLeased:
		// 与 idle 同一年龄判据（HardLimit 是唯一寿命权威）：在租连接同样会被
		// 年龄回收（计 ReapedInFlight），"病态消费方卡死"的兜底就在这里。
		// ★ 同 acquireIdle：这一行的谓词与出租闸必须同步修改。
		if p.cfg.HardLimit > 0 && now.Sub(st.born) >= p.cfg.HardLimit {
			return reapAge
		}
	}
	return reapKeep
}

// trimIdleLocked 按"最久未用先关"执行 ① 每键空闲上限 ② 空闲总量上限。
// 被选中的连接在锁内直接置 dead（避免"刚选中就被出租"），返回值交给调用方在
// 解锁后真正关闭。只在计数器显示超限时才被调用（§3.11-2）。
func (p *Pool) trimIdleLocked() []*poolConn {
	type idleInfo struct {
		st     *poolConn
		key    PoolKey
		idleAt time.Time
	}
	var idles []idleInfo
	for key, list := range p.keys {
		for _, st := range list {
			st.mu.Lock()
			if st.state == stateIdle {
				idles = append(idles, idleInfo{st: st, key: key, idleAt: st.idleAt})
			}
			st.mu.Unlock()
		}
	}
	if len(idles) == 0 {
		return nil
	}
	sort.Slice(idles, func(i, j int) bool { return idles[i].idleAt.Before(idles[j].idleAt) })

	evict := make(map[*poolConn]bool, len(idles))
	perKey := make(map[PoolKey]int, len(p.keys))
	for _, it := range idles { // 最久未用优先淘汰
		perKey[it.key]++
		if p.cfg.MaxIdlePerKey > 0 && perKey[it.key] > p.cfg.MaxIdlePerKey {
			evict[it.st] = true
		}
	}
	survivors := 0
	for i := len(idles) - 1; i >= 0; i-- { // 从最新往回数，保住最近的 MaxIdleTotal 条（≤0 不限）
		it := idles[i]
		if evict[it.st] {
			continue
		}
		survivors++
		if p.cfg.MaxIdleTotal > 0 && survivors > p.cfg.MaxIdleTotal {
			evict[it.st] = true
			survivors--
		}
	}
	var victims []*poolConn
	for _, it := range idles {
		if evict[it.st] && p.killLocked(it.st, errConnIdle) {
			victims = append(victims, it.st)
		}
	}
	return victims
}

// reapIfNeeded 只在计数器显示超限时才做 LRU 收敛（§3.11-2 / I-3）。
//
// 正常请求收尾因此不再跑 O(键数) 的全池扫描 + 排序；计数口径见
// noteIdleGainedLocked / noteIdleGoneLocked（与 Stats().Idle 由同一状态迁移维护）。
func (p *Pool) reapIfNeeded(key PoolKey) {
	p.mu.Lock()
	over := p.cfg.MaxIdleTotal > 0 && p.idleTotal > p.cfg.MaxIdleTotal
	if !over && p.cfg.MaxIdlePerKey > 0 && p.idleCount[key] > p.cfg.MaxIdlePerKey {
		over = true
	}
	var victims []*poolConn
	if over {
		victims = p.trimIdleLocked()
	}
	p.mu.Unlock()
	for _, st := range victims {
		p.finishDead(st)
		p.reapedIdle.Add(1)
	}
}

// noteIdleGainedLocked 记一条连接进入空闲（调用方已持 p.mu）。
func (p *Pool) noteIdleGainedLocked(key PoolKey) {
	p.idleTotal++
	p.idleCount[key]++
}

// noteIdleGoneLocked 记一条连接离开空闲（调用方已持 p.mu）。
func (p *Pool) noteIdleGoneLocked(key PoolKey) {
	if p.idleTotal > 0 {
		p.idleTotal--
	}
	if n := p.idleCount[key]; n > 1 {
		p.idleCount[key] = n - 1
	} else {
		delete(p.idleCount, key)
	}
}

// ---------------------------------------------------------------------------
// poolConn
// ---------------------------------------------------------------------------

type poolState uint8

const (
	stateIdle poolState = iota
	stateLeased
	stateDead
)

type poolFrame struct {
	gen  uint64 // A2：租约代号
	data []byte
}

type poolConn struct {
	pool *Pool
	c    *Client
	tok  *ownerToken
	key  PoolKey // 登记时的键（空闲计数需要）

	mu         sync.Mutex
	state      poolState
	pinging    bool   // A3：ping 在飞时不可出租
	gen        uint64 // A2：租约代号（Release 时 ++）
	queue      []poolFrame
	queueBytes int
	blocked    bool   // A1-①：pump 停读等待中
	blockedSeq uint64 // A1-③：进入停读的单调计数（不得用 bool 快照判死）
	deadCause  error
	doneClosed bool
	born       time.Time
	idleAt     time.Time

	done     chan struct{}
	notify   chan struct{} // 唤醒消费者（Recv）
	room     chan struct{} // 唤醒 pump（A1-① 的对侧）
	pumpDone chan struct{}
	pingDone chan struct{}
}

// pump 是连接的唯一读者（F1/F2）。idle 丢弃、leased 入队（A2 复检）、dead 退出。
func (st *poolConn) pump() {
	defer close(st.pumpDone)
	for {
		st.mu.Lock()
		if st.state == stateDead {
			st.mu.Unlock()
			return
		}
		st.mu.Unlock()

		data, err := st.c.recvOwned(st.tok, st.c.ctx) // 唯一读点（可能跨越 Release/换代）
		if err != nil {
			st.pool.killConn(st, err)
			return
		}

		st.mu.Lock()
		switch st.state {
		case stateDead:
			st.mu.Unlock()
			return
		case stateIdle:
			// F3：请求结束后仍会来无主帧，空闲即丢弃（A1-④：队列恒空 ⇒ ping 常态生效）
			st.mu.Unlock()
			st.pool.dropped.Add(1)
		case stateLeased:
			// ★ A2(v10)：归属 = 【读出时刻】的连接代。Read 常跨过 Release——
			// 若以"读起点"代号归属，新租约的第一帧会带着上一代代号被丢掉
			// （每次复用丢 response.created ⇒ 复用不可用）。
			gen := st.gen
			waited := false
			for !st.roomLocked(len(data)) && st.state == stateLeased && st.gen == gen {
				if !waited {
					waited = true
					st.pool.queueBlocked.Add(1) // A1-①：停读让位（不丢帧、不杀流）
				}
				st.blocked = true
				st.blockedSeq++ // A1-③：单调计数（ping 判死以它为准）
				st.mu.Unlock()
				select {
				case <-st.room:
				case <-st.done:
				}
				st.mu.Lock()
			}
			st.blocked = false
			if st.state != stateLeased || st.gen != gen {
				// §3.10-5：等待期间可能发生 Release/Discard，唤醒后必须复检。
				//
				// 残余窗口（写死在代码里，勿删）：帧在 Release **之前**到达、却由一次
				// 跨越换代的 Read 返回 ⇒ 按 v10 语义归新租约。兜底在网关 C5 的状态机：
				// 只丢 completed 之后的可识别尾帧（rate_limits/metadata 等），created
				// 之前收到的 type:error / status:429 必须按错误终止并 Discard，不能当
				// 尾帧吞掉（spec §5-C5 / §6-R6）。
				st.mu.Unlock()
				st.pool.dropped.Add(1)
				continue
			}
			st.queue = append(st.queue, poolFrame{gen: gen, data: data})
			st.queueBytes += len(data)
			st.mu.Unlock()
			st.pool.delivered.Add(1)
			signal(st.notify) // 唤醒等数据的 Recv
			continue
		default:
			st.mu.Unlock()
		}
	}
}

// roomLocked 报告能否再收一帧（帧数 + 字节预算）。
// 字节预算实现为"队列非空时才卡"：单帧可超预算（§3.3 / R4：单连接最坏占用 =
// QueueBytes + 2×ReadLimit——队列空时允许一帧超预算，且 pump 手上还有一帧在飞），
// 否则一个超过 QueueBytes 的合法大帧会让 pump 永久停读。
func (st *poolConn) roomLocked(add int) bool {
	if len(st.queue) >= st.pool.cfg.FrameQueue {
		return false
	}
	if len(st.queue) > 0 && st.queueBytes+add > st.pool.cfg.QueueBytes {
		return false
	}
	return true
}

// pingLoop 是池内唯一 ping 所有者（§3.6）：探针只在"没有帧在飞"时发——
// 空闲态恒发（半开判死 ≤ PingInterval+PongTimeout），租用态在上游静默时也发
// （否则半开连接要等 HardLimit 才发现）；停读/有帧在飞时抑制（A1-②）；
// 本次飞行期间发生过停读 ⇒ 超时不计死（A1-③，用 blockedSeq 判，不用布尔快照）。
func (st *poolConn) pingLoop() {
	defer close(st.pingDone)
	iv := st.pool.cfg.PingInterval
	if iv <= 0 {
		return
	}
	t := time.NewTicker(iv)
	defer t.Stop()
	for {
		select {
		case <-st.done:
			return
		case <-t.C:
		}
		st.mu.Lock()
		if st.state == stateDead {
			st.mu.Unlock()
			continue
		}
		if st.blocked {
			// A1-②：停读期间 ping 无意义（数据正在流动），抑制并计数。
			// 先于 pinging 判定：停读期间本就不该有 ping 在飞，即便有也在飞那次
			// 已由 blockedSeq 负责不判死，后续 tick 一律记抑制。
			st.pool.pingSuppressed.Add(1)
			st.mu.Unlock()
			continue
		}
		if st.pinging {
			st.mu.Unlock()
			continue
		}
		if st.state == stateLeased && len(st.queue) > 0 {
			// 队列里有帧在飞：活性已被数据本身证明，本轮不发 ping。
			st.mu.Unlock()
			continue
		}
		st.pinging = true
		seqAt := st.blockedSeq // ★ 快照用单调计数，不用 blocked 布尔
		st.mu.Unlock()

		ctx, cancel := context.WithTimeout(st.c.ctx, st.pool.cfg.PongTimeout)
		err := st.c.pingOwned(st.tok, ctx) // 锁外；此刻该连接不会被出租（A3）
		cancel()

		st.mu.Lock()
		st.pinging = false
		blockedDuring := st.blockedSeq != seqAt
		dead := st.state == stateDead
		st.mu.Unlock()
		if err == nil || blockedDuring || dead || st.c.ctx.Err() != nil {
			// A1-③：本次 ping 期间发生过停读 ⇒ 超时是"下游慢"不是"上游死"，不计死。
			continue
		}
		// A1-③ 未成立：本次 ping 的等待期间没发生过停读 ⇒ 判死（死因可区分）。
		st.pool.killConn(st, ErrPingTimeout)
		return
	}
}

// killLocked 把连接置 dead（幂等）：记录可区分的死因（§3.10-1）、维护空闲计数。
//
// 调用方必须已持 p.mu；本函数只摘除、不做任何 I/O（锁纪律 §3.11-1）。
// 返回"需要真正关闭 socket"（首次置死为 true），关闭动作由调用方在解锁后做。
// 队列不清空：先让租约把已入队的帧取完，取空后 Recv 返回死因。
func (p *Pool) killLocked(st *poolConn, cause error) bool {
	st.mu.Lock()
	if st.state == stateDead {
		st.mu.Unlock()
		return false
	}
	if st.state == stateIdle {
		p.noteIdleGoneLocked(st.key) // I-3：离开空闲的唯一状态迁移点
	}
	st.state = stateDead
	if st.deadCause == nil {
		if cause == nil {
			cause = ErrLeaseClosed
		}
		st.deadCause = cause
	}
	st.blocked = false
	needClose := !st.doneClosed
	st.doneClosed = true
	st.mu.Unlock()
	return needClose
}

// finishDead 真正关闭连接（close(done) + CloseNow），必须在解锁后调用。
func (p *Pool) finishDead(st *poolConn) {
	close(st.done) // pump / pinger / Recv 立刻退出
	if err := st.c.closeNowOwned(st.tok); err != nil {
		p.closeErrors.Add(1) // fd 可能悬空——观测用（M-2：不再静默丢错）
	}
}

// killConn 判死一条连接（幂等）：锁内摘除，解锁后关闭。供 pump / pinger /
// 租约 / 拨号失败路径使用（这些位置都不持 p.mu，也不会在 p.mu 里被调用）。
func (p *Pool) killConn(st *poolConn, cause error) bool {
	p.mu.Lock()
	needClose := p.killLocked(st, cause)
	p.mu.Unlock()
	if !needClose {
		return false
	}
	p.finishDead(st)
	return true
}

// discardLease 只在本代租约仍然有效时判死：避免"已归还/已被下一租约拿走"的连接
// 被迟到的 Discard 误杀。
//
// 原子性来自 p.mu：检查与置死都在**同一个 p.mu 临界区**内（st.mu 只是内层锁，
// 中途解锁 st.mu 用于检查不破坏原子性）——所有 state 迁移（leased↔idle↔dead）
// 都在 p.mu 内完成，因此检查与本代置死之间不可能插入 Release/Acquire。
func (p *Pool) discardLease(st *poolConn, gen uint64) bool {
	p.mu.Lock()
	st.mu.Lock()
	if st.state != stateLeased || st.gen != gen {
		st.mu.Unlock()
		p.mu.Unlock()
		return false
	}
	st.mu.Unlock()
	needClose := p.killLocked(st, ErrLeaseClosed)
	p.mu.Unlock()
	if !needClose {
		return false
	}
	p.finishDead(st)
	return true
}

// deathErr 返回 Recv/Send 应暴露的死因。
func (st *poolConn) deathErr() error {
	st.mu.Lock()
	defer st.mu.Unlock()
	return st.deathErrLocked()
}

func (st *poolConn) deathErrLocked() error {
	if st.deadCause != nil {
		return st.deadCause
	}
	return ErrLeaseClosed
}

// usableLocked 判定某代租约此刻能否动 socket。
func (st *poolConn) usableLocked(gen uint64) error {
	switch st.state {
	case stateDead:
		return st.deathErrLocked()
	case stateLeased:
		if st.gen == gen {
			return nil
		}
	}
	return ErrLeaseClosed
}

// release 归还连接（§3.5）：锁内 gen++ → idle → 清队列 → 清 turn-state → 记空闲计数；
// 解锁后发两条信号。
//
// turn-state 的清除与状态迁移在同一个临界区（I-1）：否则并发的 Acquire→Send
// 能读到上一轮的 turn-state（client.turnState 是 atomic.Value，锁内调用无死锁风险）。
func (st *poolConn) release(gen uint64) bool {
	p := st.pool
	now := p.now()
	p.mu.Lock()
	st.mu.Lock()
	if st.state != stateLeased || st.gen != gen {
		st.mu.Unlock()
		p.mu.Unlock()
		return false
	}
	st.gen++ // A2：换代之后再清队列，旧代帧一律不可投递
	st.state = stateIdle
	st.idleAt = now
	st.queue = nil
	st.queueBytes = 0
	_ = st.c.setTurnStateOwned(st.tok, "") // 跨轮不得回传 turn-state
	p.noteIdleGainedLocked(st.key)
	st.mu.Unlock()
	p.mu.Unlock()

	signal(st.notify) // 唤醒仍阻塞在 Recv 的消费者（gen 已变 ⇒ ErrLeaseClosed）
	signal(st.room)   // 唤醒 blocked 的 pump（复检发现换代 ⇒ 丢帧后继续读）
	return true
}

// signal 非阻塞发一条唤醒（cap=1 通道，重复发合并）。
func signal(ch chan struct{}) {
	select {
	case ch <- struct{}{}:
	default:
	}
}

// ---------------------------------------------------------------------------
// poolLease
// ---------------------------------------------------------------------------

var _ Lease = (*poolLease)(nil)

type poolLease struct {
	pool   *Pool
	st     *poolConn
	gen    uint64
	reused bool
}

func (l *poolLease) Send(ctx context.Context, frame []byte) error {
	l.st.mu.Lock()
	err := l.st.usableLocked(l.gen)
	l.st.mu.Unlock()
	if err != nil {
		return err
	}
	return l.st.c.sendOwned(l.st.tok, ctx, frame)
}

// SetTurnState 按请求注入本租约后继帧的 x-codex-turn-state（A4 的池内入口）。
// 守卫与写入在同一临界区：否则 Release 的清除可能被一次迟到的写入覆盖，
// 反过来也一样（两者都会让 turn-state 串到下一个租约）。
func (l *poolLease) SetTurnState(state string) error {
	st := l.st
	st.mu.Lock()
	defer st.mu.Unlock()
	if err := st.usableLocked(l.gen); err != nil {
		return err
	}
	return st.c.setTurnStateOwned(st.tok, state)
}

// Recv 取本代下一帧（§3.4）。每次 pop 后必须 signal(room) 唤醒 pump，否则互相等待。
//
// 队列优先于死因：致命错误之前把已入队的帧交给调用方（否则 response.completed
// 可能随连接死亡一起丢掉，C4 完成判定需要它）；取空后才返回死因。
func (l *poolLease) Recv(ctx context.Context) ([]byte, error) {
	st := l.st
	for {
		st.mu.Lock()
		popped := false
		var (
			data  []byte
			found bool
		)
		for len(st.queue) > 0 {
			f := st.queue[0]
			st.queue[0] = poolFrame{}
			st.queue = st.queue[1:]
			st.queueBytes -= len(f.data)
			popped = true
			if f.gen == l.gen {
				data, found = f.data, true
				break
			}
		}
		var stateErr error
		if !found {
			stateErr = st.usableLocked(l.gen)
		}
		st.mu.Unlock()

		if found {
			if popped {
				signal(st.room) // ★ A1-① 对侧：pop 后立刻告诉 pump"有空位了"
			}
			return data, nil
		}
		if popped {
			signal(st.room) // 丢弃了旧代帧，同样要让 pump 继续
		}
		if stateErr != nil {
			return nil, stateErr
		}
		select {
		case <-st.notify:
		case <-st.done:
			return nil, st.deathErr()
		case <-ctx.Done():
			return nil, ctx.Err() // ctx 取消不触碰 socket（§3.4）
		}
	}
}

func (l *poolLease) Reused() bool { return l.reused }

func (l *poolLease) Age() time.Duration { return l.pool.now().Sub(l.st.born) }

func (l *poolLease) Gen() uint64 { return l.gen }

// Release 归还连接：dead 连接上 no-op；幂等。
func (l *poolLease) Release() {
	if !l.st.release(l.gen) {
		return
	}
	l.pool.released.Add(1)
	// 归还后**只在计数器超限时**才做 LRU 收敛（I-3 / §3.11-2）：
	// 正常请求收尾不再跑 O(键数) 的全池扫描 + 排序。
	l.pool.reapIfNeeded(l.st.key)
}

// Discard 丢弃连接：置 dead + CloseNow；幂等。
//
// 已结束/已换代的租约上调用是 no-op——否则会误杀已经归还给池（空闲）
// 甚至已被下一个租约拿走的连接。
func (l *poolLease) Discard() {
	if l.pool.discardLease(l.st, l.gen) {
		l.pool.discarded.Add(1)
	}
}
