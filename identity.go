package codexsdk

import (
	"crypto/rand"
	"encoding/binary"
	"hash/fnv"
	"math/big"
	"strconv"
)

// Identity 是一次 responses 请求（或一条 WS 连接）的全部身份数据。零值 = 匿名。
//
// 身份是**请求数据**：HTTPClient 只承载账号/传输级配置（Auth、transport、
// readLimit、池容量、超时）；每次调用把身份经本结构传入，默认头里不允许出现
// 身份键（保留键在构造期 panic，见 WithHeader）。
//
// 匿名（Session.SessionID == ""）：不发身份头、不写 client_metadata、不发
// turn-metadata、删除 prompt_cache_key、不带 turn-state。SessionID 为空而其余
// 字段非空时同样按匿名处理，并计入 ShapeStats.PartialIdentity。
type Identity struct {
	InstallationID string        // client_metadata x-codex-installation-id / turn-metadata installation_id
	Session        Session       // SessionID / ThreadID / WindowID("{thread}:{n}") / ClientRequestID（空=ThreadID）
	Window         Window        // 上下文窗口
	Turn           Turn          // 轮次
	Sink           TurnStateSink // 上游 turn-state 落点（nil = 忽略）
}

// Window 是上下文窗口身份。
type Window struct {
	Number    uint64 // window_number；必须与 Session.WindowID 的 n 一致
	ContextID string // context_window_id（UUIDv7）；空 = 每请求生成
}

// Turn 是轮次身份。
type Turn struct {
	ID          string // turn_id / root_turn_id；空 = 每请求生成 UUIDv7
	StartedAtMs int64  // turn_started_at_unix_ms；0 = 取请求时刻
	State       string // 要带给上游的 turn-state（"" = 不带）
	First       bool   // 本请求是一轮的第一个请求（含客户端重试）
}

// anonymous 报告本身份是否按匿名整形（SessionID 为空即匿名）。
func (id Identity) anonymous() bool { return id.Session.SessionID == "" }

// partial 报告"按匿名处理但其余字段非空"的残缺身份（计数用）。
func (id Identity) partial() bool {
	return id.anonymous() && (id.InstallationID != "" || id.Session != (Session{}) ||
		id.Window != (Window{}) || id.Turn != (Turn{}))
}

// TurnStateSink 接收上游签发的 turn-state。用接口而不是闭包：会话对象按指针
// 塞进接口不产生分配（闭包会把捕获变量一起拖进堆），且把"ts 写到哪"变成显式
// 边界。
//
// 实现约束：可做有界的非阻塞工作（原子操作、sync.Map 写、非阻塞 channel
// 发送），不得阻塞、不得回调 SDK。Sink 只在调用/Read 栈内同步执行；
// Body.Close 返回后 SDK 不再调用 Sink。同一轮内"首值生效"由实现方负责。
type TurnStateSink interface{ SetTurnState(string) }

// threadKey 是连接复用的线程身份：InstallationID|SessionID|ThreadID。
// 匿名返回 ""：匿名没有会话语义，不参与连接复用（见 connKey）。
func (id Identity) threadKey() string {
	if id.anonymous() {
		return ""
	}
	return id.InstallationID + "|" + id.Session.SessionID + "|" + id.Session.ThreadID
}

// deliverTurnState 把上游签发值交给 Sink（空值与 nil Sink 均忽略）。
func (id Identity) deliverTurnState(v string) {
	if v != "" && id.Sink != nil {
		id.Sink.SetTurnState(v)
	}
}

// 身份轮换（网关伪装身份的“会话池”语义）：每槽一个 thread_id，window_id =
// "{thread_id}:{n}"。窗口推进由**本槽已服务的完成轮数**驱动（与观测 token 解耦）：
// 每完成一轮 Turns++；跨过本窗口阈值 → window_number +1，阈值按随机 span ∈ [spanLo,
// spanHi] 递进（窗口边界不等间隔）。本线程窗口数达 WMax → 退休换新线程。
//
// 依据（openai/codex c248f6d48）：window_id = "{thread_id}:{window_number}"，
// window_number 是“上下文窗口被填满过几次”（core/src/session/mod.rs current_window）。
// codex 本身没有“线程退休”；WMax 是本 SDK 为伪装身份合成的策略。
const (
	spanLo = 48 // 每窗口最少轮数
	spanHi = 96 // 每窗口最多轮数
)

// RotatePolicy 是轮换策略（网关侧参数；SDK 负责按它抽样与演进）。
type RotatePolicy struct {
	// WMaxLo / WMaxHi：每个线程最多活几个窗口，开线程时在 [Lo,Hi] 内均匀抽。
	// WMaxHi == 0 表示不退休（WMax 恒 0，线程不换）。
	WMaxLo uint64
	WMaxHi uint64
}

// DefaultRotatePolicy 是默认轮换策略：每线程活 16–48 个窗口。
func DefaultRotatePolicy() RotatePolicy {
	return RotatePolicy{WMaxLo: 16, WMaxHi: 48}
}

// windowSpan 由 (ThreadID, 窗口序号) 确定性派生该窗口的轮数跨度 ∈ [spanLo, spanHi]。
// 纯函数、无 RNG、无全局态 → Step 对给定状态确定可测；对外表现为窗口边界不等间隔。
func windowSpan(threadID string, windowIndex uint64) uint64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(threadID))
	var b [8]byte
	binary.LittleEndian.PutUint64(b[:], windowIndex)
	_, _ = h.Write(b[:])
	return spanLo + h.Sum64()%(spanHi-spanLo+1)
}

// IdentityState 是一个槽位的身份状态。纯值类型（可整体拷贝），由网关侧持有，
// SDK 不落任何存储。Step 是唯一变更入口。
type IdentityState struct {
	InstallationID string // 账号级永久
	ThreadID       string // 当前线程（UUIDv7）
	Turns          uint64 // 已服务完成轮数——唯一驱动量
	WindowN        uint64 // 当前 window_number
	// NextWindowAt 下一个窗口递增的 Turns 阈值（= Σ_{i=0..WindowN} windowSpan(ThreadID,i)）。
	NextWindowAt uint64
	WMax         uint64 // 本线程窗口数上限（0 = 不退休）
	// ContextWindowID 是当前窗口的 context_window_id（UUIDv7）：开线程与每次
	// 窗口推进时重新生成。
	ContextWindowID string
}

// NewIdentityState 开一个新线程：新 thread_id、Turns=0、WindowN=0、下一个窗口阈值按
// windowSpan(thread_id,0) 起算、按策略抽 WMax。
func NewIdentityState(installationID string, p RotatePolicy) IdentityState {
	tid := NewUUIDv7()
	return IdentityState{
		InstallationID: installationID,
		ThreadID:       tid,
		Turns:          0,
		WindowN:        0,
		NextWindowAt:   windowSpan(tid, 0),
		WMax:           drawWMax(p),

		ContextWindowID: NewUUIDv7(),
	}
}

// WindowID 返回注入用的 window_id："{thread_id}:{window_n}"。
func (s IdentityState) WindowID() string {
	return s.ThreadID + ":" + strconv.FormatUint(s.WindowN, 10)
}

// Session 返回注入用的会话标识（根线程语义：session_id == thread_id）。
func (s IdentityState) Session() Session {
	return Session{SessionID: s.ThreadID, ThreadID: s.ThreadID, WindowID: s.WindowID()}
}

// Identity 返回槽位路径的请求身份：Session 同 Session()，Window 取当前窗口，
// Turn 只带 First（ID 空 = 每请求生成）。first 由调用方按
// !AnalyzeConversationInput(payload).FollowUp() 给出。
func (s IdentityState) Identity(sink TurnStateSink, first bool) Identity {
	return Identity{
		InstallationID: s.InstallationID,
		Session:        s.Session(),
		Window:         Window{Number: s.WindowN, ContextID: s.ContextWindowID},
		Turn:           Turn{First: first},
		Sink:           sink,
	}
}

// Step 推进一个完成轮：Turns++；跨过本窗口阈值 → WindowN++ 并叠加下一个随机 span；
// window_number 达 WMax（且 WMax>0）→ 退休换新线程。
//
// 仅接受 NewIdentityState / 前次 Step 产出的状态（保证 NextWindowAt-Turns ≥ spanLo）。
func Step(s IdentityState, p RotatePolicy) IdentityState {
	if s.ThreadID == "" {
		s = NewIdentityState(s.InstallationID, p)
	}
	s.Turns++
	if s.Turns >= s.NextWindowAt {
		s.WindowN++
		s.NextWindowAt += windowSpan(s.ThreadID, s.WindowN)
		s.ContextWindowID = NewUUIDv7()
	}
	if s.WMax > 0 && s.WindowN >= s.WMax {
		s = NewIdentityState(s.InstallationID, p)
	}
	return s
}

// drawWMax 按策略抽本线程的窗口数上限；WMaxHi == 0 → 不退休（返回 0）。
// crypto/rand 失败 → panic（导出构造器无 error 通道，理由见包级文档
// 「crypto/rand 失败」）。
func drawWMax(p RotatePolicy) uint64 {
	lo, hi := p.WMaxLo, p.WMaxHi
	if hi == 0 {
		return 0
	}
	if hi < lo {
		lo, hi = hi, lo
	}
	span := hi - lo + 1
	n, err := rand.Int(rand.Reader, big.NewInt(int64(span)))
	if err != nil {
		panic("codexsdk: crypto/rand 失败: " + err.Error())
	}
	return lo + n.Uint64()
}
