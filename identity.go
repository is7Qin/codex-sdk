package codexsdk

import (
	"crypto/rand"
	"encoding/binary"
	"hash/fnv"
	"math/big"
	"strconv"
)

// 身份轮换（网关伪装身份的“会话池”语义）：每槽一个 thread_id，window_id =
// "{thread_id}:{n}"。窗口推进由**本槽已服务的完成轮数**驱动（与观测 token 解耦）：
// 每完成一轮 Turns++；跨过本窗口阈值 → window_number +1，阈值按随机 span ∈ [spanLo,
// spanHi] 递进（窗口边界不等间隔）。本线程窗口数达 WMax → 退休换新线程。
//
// 依据（openai/codex c248f6d48）：window_id = "{thread_id}:{window_number}"，
// window_number 是“上下文窗口被填满过几次”（core/src/session/mod.rs current_window）。
// codex 本身没有“线程退休”；WMax 是本 SDK 为伪装身份合成的策略。
//
// 注：旧实现按观测 token 水位（θ_w 上升沿 + 回落到 θ_w 以下重新武装）推进；该口径在
// 「一条槽被多个 vibe 用户交替复用」时按同槽 in-band 用户数近似倍率放大（window 虚高
// → 线程早退），已废弃，改为与 token 完全解耦的轮次驱动。
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
	}
	if s.WMax > 0 && s.WindowN >= s.WMax {
		s = NewIdentityState(s.InstallationID, p)
	}
	return s
}

// drawWMax 按策略抽本线程的窗口数上限；WMaxHi == 0 → 不退休（返回 0）。
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
