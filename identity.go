package codexsdk

import (
	"crypto/rand"
	"fmt"
	"math/big"
)

// 身份轮换（网关伪装身份的“会话池”语义）：把 codex 的自动压缩规则搬过来，
// 用观测到的上下文水位驱动 thread_id / window_id 的演化。
//
// 依据（openai/codex c248f6d48）：
//   - window_id = "{thread_id}:{window_number}"，window_number 是“上下文窗口被
//     填满过几次”（core/src/session/mod.rs current_window）。
//   - codex 默认口径 AutoCompactTokenLimitScope::Total：用最近一轮的
//     total_tokens 水位直接比 θ_w（protocol/src/config_types.rs 默认 Total）。
//   - θ_w = min(auto_compact_token_limit, resolved_context_window × 9/10)。
//   - codex 本身没有“线程退休”；WMax 是本 SDK 为伪装身份合成的策略。

// CompactScope 对应 codex AutoCompactTokenLimitScope。
type CompactScope int

const (
	// ScopeTotal 用最近一轮的 total_tokens 水位直接比 θ_w（codex 默认口径）。
	ScopeTotal CompactScope = iota
	// ScopeBodyAfterPrefix 只计入“窗口起点之后增长的部分”（θ_w 不变）。
	ScopeBodyAfterPrefix
)

// RotatePolicy 是轮换策略（网关侧参数；SDK 负责按它抽样与演进）。
type RotatePolicy struct {
	// WMaxLo / WMaxHi：每个线程最多活几个窗口，开线程时在 [Lo,Hi] 内均匀抽。
	// WMaxHi == 0 表示不退休（WMax 恒 0，线程不换）。
	WMaxLo uint64
	WMaxHi uint64
	// Scope 决定用哪种口径比 θ_w；零值 = ScopeTotal。
	Scope CompactScope
}

// IdentityState 是一个槽位的身份状态。可整体拷贝/序列化，用于跨实例借用时的
// 快照传递（网关侧持有，SDK 不落任何存储）。
type IdentityState struct {
	InstallationID string // 账号级永久
	ThreadID       string // 当前线程（UUIDv7）
	WindowN        uint64 // window_id 后缀
	// Baseline 仅 ScopeBodyAfterPrefix 用：本窗口起点水位；-1 = 未初始化。
	Baseline int64
	// Armed 仅 ScopeTotal 用：水位已回落到 θ_w 以下、可再次触发边沿。
	Armed bool
	// WMax 本线程的窗口数上限（0 = 不退休）。
	WMax uint64
}

// NewIdentityState 开一个新线程：新 thread_id、WindowN=0、Armed=true、按策略
// 抽 WMax。
func NewIdentityState(installationID string, p RotatePolicy) IdentityState {
	return IdentityState{
		InstallationID: installationID,
		ThreadID:       NewUUIDv7(),
		WindowN:        0,
		Baseline:       -1,
		Armed:          true,
		WMax:           drawWMax(p),
	}
}

// WindowID 返回注入用的 window_id："{thread_id}:{window_n}"。
func (s IdentityState) WindowID() string {
	return fmt.Sprintf("%s:%d", s.ThreadID, s.WindowN)
}

// Session 返回注入用的会话标识（根线程语义：session_id == thread_id）。
func (s IdentityState) Session() Session {
	return Session{SessionID: s.ThreadID, ThreadID: s.ThreadID, WindowID: s.WindowID()}
}

// Step 用一次观测（该响应的 total_tokens）推进身份状态，返回新状态。
//
//	ScopeTotal：水位跨过 θ_w 的上升沿 → WindowN++（持续高位不重复计数；回落
//	            到 θ_w 以下后重新武装）。
//	ScopeBodyAfterPrefix：先记 baseline；之后 (observed - baseline) ≥ θ_w 时
//	            WindowN++ 并把 baseline 前移。
//	两种口径下：WindowN ≥ WMax（且 WMax>0）→ 退休换新线程。
//
// slug 未知 → 用 fallback 模型的 θ_w。
func Step(s IdentityState, observedTotalTokens int64, slug string, p RotatePolicy) IdentityState {
	if s.ThreadID == "" {
		s = NewIdentityState(s.InstallationID, p)
	}
	limit := AutoCompactTokens(slug)
	switch p.Scope {
	case ScopeBodyAfterPrefix:
		if s.Baseline < 0 {
			s.Baseline = observedTotalTokens
		} else if observedTotalTokens-s.Baseline >= limit {
			s.WindowN++
			s.Baseline = observedTotalTokens
		}
	default:
		if observedTotalTokens >= limit {
			if s.Armed {
				s.WindowN++
				s.Armed = false
			}
		} else {
			s.Armed = true
		}
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
