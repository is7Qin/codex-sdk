package codexsdk

import "testing"

// TestIdentityWindowIDAndSession：window_id = "{thread_id}:{n}"，session 语义为
// 根线程（session_id == thread_id == 当前线程）。
func TestIdentityWindowIDAndSession(t *testing.T) {
	s := IdentityState{ThreadID: "0193-tid", WindowN: 3}
	if got := s.WindowID(); got != "0193-tid:3" {
		t.Fatalf("WindowID = %q, want %q", got, "0193-tid:3")
	}
	sess := s.Session()
	if sess.SessionID != "0193-tid" || sess.ThreadID != "0193-tid" || sess.WindowID != "0193-tid:3" {
		t.Fatalf("Session = %+v, want session=thread=tid, window=tid:3", sess)
	}
}

// TestNewIdentityState：开新线程的初始不变量（新 UUIDv7 / n=0 / Armed / baseline
// 未初始化 / WMax 落在策略区间）。
func TestNewIdentityState(t *testing.T) {
	s := NewIdentityState("inst-1", RotatePolicy{WMaxLo: 3, WMaxHi: 5})
	if s.InstallationID != "inst-1" {
		t.Fatalf("InstallationID = %q", s.InstallationID)
	}
	if s.ThreadID == "" {
		t.Fatal("ThreadID 不应为空")
	}
	if s.WindowN != 0 {
		t.Fatalf("WindowN = %d, want 0", s.WindowN)
	}
	if !s.Armed {
		t.Fatal("新线程应 Armed")
	}
	if s.Baseline != -1 {
		t.Fatalf("Baseline = %d, want -1", s.Baseline)
	}
	if s.WMax < 3 || s.WMax > 5 {
		t.Fatalf("WMax = %d, 应在 [3,5]", s.WMax)
	}
}

// TestStepScopeTotalEdge：ScopeTotal 下水位跨 θ_w 的上升沿才 +1；持续高位不重复
// 计数；回落后重新武装。
func TestStepScopeTotalEdge(t *testing.T) {
	p := RotatePolicy{Scope: ScopeTotal} // WMaxHi=0 → 不退休
	limit := AutoCompactTokens("gpt-5.5")
	s := NewIdentityState("inst", p)
	tid := s.ThreadID

	s = Step(s, limit-1, "gpt-5.5", p)
	if s.WindowN != 0 || !s.Armed {
		t.Fatalf("低于 θ_w：WindowN=%d Armed=%v, want 0/true", s.WindowN, s.Armed)
	}

	s = Step(s, limit, "gpt-5.5", p)
	if s.WindowN != 1 || s.Armed {
		t.Fatalf("触达 θ_w：WindowN=%d Armed=%v, want 1/false", s.WindowN, s.Armed)
	}

	s = Step(s, limit+12345, "gpt-5.5", p)
	if s.WindowN != 1 {
		t.Fatalf("持续高位不应重复计数：WindowN=%d, want 1", s.WindowN)
	}

	s = Step(s, limit-1, "gpt-5.5", p)
	if s.WindowN != 1 || !s.Armed {
		t.Fatalf("回落应重新武装：WindowN=%d Armed=%v, want 1/true", s.WindowN, s.Armed)
	}

	s = Step(s, limit, "gpt-5.5", p)
	if s.WindowN != 2 {
		t.Fatalf("再次上升沿：WindowN=%d, want 2", s.WindowN)
	}

	if s.ThreadID != tid {
		t.Fatalf("不退休时线程不应变化: %q → %q", tid, s.ThreadID)
	}
}

// TestStepScopeBodyAfterPrefix：ScopeBodyAfterPrefix 记本窗口 baseline，窗口内
// 增长达 θ_w 才 +1 并把 baseline 前移。
func TestStepScopeBodyAfterPrefix(t *testing.T) {
	p := RotatePolicy{Scope: ScopeBodyAfterPrefix}
	limit := AutoCompactTokens("gpt-5.5")
	s := NewIdentityState("inst", p)

	s = Step(s, 1000, "gpt-5.5", p)
	if s.Baseline != 1000 || s.WindowN != 0 {
		t.Fatalf("首观测应记 baseline：Baseline=%d WindowN=%d, want 1000/0", s.Baseline, s.WindowN)
	}

	s = Step(s, 1000+limit-1, "gpt-5.5", p)
	if s.WindowN != 0 || s.Baseline != 1000 {
		t.Fatalf("增长不足 θ_w：WindowN=%d Baseline=%d, want 0/1000", s.WindowN, s.Baseline)
	}

	s = Step(s, 1000+limit, "gpt-5.5", p)
	if s.WindowN != 1 || s.Baseline != 1000+limit {
		t.Fatalf("增长达 θ_w：WindowN=%d Baseline=%d, want 1/%d", s.WindowN, s.Baseline, 1000+limit)
	}

	s = Step(s, 1000+2*limit, "gpt-5.5", p)
	if s.WindowN != 2 || s.Baseline != 1000+2*limit {
		t.Fatalf("再满一窗：WindowN=%d Baseline=%d, want 2/%d", s.WindowN, s.Baseline, 1000+2*limit)
	}
}

// TestStepRetiresAtWMax：WindowN 达到 WMax 时退休（换新 thread / n 归 0 / 重抽
// WMax）。
func TestStepRetiresAtWMax(t *testing.T) {
	p := RotatePolicy{WMaxLo: 2, WMaxHi: 2, Scope: ScopeTotal}
	limit := AutoCompactTokens("gpt-5.5")
	s := NewIdentityState("inst", p)
	if s.WMax != 2 {
		t.Fatalf("WMax = %d, want 2", s.WMax)
	}
	tid := s.ThreadID

	s = Step(s, limit, "gpt-5.5", p)   // WindowN 1
	s = Step(s, limit-1, "gpt-5.5", p) // 重新武装
	s = Step(s, limit, "gpt-5.5", p)   // WindowN 2 → 退休
	if s.WindowN != 0 {
		t.Fatalf("退休后 WindowN 应归 0, got %d", s.WindowN)
	}
	if s.ThreadID == tid {
		t.Fatal("退休后应换新 thread_id")
	}
	if s.Baseline != -1 || !s.Armed {
		t.Fatalf("退休后应重置 baseline/Armed, got Baseline=%d Armed=%v", s.Baseline, s.Armed)
	}
	if s.WMax != 2 {
		t.Fatalf("退休后 WMax 应重抽为 2, got %d", s.WMax)
	}
}

// TestStepWMaxZeroNeverRetires：WMaxHi==0（WMax 恒 0）→ 线程永不退休。
func TestStepWMaxZeroNeverRetires(t *testing.T) {
	p := RotatePolicy{WMaxHi: 0}
	limit := AutoCompactTokens("gpt-5.5")
	s := NewIdentityState("inst", p)
	tid := s.ThreadID

	for i := 0; i < 10; i++ {
		s = Step(s, limit, "gpt-5.5", p) // 上升沿 +1
		s = Step(s, 0, "gpt-5.5", p)     // 回落重新武装
	}
	if s.WindowN != 10 {
		t.Fatalf("10 次上升沿后 WindowN = %d, want 10", s.WindowN)
	}
	if s.ThreadID != tid {
		t.Fatalf("WMax=0 时不应退休: %q → %q", tid, s.ThreadID)
	}
}

// TestStepInitializesEmptyState：零值 state 先补一个线程再推进。
func TestStepInitializesEmptyState(t *testing.T) {
	p := RotatePolicy{WMaxHi: 0}
	var s IdentityState
	s = Step(s, 0, "gpt-5.5", p)
	if s.ThreadID == "" || s.WindowN != 0 || !s.Armed {
		t.Fatalf("零值 state 应被初始化为新线程, got %+v", s)
	}
}

// TestDrawWMaxNoRetire：WMaxHi==0 → 返回 0（不退休）。
func TestDrawWMaxNoRetire(t *testing.T) {
	if got := drawWMax(RotatePolicy{WMaxHi: 0}); got != 0 {
		t.Fatalf("WMaxHi=0 应返回 0, got %d", got)
	}
}

// TestDrawWMaxRange：抽样落在 [Lo,Hi] 且覆盖多个取值；区间倒置自动纠正。
func TestDrawWMaxRange(t *testing.T) {
	p := RotatePolicy{WMaxLo: 3, WMaxHi: 7}
	seen := map[uint64]bool{}
	for i := 0; i < 500; i++ {
		w := drawWMax(p)
		if w < 3 || w > 7 {
			t.Fatalf("drawWMax = %d, 应落在 [3,7]", w)
		}
		seen[w] = true
	}
	if len(seen) < 2 {
		t.Fatalf("500 次抽样只见到 %d 个取值，疑似退化为定值", len(seen))
	}

	inverted := RotatePolicy{WMaxLo: 7, WMaxHi: 3}
	for i := 0; i < 200; i++ {
		if w := drawWMax(inverted); w < 3 || w > 7 {
			t.Fatalf("倒置区间 drawWMax = %d, 应落在 [3,7]", w)
		}
	}
}
