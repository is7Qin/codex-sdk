package codexsdk

import "testing"

// TestWindowIDDerivesWindowN：window_id = "{thread_id}:{window_n}"，Session 同步。
func TestWindowIDDerivesWindowN(t *testing.T) {
	s := IdentityState{ThreadID: "0193-tid", WindowN: 3}
	if got := s.WindowID(); got != "0193-tid:3" {
		t.Fatalf("WindowID() = %q, want %q", got, "0193-tid:3")
	}
	if got := s.Session().WindowID; got != "0193-tid:3" {
		t.Fatalf("Session().WindowID = %q, want %q", got, "0193-tid:3")
	}
}

// TestNewIdentityState：开新线程的初值。
func TestNewIdentityState(t *testing.T) {
	s := NewIdentityState("inst-1", RotatePolicy{WMaxLo: 3, WMaxHi: 5})
	if s.InstallationID != "inst-1" {
		t.Fatalf("InstallationID = %q, want %q", s.InstallationID, "inst-1")
	}
	if s.ThreadID == "" {
		t.Fatal("ThreadID 不应为空")
	}
	if s.Turns != 0 {
		t.Fatalf("Turns = %d, want 0", s.Turns)
	}
	if s.WindowN != 0 {
		t.Fatalf("WindowN = %d, want 0", s.WindowN)
	}
	if want := windowSpan(s.ThreadID, 0); s.NextWindowAt != want {
		t.Fatalf("NextWindowAt = %d, want %d", s.NextWindowAt, want)
	}
	if s.WMax < 3 || s.WMax > 5 {
		t.Fatalf("WMax = %d, want ∈[3,5]", s.WMax)
	}
}

// TestWindowSpanRangeAndIrregular：确定性纯函数、落 [spanLo,spanHi]、边界不等间隔。
func TestWindowSpanRangeAndIrregular(t *testing.T) {
	const tid = "0193-fixed-tid-for-span"
	min, max := uint64(1<<63), uint64(0)
	for i := uint64(0); i < 16; i++ {
		got := windowSpan(tid, i)
		if got < spanLo || got > spanHi {
			t.Fatalf("windowSpan(%q,%d) = %d, want ∈[%d,%d]", tid, i, got, spanLo, spanHi)
		}
		if again := windowSpan(tid, i); again != got {
			t.Fatalf("windowSpan 非纯函数：%d vs %d", got, again)
		}
		if got < min {
			min = got
		}
		if got > max {
			max = got
		}
	}
	if max-min < 1 {
		t.Fatalf("窗口跨度不等间隔：min=%d max=%d", min, max)
	}
}

// TestStepAccumulatesTurns：WMaxHi=0 不退休，每 Step 一次 Turns+1。
func TestStepAccumulatesTurns(t *testing.T) {
	p := RotatePolicy{}
	s := NewIdentityState("i", p)
	for i := 1; i <= 100; i++ {
		s = Step(s, p)
		if s.Turns != uint64(i) {
			t.Fatalf("第 %d 次 Step 后 Turns=%d, want %d", i, s.Turns, i)
		}
	}
}

// TestStepWindowBoundary：跨过 span(0) 时 WindowN++ 且阈值叠加 span(1)。
func TestStepWindowBoundary(t *testing.T) {
	p := RotatePolicy{}
	s := NewIdentityState("i", p)
	s0 := windowSpan(s.ThreadID, 0)
	s1 := windowSpan(s.ThreadID, 1)
	for i := uint64(0); i < s0-1; i++ {
		s = Step(s, p)
	}
	if s.WindowN != 0 {
		t.Fatalf("前 %d 次 Step 后 WindowN=%d, want 0", s0-1, s.WindowN)
	}
	s = Step(s, p) // 第 s0 次
	if s.WindowN != 1 {
		t.Fatalf("第 %d 次 Step 后 WindowN=%d, want 1", s0, s.WindowN)
	}
	if want := s0 + s1; s.NextWindowAt != want {
		t.Fatalf("NextWindowAt = %d, want %d", s.NextWindowAt, want)
	}
}

// TestStepRetiresAtWMax：WindowN 达 WMax 时退休换新线程。
func TestStepRetiresAtWMax(t *testing.T) {
	p := RotatePolicy{WMaxLo: 2, WMaxHi: 2}
	s := NewIdentityState("i", p)
	old := s.ThreadID
	total := windowSpan(old, 0) + windowSpan(old, 1)
	for i := uint64(1); i <= total-1; i++ {
		s = Step(s, p)
		if s.ThreadID != old {
			t.Fatalf("第 %d 次 Step 提前退休", i)
		}
	}
	s = Step(s, p) // 第 total 次
	if s.ThreadID == old {
		t.Fatal("第 total 次 Step 应退休换新线程")
	}
	if s.Turns != 0 {
		t.Fatalf("退休后 Turns=%d, want 0", s.Turns)
	}
	if s.WindowN != 0 {
		t.Fatalf("退休后 WindowN=%d, want 0", s.WindowN)
	}
	if want := windowSpan(s.ThreadID, 0); s.NextWindowAt != want {
		t.Fatalf("退休后 NextWindowAt = %d, want %d", s.NextWindowAt, want)
	}
}

// TestStepInitializesEmptyState：空状态先开新线程再推进。
func TestStepInitializesEmptyState(t *testing.T) {
	var s IdentityState
	s = Step(s, RotatePolicy{WMaxHi: 0})
	if s.ThreadID == "" {
		t.Fatal("ThreadID 不应为空")
	}
	if s.Turns != 1 {
		t.Fatalf("Turns = %d, want 1", s.Turns)
	}
}

// TestStepWMaxZeroNeverRetires：WMaxHi=0 → 永不退休。
func TestStepWMaxZeroNeverRetires(t *testing.T) {
	p := RotatePolicy{WMaxHi: 0}
	s := NewIdentityState("i", p)
	old := s.ThreadID
	for i := 0; i < 10*spanHi; i++ {
		s = Step(s, p)
	}
	if s.ThreadID != old {
		t.Fatalf("ThreadID 变了：%q → %q", old, s.ThreadID)
	}
}

// TestDefaultRotatePolicy：默认 {16,48}。
func TestDefaultRotatePolicy(t *testing.T) {
	p := DefaultRotatePolicy()
	if p.WMaxLo != 16 || p.WMaxHi != 48 {
		t.Fatalf("DefaultRotatePolicy = %+v, want {16 48}", p)
	}
}

// TestDrawWMax：WMaxHi==0 → 0；正序/反序均落 [3,7]。
func TestDrawWMax(t *testing.T) {
	if got := drawWMax(RotatePolicy{WMaxLo: 3, WMaxHi: 0}); got != 0 {
		t.Fatalf("WMaxHi=0 时 drawWMax = %d, want 0", got)
	}
	for _, p := range []RotatePolicy{{WMaxLo: 3, WMaxHi: 7}, {WMaxLo: 7, WMaxHi: 3}} {
		for i := 0; i < 200; i++ {
			got := drawWMax(p)
			if got < 3 || got > 7 {
				t.Fatalf("drawWMax(%+v) = %d, want ∈[3,7]", p, got)
			}
		}
	}
}
