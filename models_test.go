package codexsdk

import "testing"

// TestModelLimitsForKnown：目录内已知 slug 返回其窗口（gpt-5.5 = 272000/272000）。
func TestModelLimitsForKnown(t *testing.T) {
	l := ModelLimitsFor("gpt-5.5")
	if l.ContextWindow != 272000 || l.MaxContextWindow != 272000 {
		t.Fatalf("gpt-5.5 窗口应为 272000/272000, got %d/%d", l.ContextWindow, l.MaxContextWindow)
	}
	if l.AutoCompactTokenLimit != nil {
		t.Fatalf("gpt-5.5 auto_compact_token_limit 应为 nil, got %v", *l.AutoCompactTokenLimit)
	}
}

// TestModelLimitsForUnknownFallback：未知 slug 走 fallback（对齐 codex
// model_info_from_slug：272000/272000）。
func TestModelLimitsForUnknownFallback(t *testing.T) {
	l := ModelLimitsFor("no-such-model-xyz")
	if l.ContextWindow != 272000 || l.MaxContextWindow != 272000 {
		t.Fatalf("未知 slug 应 fallback 272000/272000, got %d/%d", l.ContextWindow, l.MaxContextWindow)
	}
	if l.AutoCompactTokenLimit != nil {
		t.Fatalf("未知 slug auto_compact_token_limit 应为 nil")
	}
}

// TestResolvedContextWindow：resolved = context_window ?? max_context_window；
// 未知 slug → fallback 的 context_window。
func TestResolvedContextWindow(t *testing.T) {
	cases := []struct {
		slug string
		want int64
	}{
		{"gpt-5.5", 272000},                 // context_window 命中
		{"gpt-daybreak-red-latest", 372000}, // 372000/372000
		{"gpt-6-astra", 272000},             // 272000/872000 → 取 context_window
		{"unknown-slug", 272000},            // fallback
	}
	for _, c := range cases {
		if got := ResolvedContextWindow(c.slug); got != c.want {
			t.Fatalf("ResolvedContextWindow(%q) = %d, want %d", c.slug, got, c.want)
		}
	}
}

// TestAutoCompactTokensFormula：θ_w = resolved × 9/10（目录内该字段全为 null）。
func TestAutoCompactTokensFormula(t *testing.T) {
	cases := []struct {
		slug string
		want int64
	}{
		{"gpt-5.5", 272000 * 9 / 10},                 // 244800
		{"gpt-daybreak-red-latest", 372000 * 9 / 10}, // 334800
		{"unknown-slug", 272000 * 9 / 10},            // fallback
	}
	for _, c := range cases {
		if got := AutoCompactTokens(c.slug); got != c.want {
			t.Fatalf("AutoCompactTokens(%q) = %d, want %d", c.slug, got, c.want)
		}
	}
}

// TestAutoCompactTokensHonorsLimit：auto_compact_token_limit 非 nil 且更小时取
// min（目录当前无此值，用临时目录验证派生公式）。
func TestAutoCompactTokensHonorsLimit(t *testing.T) {
	orig := modelCatalog
	t.Cleanup(func() { modelCatalog = orig })

	small := int64(100000)
	big := int64(999999)
	modelCatalog = map[string]ModelLimits{
		"t-small": {ContextWindow: 272000, MaxContextWindow: 272000, AutoCompactTokenLimit: &small},
		"t-big":   {ContextWindow: 272000, MaxContextWindow: 272000, AutoCompactTokenLimit: &big},
	}
	if got := AutoCompactTokens("t-small"); got != 100000 {
		t.Fatalf("limit 更小时应取 limit, got %d", got)
	}
	if got := AutoCompactTokens("t-big"); got != 244800 {
		t.Fatalf("limit 更大时应取 resolved×9/10, got %d", got)
	}
}
