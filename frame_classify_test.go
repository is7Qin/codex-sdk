package codexsdk

import (
	"strings"
	"testing"
)

// TestClassifyAuthFatalFrame_HitViaCodeAndType 命中双路径（error.code / error.type）
// × 大小写变体（upper / lower / mixed），返回 Code 小写 + Raw 原帧。
func TestClassifyAuthFatalFrame_HitViaCodeAndType(t *testing.T) {
	cases := []struct {
		name string
		code string
		path string
	}{
		{"code lower revoked", "token_revoked", "error.code"},
		{"code upper revoked", "TOKEN_REVOKED", "error.code"},
		{"code mixed revoked", "Token_Revoked", "error.code"},
		{"code lower invalidated", "token_invalidated", "error.code"},
		{"code upper invalidated", "TOKEN_INVALIDATED", "error.code"},
		{"code mixed invalidated", "Token_Invalidated", "error.code"},
		{"type lower revoked", "token_revoked", "error.type"},
		{"type upper revoked", "TOKEN_REVOKED", "error.type"},
		{"type mixed invalidated", "Token_Invalidated", "error.type"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var frame string
			if tc.path == "error.code" {
				frame = `{"type":"error","error":{"code":"` + tc.code + `"}}`
			} else {
				frame = `{"type":"error","error":{"type":"` + tc.code + `"}}`
			}
			got := ClassifyAuthFatalFrame([]byte(frame))
			if got == nil {
				t.Fatalf("ClassifyAuthFatalFrame 应命中 %s=%q, got nil", tc.path, tc.code)
			}
			if got.Code != strings.ToLower(tc.code) {
				t.Fatalf("Code = %q, 期望 %q", got.Code, strings.ToLower(tc.code))
			}
			if string(got.Raw) != frame {
				t.Fatalf("Raw 应原帧透传")
			}
			// error.code 与 error.type 均覆盖：另一路径同样命中
		})
	}
}

// TestClassifyAuthFatalFrame_TypeCaseInsensitive type 字段值大小写经 EqualFold 兜底，
// 但热路径预筛仅匹配 `"type":"error"` 精确小写；大写变体预筛漏过 → nil（契约内，
// 上游固定小写，大小写兜底仅在预筛命中后生效）。
func TestClassifyAuthFatalFrame_TypeCaseInsensitive(t *testing.T) {
	// 小写命中
	frame := `{"type":"error","error":{"code":"token_revoked"}}`
	if got := ClassifyAuthFatalFrame([]byte(frame)); got == nil {
		t.Fatal("type=error 应命中")
	}
	// 大写变体因预筛精确匹配而漏过 → nil（符合热路径契约）
	for _, typ := range []string{"Error", "ERROR", "ErRoR"} {
		f := `{"type":"` + typ + `","error":{"code":"token_revoked"}}`
		if got := ClassifyAuthFatalFrame([]byte(f)); got != nil {
			t.Fatalf("type=%q 预筛精确匹配应漏过, got %v", typ, got)
		}
	}
	// 码值本身大小写不敏感（预筛命中后 EqualFold）
	for _, code := range []string{"TOKEN_REVOKED", "Token_Revoked"} {
		f := `{"type":"error","error":{"code":"` + code + `"}}`
		if got := ClassifyAuthFatalFrame([]byte(f)); got == nil {
			t.Fatalf("code=%q 应大小写不敏感命中", code)
		}
	}
}

// TestClassifyAuthFatalFrame_NonErrorFrameContainingMarker 业务帧误含标记字符串不判死。
func TestClassifyAuthFatalFrame_NonErrorFrameContainingMarker(t *testing.T) {
	// 帧的 type 非 error，但内容字符串中包含 `"type":"error"` 子串
	frame := `{"type":"response.output_text.delta","delta":"prefix \"type\":\"error\" suffix","error":{"code":"token_revoked"}}`
	if got := ClassifyAuthFatalFrame([]byte(frame)); got != nil {
		t.Fatalf("非 error 事件帧误含标记不应判死, got %v", got)
	}
	// 另一形态：type 缺失但含标记子串
	frame2 := `{"foo":"\"type\":\"error\"","error":{"code":"token_invalidated"}}`
	if got := ClassifyAuthFatalFrame([]byte(frame2)); got != nil {
		t.Fatalf("非 error 帧不应判死, got %v", got)
	}
}

// TestClassifyAuthFatalFrame_BusinessFrame 普通业务帧不判死。
func TestClassifyAuthFatalFrame_BusinessFrame(t *testing.T) {
	frames := []string{
		`{"type":"response.completed","id":"resp_1"}`,
		`{"type":"response.output_text.delta","delta":"hello"}`,
		`{"type":"error","error":{"code":"invalid_request_error","message":"bad"}}`,
		`{"type":"error","error":{"code":"rate_limit_exceeded"}}`,
	}
	for _, f := range frames {
		if got := ClassifyAuthFatalFrame([]byte(f)); got != nil {
			t.Fatalf("业务帧 %s 不应判死, got %v", f, got)
		}
	}
}

// TestClassifyAuthFatalFrame_EmptyFrame 空帧不判死。
func TestClassifyAuthFatalFrame_EmptyFrame(t *testing.T) {
	if got := ClassifyAuthFatalFrame(nil); got != nil {
		t.Fatalf("nil 帧不应判死")
	}
	if got := ClassifyAuthFatalFrame([]byte(``)); got != nil {
		t.Fatalf("空帧不应判死")
	}
	if got := ClassifyAuthFatalFrame([]byte(`{}`)); got != nil {
		t.Fatalf("空对象不应判死")
	}
}

// TestClassifyAuthFatalFrame_DetailUnauthorizedNotIncluded detail=="Unauthorized" 不并入帧分类。
func TestClassifyAuthFatalFrame_DetailUnauthorizedNotIncluded(t *testing.T) {
	frame := `{"type":"error","detail":"Unauthorized"}`
	if got := ClassifyAuthFatalFrame([]byte(frame)); got != nil {
		t.Fatalf("detail Unauthorized 不应触发帧判死, got %v", got)
	}
	frame2 := `{"type":"error","detail":"unauthorized","error":{"code":"something_else"}}`
	if got := ClassifyAuthFatalFrame([]byte(frame2)); got != nil {
		t.Fatalf("仅 detail 不应判死, got %v", got)
	}
}

// TestClassifyAuthFatalFrame_ZeroAllocOnMiss 热路径未命中零分配。
func TestClassifyAuthFatalFrame_ZeroAllocOnMiss(t *testing.T) {
	// 未含标记的业务帧：bytes.Contains 预筛直接返回，不应分配
	business := []byte(`{"type":"response.completed","id":"resp_1"}`)
	allocs := testing.AllocsPerRun(1000, func() {
		_ = ClassifyAuthFatalFrame(business)
	})
	if allocs != 0 {
		t.Fatalf("未命中帧零分配契约破坏: allocs=%.1f, 期望 0", allocs)
	}
	// 含标记但 type 非 error：预筛命中但二次校验返回 nil，分配应极小（gjson 解析）
	// 此路径允许少量分配，但主热路径（无标记）必须零分配已在上断言
	empty := []byte(``)
	allocs2 := testing.AllocsPerRun(1000, func() {
		_ = ClassifyAuthFatalFrame(empty)
	})
	if allocs2 != 0 {
		t.Fatalf("空帧零分配契约破坏: allocs=%.1f, 期望 0", allocs2)
	}
	// 完全不含标记的空业务帧
	plain := []byte(`{"type":"response.delta","x":1}`)
	allocs3 := testing.AllocsPerRun(1000, func() {
		_ = ClassifyAuthFatalFrame(plain)
	})
	if allocs3 != 0 {
		t.Fatalf("普通帧零分配契约破坏: allocs=%.1f, 期望 0", allocs3)
	}
}
