package codexsdk

import (
	"context"
	"strings"
	"testing"
)

// S11：WithHeader 的身份/turn-state 保留键在构造期 panic，大小写不敏感，且
// NewHTTPClient 与 Dial 两条构造入口都覆盖。
func TestS11ReservedHeaderPanics(t *testing.T) {
	keys := []string{
		HeaderSessionID,
		HeaderThreadID,
		HeaderClientRequestID,
		HeaderWindowID,
		HeaderInstallationID,
		HeaderTurnState,
	}
	// 追加大小写变体：保留键匹配必须 EqualFold。
	keys = append(keys, strings.ToUpper(HeaderSessionID), "X-CoDeX-TuRn-StAtE")

	for _, key := range keys {
		key := key
		t.Run("NewHTTPClient/"+key, func(t *testing.T) {
			expectPanic(t, func() {
				_ = NewHTTPClient(PAT("t"), WithHeader(key, "x"))
			})
		})
		t.Run("Dial/"+key, func(t *testing.T) {
			expectPanic(t, func() {
				_, _ = Dial(context.Background(), PAT("t"), Identity{}, WithHeader(key, "x"))
			})
		})
	}
}

func expectPanic(t *testing.T, fn func()) {
	t.Helper()
	defer func() {
		if recover() == nil {
			t.Fatal("期望构造期 panic，got nil")
		}
	}()
	fn()
}
