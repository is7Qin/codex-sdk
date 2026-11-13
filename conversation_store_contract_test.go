package codexsdk_test

import (
	"sync/atomic"
	"testing"
	"time"

	codexsdk "github.com/is7Qin/codex-sdk"
	"github.com/is7Qin/codex-sdk/internal/storecontract"
)

// TestConversationStoreContract（内存实现；Redis 实现见 redisstore 包同名测试）。
func TestConversationStoreContract(t *testing.T) {
	storecontract.Run(t, func(t *testing.T) storecontract.Harness {
		var off atomic.Int64
		base := time.Unix(1_800_000_000, 0)
		now := func() time.Time { return base.Add(time.Duration(off.Load())) }
		return storecontract.Harness{
			Store:   codexsdk.NewMemoryConversationStoreWithClock(now),
			Advance: func(d time.Duration) { off.Add(int64(d)) },
		}
	})
}
