package codexsdk

import "time"

// NewMemoryConversationStoreWithClock 只在测试中导出：契约测试推进内存实现的时钟。
func NewMemoryConversationStoreWithClock(now func() time.Time) ConversationStore {
	return newMemoryStore(now)
}
