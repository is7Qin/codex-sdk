package codexsdk

import "sync/atomic"

// ShapeStats 是请求整形的进程级计数快照（单调递增，自进程启动起累计）。SDK 不打
// 日志，只提供计数。
type ShapeStats struct {
	Anonymous             int64 // 匿名请求数（含身份残缺）
	PartialIdentity       int64 // Session.SessionID 为空而其余身份字段非空、按匿名处理的请求数
	DuplicateKeys         int64 // 被丢弃的重复顶层键数（同名键只认第一次）
	NonStringInstructions int64 // 非字符串 instructions（丢弃）数
	DualToolSources       int64 // input 已有 additional_tools 而顶层 tools 被丢弃的请求数
	UnknownModels         int64 // 模型表外的请求数
	// IncrementalReinjects 是 WS→WS 带 previous_response_id 的增量帧因指令/工具
	// 插入项与本连接上次不同而重新插入的次数（相同则不插，不计；连接上此前没有
	// 插入记录时的第一次插入也不计）。
	IncrementalReinjects int64
}

// shapeCounters 是 ShapeStats 的原子计数器。
type shapeCounters struct {
	anonymous             atomic.Int64
	partialIdentity       atomic.Int64
	duplicateKeys         atomic.Int64
	nonStringInstructions atomic.Int64
	dualToolSources       atomic.Int64
	unknownModels         atomic.Int64
	incrementalReinjects  atomic.Int64
}

func (c *shapeCounters) snapshot() ShapeStats {
	return ShapeStats{
		Anonymous:             c.anonymous.Load(),
		PartialIdentity:       c.partialIdentity.Load(),
		DuplicateKeys:         c.duplicateKeys.Load(),
		NonStringInstructions: c.nonStringInstructions.Load(),
		DualToolSources:       c.dualToolSources.Load(),
		UnknownModels:         c.unknownModels.Load(),
		IncrementalReinjects:  c.incrementalReinjects.Load(),
	}
}

// shapeStats 是进程级整形计数（所有 HTTPClient / Client 共用）。
var shapeStats shapeCounters

// ShapeStatsSnapshot 返回进程级整形计数快照（HTTP→HTTP、HTTP→WS、WS→WS 合计）。
func ShapeStatsSnapshot() ShapeStats { return shapeStats.snapshot() }
