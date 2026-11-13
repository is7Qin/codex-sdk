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

	// HTTP→WS 预热（新建上游连接且 Identity.Turn.First 时，SDK 先发一帧
	// generate:false 的预热帧，读完其响应再发真实帧）。
	Prewarms int64 // 发出的预热帧数
	// PrewarmConnFailures：预热阶段（写预热帧到读到 response.completed）连接出错、
	// EOF、空闲超时或 ctx 结束。真实请求未发出，以连接类错误返回调用方。
	PrewarmConnFailures int64
	// PrewarmRejections：预热收到 4xx（含 429）error 帧，以 *HTTPError 直接返回。
	PrewarmRejections int64
	// PrewarmRedials：预热收到 response.failed / response.incomplete、无状态或 5xx
	// 的 error 帧；关闭该连接、重拨一次、不预热、发全量。
	PrewarmRedials int64
	// PrewarmChained / PrewarmUnchained：预热成功后真实帧带 / 不带
	// previous_response_id（不带 = 真实 text 带 format，或预热响应没有 id）。
	PrewarmChained   int64
	PrewarmUnchained int64
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
	prewarms              atomic.Int64
	prewarmConnFailures   atomic.Int64
	prewarmRejections     atomic.Int64
	prewarmRedials        atomic.Int64
	prewarmChained        atomic.Int64
	prewarmUnchained      atomic.Int64
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
		Prewarms:              c.prewarms.Load(),
		PrewarmConnFailures:   c.prewarmConnFailures.Load(),
		PrewarmRejections:     c.prewarmRejections.Load(),
		PrewarmRedials:        c.prewarmRedials.Load(),
		PrewarmChained:        c.prewarmChained.Load(),
		PrewarmUnchained:      c.prewarmUnchained.Load(),
	}
}

// shapeStats 是进程级整形计数（所有 HTTPClient / Client 共用）。
var shapeStats shapeCounters

// ShapeStatsSnapshot 返回进程级整形计数快照（HTTP→HTTP、HTTP→WS、WS→WS 合计）。
func ShapeStatsSnapshot() ShapeStats { return shapeStats.snapshot() }
