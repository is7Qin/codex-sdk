package codexsdk

import (
	"errors"
	"time"

	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

// responsesCreateType 是 WS response.create 帧的 type 值（WS 面恒带，HTTP
// 面没有该字段）。
const responsesCreateType = "response.create"

// errResponsesFrameInvalid 是请求体无法构造成 WS response.create 帧（本地输入
// 错误，非上游错误）：非 JSON / 非对象根。
//
// 为什么本地拒绝而不是原样发出：HTTP 面对坏 body 是把字节原样发给上游、由
// 上游 400 兜底；WS 面没有这条等价路径——sjson/gjson 对 `null`/标量/数组根
// 会**静默**把请求体改写成 `{}`，发出去就成了"一个只剩 type/stream 的空帧"，
// 与 HTTP 臂语义立刻不同。白名单过滤后为空（ErrEmptyFrame）也归在此列。
var errResponsesFrameInvalid = errors.New("codexsdk: 无法构造 ws response.create 帧：请求体必须是 JSON 对象")

// encodeMode 选择 wire 编码形态。
type encodeMode uint8

const (
	// encodeNativeWS 只做 WS 归一（白名单 + store:false + client_metadata），
	// 用于公开 Client.Send 的原生帧。
	encodeNativeWS encodeMode = iota
	// encodeWSResponses 额外做 HTTP→WS 的协议形状差（type 覆写 / 删续接键 /
	// 强制 stream），用于 Responses 传输臂。
	encodeWSResponses
)

// frameInput 是一次编码的请求级输入（快照，不随后续状态变化）。
type frameInput struct {
	TurnState string    // 请求级 turn-state 快照
	Turn      uint64    // 0 = 不生成 turn_metadata
	Now       time.Time // 环境时间改写基准
}

// frameCodec 负责把请求体 / 原生帧编码成 wire 字节。**无可变状态**（配置在
// 构造期固定），可并发使用；请求级差异全部经 frameInput 传入。
type frameCodec struct {
	meta            *CodexMeta
	metaPassthrough []metadataEntry
	session         *Session
	trace           *TraceContext
	traceAuto       bool
	turnAuto        bool
	turnProvider    func(turn uint64) string
}

// newFrameCodec 从 options 构造 codec（值拷贝语义：之后改 options 不影响它）。
func newFrameCodec(o *options) *frameCodec {
	return &frameCodec{
		meta:            o.meta,
		metaPassthrough: o.metaPassthrough,
		session:         o.session,
		trace:           o.trace,
		traceAuto:       o.traceAuto,
		turnAuto:        o.turnAuto,
		turnProvider:    o.turnProvider,
	}
}

// encode 产出 wire 字节：先按形态做协议形状归一，再套伪装层。
func (fc *frameCodec) encode(raw []byte, mode encodeMode, in frameInput) ([]byte, error) {
	frame := raw
	if mode == encodeWSResponses {
		var err error
		frame, err = buildResponsesCreateFrame(frame)
		if err != nil {
			return nil, err
		}
	}
	return fc.normalize(frame, in)
}

// normalize 应用伪装层：WS 归一（无条件生效——FilterCodexWsPayload 顶层白名单
// 过滤 + 强制 store:false）+ client_metadata 组装。优先级：CodexMeta 静态值 >
// WithSession > turn-state 回传 > WithClientMetadata 透传 > 自动机制（turn_id /
// trace）> turn_metadata 回调；注入**恒覆盖**帧内已有同 key——codex 面客户端自带
// 的 client_metadata 永不透传。
func (fc *frameCodec) normalize(frame []byte, in frameInput) ([]byte, error) {
	now := in.Now
	if now.IsZero() {
		now = time.Now()
	}
	frame = RewriteEnvironmentContextTime(frame, now)
	frame, err := FilterCodexWsPayload(frame) // 始终过滤（不再有开关）
	if err != nil {
		return nil, err
	}
	frame = forceCodexStoreFalse(frame)
	// 非法 JSON 放弃注入、帧原样返回（对齐 HTTP 面 injectResponsesClientMetadata
	// 前置守卫——实测 sjson.SetRawBytes 对非法 JSON 静默产出损坏字节且 err=nil）。
	if !gjson.ValidBytes(frame) {
		return frame, nil
	}
	// 惰性组装 entries：先在栈上预留 8 项（典型身份键数），超出才转堆——
	// shared helper 下须显式保留栈缓冲，否则返回切片的 helper 会使 entries
	// 逃逸到堆、平白多一次分配（HTTP 面同理）。
	var entryBuf [8]metadataEntry
	entries := metaEntries(entryBuf[:0])
	entries = entries.withCodexMetaIdentity(fc.meta)
	if m := fc.meta; m != nil {
		entries = entries.add(codexMetaTurnMetadataKey, m.TurnMetadata)
		entries = entries.add(codexMetaTraceparentKey, m.Traceparent)
		entries = entries.add(codexMetaTracestateKey, m.Tracestate)
	}
	entries = entries.withSessionIdentity(fc.session)
	// turn-state 回传（TurnState 非空时帧 metadata 恒带）：真实 codex WS
	// 发送路径显式追加注入（client.rs:1626-1631，帧体携带 :1702-1711），与
	// HTTP 头面（client.rs:1202）构成双面机制——此注入与真实逐点一致，勿删
	// （完整实证见 doc.go"上游协议与参考"）。
	if in.TurnState != "" {
		entries = entries.add(codexMetaTurnStateKey, in.TurnState)
	}
	// 调用方透传键（WithClientMetadata）：只透传不解析，恒覆盖帧内已有同 key
	//（网关值为准——注入顺序在自动机制之前，同 key 由网关决定）。
	for _, e := range fc.metaPassthrough {
		entries = entries.add(e.key, e.value)
	}
	// turn_id 恒带且覆盖：帧内已有 client_metadata.turn_id 不参与透传——
	// 无静态值时每帧自动生成 UUIDv7（CodexMeta.TurnID 静态值优先），网关值恒为准。
	if fc.turnAuto && (fc.meta == nil || fc.meta.TurnID == "") {
		entries = entries.add(codexMetaTurnKey, NewUUIDv7())
	}
	// 每帧 trace：外部 WithTraceContext 静态值优先，否则每帧自动生成
	// （真实客户端每请求新 span，同轮多请求 traceparent 不同；帧内已有
	// traceparent 不参与透传，网关值恒为准）。
	if fc.trace != nil {
		entries = entries.add(codexMetaTraceparentKey, fc.trace.Traceparent)
		entries = entries.add(codexMetaTracestateKey, fc.trace.Tracestate)
	} else if fc.traceAuto {
		tc := NewTraceContext()
		entries = entries.add(codexMetaTraceparentKey, tc.Traceparent)
		entries = entries.add(codexMetaTracestateKey, tc.Tracestate)
	}
	if fc.turnProvider != nil && in.Turn > 0 {
		entries = entries.add(codexMetaTurnMetadataKey, fc.turnProvider(in.Turn))
	}
	return injectClientMetadataKeys(frame, entries), nil
}

// buildResponsesCreateFrame 把 HTTP Responses 请求体转成 WS response.create 帧。
// 规则（缺一不可——写错会让两个传输不可比）：
//  1. 自校验 JSON 合法性**与对象根**（见 errResponsesFrameInvalid）；
//  2. 覆写 type:"response.create"（不是"缺了才补"——HTTP 面没有 type，客户端
//     也可以塞任意值）；
//  3. 删 previous_response_id / generate（HTTP 面没有这两项，透传等于让 WS 走
//     服务端续接，两臂语义立刻不同）；
//  4. 强制 stream:true（stream:false 的 WS 语义未知；非流式由聚合器合成）；
//  5. 过 FilterCodexWsPayload（19 键白名单）；白名单后为空 → ErrEmptyFrame。
//
// 帧的其余归一**不在这里**（store:false 强制、client_metadata 组装、turn-state
// 回传）：它们在 frameCodec.normalize 内无条件执行，是 WS 面共用的唯一归一
// 入口；本函数只处理 HTTP→WS 的协议形状差。
func buildResponsesCreateFrame(payload []byte) ([]byte, error) {
	if !gjson.ValidBytes(payload) || !isJSONObjectRoot(payload) {
		return nil, errResponsesFrameInvalid
	}
	frame, err := sjson.SetBytes(payload, "type", responsesCreateType)
	if err != nil {
		return nil, errResponsesFrameInvalid
	}
	// 删除吞错：键为静态字面量、输入已过 gjson.ValidBytes，删除不会失败
	//（与 filterCodexPayload 吞 sjson 错误同款理由）。
	frame, _ = sjson.DeleteBytes(frame, "previous_response_id")
	frame, _ = sjson.DeleteBytes(frame, "generate")
	if frame, err = sjson.SetBytes(frame, "stream", true); err != nil {
		return nil, errResponsesFrameInvalid
	}
	return FilterCodexWsPayload(frame)
}

// isJSONObjectRoot 报告 raw 是否为对象根（`{` 开头，允许前导 JSON 空白）。
// 调用方须先过 gjson.ValidBytes——本函数只做根形状判定，不校验结构。
func isJSONObjectRoot(raw []byte) bool {
	for i := 0; i < len(raw); i++ {
		switch raw[i] {
		case ' ', '\t', '\n', '\r':
			continue
		default:
			return raw[i] == '{'
		}
	}
	return false
}
