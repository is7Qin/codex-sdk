package codexsdk

import (
	"errors"

	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

// Transport 是 HTTPClient 的 Responses 出站传输选择。零值 "" 与
// TransportHTTP 等价（默认）；任何未知值同样按 HTTP 处理——WS 只在显式
// TransportWS 时启用，未知配置不会静默切走传输。
type Transport string

const (
	// TransportHTTP 走既有 HTTP POST /responses（SSE）路径：StreamBody 交付
	// 未读的原始 *http.Response，Responses 走 Stream + 聚合器。默认 / 零值。
	TransportHTTP Transport = "http"
	// TransportWS 走上游 Responses WebSocket（Dial + 单帧 response.create），
	// 对外形状与 HTTP 完全一致（*http.Response{200, Body: SSE 字节} /
	// HTTPResponse 合成体），调用方按 HTTP 消费即可。
	TransportWS Transport = "ws"
)

// WithResponsesTransport 选择 Responses 出站传输（只影响 StreamBody 与
// Responses 两条取数路径；Dial、Do、Stream、GenerateImage / Search /
// GetUsage 不受影响，恒走各自的既有路径）。
//
// 默认 / 零值 / 未知值 = TransportHTTP，且 HTTP 路径零改动（分支之外的代码
// 与行为逐字节相同，不提前构造任何 WS 侧对象）。
func WithResponsesTransport(t Transport) Option {
	return func(o *options) { o.responsesTransport = t }
}

// responsesCreateType 是 WS response.create 帧的 type 值（WS 面恒带，HTTP
// 面没有该字段）。
const responsesCreateType = "response.create"

// errResponsesFrameInvalid 是请求体无法构造成 WS response.create 帧（本地输入
// 错误，非上游错误）：非 JSON / 非对象根。
//
// 为什么本地拒绝而不是原样发出：HTTP 面对坏 body 是把字节原样发给上游、由
// 上游 400 兜底；WS 面没有这条等价路径——sjson/gjson 对 `null`/标量/数组根
// 会**静默**把请求体改写成 `{}`（本库既有记录见 forceCodexStoreFalse 附近的
// 同款行为），发出去就成了"一个只剩 type/stream 的空帧"，与 HTTP 臂语义立刻
// 不同。白名单过滤后为空（ErrEmptyFrame）也归在此列。
var errResponsesFrameInvalid = errors.New("codexsdk: 无法构造 ws response.create 帧：请求体必须是 JSON 对象")

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
// 回传）：它们在 Client.Send → prepareFrame 内无条件执行，是 WS 面共用的唯一
// 归一入口；本函数只处理 HTTP→WS 的协议形状差。
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
