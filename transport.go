package codexsdk

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
