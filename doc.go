// Package codexsdk 提供面向 Codex 上游的 OpenAI Responses 客户端库（WS + HTTP 双形态），
// 单一 raw 字节通道 API 面。
//
// # 边界
//
// 本库只负责协议 / 传输 / 鉴权 / 伪装：
//   - Responses WebSocket：Dial / 帧字节收发（文本与二进制帧原样透传）/ 心跳保活 /
//     关闭码透传（Close(status, reason)，急断 CloseNow）
//   - Responses HTTP：POST /v1/responses 请求构造 / 非流式响应 / 流式 SSE 事件帧提取
//   - 升级与请求鉴权注入（PAT 静态 / OAuth 刷新回调，Auth 接口）
//   - 伪装层（真实 codex 客户端形态对齐，对照见 IMPERSONATION.md）：默认
//     codex-tui UA/originator（0.147.0 + Ubuntu 指纹，用户拍板默认）、beta 头（现役唯一 2026-02-06）、头常量导出、
//     Send 帧顶层 key 白名单过滤（18 字段）、client_metadata 组装（8 key 恒发：
//     installation_id/session_id/thread_id/turn_id/window_id/turn-metadata/
//     traceparent/tracestate）、透传（HTTP 头 WithHeader / WS client_metadata
//     任意键 WithClientMetadata，只透传不解析——如 responses-lite 标记
//     HeaderResponsesLite / MetaResponsesLiteKey）、会话标识握手头（WithSession）、
//     x-codex-turn-state（WS：升级响应头签发 → 帧内 client_metadata 回传；
//     HTTP：仅响应侧捕获，请求不带头）、每帧新 trace 与 turn_id（UUIDv7）
//
// SDK 零协议解析：type / usage / 事件构造与业务语义（计费、透传编排、failover、
// 会话粘性、内容审核）全部在网关侧（go-proxy-mini）——网关在 SDK 交付的完整
// 字节上自行解析。不做事件层、不做模式枚举、不做任何业务钩子。
//
// # 性能语义（性能优先：懒构建 + 热路径低分配）
//
//   - 无全局可变状态：连接/客户端均按需构建，心跳 goroutine 仅连接存活期间存在
//   - WS 帧收发零额外分配：Send 直传帧字节（零拷贝）；Recv 返回 coder/websocket
//     每次 Read 独立分配的读缓冲（跨次调用有效，无需拷贝即可保留）
//   - 伪装层默认开启（白名单过滤 + client_metadata 注入 + 每帧 trace/turn_id），
//     Send 的 JSON 组装开销仅在开启时发生；WithPayloadFiltering(false) /
//     WithTraceAuto(false) / WithTurnAuto(false) 且无任何注入时回到
//     零拷贝零分配快速路径
//   - 常驻读循环是硬性要求：Ping 与心跳依赖 Recv 处理 pong 控制帧
//     （coder/websocket：Ping 必须与 Reader 并发，否则等不到 pong）；
//     网关透传编排天然常驻 Recv 循环，满足该前提
//   - HTTP 流式解析零拷贝：bufio.Scanner 复用缓冲 + 行切片提取 data: 帧内容，
//     回调内的原始字节引用 scanner 缓冲（仅在回调执行期间有效）
//   - HTTP 客户端懒构建：NewHTTPClient 零开销，首次 Do/Stream 才创建 http.Client
//     （连接池复用；WithTransport 可注入自定义 RoundTripper）
//
// # 上游协议与参考
//
// 目标协议为 OpenAI Responses WebSocket（现役唯一 beta：responses_websockets=
// 2026-02-06，仅 WS 握手注入；HTTP 默认不发 OpenAI-Beta，需要时 WithHeader
// 显式注入）。真实客户端行为对齐：WS 握手与 HTTP 请求均不发 trace 头（trace
// 只进每帧 client_metadata，每帧新值）；session-id/thread-id/x-client-request-id/
// x-codex-window-id 会话级握手头；x-codex-turn-state 仅响应侧捕获：WS 路径
// 升级响应头签发 → 帧内 client_metadata 回传（Client.TurnState 缓存 + 网关
// SetTurnState("") 清除，跨轮不得回传）；HTTP 路径请求不携带该头，捕获后经
// TurnState() 暴露。responses-lite 非独立端点：与 /responses 同端点同事件集，
// 仅 internal 标记区分——HTTP 头 x-openai-internal-codex-responses-lite（WithHeader
// 透传）与 WS client_metadata 键 ws_request_header_x_openai_internal_codex_responses_lite
// （WithClientMetadata 透传），SDK 只透传不解析。
// 传输常量对齐参考实现：16MiB ReadLimit（coder 默认 32KB 过小）、
// CompressionContextTakeover 压缩、WS 层 ping 心跳（30s 间隔 + 2s 超时）、
// data: SSE 行提取与 [DONE] 终止、response.create 18 字段白名单、
// client_metadata 恒发 8 key 集合（session_id/thread_id/turn_id 为 snake_case，
// trace 的 metadata key 名与头名不同）。
//
// 依赖：github.com/coder/websocket（纯标准库实现，无 CGO）+
// github.com/tidwall/gjson / github.com/tidwall/sjson（raw JSON 修补）。
package codexsdk
