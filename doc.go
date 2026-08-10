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
//   - 伪装层（Codex 客户端形态对齐）：默认 codex UA / originator / beta 头、
//     头常量导出、Send 帧顶层 key 白名单过滤、client_metadata 组装（静态载体 /
//     trace 自动生成 / turn 计数）、HTTP 与 WS 各自的 beta 取值
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
//   - 伪装层默认开启（白名单过滤 + client_metadata 注入），Send 的 JSON 组装
//     开销仅在开启时发生；WithPayloadFiltering(false) 且无任何注入时回到
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
// 目标协议为 OpenAI Responses WebSocket（beta：responses_websockets=2026-02-04/06，
// 默认注入 V1，WithBeta 可切 V2；HTTP 路径固定 responses=v1）。
// 传输常量与伪装层对齐参考实现（sub2api openai_ws_*）：16MiB ReadLimit
// （coder 默认 32KB 过小）、CompressionContextTakeover 压缩、WS 层 ping 心跳
// （30s 间隔 + 2s 超时）、data: SSE 行提取与 [DONE] 终止、wsallowlist 17 字段
// 白名单、build_ws_client_metadata key 集合（trace 的 metadata key 名与头名不同）。
//
// 依赖：github.com/coder/websocket（纯标准库实现，无 CGO）+
// github.com/tidwall/gjson / github.com/tidwall/sjson（raw JSON 修补）。
package codexsdk
