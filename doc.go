// Package codexsdk 提供面向 Codex 上游的 OpenAI Responses 客户端库（WS + HTTP 双形态），
// 单一 raw 字节通道 API 面。
//
// # 边界
//
// 本库只负责协议 / 传输 / 鉴权 / 伪装：
//   - Responses WebSocket：Dial / 帧字节收发（文本与二进制帧原样透传）/ 心跳保活 /
//     关闭码透传（Close(status, reason)，急断 CloseNow）
//   - Responses HTTP：POST 请求构造 / 非流式响应 / 流式 SSE 事件帧提取；
//     HTTPClient 的 StreamBody / Responses 可经 WithResponsesTransport 改走上游
//     Responses WS（对外形状不变，帧构造 / 错误语义 / 等价面见 TRANSPORT.md）
//   - 生图（HTTPClient 方法）：GenerateImage 非流式直连 images 端点
//     （generations/edits、JSON 非流式——上游无流式路径；输入图 Raw 字节 →
//     data URL 直嵌、usage image_tokens 提取；端点固定官方 DefaultImagesURL /
//     DefaultImagesEditsURL；传输层复用 Do：401 轮转 / 判死分类 / 错误透传零新增）；
//     GenerateImageStream 合成流式（内部调 GenerateImage，等待期间每 60s
//     合成 keepalive 事件——CF 524 免疫；成功后每张图合成一个
//     image_generation.completed 事件回调——usage 仅最后一个携带；错误原样
//     透传、无 partial_image 合成、无会话维持）
//   - 上游 URL 固定官方端点：DefaultResponsesURL（responses HTTP）、
//     DefaultResponsesWSURL（responses WS wss）、DefaultSearchURL、
//     DefaultImagesURL / DefaultImagesEditsURL、DefaultUsageURL；所有操作直接使用对应常量，不可覆盖或追加 query
//   - 升级与请求鉴权注入（PAT 静态 / OAuth 刷新回调 / OAuthWithRotation 轮转
//     状态机，Auth 接口）：401 自动轮转（判死分类 + 单飞 refresh + 重试一次，
//     WS 升级 401 自动重连一次并带 DialError.Refreshed 标记）、RT 判死码集 /
//     token 端点 401 / 账号禁用类 → 账号级终止（导出错误类型 errors.As 区分，
//     OnAuthFatal 通知），Invalidate() / Fatal(err) 显式入口（网关解析 WS 判死
//     事件帧时调用）
//   - 伪装层（逐点对齐真实 codex 客户端源码）：默认
//     codex-tui UA/originator（0.154.0 + Ubuntu 指纹）、beta 头（现役唯一 2026-02-06）、头常量导出、
//     WS/HTTP 各自顶层白名单过滤（含 store:false 强制），无条件生效、client_metadata **整体替换**组装
//     （WS 帧面 8 key 恒发：installation_id/session_id/thread_id/turn_id/
//     window_id/turn-metadata/traceparent/tracestate；HTTP 体面恒 4 key +
//     turn_id + 条件键，不含 trace/turn-state，见 injectResponsesClientMetadata；
//     客户端自带的 client_metadata 永不透传）、身份完全请求级（Identity：
//     Session/Meta/TurnState/Sink；WS 帧与 HTTP 体 client_metadata、WS 握手头都从
//     Identity 取；默认头不得携带身份键——WithHeader 遇保留键构造期 panic）、
//     x-codex-turn-state（轮级 token：请求侧 Identity.TurnState，值落 HTTP 请求头
//     或 WS 帧内 client_metadata；响应侧 HTTP 头 / WS 握手头 / WS
//     response.metadata 事件三处捕获后只回调 Identity.Sink，SDK 不保留状态）、
//     每帧新 trace 与 turn_id（UUIDv7）
//     ChatGPT-Account-ID（AccountIDProvider 可选接口：rotationAuth 经
//     WithOAuthAccountID / patAuth 经 WithPATAccountID 显式注入——空 = 不发，
//     向后兼容；OAuth(tokenProvider) 回调型不实现该接口）：上线形态经 Go
//     textproto 规范化为 Chatgpt-Account-Id（头名大小写不敏感）；来源二选一——
//     OAuth 系 AccountIDFromToken 离线解析 access_token/id_token JWT 的
//     https://api.openai.com/auth.chatgpt_account_id（不验签/exp，rt 无 claims），
//     PAT 系 FetchPATMetadata 在线 whoami（base 默认
//     https://auth.openai.com/api/accounts，env CODEX_AUTHAPI_BASE_URL 覆盖，
//     默认 10s 超时）；SDK 零隐式派生 + 构造零网络（落库点显式注入，拨号侧不派生）
//
// SDK 不做业务层协议处理：usage / 事件构造与业务语义（计费、透传编排、failover、
// 会话粘性、内容审核）全部在网关侧（go-proxy-mini）——网关在 SDK 交付的完整
// 字节上自行解析。不做业务钩子、不做模式枚举。
//
// 但**实现必须读顶层 `type`**：SSE 表示要为每帧写 `event:` 行，传输臂要判断终态
// （completed / failed / incomplete）才能提交并收尾。这层解析限于"顶层 type 字符串"
// （零分配扫描，见 frameEventType），不展开 usage、不解释事件负载。
//
// OAuthWithRotation 边界：refresh 轮转协议 / 响应解析 / 判死分类 / 退避重试
// 属鉴权面，SDK 自包含（"不解析业务协议"的纪律指 responses 的业务
// 语义——usage / 事件构造 / 计费不解析，鉴权错误分类不在此列）。RefreshResponse 仅非空覆盖
// （缺 refresh_token 保留旧 rt，回调与后续 refresh 均用保留值）；refresh 退避
// SDK 自有默认（base 200ms / cap 30s / 上限 3 次，WithBackoff 可调）；token
// 端点 401 无条件判死、RT 判死码 10 个（大小写不敏感）、账号禁用类（400 org
// disabled / KYC / 402）、AT 401 判死码（token_invalidated / token_revoked /
// detail:"Unauthorized"）→ OnAuthFatal 一次性 + Fatal 态；网络/5xx/429/其他
// 非 2xx 退避重试，耗尽 → RefreshError（非 fatal，下次可再试）。空 refreshToken
// 构造 panic（构造器返回 Auth 接口无 error 通道，签名约束下唯一选择）。
// refresh 请求走 http.DefaultClient（env override 换端点），不受 WithTransport /
// WithTimeout 影响。
//
// # crypto/rand 失败
//
// 身份 / 会话 id（NewUUIDv7）与轮换窗口上限抽样（NewIdentityState → drawWMax）
// 依赖 crypto/rand。这些构造器处于请求相关路径，但导出签名无 error 通道
// （NewUUIDv7() string / NewIdentityState(...) IdentityState）；若改为返回 error，
// 将波及全部调用链（含网关）且收益不抵 churn。故 crypto/rand 读取失败一律
// panic——失败属系统级异常（OS / 内核 RNG 不可用），正常环境不可达；而静默复用
// 或退化为可预测值会破坏伪装身份的唯一性与不可区分性（相邻请求共享 thread_id /
// 可预测 window_id），比进程级 panic 危险得多。调用方可在外层 recover 决定
// fail-fast 或降级。
//
// # 性能语义（性能优先：懒构建 + 热路径低分配）
//
//   - 无全局可变状态：连接/客户端均按需构建，心跳 goroutine 仅连接存活期间存在
//   - WS 帧编码是**单遍**：一次顶层扫描、一份输出缓冲，把协议形状差（type/续接键/
//     stream）、白名单过滤、store:false 与 client_metadata 注入合并落地；只在
//     "紧凑 / 无转义键 / 无重复键 / 顶层键全在白名单内"时走快路径，其余形态回退旧链
//     （每步整帧重写）以保证字节一致（3.7KB 体实测 6.2µs / 5 allocs；旧链 34.9µs / 26）
//   - 帧未被改写时零拷贝：快路径判定"一个字节都不用改"时返回原切片；含
//     <environment_context> 的帧先经 RewriteEnvironmentContextTime 改写值内文本
//     （该步有 payload 级分配，非本路径）
//   - WS 帧收发零额外分配：Send 直传帧字节；Recv 返回 coder/websocket 每次 Read
//     独立分配的读缓冲（跨次调用有效，无需拷贝即可保留）
//   - 连接读模型分两种模式（构造期固定、互斥；同一条连接只有一个 socket 读点、
//     一个写闸、一个终止入口）。公开 Dial → Client 是**纯拉**：Recv 直读、
//     无常驻读者、无队列，因此 pong 由调用方的常驻 Recv 循环服务
//     （coder/websocket：Ping 必须与 Reader 并发，否则等不到 pong）——网关透传
//     编排天然满足该前提，这是既有契约；Close 走 coder 关闭握手。传输臂
//     （WithResponsesTransport(TransportWS) 的 StreamBody / Responses）是
//     **常驻读者 + 有界队列**：读者除入队临界区外恒在 Read，所以下游再慢也不会
//     让心跳误判成连接死亡；队列饱和（256 帧 / 8MiB，空队列允许单帧超预算）报
//     ErrWSBackpressure（可归因，非静默截断、非无限缓冲），心跳探测未在 2s 预算内
//     完成报 ErrKeepaliveTimeout；终态后可归还缓存，放弃或归还失败则强断。
//     StreamBody 由 Body.Close 触发归还；Responses 消费终态后自动归还。
//     注意：WithPingInterval(0) 关闭心跳后，传输臂仍有常驻读者服务 pong，但不再
//     探测"对端是否还活着"（半开连接只能靠读写错误或调用方 ctx 兜底）。
//   - WithWSConnCache(n) 开启默认关闭的 WS 连接复用：同一个 HTTPClient 上同一
//     thread 的连续请求——含跨轮跨窗口——命中后不再付握手（连接身份 =
//     (threadKey, readLimit)）；匿名（Identity{}）不参与复用。线程退休走
//     HTTPClient.RetireThread（只摘 idle，不设墓碑）。
//   - HTTP 流式解析零拷贝：bufio.Scanner 复用缓冲 + 行切片提取 data: 帧内容，
//     回调内的原始字节引用 scanner 缓冲（仅在回调执行期间有效）
//   - HTTP 客户端懒构建：NewHTTPClient 不创建 http.Client（只做选项快照），
//     首次 Do/Stream 才创建 http.Client
//     （连接池复用；WithTransport 可注入自定义 RoundTripper）
//
// # 上游协议与参考
//
// 目标协议为 OpenAI Responses WebSocket（现役唯一 beta：responses_websockets=
// 2026-02-06，仅 WS 握手注入；HTTP 默认不发 OpenAI-Beta，需要时 WithHeader
// 显式注入）。真实客户端行为对齐：WS 握手与 HTTP 请求均不发 trace 头（trace
// 只进每帧 client_metadata，每帧新值）；session-id/thread-id/x-client-request-id/
// x-codex-window-id 会话级握手头；x-codex-turn-state 是轮级机制（真实源码
// 行号实证，防未来误删 WS 帧注入）：
//   - HTTP 面：真实客户端请求头携带 x-codex-turn-state（client.rs:1202
//     build_responses_headers(..., Some(&self.turn_state))）；本 SDK 的请求侧
//     取值是 Identity.TurnState，响应侧捕获后只回调 Identity.Sink
//   - WS 面：帧 metadata 恒带（Identity.TurnState 非空时）——真实发送路径
//     client.rs:1626-1631（stream_responses_websocket）在 build_ws_client_metadata
//     （:779-792，辅助函数本身无 turn-state）之后显式追加
//     client_metadata.insert(X_CODEX_TURN_STATE_HEADER, ...)，流入
//     ResponseCreateWsRequest.client_metadata（:1702-1711 帧体携带）；SDK 帧注入
//     （frameCodec.encode 的 client_metadata 落地：单遍快路径直接写入，回退链经
//     normalize → injectClientMetadataObject）逐点一致：每请求取值 → 帧内回传。
//     跨轮不得重放：请求侧只读 Identity.TurnState，SDK 不缓存、不回落。
//
// WS 传输臂的连接身份 = (threadKey, readLimit)：连接是 thread 级、跨轮跨窗口
// 复用，ts 是轮级、每请求下发——与真实 codex 的换连接条件一致（因为
// responses_headers 不含 ts）。TS 的发现通道只有 Sink：握手响应头在解析响应头时
// 同步回调，事件级 ts（response.metadata 的 headers）经 turnStateTap 在 Read 栈内
// 逐帧旁路捕获：created 先记 response_id，归属匹配才写 Sink，fail-safe 是
// "created 之前的 metadata 一律不捕获"。Body.Close 之后不再回调。
//
// responses-lite 非独立端点：与 /responses 同端点同事件集，仅 internal 标记区分
// ——HTTP 头 x-openai-internal-codex-responses-lite 可用 WithHeader 透传；S4 起
// 不再提供任意 WS client_metadata 透传入口。
// 传输常量对齐参考实现：16MiB ReadLimit（coder 默认 32KB 过小）、
// CompressionContextTakeover 压缩、WS 层 ping 心跳（30s 间隔 + 2s 超时）、
// data: SSE 行提取与 [DONE] 终止、response.create 19 字段白名单、
// client_metadata 恒发 8 key 集合（session_id/thread_id/turn_id 为 snake_case，
// trace 的 metadata key 名与头名不同）。HTTP /responses 面注入（Stream
// 发送前统一执行）恒 4 key（x-codex-installation-id/session_id/thread_id/
// x-codex-window-id）+ 恒带 turn_id + 条件键（x-openai-subagent/
// x-codex-parent-thread-id/parent_turn_id/x-codex-turn-metadata）——不含
// trace/turn-state（trace 仅 WS 帧面；turn-state 在 HTTP 面走请求头，不在体面）。
//
// 依赖：github.com/coder/websocket（纯标准库实现，无 CGO）+
// github.com/tidwall/gjson / github.com/tidwall/sjson（raw JSON 修补）。
package codexsdk
