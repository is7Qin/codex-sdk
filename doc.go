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
//     客户端自带的 client_metadata 永不透传）、透传（HTTP 头 WithHeader / WS
//     client_metadata 任意键 WithClientMetadata，只注入不解析——如 responses-lite
//     标记 HeaderResponsesLite / MetaResponsesLiteKey）、会话标识握手头（WithSession）、
//     x-codex-turn-state（WS：升级响应头签发 → 帧内 client_metadata 回传；
//     HTTP：仅响应侧捕获，请求不带头）、每帧新 trace 与 turn_id（UUIDv7）
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
// SDK 零协议解析：type / usage / 事件构造与业务语义（计费、透传编排、failover、
// 会话粘性、内容审核）全部在网关侧（go-proxy-mini）——网关在 SDK 交付的完整
// 字节上自行解析。不做事件层、不做模式枚举、不做任何业务钩子。
//
// OAuthWithRotation 边界：refresh 轮转协议 / 响应解析 / 判死分类 / 退避重试
// 属鉴权面，SDK 自包含（"零协议解析"纪律指 responses 业务协议——type/usage/
// 事件构造/计费语义不解析，鉴权错误分类不在此列）。RefreshResponse 仅非空覆盖
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
//   - WS 帧收发零额外分配：Send 直传帧字节（帧未被归一改写时零拷贝）；Recv 返回
//     coder/websocket 每次 Read 独立分配的读缓冲（跨次调用有效，无需拷贝即可保留）
//   - 伪装层恒开（WS/HTTP 各自顶层白名单过滤 + 强制 store:false + client_metadata
//     注入 + 每帧 trace/turn_id；归一不可关）；WithTraceAuto(false) /
//     WithTurnAuto(false) 且无任何注入时，帧已含 store:false 且无白名单外顶层键
//     则归一不重写、Send 零拷贝返回原字节；顶层键扫描零拷贝零分配（仅
//     forceCodexStoreFalse 的 store 探测有常量级小分配）。含
//     <environment_context> 的帧另经 RewriteEnvironmentContextTime 重写
//     （该步有 payload 级分配，非本路径。forceCodexStoreFalse 对已 false 的
//     store 短路——真 codex 帧恒带 store:false）
//   - 常驻读循环是硬性要求：Ping 与心跳依赖 Recv 处理 pong 控制帧
//     （coder/websocket：Ping 必须与 Reader 并发，否则等不到 pong）；
//     网关透传编排天然常驻 Recv 循环，满足该前提
//   - HTTP 流式解析零拷贝：bufio.Scanner 复用缓冲 + 行切片提取 data: 帧内容，
//     回调内的原始字节引用 scanner 缓冲（仅在回调执行期间有效）
//   - HTTP 客户端懒构建：NewHTTPClient 零开销，首次 Do/Stream 才创建 http.Client
//     （连接池复用；WithTransport 可注入自定义 RoundTripper）
//
// # Responses WS 连接池
//
// Pool 把一条 WS 连接的 2–3 RTT 握手下沉成一次性成本：同一握手身份（键）的
// 多次请求复用同一条连接。非目标：帧解析 / 事件过滤 / usage 嗅探（仍在网关）。
//
// 形状：socket 的读者属于连接，不属于请求；租约只是从这条连接的帧队列里
// 取走属于自己的那一段。每连接常驻 2 个 goroutine（pump 唯一读者 + pinger
// 唯一 ping 所有者），都随连接 done 退出。
//
// 资源上界（按此做容量规划，**别把 idle 限额当总上限**）：
//   - 单连接最坏占用 = `QueueBytes + 2×ReadLimit`：队列空时允许一帧超预算，
//     且 pump 手上还可能有一帧正在投递；`ReadLimit` 默认 16MiB ⇒ ≈36MiB。
//     `WithReadLimit(-1)` 取消单帧上限，**禁止**用于要求有界内存的池。
//   - `MaxIdleTotal` 只限**空闲**连接数，默认 0 = 不限。容量应由键空间的真实形状
//     决定：空闲数 ≈ min(键数, IdleTTL 窗口内被触及的去重键数)——`IdleTTL` 是时间
//     维度的自愈闸，`MaxIdlePerKey=1` 是每键闸。它**不限在租 / 在拨**：同键可并发
//     多条租约，拨号本身也不占名额。
//   - 全池（在租 + 拨号并发）的准入是**调用方（网关）**的责任（阶段二：全局/账号
//     semaphore，在 Acquire 之前取、请求真正结束才释放，并配有限 Dial/写超时）。
//
// 四条公理（Lease 的一切可见行为都由它们解释）：
//   - A1 读让位于投递，但 ping 让位于读：队列满 ⇒ pump 停读等待（真背压，
//     不丢帧、不杀流，Stats.QueueBlocked 可观测）；停读期间 ping 抑制
//     （Stats.PingSuppressed），且这次在途 ping 超时不计死（用单调 blockedSeq
//     判定，不用布尔快照）；空闲态队列恒空 ⇒ ping 常态生效，半开/挂起连接
//     在 ≤ PingInterval+PongTimeout 内判死。
//   - A2 帧交接原子（**队列交接原子**，不是业务隔离）：投递 / 归还 / 换代在同一
//     临界区；帧的归属以**读出时刻**的连接代为准（pump 持锁时取 gen），背压等待
//     期间换代 ⇒ 该帧丢弃；Release 先 gen++ 再清队列；Recv 只收本代。注意本地
//     gen **不是上游 response id**：它只保证"帧没跨租约漏进队列"，不证明业务帧
//     属于本次请求——业务归属由网关 C5（created 绑定 response id）负责。
//   - A3 交接与 ping 互斥：pinging 的连接不被出租（Acquire 跳过并继续找），
//     ping 结束后按 blockedSeq 决定是否判死。
//   - A4 所有权在代码里：DialFunc 返回的连接被池 claim 之后，外部
//     Send/Recv/Ping/Close/CloseNow/SetTurnState 一律 ErrNotOwner
//     （池内部走 *Owned 变体，按请求注入 turn-state 走 Lease.SetTurnState）。
//     "调用方不要动这个指针"不是契约，ErrNotOwner 才是。
//
// Lease 契约：Send/Recv/SetTurnState 只能由持有者调用；Recv 是**单消费者**——
// 同一租约并发调用 Recv 未定义（notify 通道 cap=1，只保证一个等待者）；Recv 的
// ctx 取消不触碰 socket（取消后队列里已有帧仍可取）；Release 归还（换代、清队列、
// 清 turn-state，清除与状态迁移在同一临界区内完成），Discard 直接判死；两者幂等，
// 已结束租约的 Send/Recv/SetTurnState 报错。**`Acquire` 之后必须 `defer l.Discard()`**
// （`Release` 之后再 `Discard` 是 no-op，见 `poolLease.Discard`：换代/已归还一律跳过）；
// **不要用「每条路径显式 `Release`」替代它**——panic 路径漏掉的恰恰是 `Release`。
// 池不再包住调用方 panic（删掉 Do/safeCall 后没有那层 recover），只有这个 defer 能在
// panic 时把连接判死；漏了它，连接会以 leased 状态泄漏到 `HardLimit`（最坏 60min）。
// panic 安全由调用方负责，`HardLimit` 是唯一兜底。
// 定序：**同一租约内先 SetTurnState 再
// Send**——Send 在锁外读 turnState，跨调用没有内存定序保证。请求时长的权威是
// **调用方 ctx**（池不设租约超时）；兜底只有连接年龄：`age ≥ HardLimit` 对
// idle/leased 一视同仁地回收（死因 `ErrConnExpired`，在租态计 `Stats.ReapedInFlight`），
// 病态消费方因此不会永久占坑。
//
// 键规范（PoolKey 是 opaque 值，由网关算好传入，SDK 只做等值比较、不解析不派生）：
// **握手身份维度必须进键**——账号、凭据、出口、安装 ID、sessionID、threadID、
// windowID、clientRequestID、modelSlug（上游 slug）；连接身份含握手头，头变化则
// 连接不可复用。必须剔除 turn_metadata / parent_turn_id / parent_thread_id /
// turn_id / traceparent / tracestate / x-codex-turn-state（每轮每帧变化，进键会让池
// 退化成一次一连接）。编码自选，池不校验内容。
//
// 心跳所有权：池是唯一 ping 所有者（DialFunc 必须 WithPingInterval(0)，
// 否则 ErrHeartbeatConflict 并关闭该连接）。心跳只在"没有帧在飞"时发探针——
// 空闲态恒发；租约态在上游静默时发（半开判死）；数据流动/停读期间抑制
// （数据本身已证明活性）。SDK 默认心跳（30s/2s 超时）不适用于池内连接。
//
// **`PingInterval+PongTimeout`（默认 ≤40s）不是背压/写锁等待下的 SLA**：停读
// （队列满）期间既不 ping 也不判死；`Send` 与 `Ping` 共用 writeMu，一次慢写同样
// 会推迟探测。这些场景的兜底是 `HardLimit` 与调用方自己的写超时。
//
// 会话不进池：客户端 WS 会话独占一条连接（会话与连接一一绑定，进池会与
// 上游 60min 硬限、会话粘性冲突）。
//
// 网关侧必须自己做的两件事（SDK 不替你做）：
//   - C5 帧序（是状态机，不是"全丢"）：**只能丢可识别的尾帧**——`response.completed`
//     之后的无主事件（`codex.rate_limits`、`codex.response.metadata` 等）。
//     `response.created` **之前**收到的 `type:error` / `status:429` / 带 headers 的
//     拒绝帧**不得当尾帧丢弃**：协议不保证拒绝前一定有 `created`，这些帧必须立即
//     按错误分类终止本次请求并 `Discard` 连接（否则会把本次错误吞掉、一直等到超时）。
//     拿到 `created` 后绑定 response id，之后的帧才属于本次请求；无 id 的
//     rate_limits 只能当连接级信息，不能算本轮 usage。
//     残余窗口（唯一）：帧在 `Release` 之前到达、却由一次跨越换代的 `Read` 返回
//     ⇒ 按"读出时刻"归属语义会归新租约；只靠上面三条兜底，网关代码必须写明。
//   - C4 完成判定与重放：只有读到 `response.completed` 才算完成；截断/中断按错误终止。
//     **重放权限归请求编排层**（SDK 只提供 Send/Recv/Release，不做自动重试）：上游已收到
//     `response.create`、开始生成、回程断开 ⇒ 重放会重复生成（可能重复计费）。
//     **已经 `Send` 过的请求不得仅凭链路错误自动重试**；只有"Send 本身失败 / 首字节确实
//     没发出"或已有服务端幂等去重时才可重放。
//
// 可观测口径：Stats() 是**近似快照**（原子计数与逐连接汇总不在同一时刻读取），
// 只用于观测/告警，不得用于断言跨字段恒等式。`Stats.QueuedBytes` 只统计**队列里**
// 的字节，不含 pump 手上正在投递的那一帧 ⇒ 单连接最坏占用见上面「资源上界」
// （`QueueBytes + 2×ReadLimit`）。
//
// 签名变化（"返回 ErrNotOwner 的唯一形态"；全仓调用点仍是语句/defer，源码兼容）：
// Client.CloseNow() error、Client.SetTurnState(string) error。池内按请求注入
// turn-state 走 Lease.SetTurnState。
//
// **Breaking**：`Lease` 接口新增 `SetTurnState(string) error`——第三方实现者
// （自定义 Lease）需补该方法；仓内唯一实现是 poolLease，无外部实现者。
//
// # 上游协议与参考
//
// 目标协议为 OpenAI Responses WebSocket（现役唯一 beta：responses_websockets=
// 2026-02-06，仅 WS 握手注入；HTTP 默认不发 OpenAI-Beta，需要时 WithHeader
// 显式注入）。真实客户端行为对齐：WS 握手与 HTTP 请求均不发 trace 头（trace
// 只进每帧 client_metadata，每帧新值）；session-id/thread-id/x-client-request-id/
// x-codex-window-id 会话级握手头；x-codex-turn-state 是双面机制（真实源码
// 行号实证，防未来误删 WS 帧注入）：
//   - HTTP 面：真实客户端请求头携带 x-codex-turn-state（client.rs:1202
//     build_responses_headers(..., Some(&self.turn_state))）；本 SDK HTTP 路径
//     不发送该头——仅响应侧捕获 + HTTPResponse.TurnState / TurnState() 暴露，
//     同轮回传由网关转发层（c3api）实现
//   - WS 面：帧 metadata 恒带（TurnState 非空时）——真实发送路径
//     client.rs:1626-1631（stream_responses_websocket）在 build_ws_client_metadata
//     （:779-792，辅助函数本身无 turn-state）之后显式追加
//     client_metadata.insert(X_CODEX_TURN_STATE_HEADER, ...)，流入
//     ResponseCreateWsRequest.client_metadata（:1702-1711 帧体携带）；SDK 帧注入
//     （client.go prepareFrame）逐点一致：升级响应头签发 → 帧内回传
//     （Client.TurnState 缓存 + 网关 SetTurnState("") 清除，跨轮不得回传）。
//
// responses-lite 非独立端点：与 /responses 同端点同事件集，
// 仅 internal 标记区分——HTTP 头 x-openai-internal-codex-responses-lite（WithHeader
// 透传）与 WS client_metadata 键 ws_request_header_x_openai_internal_codex_responses_lite
// （WithClientMetadata 注入），SDK 只注入不解析。
// 传输常量对齐参考实现：16MiB ReadLimit（coder 默认 32KB 过小）、
// CompressionContextTakeover 压缩、WS 层 ping 心跳（30s 间隔 + 2s 超时）、
// data: SSE 行提取与 [DONE] 终止、response.create 19 字段白名单、
// client_metadata 恒发 8 key 集合（session_id/thread_id/turn_id 为 snake_case，
// trace 的 metadata key 名与头名不同）。HTTP /responses 面注入（Stream
// 发送前统一执行）恒 4 key（x-codex-installation-id/session_id/thread_id/
// x-codex-window-id）+ 恒带 turn_id + 条件键（x-openai-subagent/
// x-codex-parent-thread-id/parent_turn_id/x-codex-turn-metadata）——不含
// trace/turn-state（trace 仅 WS 帧面；turn-state 的请求头属 HTTP 头面）。
//
// 依赖：github.com/coder/websocket（纯标准库实现，无 CGO）+
// github.com/tidwall/gjson / github.com/tidwall/sjson（raw JSON 修补）。
package codexsdk
