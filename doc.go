// Package codexsdk 提供面向 Codex 上游的 OpenAI Responses 客户端库（WS + HTTP 双形态），
// 单一 raw 字节通道 API 面。
//
// # 边界
//
// 本库只负责协议 / 传输 / 鉴权 / 伪装：
//   - Responses WebSocket：Dial / 帧字节收发（文本与二进制帧原样透传）/ 关闭码
//     透传（Close(status, reason)，急断 CloseNow）；**不主动发心跳**（对齐真实
//     codex 客户端：只在收到 Ping 时回 Pong）
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
//   - 伪装层（逐点对齐官方 codex-tui 0.169.0 源码与抓包，见下文「请求整形」）：
//     UA / originator / version / x-codex-beta-features / x-codex-routing-hint、
//     WS beta 头（现役唯一 2026-02-06）、头常量导出；身份完全请求级（Identity：
//     InstallationID / Session / Window / Turn / Sink），默认头不得携带身份或形态
//     键（WithHeader 遇保留键构造期 panic）；x-codex-turn-state 请求侧取
//     Identity.Turn.State（HTTP 走请求头、WS 走帧内 client_metadata，握手不带），
//     响应侧只认 HTTP 响应头与 response.metadata 事件，只回调 Identity.Sink；
//     ChatGPT-Account-ID（AccountIDProvider 可选接口：rotationAuth 经
//     WithOAuthAccountID / patAuth 经 WithPATAccountID 显式注入——空 = 不发；
//     OAuth(tokenProvider) 回调型不实现该接口）：以小写 chatgpt-account-id 写出；
//     来源二选一——OAuth 系 AccountIDFromToken 离线解析 access_token/id_token JWT 的
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
// 设备码登录（DeviceAuthClient）：StartDeviceFlow / PollDeviceFlow——ChatGPT
// OAuth device authorization grant，逐点对齐真客户端 codex-rs/login/src/
// device_code_auth.rs + server.rs::exchange_code_for_tokens（usercode JSON →
// 轮询 403/404=pending → 表单编码换码，redirect_uri={issuer}/deviceauth/callback）。
// **SDK 不持有登录状态、不做阻塞轮询**：待授权会话由调用方保存（网关把它交给
// 发起者本人），每次 PollDeviceFlow 只轮询一次，pending 由调用方决定何时重试。
// PollDeviceFlow 在授权完成后**自动换码**并返回 DeviceTokens；三个哨兵错误
// 区分 pending / expired / denied（denied 认 access_denied、authorization_denied
// 与真客户端实测的 401 authorization_declined），其余非 2xx → *DeviceAuthError
// （状态码 + 错误体，供调用方诊断）。DeviceCode.ExpiresIn 恒为正：服务端未下发
// 时按真客户端的 15 分钟上限补齐，调用方可以据此维护截止时间。issuer 默认
// DefaultIssuer，WithIssuer 可覆盖（测试/自托管）；client_id 与 refresh 路径
// **同源**（CODEX_APP_SERVER_LOGIN_CLIENT_ID），否则登录签发的 refresh token
// 与后续刷新请求的 client_id 不一致会 invalid_client。令牌刷新沿用既有
// OAuthWithRotation，DeviceTokens 只交付首登凭据束。
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
//   - 无全局可变状态：连接/客户端均按需构建，唯一的常驻 goroutine 是 pumped 读者
//     （以及每个 WS 缓存一个维护 ticker），都只在所属对象存活期间存在
//   - 请求整形是两段式：parseRequest 一次顶层扫描（白名单、同名键首现，值只
//     记原始片段不复制）→ shapeRequest 只做身份与模型相关的变换 → assembleHTTP /
//     assembleWS 按官方键序一次写出。匿名且输入已是规范键序与规范值时返回原切片、
//     整条链零分配（TestEncodeZeroCopyAllocFree）；其余请求按官方形态重建一次
//     （3.7KB 体约 20–30µs / 11 allocs，84KB 官方体整形 + zstd 约 0.8ms）。
//     含 <environment_context> 的请求先改写值内时间文本（线性扫描，只解码含标签
//     的字符串）
//   - HTTP /responses 请求体 zstd 压缩：编码器经 sync.Pool 复用，每次
//     Reset → Write → Flush → Close（帧头与官方一致 28b52ffd0058）
//   - WS 出站帧不压缩（协商 permessage-deflate，阈值 MaxInt）：不建常驻 flate
//     写入器（context takeover 每连接约 1.2MB）
//   - WS 帧收发零额外分配：Send 直传帧字节；Recv 返回 coder/websocket 每次 Read
//     独立分配的读缓冲（跨次调用有效，无需拷贝即可保留）
//   - 连接读模型分两种模式（构造期固定、互斥；同一条连接只有一个 socket 读点、
//     一个写闸、一个终止入口）。公开 Dial → Client 是**纯拉**：Recv 直读、
//     无常驻读者、无队列，因此 pong 由调用方的常驻 Recv 循环服务
//     （coder/websocket：Ping 必须与 Reader 并发，否则等不到 pong）——网关透传
//     编排天然满足该前提，这是既有契约；Close 走 coder 关闭握手。传输臂
//     （WithResponsesTransport(TransportWS) 的 StreamBody / Responses）是
//     **常驻读者 + 有界队列**：读者除入队临界区外恒在 Read，所以下游再慢也不会
//     丢失对端控制帧（已无任何心跳探测：与真实 codex 客户端一致，半开连接只能靠
//     读写错误或调用方 ctx 兜底）；队列饱和（256 帧 / 8MiB，空队列允许单帧超预算）报
//     ErrWSBackpressure（可归因，非静默截断、非无限缓冲）；**正常**终态后可归还缓存，
//     失败终态（failed/incomplete）即退役，放弃或归还失败则强断。
//     StreamBody 由 Body.Close 触发归还（failed/incomplete 已在渲染层标记，不会归还）；
//     Responses 消费终态后自动归还。
//     注意：WithPingInterval 已无效果（纯兼容面）；需要主动探活的调用方自己做。
//   - **读空闲上限（idle read timeout，默认 300s）**：计时单位是"一次消费侧取帧
//     等待"，不是"socket 多久没字节"——pumped 挂在 wsExchange.Next 里队列为空、真正
//     阻塞的那一段（队列有帧 ⇒ 立即交付、不计时；连接在缓存里或消费者没在等帧 ⇒
//     无计时器），pull 挂在一次 Client.Recv 上。超时 ⇒ ErrWSIdleTimeout + 经
//     beginFinish/租约 CAS 裁决后退役（不归还、不重试、不重拨）。Ping/Pong、写帧
//     都不续命；计时只用复用的 *time.Timer（每 exchange 至多 1 个，无看门狗
//     goroutine）。WithWSIdleTimeout(d) 可配，d<=0 关闭。
//     **pull 契约变更**：Client.Recv 由"无期限读 socket"变为"每次调用有上限"——
//     一次调用内无完整数据消息就关闭该连接；网关 relay 的读循环跨轮常驻 ⇒ 轮间
//     静默超过预算会关闭整条 WS 会话（Closed() 语义不变，仍只表示本地 Close）。
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
// # 请求整形（官方 codex-tui 0.169.0 形态）
//
// StreamBody / Stream / Responses（HTTP→HTTP、HTTP→WS）与 Client.Send（WS→WS）发出的
// 每个 Responses 请求都按官方形态整形（HTTPClient.Do 不整形，不得用于 Codex
// responses）：
//   - 白名单：model, instructions, input, tools, tool_choice, parallel_tool_calls,
//     reasoning, store, stream, stream_options, include, service_tier,
//     prompt_cache_key, text, access_programs；WS→WS 另认 previous_response_id、
//     generate（原样，由上游判断）。type、client_metadata 与白名单外键丢弃；同名键
//     只认第一次出现（按解码后的名字，转义拼写同样）。请求体不是 JSON 对象或缺少
//     字符串 model → ErrInvalidRequestBody（本地拒绝）。
//   - stream:true、store:false；reasoning 恒写（effort 按模型表补，lite 写
//     context:"all_turns"，非 lite 删 context）；include、tool_choice、
//     parallel_tool_calls、text.verbosity 按官方缺省补；键序按官方
//     ResponsesApiRequest / ResponseCreateWsRequest。
//   - 顶层 instructions → developer 消息（非 lite 的 id 与官方 UUIDv5 逐字节一致，
//     lite 由窗口 UUID 时间戳 + 内容哈希派生）；lite 模型（静态表 models.go，或
//     input 已有 additional_tools）的顶层 tools 转成 additional_tools 项（按官方
//     分组规则，Chat Completions 风格先拍平）。
//   - client_metadata 与 x-codex-turn-metadata 全部由 Identity 合成（客户端自带的
//     整个丢弃，其中的 sandbox / workspaces 也不取）：client_metadata 键序每次随机
//     （官方是 HashMap），turn-metadata 固定键序、纯 ASCII，sandbox 恒为 none、
//     sandbox_mode 恒为 danger-full-access。匿名（Session.SessionID 为空）不写
//     client_metadata、不发身份头与 turn-metadata、删除 prompt_cache_key。
//   - 请求头小写直接赋值（User-Agent 除外）：version、x-codex-beta-features、
//     x-codex-routing-hint、身份头、x-codex-turn-metadata；HTTP 另有 accept、
//     content-encoding: zstd、lite 标记头；WS 握手带这条连接第一帧的 turn-metadata
//     （WS→WS 经 WithFirstFrame 提供首帧）。Accept-Encoding 由调用方 Transport 的
//     DisableCompression 关闭。
//   - WS 帧的 x-codex-ws-stream-request-start-ms 在取得写闸之后写入发送时刻。
//   - 计数见 ShapeStatsSnapshot（进程级；SDK 不打日志）。
//
// WS 传输臂的连接身份 = (threadKey, readLimit)：连接是 thread 级、跨轮跨窗口
// 复用，ts 是轮级、每请求下发。turn-state 的发现通道只有 Sink：HTTP 响应头在解析
// 响应头时同步回调；事件级 ts（response.metadata 的 headers）经 turnStateTap
// （HTTP→WS）或 Client.Recv（WS→WS 静态身份）在读栈内逐帧旁路捕获：created 先记
// response_id，归属匹配才写 Sink，"created 之前的 metadata 一律不捕获"。握手响应头
// 里的 turn-state 不取（官方只认事件）。Body.Close 之后不再回调。
//
// 传输常量对齐参考实现：16MiB ReadLimit（coder 默认 32KB 过小）、data: SSE 行提取
// 与 [DONE] 终止。
//
// 依赖：github.com/coder/websocket（纯标准库实现，无 CGO）+
// github.com/tidwall/gjson（raw JSON 读取）+ github.com/klauspost/compress/zstd
// （/responses 请求体压缩，钉 v1.17.11）。
package codexsdk
