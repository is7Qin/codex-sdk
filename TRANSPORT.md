# Responses 出站传输：HTTP / WS

`HTTPClient` 的 `StreamBody` 与 `Responses` 有两条出站传输，由**一个**开关选择；
调用方（网关）只需把模板开关透传进来，消费代码零改动。

## 契约（冻结）

```go
type Transport string
const (
    TransportHTTP Transport = "http" // 默认 / 零值
    TransportWS   Transport = "ws"
)
func WithResponsesTransport(t Transport) Option
```

- 只影响 `StreamBody` / `Responses`。`Dial` / `Do` / `Stream` / `GenerateImage` /
  `Search` / `GetUsage` 不受影响。
- 零值 `""`、`TransportHTTP`、**任何未知值**都走 HTTP：只有显式 `TransportWS`
  才启用 WS，写错的值不会静默切走传输。
- HTTP 路径零改动：分支判断之外不构造任何 WS 对象、不改写载荷字节。

## 分层（一条连接 = 一个读点 + 一个写闸 + 一个终止入口）

| 层 | 文件 | 职责 |
|---|---|---|
| socket | `conn.go` | `conn`：唯一 `ws.Read` 调用点（`readFrame`）、写/公开 Ping 闸 `gate`、唯一终止入口 `terminate`（经 `finish` 发布）；**无任何主动心跳**（只在对端发 Ping 时回 Pong） |
| 队列 | `conn.go` | `frameInbox`：pumped 的有界入站队列（帧数 + 字节双闸） |
| 拨号 | `dial.go` | `dialConn`：升级头组装 / 401 轮转 / 升级错误面 |
| 请求 | `exchange.go` | `wsExchange`：一次请求 = 一次 exchange；连接可跨请求（含跨轮）复用（默认关）；**不驻留 turn-state**（ts 每请求随 `Identity` 进帧，签发值只经 `Identity.Sink` 出去）；`Next` 取帧 / `Release` 归还 |
| 请求整形 | `shape.go` | 两段式：`parseRequest`（一次顶层扫描）→ `shapeRequest`（身份与模型相关变换）→ `assembleHTTP` / `assembleWS`（官方键序一次写出）；无可变状态 |
| SSE 表示 | `sse.go` | `sseView`：帧 → SSE 字节流（仅 `StreamBody`） |

### 两种连接模式（构造期固定、互斥）

| 模式 | 谁用 | 读 | pong 由谁服务 | 关闭 |
|---|---|---|---|---|
| `modePull` | 公开 `Dial` → `Client` | `Client.Recv` 直读 `readFrame`（无 pump、无队列） | 调用方的常驻 `Recv`（**既有契约不变**） | `closeGracefully`（coder 关闭握手，时延上界 ~10s） |
| `modePumped` | 传输臂（`StreamBody` / `Responses`） | 常驻 `readLoop` + `frameInbox` | pump 恒在 Read（除入队临界区） | 归还路径不 terminate：`Release` 成功即交回缓存；放弃或归还失败时 `terminate` 强断 TCP（不做关闭握手） |

为什么必须分两种：`coder/websocket` 的 `Read(ctx)` 被取消时会**关闭底层 socket**，
所以"先取消读、再做关闭握手"不可能（关闭帧永远发不出去）。而传输臂又必须
"读者永不停"才能既服务 pong、又在下游慢时报可归因的背压。

### pumped 的读者永不阻塞

`readLoop` → `frameInbox.push`：**满即 `ErrWSBackpressure`**（不阻塞、不丢帧、不
无限缓冲），`readLoop` 收到即 `terminate`。双闸：`256` 帧 / `8MiB`；**空队列允许
单帧超预算**（64MiB 的合法巨帧不能因为比字节闸大就被判死），但连续两个
`> queueBytes/2` 的巨帧在消费者暂停时会触发背压——这是已知取舍（W5b 固化）。

内存上界（pumped）= `queueBytes` + 1 帧（空队列例外）+ pump 正在投递的 1 帧 +
消费侧持有的 1 帧；传输臂的读上限是常量（流式 64MiB / 聚合 16MiB），`WithReadLimit`
不作用于它（只用于公开 `Dial`），所以这里**没有**“无限上界”形态。

`pop` 的顺序是契约：① ctx 已结束 → 立即返回 ctx 错误（不 pop）；② 有帧 → 交付
（不再查 ctx）；③ 已关闭且空 → 返回终止 cause；④ 等 `notify` / ctx；
⑤ 有 idle 预算时再加一个"本次等待"的期限（见下节）。

### 读空闲上限（idle read timeout）

给"等一帧"加预算，**不是**请求总超时，也不是"socket 多久没字节"。

- 默认 **300s**（对齐真实客户端 `DEFAULT_STREAM_IDLE_TIMEOUT_MS = 300_000`），
  `WithWSIdleTimeout(d)` 可配，`d<=0` = 关闭（显式 opt-out）。默认**开**。
- **计时单位 = 一次消费侧取帧等待**：
  - pumped：`wsExchange.Next` 里队列为空、`pop` 真正阻塞的那一段（deadline 是
    `pop` 的局部变量，只在第一次阻塞时初始化）。队列里已有帧 ⇒ 立即交付、
    不消耗预算；连接躺在缓存里、或消费者还没请求下一帧 ⇒ **没有计时器在跑**。
  - pull：一次 `Client.Recv` 调用的持续时间。
  - 因此慢消费者不会被杀，终态排队也不会失去归还资格；`Ping`/`Pong`、写帧、
    心跳都不续命也不重置（控制帧由 coder 在 `Read` 内就地消化，不产生队列投递）。
- 超时 ⇒ `ErrWSIdleTimeout`（独立哨兵，**不**包装 `context.DeadlineExceeded`，
  否则续接 ACK 的 `errors.Is(err, context.DeadlineExceeded)` 判定会误命中）+
  **经所有权裁决**（`beginFinish` + `lease.CompareAndSwap`，与 `Release` 同一
  收尾序）退役该连接 + **不重试**（不重拨、不重发 `create`）。
- 收尾顺序与 `teardown` 有意不同：`stopWatch → 租约 CAS → cancel → terminate`。
  否则取消守卫的 `AfterFunc` 会在 `cancel` 当下抢走 CAS，把终止原因写成
  `context.Canceled`，把"空闲超时"洗成"调用方取消"（`finish` 首因胜）。
- 两条读路径都覆盖：pumped（`StreamBody`/`Responses` 的 WS 臂）与 pull
  （`Dial → Client.Recv`）。**pull 是本地契约变更**：

  ```
  Client.Recv: 无期限读 socket  →  每次调用有上限（默认 300s）
  一次调用内没有完整数据消息 ⇒ ErrWSIdleTimeout + 关闭该连接
  ```

  `Client.Closed()` 语义不变（仍只表示本地 `Close/CloseNow`）；不调用 `Recv`
  时没有计时器，所以不保证诊断半开连接。网关 relay 的读循环跨轮常驻 ⇒
  **轮间静默超过预算会关闭整条 WS 会话**（客户端收到上游错误关闭），这是接受的
  代价。`WithWSIdleTimeout(0)` 是精确回滚。
- 计时只用 `time.Timer`：pumped 每个 exchange 懒建 1 个、全部等待复用它
  （`resetTimer`，Go ≥1.23 语义）：已在队帧 0 新增分配；没有每连接看门狗 goroutine
  （那个设计被否：观察 deadline ≠ 终止权，见"被否方案"）。
- 被否方案：把期限挂到 `conn.readFrame`（socket 计时 ⇒ 慢消费者/排队终态被误杀）；
  每次 `readFrame` 派生 `context.WithDeadline`（每帧 1 timer + 1 ctx）；把预算塞进
  `exchangeCtx`（退化成请求总超时，错误类型也会被翻成 `DeadlineExceeded`）；
  连接级 `atomic` deadline 复核后 terminate（复核与 terminate 之间仍有窗口）。

### 终止（无心跳）

- **没有主动心跳探测**：连接只在收到对端 Ping 时就地回 Pong（由 coder 在读循环里
  完成）。依据是真实 codex 客户端：它的 Responses WS pump 没有 ticker、没有 pong
  deadline；空闲 EOF/Close 由常驻读者观测，复用前用 `releasable()` 复查。
- 代价：半开连接不再由本地 ping 判死，只能靠读写错误或调用方 ctx 兜底。公开
  `Client.Ping` 仍可用（完全沿用调用方 ctx，不加内部封顶），要不要探活由调用方
  决定；`WithPingInterval` 已变成无效果的兼容面。
- `ErrKeepaliveTimeout` 保留为**已退役**的错误值（无产生点），只为不破坏调用方
  的 `errors.Is` 编译面。
- `finish` 是唯一的状态/结果发布点：裁决 + 首因选择 + 发布在同一个 `mu` 临界区内
  完成；首个 cause 生效，后续终止不覆盖首因。
- 缓存 TTL/maxAge 的退役改由 `connCache` 自持的维护 ticker（只 sweep、不探测）：
  连接不再知道缓存存在，也不为它开定时器。

## 等价面（对外形状与语义）

| 面 | HTTP 臂 | WS 臂 |
|---|---|---|
| `StreamBody` | `*http.Response{200, 未读 SSE body, 上游头}` | `*http.Response{200 OK, 帧渲染的 SSE body, 合成头只有 Content-Type: text/event-stream}`（不带 ts） |
| `Responses` | `Stream` → `responsesAggregator` → `HTTPResponse{200, Raw}`（**无 ts 字段**：签发值只走 `Identity.Sink`） | `responsesStreamWS` **直接从 exchange 取帧**喂同一个聚合器（不经 `sseView`/`scanSSE` 的渲染-回扫回路）→ 同形态、逐字段同构 |
| turn-state | 请求头 `x-codex-turn-state`（值来自 `Identity.Turn.State`，逐请求传入）；捕获点：HTTP 响应头 | 帧 `client_metadata."x-codex-turn-state"`（同一来源）；捕获点：属于本次 response 的 `response.metadata` 事件（**握手响应头不取**，官方只认事件）；统一回调 `Identity.Sink`；**合成响应头不带 ts**，SDK 不保留任何 ts 状态 |
| 读上限 | 聚合器单行 16MiB；裸 body 由下游决定 | 按取数路径固定：聚合 16MiB / 流式 64MiB（`WithReadLimit` 只作用于公开 `Dial`，传输臂的上限由取数路径决定）；两条取数路径不共享连接（16MiB / 64MiB） |
| 请求整形 | `parseRequest(surfaceHTTP)` → `shapeRequest` → `assembleHTTP`（官方 HTTP 键序）→ zstd 压缩，`content-encoding: zstd` | `parseRequest(surfaceWSFromHTTP)` → `shapeRequest` → `assembleWS`（`type:"response.create"`、丢弃客户端 `previous_response_id`/`generate`、官方 WS 键序）；`x-codex-ws-stream-request-start-ms` 取得写闸后写入 |
| 401 轮转 | 判死分类 + 单飞 refresh + 重试一次 | 同（`dialConn` 内，`OAuthWithRotation`） |

帧渲染（`sseView`，一帧一块）：

```
event:<data.type>\n
data:<帧字节 verbatim>\n
\n
```

- `response.completed` → 该块之后补 `data: [DONE]\n\n` 并结束。
- `response.failed` / `response.incomplete` → 帧透传后结束（不补 `[DONE]`）。
- `type:"error"` 且 `status`/`status_code` ∈ 4xx/5xx → **不渲染**：以
  `*HTTPError{StatusCode, Raw: 原帧}` 结束该流（连接终止、不入缓存）。缺 `status`
  或 `status` 是 2xx → 帧透传并继续读（与 HTTP 臂 200 流里的 in-band error 同义）。
- 无 `type` 的帧只出 data 行（不写空的 `event:` 名）。
- 收到**任意** close frame（`CloseStatus >= 0`，含 1000/1001/1005/1011）= EOF；
  只有 `CloseStatus == -1`（对端没发 close frame：直接断 TCP / 连接重置 / 超
  ReadLimit）才是传输错误，且原样返回底层错误。

帧构造规则：JSON 合法 + **对象根** + 字符串 `model` → 白名单（同名键只认第一次）→
覆写 `type:"response.create"`、`stream:true`、`store:false` → 丢弃客户端
`previous_response_id` / `generate`（HTTP→WS）→ 官方键序写出。**全部在拨号之前完成**；
握手头 `x-codex-routing-hint` 与 `x-codex-turn-metadata` 取自这一帧的整形结果。

整形后的字节不再做任何 sjson 改写；唯一的原地改写是发送时刻的 13 位数字
（`conn.writeStamped` 在取得写闸后覆写）。重复顶层键（含转义拼写）只认第一次出现，
其余丢弃并计数——`store:true`、`previous_response_id` 的第二份不会出网。

## 终态提交

**取帧即提交**：终态帧一旦由 `wsExchange.Next` 交付，本次 exchange 即视为完成
——`sseView` 把它（含 `[DONE]`）吐完就结束取帧，`Responses` 把终态帧喂给聚合器
后不再追加 `Next`。因此：

- 终态帧之前的取消/超时 ⇒ 可 `errors.Is` 的 ctx 错误；
- 终态帧之后（已提交）的取消/超时 ⇒ **不改写结果**；
- **已入队的终态帧优先于 deadline**：帧已在队列里 = 上游已完成，此时消费侧
  deadline 到点仍应交付并成功（否则会把已完成的请求判为失败）。

提交只屏蔽取消/超时，不屏蔽事件语义：`response.failed` 仍由聚合器转成错误，
`response.incomplete` 仍按"未完成"报截断错误。

## 错误语义

| 场景 | HTTP 臂 | WS 臂 |
|---|---|---|
| `StreamBody` 非 200 / 升级失败（有响应） | `*HTTPError{StatusCode, Raw: body}` | `*HTTPError{StatusCode, Raw: ≤1KiB 截断 body, cause: *DialError}`（`errors.As` 可回溯到 `*DialError`，两侧错误分类面对称） |
| 拨号失败（**无**响应：连接拒绝 / 拨号中取消） | `*url.Error` | 原始错误链（`%w`），**不是** `*HTTPError`（`errors.Is(ctx)` 保持成立） |
| 预取消 / 拨号中取消 | ctx 错误 | `errors.Is(ctx)` 成立，无 `*HTTPError` |
| 读 body 出错 | 裸错误（`*url.Error` 等） | 裸错误（底层读错；不包成 `*HTTPError`） |
| close frame | 无此形态 | pumped：`io.EOF`；pull：`Client.Recv` 返回 `*CloseError` |
| pull 模式 `Recv` 的 ctx 取消 | — | 按 coder 语义**关闭连接**并返回 ctx 错误（不是"只影响本次取帧"） |
| 下游消费过慢（队列饱和） | 无此形态（背压直接传到 TCP） | `ErrWSBackpressure`（传输层错误，触发即终止连接，不得当 EOF） |
| 心跳探测超预算 | 无 | **无此形态**（主动探测已删除；`ErrKeepaliveTimeout` 仅保留为无产生点的已退役值） |
| `Responses` 读错 | `读取响应流失败: %w`（`scanSSE` 内包） | 同文案（`responsesStreamWS` 包 `exchange.Next` 的原始错误）；**错误链不同**：WS 臂不经过 scanner |
| `response.failed` | `*HTTPError{500, Raw: 事件 error 原始 JSON（缺失则整个事件）}` | 逐字相同（同一个 `failedError`） |
| `response.incomplete` / 无终态 | 普通错误（"事件流截断"） | 同文案（普通错误，不是 5xx 信封） |
| 复用连接写失败 | — | 原始错误链（终止连接，不重试、不轮转）；**复用命中**时错误文案额外带「连接年龄 + 本轮已交付帧数」诊断（`%w` 包装，分类不变） |
| 复用连接的首帧 | — | **不做任何判定**：首帧就是普通帧，不认识的事件由消费者忽略（旧尾帧也一样） |
| 服务端 60min 上限 | — | 本地 30min 在空闲时提前退役；若仍收到 `websocket_connection_limit_reached`（该帧不带 status），按普通 in-band error 透传，不重试 |
| 坏请求体（坏 JSON / 非对象根 / 缺字符串 `model`） | 本地错误 `ErrInvalidRequestBody`：**不发请求** | 本地错误 `ErrInvalidRequestBody`：**不拨号、不发帧** |

`ErrInvalidRequestBody` 是本地输入错误（裸 error，不是 `*HTTPError`），两臂一致。

`HTTPError` 新增未导出 `cause` + `Unwrap()`：WS 臂的升级失败在保持
`StatusCode`/`Raw` 形状的同时，让 `errors.As` 能回溯 `*DialError`、
`errors.Is` 能回溯 ctx 错误——网关的判死/重试分类不必按臂分叉。

## 连接复用

默认关闭，`WithWSConnCache(n)`（n > 0，建议 2）开启每个 `HTTPClient` 的空闲连接缓存。
连接身份是 `(threadKey, readLimit)`，`threadKey = InstallationID|Session.SessionID|Session.ThreadID`
（`Session.SessionID` 为空 = 匿名）：同一 thread、同取数路径才复用；流式 64MiB 与聚合 16MiB 不共享连接；
**window 变化不换连接**（与真实 codex 一致——换连接条件里没有 window），ts 每请求下发、不进连接身份。
**匿名（`Identity{}`）不复用**：不取缓存、读完即终止，因为匿名没有会话语义，共享连接等于让无关请求
互相污染路由状态。
Authorization token 必须仍一致，取出即独占，不做请求多路复用。

归还资格是读到**正常**终态（`response.completed`）、队列空且连接可用；**失败终态
（`response.failed` / `response.incomplete`）、取消、读写错误、提前关闭均终止连接**
（客户端语义：错误即退役）。
判定发生在**已经解析事件**的地方（聚合器 / `sseView`）：它们看到失败终态就调
`frameStream.MarkFailed()`，`Release` 见到标记一律拒绝并终止——传输层仍然不做任何帧类型判定。
`StreamBody` 须读到终态后关闭 body 才归还（failed/incomplete 已自动标记）；`Responses`
消费终态后自动归还。空闲期收到任何帧
即判废（fence/poisoned，见下）；取用前复查连接并解栏，失败则在发送前新拨，开始写 create 后不再重试。

**字节透传调用方的契约**：若自己消费帧而不经过上述两层（如网关 relay），看到失败终态必须
调 `MarkFailed()` 或直接 `Close()` 再收尾，否则连接会被当成干净结束而复用（内置 `sseView`
已自动标记，所以走 `StreamBody` 的 relay 无需自改）。

**帧不做任何类型判定**：首帧就是普通帧，旧尾帧/未知事件由消费者（聚合器 / relay）自己忽略。
依据是真实 codex 客户端：它没有首帧白名单、没有 `created` 门槛、没有 `response_id` 归属过滤。
明确不做：响应 ID 过滤、created 门槛、归还后静默窗、写后自动重试（create 非幂等 ⇒ 重复计费）。

**唯一有意偏差**：`errIdleFrame` / `fence` / `poisoned` 保留——空闲期（已归还、未取用）收到帧
仍终止该连接、不交给下一个请求。理由：网关从 `response.completed` 嗅探 usage 计费，
把上一轮终态记到新请求上是账目错误；客户端不承担这个职责。这是**复用准入**的保守，不是协议判定。

空闲 TTL 为 5min，寿命从最初拨号累计 30min，复用不重置寿命；仅清扫缓存中的
空闲条目，不中断在飞请求。清扫由 `connCache` 自持的维护 ticker（默认 30s）驱动，
外加 get/put 的惰性清理：不再搭载心跳（主动探测已删除）。ticker 的生命周期与
"表里有没有东西"绑定（首次 `put` 惰性启动、表空即停、`Close` 停），不再存在
"表永远空、ticker 永远转"的残留。
`CloseIdleConnections()` 清空 WS 缓存里的空闲连接（缓存本身仍可用，之后新建的连接照常复用），
**并**对底层 `*http.Transport` 断言后清空其空闲池（包装型 transport 断言失败即静默 no-op）；
`CloseWSIdleConnections()` 只**关闭** WS 缓存（含维护 ticker）、不碰 `*http.Transport`：清空现有 idle，此后取用也
直接未命中、归还一律拒收（由 `Release` 终止）——用于"这个 client 要被丢弃了"的场景，**不可逆**。
线程退休用 `RetireThread(threadKey)`：只摘该 thread 的 idle（前置条件：该 thread 无在飞 exchange，
由会话独占租借保证），不设墓碑；`Close` 之后 idles 已空，本调用自然 no-op。

观测使用内部原子 `hits` / `misses`（不新增公开 API）：开启缓存且非匿名时，每次打开 exchange 计一次
（未开缓存 / 匿名请求不入统计），只有连接真的被移交（`releasable` 复查 + `unfence` 通过）才算 hit。
bench 的 P6 验证一次 accept 与每个计时请求命中，R18 验证 `accepts == misses` 且
`hits + misses == 请求数`。
从同一 thread 的**第 2 个请求**起就有收益（跨轮同样命中），收益 ∝ 命中次数；
每实例、每条取数路径各留一条空闲连接，容量按 2 规划。

## 已知限制

- **`DialError` 的 body 有上限**：升级失败的响应体只保留前 1024B（够网关取
  `error.message` / `error_code`，不整篇透传）。`DialError.Refreshed`（401 已轮转过）
  在 `*HTTPError` 信封上不可见（`StatusCode` + `Raw` + cause 链可见；`RawJSON()`
  给出 `Raw` 的文本视图——网关 `upstreamBody` 的信封协议，WS 错误帧翻成的
  `*HTTPError` 也走同一条路）。
  无升级响应的失败不再是 `DialError`（`StatusCode==0` 形态消失）。
- **turn-state 的来源与跨轮**：请求侧只有一个来源——`Identity.Turn.State`（`""` = 本次不带）。
  捕获到的签发值只回调 `Identity.Sink`，SDK **不缓存、不回落、不回填响应头**：重放策略由调用方
  在构造 `Identity` 时决定（网关侧是会话对象上的一个策略位）。
  **身份头与形态头是保留键**：`WithHeader` 遇到 `session-id` / `thread-id` / `x-client-request-id` /
  `x-codex-window-id` / `x-codex-installation-id` / `x-codex-turn-state` / `version` /
  `x-codex-routing-hint` / `x-codex-beta-features` / `x-codex-turn-metadata` /
  `x-openai-internal-codex-responses-lite` / `accept` / `content-encoding` 会在构造期 panic——
  身份只能经 `Identity` 传入、形态头只由 SDK 合成，默认头不能成为第二条通道。
- **与官方的已知偏差**：握手压缩扩展是 `permessage-deflate`（官方另带
  `client_max_window_bits`，coder 产不出）；HTTP/1.1 头顺序由 Go 决定；
  `Accept-Encoding` 由调用方 Transport 的 `DisableCompression` 关闭。
- **真实上游 in-band 429 未实测**：真实上游在 WS 帧里以 `type:"error"` + `status:429`
  表达额度耗尽的情况尚未拿到活体验证。代码侧已按参考实现处理：带 `status`/`status_code`
  且 ∈ 4xx/5xx → 翻译成 `*HTTPError{StatusCode}`（流式与非流式两臂都是）；**缺 status 的
  错误帧仍然透传**（与 HTTP 臂的 200 流一致）——那种形状在非流式臂上仍会表现为
  "事件流截断"，若上游确实发这种帧，需要先实测帧形再裁定。
- **读模型（传输臂）：常驻读者 + 有界队列**。拨号成功后 SDK 起一个 goroutine
  持续读 socket，把帧压进队列（256 帧 / 8MiB）；`Read` 只从队列取。**为什么不能
  纯拉**：对端 ping / pong 等控制帧只在有 Read 在飞时才被服务，纯拉模型下
  下游一慢就无人应答——那是只属于 WS 臂的截断源。队列饱和 ⇒ `ErrWSBackpressure`
  （快速失败；单帧大于字节闸但队列为空时照常通过）。
- **读空闲上限对 pull 是契约变更（默认开）**：`Client.Recv` 由"无期限读 socket"
  变为"每次调用有 300s（`WithWSIdleTimeout` 可配）上限"——一次调用内没有完整数据
  消息就返回 `ErrWSIdleTimeout` 并**关闭该连接**。跨轮常驻的读循环（网关 relay）
  因此会在**轮间静默超过预算**时关掉整条会话；`Closed()` 语义不变（仍只表示本地
  `Close/CloseNow`），不调用 `Recv` 时没有计时器。
  **运维限制**：模板 ext 目前还不能调这个值（与 `responsesWSFirstFrameTimeout`
  同状态），今天只能改 SDK option 或代码；精确回滚用 `WithWSIdleTimeout(0)`。
- **无主动心跳的作用面**：没有本地探针 ⇒ 半开连接不会被本地判死（客户端也如此）；
  存活只能靠读写错误、对端 close、或调用方 ctx / 自行 `Client.Ping` 兜底。
  这是对齐客户端后接受的代价，不是遗漏。
- **连接可复用（默认关）**：默认关闭，`WithWSConnCache(n)` 开启；`StreamBody`
  读到终态后由 `Body.Close()` 尝试归还，否则强断。从同一 thread 的第 2 个请求起
  就有收益（跨轮同样命中），收益 ∝ 命中次数；每实例、每条取数路径各一条空闲连接。
- **空闲连接的退役不依赖心跳**：`connCache` 自带维护 ticker（默认 30s，只 sweep）
  与 get/put 惰性清理；`WithPingInterval` 已无效果，不再影响退役时延。
- **事件流 turn-state**：`response.metadata` 的 `headers` 里带 ts 时，`turnStateTap`
  在取帧旁路捕获（不复制/不改写/不重排/不丢帧）。判定顺序：每帧先取事件类型 →
  `response.created` 记 `response_id` → `*.metadata` 做子串快筛 → 归属匹配
  （`response_id == seenID && seenID != ""`）才写 `Identity.Sink`。fail-safe：带 ts 的 metadata
  若 `seenID` 仍为空（`response.created` 未到）或 `response_id` 不归属本轮，一律不捕获（`dropped`
  计数 +1，帧照旧透传；不带 ts 的 metadata 连计数都不加）。`Sink` 只在调用/`Read` 栈内同步回调，
  `Body.Close` 返回后不再回调——所以不需要代际标志。
- **回退链的白名单扫描是两次**（`buildResponsesCreateFrame` 内一次 + `normalize`
  再一次，与 T3 完全一致）：只有形态不在单遍覆盖内（非紧凑 / 转义键 / 重复键 /
  白名单外键）才走到；真实 codex 帧恒走单遍，那是 1 次。代价是这类形态比 T3 多付
  一次 `gjson.ValidBytes`（单遍的前置校验），3.7KB 体上约 +2µs——"不劣化"只对
  主路径成立（P1 测的正是主路径）。
- **转发到拨号的选项**：`WithTransport` / `WithHeader`（身份保留键会在构造期 panic）/
  trace / turn / compression / ping / beta；身份（session/thread/window/metadata/ts）
  不在此列——它随 `Dial` / `StreamBody` / `Responses` 的 `Identity` 实参进入握手头与帧/体的
  `client_metadata`。
  `WithTimeout` 现在**对 WS 臂生效**：`>0` 时设在请求 exchange 的 ctx 上，
  覆盖请求使用期，含拨号/升级（T5 起）；`=0` 时沿用调用方的取消与 deadline。
  两者是**独立预算**，不承诺墙钟严格"先到"：同时就绪时调用方 ctx 优先
  （沿用取帧的既有次序）。`WithTimeout` 会按墙钟砍断长流，**不能**当 idle 用。
  `WithWSIdleTimeout` 只作用于"等一帧"（默认 300s；`<=0` 关闭）。
  `WithMaxLineSize` 仍只作用于 HTTP SSE 行扫描。
- **单消费者**：`Client.Recv` 与 `wsExchange.Next` 都不支持并发调用（未定义）；
  写与 ping 由内核写闸串行化。

## 性能预算

三条 benchmark 是硬门槛（`bench_ws_test.go`，T0 基线固化在同一文件里；数字为
AMD Ryzen 7 7840H 上 `-count=10` 的中位数，换机器只比"与基线的比值"，不比绝对值）：

| 门槛 | benchmark | T0 基线 | 现值 | 判据 |
|---|---|---|---|---|
| P1 帧编码（3.7KB 体） | `BenchmarkFrameCodecPrepare3KB` | 34,930 ns / 21,992 B / 26 allocs | **6,782 ns / 5,040 B / 5 allocs** | ≤20µs / ≤12KB / ≤12 allocs |
| P2 帧→SSE 渲染（1KB 帧） | `BenchmarkSSEViewRender1KB` | 2,312 ns / 2,720 B / 3 allocs | **72 ns / 0 B / 0 allocs** | 只判 `0 B / 0 allocs`（口径从"构建一块"变成"三段渲染 + 读尽"，ns/op 不与 T0 纵向比） |
| P3 WS 每请求（整链路含握手） | `BenchmarkWSStreamBodyPerRequest` | 468 allocs | **456–459 allocs** | 不得劣化：≤ T0 × 1.05 |

P3 的绝对值里 ~35% 是假上游（`httptest` 侧）、~22% 是握手，SDK 可控面只有 ~31
allocs——所以 P3 只保证**不劣化**，真正的分配改善在 P1/P2。

```bash
go test -run '^$' -bench '^BenchmarkFrameCodecPrepare3KB$'  -benchmem -count=10 .
go test -run '^$' -bench '^BenchmarkSSEViewRender1KB$'       -benchmem -count=10 .
go test -run '^$' -bench '^BenchmarkWSStreamBodyPerRequest$' -benchmem -benchtime=20x -count=3 .
```

整形的官方一致性不靠"读代码相信"，靠 `shape_official_test.go`：用
`testdata/official-0.169.0/` 的脱敏抓包作输入、给定抓包里的身份，HTTP / HTTP→WS /
WS→WS 三种面的组装结果与抓包逐字节相等（`client_metadata` 按集合比较，发送时刻除外；
替换规则见夹具 README）。

## 测试

`transport_ws_test.go`（`httptest` + `websocket.Accept` 假上游）：帧→SSE 逐字节
断言（含 `[DONE]` 位置）、与 HTTP 臂合成体逐字段相等、
`response.failed` 两臂同形、身份与 turn-state（请求侧只认 `Identity.Turn.State`；升级头不携带；
只有 `response.metadata` 事件捕获后回调 `Identity.Sink`，握手响应头不取；匿名时 payload 自带的
`client_metadata` 被删除；连接身份 = `(threadKey, readLimit)`，跨 window 复用、匿名不复用；
`RetireThread` 只摘 idle）、HTTP→WS 帧构造规则 + 无效请求体、关闭
语义（1011 / 1005 / 1000 / 无 close 帧）、默认路径零 WS 连接、并发（`-race`）。

`transport_ws_contract_test.go`（连接内核验收面，W1–W16）：

- 队列饱和报 `ErrWSBackpressure`（不是传输层被误关的网络错误）、消费侧取消报 ctx 错误、
  拨号被拒/预取消不产生 `*HTTPError`、升级失败保留 body 与 cause；
- 连续巨帧背压 / 单巨帧放行、停读 + `WithTimeout` 报 `DeadlineExceeded` 且
  goroutine 回落、close frame = `io.EOF`、终止先排空队列、帧数/字节双闸、
  首因唯一、终态后取消不改写、已入队终态帧优先于 deadline、**无主动 ping**
  （假上游 `OnPingReceived` 计数为 0）、帧形状全量 golden、`Close` 幂等不 panic、空队列
  `pop` 被 `close` 唤醒。

`transport_ws_resident_test.go`（常驻读 / 错误面 / 传输前提）：

- 消费者**故意空闲 3s** 后仍能读尽全部帧——纯拉模型在此用例下必红；
- `Close` 及时返回且幂等；
- 升级 400 的 body 进 `*HTTPError.Raw`——两臂 `error_message_contains` 面对称；
- **h2 能力 transport 回归**：`ForceAttemptHTTP2: true` 的出口 transport 下普通请求走
  h2、WS 升级仍成功（`net/http` 对 `Connection: Upgrade` 自动降 h1）。生产默认就是
  这条路径（`upstream.force_http2` 默认 true）。
