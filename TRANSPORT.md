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
| socket | `conn.go` | `conn`：唯一 `ws.Read` 调用点（`readFrame`）、写/探测闸 `gate`、唯一心跳 `keepalive`、唯一终止入口 `terminate`（经 `finish` 发布） |
| 队列 | `conn.go` | `frameInbox`：pumped 的有界入站队列（帧数 + 字节双闸） |
| 拨号 | `dial.go` | `dialConn`：升级头组装 / 401 轮转 / 升级错误面 |
| 请求 | `exchange.go` | `wsExchange`：一次请求 = 一条连接 + 一份 turn-state 快照 + `Next` 取帧 |
| wire 编码 | `codec.go` | `frameCodec`：无可变状态，`encode` 出字节 |
| SSE 表示 | `sse.go` | `sseView`：帧 → SSE 字节流（仅 `StreamBody`） |

### 两种连接模式（构造期固定、互斥）

| 模式 | 谁用 | 读 | pong 由谁服务 | 关闭 |
|---|---|---|---|---|
| `modePull` | 公开 `Dial` → `Client` | `Client.Recv` 直读 `readFrame`（无 pump、无队列） | 调用方的常驻 `Recv`（**既有契约不变**） | `closeGracefully`（coder 关闭握手，时延上界 ~10s） |
| `modePumped` | 传输臂（`StreamBody` / `Responses`） | 常驻 `readLoop` + `frameInbox` | pump 恒在 Read（除入队临界区） | 只有 `terminate`：收尾为 TCP 层强断（不做关闭握手；上游此时已发过 close 帧，属预期） |

为什么必须分两种：`coder/websocket` 的 `Read(ctx)` 被取消时会**关闭底层 socket**，
所以"先取消读、再做关闭握手"不可能（关闭帧永远发不出去）。而传输臂又必须
"读者永不停"才能既服务 pong、又在下游慢时报可归因的背压。

### pumped 的读者永不阻塞

`readLoop` → `frameInbox.push`：**满即 `ErrWSBackpressure`**（不阻塞、不丢帧、不
无限缓冲），`readLoop` 收到即 `terminate`。双闸：`256` 帧 / `8MiB`；**空队列允许
单帧超预算**（64MiB 的合法巨帧不能因为比字节闸大就被判死），但连续两个
`> queueBytes/2` 的巨帧在消费者暂停时会触发背压——这是已知取舍（W5b 固化）。

内存上界（pumped）= `queueBytes` + 1 帧（空队列例外）+ pump 正在投递的 1 帧 +
消费侧持有的 1 帧；`WithReadLimit(-1)` 时无总上界。

`pop` 的顺序是契约：① ctx 已结束 → 立即返回 ctx 错误（不 pop）；② 有帧 → 交付
（不再查 ctx）；③ 已关闭且空 → 返回终止 cause；④ 等 `notify` / ctx。

### 心跳与终止

- 唯一心跳：`conn.keepalive`（默认 30s ping / 2s 探测预算）。探测抢不到写闸
  （有写在飞 = 活性已证）就跳过本轮；**只有**探测超预算才 `terminate(ErrKeepaliveTimeout)`，
  其它错误（写失败等）不判活死。
- 探测的 ctx 派生自 `context.Background()`（固定 2s）：心跳不该被连接 ctx 的取消
  二次污染。公开 `Client.Ping` 则**完全沿用调用方 ctx**，不加内部封顶。
- `finish` 是唯一的状态/结果发布点：裁决 + 首因选择 + 发布在同一个 `mu` 临界区内
  完成；首个 cause 生效，心跳不覆盖首因。

## 等价面（对外形状与语义）

| 面 | HTTP 臂 | WS 臂 |
|---|---|---|
| `StreamBody` | `*http.Response{200, 未读 SSE body, 上游头}` | `*http.Response{200, 帧渲染的 SSE body, Content-Type + x-codex-turn-state 合成头}` |
| `Responses` | `Stream` → `responsesAggregator` → `HTTPResponse{200, Raw, TurnState}` | 同一个聚合器（喂 `StreamBody` 的 SSE 字节）→ 同形态、逐字段同构 |
| turn-state | 请求头 `x-codex-turn-state`（`WithHeader` 下发）+ 响应头捕获 | 帧 `client_metadata."x-codex-turn-state"`（同一份 `WithHeader` 值，或 SDK 缓存的最近签发值）；握手响应头的签发值回写 `TurnState()` 与合成响应头 |
| 读上限 | 聚合器单行 16MiB；裸 body 由下游决定 | 聚合 16MiB（`WithReadLimit`）/ 流式 64MiB——按取数路径对齐 |
| 请求体归一 | `FilterCodexHTTPPayload` + `store:false` + HTTP `client_metadata` | `frameCodec.encode(encodeWSResponses)` = `buildResponsesCreateFrame` → 归一（`FilterCodexWsPayload` + `store:false` + WS `client_metadata`） |
| 401 轮转 | 判死分类 + 单飞 refresh + 重试一次 | 同（`dialConn` 内，`OAuthWithRotation`） |

帧渲染（`sseView`，一帧一块）：

```
event:<data.type>\n
data:<帧字节 verbatim>\n
\n
```

- `response.completed` → 该块之后补 `data: [DONE]\n\n` 并结束。
- `response.failed` / `response.incomplete` → 帧透传后结束（不补 `[DONE]`）。
- `type:"error"` → **一律**帧透传并继续读（不分 created 前后；与 HTTP 臂 200 流
  里的 in-band error 同义）。
- 无 `type` 的帧只出 data 行（不写空的 `event:` 名）。
- 收到**任意** close frame（`CloseStatus >= 0`，含 1000/1001/1005/1011）= EOF；
  只有 `CloseStatus == -1`（对端没发 close frame：直接断 TCP / 连接重置 / 超
  ReadLimit）才是传输错误，且原样返回底层错误。

帧构造规则（`buildResponsesCreateFrame`）：JSON 合法 + **对象根** → 覆写
`type:"response.create"` → 删 `previous_response_id` / `generate` → 强制
`stream:true` → 过 WS 19 键白名单（空帧报错）。**全部在拨号之前完成**。

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
| 心跳探测超预算 | 无 | `ErrKeepaliveTimeout`（可 `errors.Is`） |
| `Responses` 读错 | `读取响应流失败: %w` | 同文案同错误链（同一个 `scanSSE`） |
| `response.failed` | `*HTTPError{500, Raw: 事件 error 原始 JSON（缺失则整个事件）}` | 逐字相同（同一个 `failedError`） |
| `response.incomplete` / 无终态 | 普通错误（"事件流截断"） | 同文案（普通错误，不是 5xx 信封） |
| 坏请求体（坏 JSON / 非对象根） | 原样发上游，由上游 400 兜底 | 本地错误 `errResponsesFrameInvalid`：**不拨号、不发帧** |

`errResponsesFrameInvalid` / `ErrEmptyFrame` 是本地输入错误（裸 error，不是
`*HTTPError`）。WS 面没有"把坏 body 原样发出去让上游 400"的等价路径：`sjson` 对
顶层标量会**静默**把请求体改写成 `{}`，`null` 会变成
`{"type":"response.create","stream":true}`——那与 HTTP 臂语义立刻不同，所以只能
本地拒绝。

`HTTPError` 新增未导出 `cause` + `Unwrap()`：WS 臂的升级失败在保持
`StatusCode`/`Raw` 形状的同时，让 `errors.As` 能回溯 `*DialError`、
`errors.Is` 能回溯 ctx 错误——网关的判死/重试分类不必按臂分叉。

## 已知限制

- **`DialError` 的 body 有上限**：升级失败的响应体只保留前 1024B（够网关取
  `error.message` / `error_code`，不整篇透传）。`DialError.Refreshed`（401 已轮转过）
  在 `*HTTPError` 信封上不可见（`StatusCode` + `Raw` + cause 链可见）。
  无升级响应的失败不再是 `DialError`（`StatusCode==0` 形态消失）。
- **turn-state 的来源与首轮**：WS 臂认两处——`WithHeader(HeaderTurnState, ...)`
  （网关既有的 per-request 注入手法；该头**不进升级头**，只进帧内
  `client_metadata`）优先，否则用 `HTTPClient.TurnState()`（最近一次握手签发值）。
  首帧不带 turn-state（与 HTTP 臂首请求一致），之后回传上一次签发值。跨轮清除靠
  调用方的 client 生命周期（网关按 turn-state 值重建 `HTTPClient`；构造本身零
  开销、懒建连接）——`HTTPClient` 未新增清除入口。
- **真实上游 in-band 429 未验证**：真实上游在 WS 帧里以 `type:"error"` 表达 429
  语义的情况未实证。当前按"一律透传"处理（与 HTTP 臂的 200 流一致），因此这类
  错误在非流式臂上表现为"事件流截断"而不是 429 —— 若上游确有该形态，需要重新
  裁定分类策略。
- **读模型（传输臂）：常驻读者 + 有界队列**。拨号成功后 SDK 起一个 goroutine
  持续读 socket，把帧压进队列（256 帧 / 8MiB）；`Read` 只从队列取。**为什么不能
  纯拉**：`coder/websocket` 的 `Ping` 自己不读连接，它等某次 Reader 调用把 pong
  读上来；纯拉模型下，下游一慢就没有 Read 在飞，心跳 2s 超时后会强断——那是
  只属于 WS 臂的截断源，且对端 ping 也无人应答。队列饱和 ⇒ `ErrWSBackpressure`
  （快速失败；单帧大于字节闸但队列为空时照常通过）。
- **心跳的作用面**：传输臂的保活不依赖调用方读频率（常驻读者在服务 pong）；
  但 `WithPingInterval(0)` 会**关掉探测本身**——连接照样能服务 pong，只是半开
  连接不再由 `ErrKeepaliveTimeout` 判死，只能靠读写错误或调用方 ctx 兜底。pull
  面沿用既有契约：心跳依赖调用方的常驻 `Recv`。
- **一次调用一条连接**：`StreamBody` 每次拨新连接，终态后由 `Body.Close()`
  强断（不做关闭握手）；没有连接复用 / 池化（复用属后续独立任务）。
- **转发到拨号的选项**：`WithTransport` / `WithSession` / `WithCodexMeta` /
  `WithHeader`（唯一例外：`x-codex-turn-state` 不进升级头，见上）/
  `WithClientMetadata` / trace / turn / compression / ping / beta。
  `WithTimeout` 现在**对 WS 臂生效**：`>0` 时设在 pumped 连接的 ctx 上（`=0` 时
  就是调用方 ctx）。`WithMaxLineSize` 仍只作用于 HTTP SSE 行扫描。
- **单消费者**：`Client.Recv` 与 `wsExchange.Next` 都不支持并发调用（未定义）；
  写与 ping 由内核写闸串行化。

## 测试

`transport_ws_test.go`（`httptest` + `websocket.Accept` 假上游）：帧→SSE 逐字节
断言（含 in-band error 与 `[DONE]` 位置）、与 HTTP 臂合成体逐字段相等、
`response.failed` 两臂同形、turn-state 三条路径（握手签发 → 回写 → 下一轮回传；
`WithHeader` per-request 注入；升级头不携带）、帧构造五条规则 + 非对象根、关闭
语义（1011 / 1005 / 1000 / 无 close 帧）、默认路径零 WS 连接、并发（`-race`）。

`transport_ws_contract_test.go`（连接内核验收面，W1–W16）：

- 队列饱和报 `ErrWSBackpressure`（不是心跳误判的网络错误）、消费侧取消报 ctx 错误、
  拨号被拒/预取消不产生 `*HTTPError`、升级失败保留 body 与 cause；
- 连续巨帧背压 / 单巨帧放行、停读 + `WithTimeout` 报 `DeadlineExceeded` 且
  goroutine 回落、close frame = `io.EOF`、终止先排空队列、帧数/字节双闸、
  首因唯一、终态后取消不改写、已入队终态帧优先于 deadline、半开连接报
  `ErrKeepaliveTimeout`、帧形状全量 golden、`Close` 幂等不 panic、空队列
  `pop` 被 `close` 唤醒。

`transport_ws_resident_test.go`（常驻读 / 错误面 / 传输前提）：

- 消费者**故意空闲 3s**（> 2s 探测预算）后仍能读尽全部帧——纯拉模型在此用例下必红；
- `Close` 及时返回且幂等；
- 升级 400 的 body 进 `*HTTPError.Raw`——两臂 `error_message_contains` 面对称；
- **h2 能力 transport 回归**：`ForceAttemptHTTP2: true` 的出口 transport 下普通请求走
  h2、WS 升级仍成功（`net/http` 对 `Connection: Upgrade` 自动降 h1）。生产默认就是
  这条路径（`upstream.force_http2` 默认 true）。
