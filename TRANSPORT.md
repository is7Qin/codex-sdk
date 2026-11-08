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

## 等价面（对外形状与语义）

| 面 | HTTP 臂 | WS 臂 |
|---|---|---|
| `StreamBody` | `*http.Response{200, 未读 SSE body, 上游头}` | `*http.Response{200, 帧渲染的 SSE body, Content-Type + x-codex-turn-state 合成头}` |
| `Responses` | `Stream` → `responsesAggregator` → `HTTPResponse{200, Raw, TurnState}` | 同一个聚合器（喂 `StreamBody` 的 SSE 字节）→ 同形态、逐字段同构 |
| turn-state | 请求头 `x-codex-turn-state`（`WithHeader` 下发）+ 响应头捕获 | 帧 `client_metadata."x-codex-turn-state"`（同一份 `WithHeader` 值，或 SDK 缓存的最近签发值）；握手响应头的签发值回写 `TurnState()` 与合成响应头 |
| 读上限 | 聚合器单行 16MiB；裸 body 由下游决定 | 聚合 16MiB（`WithReadLimit`）/ 流式 64MiB——按取数路径对齐 |
| 请求体归一 | `FilterCodexHTTPPayload` + `store:false` + HTTP `client_metadata` | `buildResponsesCreateFrame` → `Send`（`FilterCodexWsPayload` + `store:false` + WS `client_metadata`） |
| 401 轮转 | 判死分类 + 单飞 refresh + 重试一次 | 同（`Dial` 内，`OAuthWithRotation`） |

帧渲染（`wsReader`，一帧一块）：

```
event:<data.type>\n
data:<帧字节 verbatim>\n
\n
```

- `response.completed` → 该块之后补 `data: [DONE]\n\n` 并 EOF。
- `response.failed` / `response.incomplete` → 帧透传后 EOF（不补 `[DONE]`）。
- `type:"error"` → **一律**帧透传并继续读（不分 created 前后；与 HTTP 臂 200 流
  里的 in-band error 同义）。
- 无 `type` 的帧只出 data 行（不写空的 `event:` 名）。
- 收到**任意** close frame（`CloseStatus >= 0`，含 1000/1001/1005/1011）= EOF；
  只有 `CloseStatus == -1`（对端没发 close frame：直接断 TCP / 连接重置 / 超
  ReadLimit）才是传输错误，且原样返回底层错误。

帧构造规则（`buildResponsesCreateFrame`）：JSON 合法 + **对象根** → 覆写
`type:"response.create"` → 删 `previous_response_id` / `generate` → 强制
`stream:true` → 过 WS 19 键白名单（空帧报错）。全部在**拨号之前**完成。

## 错误语义

| 场景 | HTTP 臂 | WS 臂 |
|---|---|---|
| `StreamBody` 非 200 / 升级失败 | `*HTTPError{StatusCode, Raw: body}` | `*HTTPError{StatusCode: DialError.StatusCode, Raw: DialError.Body}`（升级响应的 ≤1024B 截断 body，两侧错误分类面对称） |
| 读 body 出错 | 裸错误（`*url.Error` 等） | 裸错误（底层读错；不包成 `*HTTPError`） |
| 下游消费过慢（队列超预算） | 无此形态（背压直接传到 TCP） | `ErrWSBackpressure`（传输层错误，不得当 EOF） |
| `Responses` 读错 | `读取 SSE 流失败: %w` | 同文案同错误链（同一个 `scanSSE`） |
| `response.failed` | `*HTTPError{500, Raw: 事件 error 原始 JSON（缺失则整个事件）}` | 逐字相同（同一个 `failedError`） |
| `response.incomplete` / 无终态 | 普通错误（"事件流截断"） | 同文案（普通错误，不是 5xx 信封） |
| 坏请求体（坏 JSON / 非对象根） | 原样发上游，由上游 400 兜底 | 本地错误 `errResponsesFrameInvalid`：**不拨号、不发帧** |

`errResponsesFrameInvalid` / `ErrEmptyFrame` 是本地输入错误（裸 error，不是
`*HTTPError`）。WS 面没有"把坏 body 原样发出去让上游 400"的等价路径：`sjson` 对
顶层标量会**静默**把请求体改写成 `{}`，`null` 会变成
`{"type":"response.create","stream":true}`——那与 HTTP 臂语义立刻不同，所以只能
本地拒绝。

## 已知限制

- **`DialError` 的 body 有上限**：升级失败的响应体只保留前 1024B（够网关取
  `error.message` / `error_code`，不整篇透传）。`DialError.Refreshed`（401 已轮转过）与
  底层 `DialError.Err` 在 `*HTTPError` 信封上仍不可见，`errors.As` 只能拿到状态码与 body。
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
- **读模型：常驻读协程 + 有界队列**。拨号成功后 SDK 立即起一个 goroutine 持续
  `Recv`，把渲染好的 SSE 块压进队列（`wsQueueFrames=256` 帧 / `wsQueueBytesBudget=8MiB`）；
  `Read` 只从队列取。**为什么不能纯拉**：`coder/websocket` 的 `Ping` 自己不读连接，
  它等某次 Reader 调用把 pong 读上来（`conn.go:218-220`）。纯拉模型下，下游一慢就没
  有 Read 在飞，心跳 2s 超时后会 `CloseNow`——那是只属于 WS 臂的截断源。
  队列字节超预算 ⇒ `ErrWSBackpressure`（快速失败；单帧大于预算但队列为空时照常通过，
  64MiB 的合法巨帧不受影响）。`Close` 取消读协程、`CloseNow`、等读协程退出（幂等）。
- **心跳依赖常驻读者，而不是调用方的读频率**：沿用 SDK 默认 30s ping / 2s pong；
  由于上面的常驻读协程，下游即使长时间不 Read，连接也不会被心跳判死。网关若要给
  codex 客户端注入 `WithPingInterval(0)`，WS 臂会**静默失去**上游保活——不要这么做。
- **一次调用一条连接**：`StreamBody` 每次拨新连接，终态后由 `Body.Close()`
  关闭（`CloseNow`，不做关闭握手）；没有连接复用 / 池化。
- **转发到 `Dial` 的选项**：`WithTransport` / `WithSession` / `WithCodexMeta` /
  `WithHeader`（唯一例外：`x-codex-turn-state` 不进升级头，见上）/
  `WithClientMetadata` / trace / turn / compression / ping / beta。
  `WithTimeout`（`http.Client.Timeout`）不适用——WS 的预算是调用方 ctx。

## 测试

`transport_ws_test.go`（`httptest` + `websocket.Accept` 假上游）：帧→SSE 逐字节
断言（含 in-band error 与 `[DONE]` 位置）、与 HTTP 臂合成体逐字段相等、
`response.failed` 两臂同形、turn-state 三条路径（握手签发 → 回写 → 下一轮回传；
`WithHeader` per-request 注入；升级头不携带）、帧构造五条规则 + 非对象根、关闭
语义（1011 / 1005 / 1000 / 无 close 帧）、默认路径零 WS 连接、并发（`-race`）。

`transport_ws_resident_test.go`（常驻读 / 错误面 / 传输前提）：

- 消费者**故意空闲 3s**（> 2s pong 超时）后仍能读尽全部帧——纯拉模型在此用例下必红；
- 队列字节预算撑爆 ⇒ `ErrWSBackpressure`（不是丢帧、也不是无限缓冲）；
- `Close` 及时返回且幂等；
- 升级 400 的 body 进 `*HTTPError.Raw`——两臂 `error_message_contains` 面对称；
- **h2 能力 transport 回归**：`ForceAttemptHTTP2: true` 的出口 transport 下普通请求走
  h2、WS 升级仍成功（`net/http` 对 `Connection: Upgrade` 自动降 h1）。生产默认就是
  这条路径（`upstream.force_http2` 默认 true）。
