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
| 请求 | `exchange.go` | `wsExchange`：一次请求 = 一次 exchange；连接可跨请求（含跨轮）复用（默认关），保留本次请求的 turn-state 快照与 `Next` 取帧 |
| wire 编码 | `codec.go` | `frameCodec`：无可变状态；`encode` = 单遍快路径（`encodeOnePass`）+ 回退链（非常规形态） |
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
| `Responses` | `Stream` → `responsesAggregator` → `HTTPResponse{200, Raw, TurnState}` | `responsesStreamWS` **直接从 exchange 取帧**喂同一个聚合器（不经 `sseView`/`scanSSE` 的渲染-回扫回路）→ 同形态、逐字段同构 |
| turn-state | 请求头 `x-codex-turn-state`（`WithTurnStateProvider` 逐请求求值，`WithHeader` 兼容）+ 响应头捕获 | 帧 `client_metadata."x-codex-turn-state"`（每请求取值）；握手响应头签发值回写 `TurnState()`；`response.metadata` 事件的 ts 由 `turnStateTap` 逐帧旁路捕获（created 先记 response_id、归属匹配才接受）；合成响应头按 S5 判据回填（`effective != "" && effective == TurnState()`），跨轮不回填旧值 |
| 读上限 | 聚合器单行 16MiB；裸 body 由下游决定 | 按取数路径固定：聚合 16MiB / 流式 64MiB（`WithReadLimit` 只作用于公开 `Dial`，传输臂的上限由取数路径决定）；两条取数路径不共享连接（16MiB / 64MiB） |
| 请求体归一 | `FilterCodexHTTPPayload` + `store:false` + HTTP `client_metadata` | `frameCodec.encode(encodeWSResponses)`：协议形状差（`type`/续接键/`stream`）+ 白名单 + `store:false` + WS `client_metadata` 由**单遍编码器**一次落地；非常规形态回退 `buildResponsesCreateFrame` → 归一链 |
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

帧构造规则（五条，`encodeOnePass` 与回退链 `buildResponsesCreateFrame` 同义）：
JSON 合法 + **对象根** → 覆写 `type:"response.create"` → 删
`previous_response_id` / `generate` → 强制 `stream:true` → 过 WS 19 键白名单
（空帧报错）。**全部在拨号之前完成**。

单遍只接**紧凑 / 无转义键 / 无重复键 / 顶层键全在白名单内**的对象（此时旧链只
原位换值、键序不变，逐字节等价）；其余形态（空白、转义键、重复键、白名单外键）
一律回退旧链——回退不是保守，是字节一致性的**唯一**保证方式（`codec.go` 的条件表
与 `codec_internal_test.go` 的逐字节护栏钉住这条边界）。

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
| `Responses` 读错 | `读取响应流失败: %w`（`scanSSE` 内包） | 同文案（`responsesStreamWS` 包 `exchange.Next` 的原始错误）；**错误链不同**：WS 臂不经过 scanner；复用路径上首帧违约另报 `errConnDesynced` |
| `response.failed` | `*HTTPError{500, Raw: 事件 error 原始 JSON（缺失则整个事件）}` | 逐字相同（同一个 `failedError`） |
| `response.incomplete` / 无终态 | 普通错误（"事件流截断"） | 同文案（普通错误，不是 5xx 信封） |
| 复用连接写失败 | — | 原始错误链，终止连接，不重试、不轮转 |
| 复用首帧违约 | — | `errConnDesynced`，终止连接 |
| 服务端 60min 上限 | — | 本地 30min 在空闲时提前退役；若仍收到 `websocket_connection_limit_reached`，按普通 in-band error 透传，不重试 |
| 坏请求体（坏 JSON / 非对象根） | 原样发上游，由上游 400 兜底 | 本地错误 `errResponsesFrameInvalid`：**不拨号、不发帧** |

`errResponsesFrameInvalid` / `ErrEmptyFrame` 是本地输入错误（裸 error，不是
`*HTTPError`）。WS 面没有"把坏 body 原样发出去让上游 400"的等价路径：`sjson` 对
顶层标量会**静默**把请求体改写成 `{}`，`null` 会变成
`{"type":"response.create","stream":true}`——那与 HTTP 臂语义立刻不同，所以只能
本地拒绝。

`HTTPError` 新增未导出 `cause` + `Unwrap()`：WS 臂的升级失败在保持
`StatusCode`/`Raw` 形状的同时，让 `errors.As` 能回溯 `*DialError`、
`errors.Is` 能回溯 ctx 错误——网关的判死/重试分类不必按臂分叉。

## 连接复用

默认关闭，`WithWSConnCache(n)`（n > 0，建议 2）开启每个 `HTTPClient` 的空闲连接缓存。
连接身份是 `readLimit`（**不含** turn-state）：同一 `HTTPClient`、同取数路径即可复用；流式 64MiB 与聚合
16MiB 不共享连接。ts 是轮级、每请求下发——与真实 codex 一致（连接 thread 级、换连接条件里没有 ts）。
Authorization token 必须仍一致，取出即独占，不做请求多路复用。

归还资格是读到终态、队列空且连接可用；取消、读写错误、提前关闭均终止连接。
`StreamBody` 须读到终态后关闭 body 才归还；`Responses` 消费终态后自动归还。空闲期收到任何帧
即判废；取用前复查连接并解栏，失败则在发送前新拨，开始写 create 后不再重试。

空闲 TTL 为 5min，寿命从最初拨号累计 30min，复用不重置寿命；仅清扫缓存中的
空闲条目，不中断在飞请求。清扫发生在心跳 tick 与取还时；`WithPingInterval(0)`
关掉心跳后**只剩取还时清扫**——被丢弃的 `HTTPClient`（网关每轮重建）若此后不再有
请求，其空闲连接只能等对端断开（服务端 60min 上限）；默认 30s 心跳下最迟
5min 加一个 tick。`CloseIdleConnections()` 同时清 WS 缓存；只想清 WS 空闲连接、
不动共享 `*http.Transport` 时用 `CloseWSIdleConnections()`。
复用首帧只接受 `response.created`、`response.in_progress`、`error`、`*.metadata`
或 `responsesapi.websocket_timing`，否则报 `errConnDesynced` 并终止。

观测使用内部原子 `hits` / `misses`（不新增公开 API）：每次打开 exchange 计一次，
只有实际移交成功才算 hit。bench 的 P6 验证一次 accept 与每个计时请求命中，R18
验证 `accepts == misses` 且 `hits + misses == 请求数`。
从同一 thread 的**第 2 个请求**起就有收益（跨轮同样命中），收益 ∝ 命中次数；
每实例、每条取数路径各留一条空闲连接，容量按 2 规划。

## 已知限制

- **`DialError` 的 body 有上限**：升级失败的响应体只保留前 1024B（够网关取
  `error.message` / `error_code`，不整篇透传）。`DialError.Refreshed`（401 已轮转过）
  在 `*HTTPError` 信封上不可见（`StatusCode` + `Raw` + cause 链可见）。
  无升级响应的失败不再是 `DialError`（`StatusCode==0` 形态消失）。
- **turn-state 的来源与跨轮**：请求侧只认两处——`WithTurnStateProvider`
  （每次请求求值，**优先**；网关的正式入口）与 `WithHeader(HeaderTurnState, ...)`
  （兼容入口：该头**不进升级头**，WS 面只进帧内 `client_metadata`）。两者都空 ⇒
  本次请求不带 ts。**不**回落 `HTTPClient.TurnState()`：缓存只作"上次签发过什么"
  的观测面，当默认值会让跨轮第一个请求重放上一轮的 ts。跨轮只需清会话里的 ts
  （provider 返回空），**连接不动**；`CloseWSIdleConnections()` 只清 WS 池。
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
- **心跳的作用面**：传输臂的保活不依赖调用方读频率（常驻读者在服务 pong），空闲期同样生效；
  但 `WithPingInterval(0)` 会**关掉探测本身**——连接照样能服务 pong，只是半开
  连接不再由 `ErrKeepaliveTimeout` 判死，只能靠读写错误或调用方 ctx 兜底。pull
  面沿用既有契约：心跳依赖调用方的常驻 `Recv`。
- **连接可复用（默认关）**：默认关闭，`WithWSConnCache(n)` 开启；`StreamBody`
  读到终态后由 `Body.Close()` 尝试归还，否则强断。从同一 thread 的第 2 个请求起
  就有收益（跨轮同样命中），收益 ∝ 命中次数；每实例、每条取数路径各一条空闲连接。
- **关闭心跳会削弱空闲连接的退役**：`WithPingInterval(0)` 下没有心跳 tick，只剩取还时清扫；
  被丢弃的 `HTTPClient` 若此后不再有请求，其空闲连接最迟等到对端断开（服务端 60min 上限）。
  默认 30s 心跳下最迟 5min 加一个 tick，不受此限。
- **事件流 turn-state（S3）**：`response.metadata` 的 `headers` 里带 ts 时，
  `turnStateTap` 在取帧旁路捕获（不复制/不改写/不重排/不丢帧）。判定顺序：每帧先
  取事件类型 → `response.created` 记 `response_id` → `*.metadata` 做子串快筛 →
  归属匹配（`response_id == seenID && seenID != ""`）才写 `TurnState()` + observer。
  fail-safe：created 到达前的 metadata 一律不捕获（`dropped` 计数 +1，帧照旧透传）。
  合成响应头不可能承载后到的值（响应头在读到任何帧之前就已定死）——依赖 observer。
- **回退链的白名单扫描是两次**（`buildResponsesCreateFrame` 内一次 + `normalize`
  再一次，与 T3 完全一致）：只有形态不在单遍覆盖内（非紧凑 / 转义键 / 重复键 /
  白名单外键）才走到；真实 codex 帧恒走单遍，那是 1 次。代价是这类形态比 T3 多付
  一次 `gjson.ValidBytes`（单遍的前置校验），3.7KB 体上约 +2µs——"不劣化"只对
  主路径成立（P1 测的正是主路径）。
- **转发到拨号的选项**：`WithTransport` / `WithSession` / `WithCodexMeta` /
  `WithHeader`（唯一例外：`x-codex-turn-state` 不进升级头，见上）/
  `WithClientMetadata` / trace / turn / compression / ping / beta。
  `WithTimeout` 现在**对 WS 臂生效**：`>0` 时设在请求 exchange 的 ctx 上，
  覆盖请求使用期，含拨号/升级（T5 起）；`=0` 时沿用调用方的取消与 deadline。
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

单遍编码器的字节等价不靠"读代码相信"，靠 `codec_internal_test.go`：把回退链当
参考实现，对同一批输入（52 条语料 + 2,000 帧固定种子随机帧 × 2 模式 × 3 种
metadata 组合）逐字节比对，另有两例分别钉住"运行期 3.7KB 形状必须走单遍"与
"该回退的形态确实回退"——防止护栏退化成"回退链 vs 回退链"的空转。

## 测试

`transport_ws_test.go`（`httptest` + `websocket.Accept` 假上游）：帧→SSE 逐字节
断言（含 in-band error 与 `[DONE]` 位置）、与 HTTP 臂合成体逐字段相等、
`response.failed` 两臂同形、turn-state 五条路径（请求侧 provider 逐请求求值 /
`WithHeader` 兼容入口；升级头不携带；握手头与 `response.metadata` 事件捕获；
S5 回填判据；**不**回落 `TurnState()` 缓存；连接身份只有 readLimit，跨轮复用）、帧构造五条规则 + 非对象根、关闭
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
