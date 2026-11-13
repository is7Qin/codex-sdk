# official-0.169.0 夹具

官方 `codex-cli 0.169.0`（`openai/codex` 标签 `rust-v0.169.0`）的请求形态抓包，供
`shape_official_test.go`、`shape_transport_test.go`、`bench_shape_test.go` 使用。
测试只读本目录，不依赖 `/tmp`。

## 来源

抓包方法：本机 `codex-cli 0.169.0`，隔离配置目录，假凭据，指向本机假上游（假上游对
预热只回 `created`、`metadata`、`completed`）。原始记录每行一个 JSON
（`kind` = `http` / `ws_handshake` / `ws_frame`，带 `headers` / `body`，请求体已解压）。

| 文件 | 抓包 | 内容 |
|---|---|---|
| `tui-handshake.headers.json` | `log169-tui.jsonl` seq 21 | 交互式客户端（codex-tui）WS 握手头 |
| `tui-prewarm.json` | 同上 seq 29 | 预热帧（`generate:false`、空输入），lite 模型 `gpt-6.1-sol` |
| `tui-turn.json` | 同上 seq 35 | 一轮的第一个真实帧（全量输入，非 resume、`thread_source=user`） |
| `tui-followup.json` | 同上 seq 40 | 同一轮的增量帧（`previous_response_id` + 工具输出） |
| `tui-chained-handshake.headers.json` | `log169b-tui.jsonl` seq 21 | 同一交互式客户端的另一次运行（假上游的 `response.completed` 带 response id）：握手头（prewarm 形态 turn-metadata） |
| `tui-chained-prewarm.json` / `tui-chained-turn.json` | 同上 seq 29 / 35 | 预热帧；预热后的真实帧（`previous_response_id` = 预热 id、全量输入、预热签发的 ts） |
| `title-prewarm.json` / `title-turn.json` | 同上 seq 39 / 42 | 标题线程（`gpt-5.6-luna`，`text.format` = JSON schema）：预热帧；真实帧不带预热 id、全量输入。`thread_source` 是 `thread_title`，比对时替换为 `user`，且 turn-metadata 不比（标题线程没有 `turn_trigger`；网关不产生这类请求） |
| `exec-ws-handshake.headers.json` | `log169-ws.jsonl` seq 11 | `codex exec` WS 握手头 |
| `exec-ws-prewarm.json` / `exec-ws-turn.json` / `exec-ws-followup.json` | 同上 seq 14 / 19 / 21 | `codex exec` 的预热、全量、增量帧 |
| `exec-http-turn.json` (+`.headers.json`) | `log169-http.jsonl` seq 18 | `codex exec` HTTP 请求体与请求头（第一轮第一个请求） |
| `exec-http-followup.json` (+`.headers.json`) | 同上 seq 20 | 同一轮后续请求（带 `x-codex-turn-state` 请求头） |
| `gpt55-handshake.headers.json` | `log169-gpt55.jsonl` seq 10 | 非 lite 模型 `gpt-5.5` 的 WS 握手头 |
| `gpt55-prewarm.json` / `gpt55-turn.json` / `gpt55-followup.json` | 同上 seq 13 / 18 / 20 | 预热（带 `tools` 与指令消息）、预热后的真实帧、增量帧 |
| `gpt55-instructions.txt` | 同上 seq 13 `input[0].content[0].text` | 指令原文（A-4：官方 UUIDv5 id 校验需要原文） |
| `http-body-turn.zst` / `http-body-followup.zst` | `raw-zstd/body-1794575967084.zst` / `…182.zst`（`log169-zstd.jsonl` seq 18 / 20） | **原始** zstd 请求体（线上字节，未脱敏），帧头 `28b52ffd0058` |
| `http-body-turn.headers.json` | `log169-zstd.jsonl` seq 18 | 上一行请求体对应的请求头 |

生成脚本：`gen.py`（`python3 gen.py [抓包目录]`，默认 `/tmp/cx-capture`）。只在更新
夹具时手动运行。

## 脱敏

- 请求头 `authorization` 换成 `Bearer <redacted>`（抓包本来就是假凭据 `test-access-token`）。
- 请求体 `input` 与 `tools` 里超过 400 字符的字符串换成
  `<redacted len=<原长> sha256=<前 12 位>>`（同一原文得到同一占位符，跨文件一致）。
  例外：含 `<environment_context>` 的字符串保留（SDK 的环境时间改写要在夹具上真实
  发生）；`gpt55-prewarm.json` 里的指令原文保留（它的 id 是原文的 UUIDv5）。
- 键序、结构、其余字节不变：JSON 以紧凑形式写出（`ensure_ascii=False`、无空白），
  与官方 serde_json 的输出逐字节一致（已对原始 zstd 体验证：Python 重新序列化的解压
  字节 == 原始字节）。
- `client_metadata`、turn-metadata、身份值都是抓包时的假身份，原样保留（测试从中还原
  身份）。`*.zst` 原样复制。

## 比对时的替换规则

SDK 伪装的是 codex-tui，且部分取值由用户裁决固定（spec §2、§5.3），所以比对前对
**期望值**（抓包）做以下替换，再与 SDK 输出逐字节比较：

| 抓包取值 | 替换为 | 原因 |
|---|---|---|
| turn-metadata `sandbox`：`seccomp` | `none` | 用户裁决：固定 `PermissionProfile::Disabled` 组合 |
| turn-metadata `sandbox_mode`：`workspace-write` / `read-only` | `danger-full-access` | 同上 |
| turn-metadata `turn_trigger`：`exec` | `user` | exec 夹具 → tui 取值 |
| turn-metadata `history_initialization`：`cold_resume` | `new` | 用户裁决：恒 `new`（本目录的帧都不是 resume，规则备用） |
| 请求头 `originator` / `user-agent`：`codex_exec…` | `codex-tui` / `DefaultCodexUserAgent` | exec 夹具 → tui 取值 |
| 环境上下文 `<current_date>` / `<timezone>` | 期望值同样经 `RewriteEnvironmentContextTime` | SDK 把本地时间改写成美东时间 |

替换同时作用于 JSON 里的转义写法（`\"sandbox\":\"seccomp\"`，turn-metadata 在请求体
里是字符串值）和请求头里的原始写法。比较方式：除 `client_metadata` 外逐字节相等；
`client_metadata` 按键值集合比较（官方是 Rust `HashMap`，键序随机），
`x-codex-ws-stream-request-start-ms` 只校验是 13 位毫秒。逐字节比对只用 tui 中非
resume、`thread_source=user` 的帧，以及按上表替换后的 exec 帧。
