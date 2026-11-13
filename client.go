package codexsdk

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync/atomic"
	"time"

	coderws "github.com/coder/websocket"
)

// 传输常量（对齐参考实现 sub2api openai_ws_*）。
const (
	// defaultReadLimit 单帧读取上限。coder/websocket 默认 32KB 过小——
	// Codex 事件（rate_limits / 大 delta）可超阈值，对齐 sub2api 的 16MiB。
	defaultReadLimit = 16 * 1024 * 1024
	// defaultPingInterval 是 WithPingInterval 的默认值。
	//
	// 注意：**主动心跳探测已删除**（对齐真实 codex 客户端：它只在收到 Ping 时回
	// Pong，从不主动周期 Ping）。这个默认值现在只用于让 WithPingInterval(0) 这类
	// 现有调用照旧编译；内核不再读它。
	defaultPingInterval = 30 * time.Second
	// defaultWSIdleTimeout 是 WS 读空闲上限的默认值：对齐真实客户端的
	// DEFAULT_STREAM_IDLE_TIMEOUT_MS = 300_000（5 分钟）。**默认开**——如果默认关，
	// "上游收下 create 却永不回帧"的会话就会在消费侧永久挂住（现场症状）。
	defaultWSIdleTimeout = 300 * time.Second
)

// StatusCode 是 WebSocket 关闭码（透传 coder/websocket.StatusCode），
// Close 关闭码透传 API。
type StatusCode = coderws.StatusCode

const (
	StatusNormalClosure   = coderws.StatusNormalClosure
	StatusGoingAway       = coderws.StatusGoingAway
	StatusProtocolError   = coderws.StatusProtocolError
	StatusPolicyViolation = coderws.StatusPolicyViolation
	StatusMessageTooBig   = coderws.StatusMessageTooBig
	StatusAbnormalClosure = coderws.StatusAbnormalClosure
	StatusInternalError   = coderws.StatusInternalError
)

// ErrMessageTooBig 单帧超过 ReadLimit（透传 coder/websocket）。
var ErrMessageTooBig = coderws.ErrMessageTooBig

// CompressionMode 是 WS 压缩协商模式（默认 CompressionContextTakeover：握手声明
// permessage-deflate，与官方客户端一致）。无论哪种模式，SDK 出站帧都不压缩
// （CompressionThreshold = math.MaxInt）：context takeover 的 flate 写入器每连接
// 常驻约 1.2MB，出站压缩不值这个内存；入站压缩帧照常解压。
type CompressionMode int

const (
	CompressionDisabled CompressionMode = iota
	// CompressionNoContextTakeover 逐消息独立压缩（无上下文延续）。
	CompressionNoContextTakeover
	// CompressionContextTakeover 跨消息复用压缩上下文（压缩率更高）。
	CompressionContextTakeover
)

func (m CompressionMode) coder() coderws.CompressionMode {
	switch m {
	case CompressionNoContextTakeover:
		return coderws.CompressionNoContextTakeover
	case CompressionContextTakeover:
		return coderws.CompressionContextTakeover
	default:
		return coderws.CompressionDisabled
	}
}

// DialError 携带 WS 升级失败信息（HTTP 状态码 + 底层错误），
// 网关据此区分鉴权失败（401/403）与可重试的传输失败。
type DialError struct {
	StatusCode int
	Err        error
	// Body 上游升级失败响应的**截断**响应体（≤ dialErrBodyLimit 字节，可为空）。
	//
	// 为什么必须有：HTTP 臂的 4xx/5xx 带 body，网关的 error_message_contains 规则与
	// 判死分类都靠它；升级失败的响应同样带 body（coder/websocket 在非 101 时返回
	// resp），丢掉它会让两臂的错误分类面不对称——同一个 400/429，HTTP 臂命中规则、
	// WS 臂命中不了，成功率之差就混进了"规则引擎差异"而不是传输差异。
	Body []byte
	// Refreshed 表示 SDK 已尝试过 OAuth 轮转刷新后仍失败（WS 升级 401 →
	// 单飞 refresh → 自动重连一次仍失败）。网关据此知道已轮转过，避免
	// 与 SDK 各自轮转造成双份刷新。仅 401 路径可置 true；PAT/oauthAuth
	// 不轮转，恒为 false。
	Refreshed bool
}

func (e *DialError) Error() string {
	if e.StatusCode > 0 {
		return fmt.Sprintf("codexsdk: websocket 升级失败（HTTP %d）: %v", e.StatusCode, e.Err)
	}
	return fmt.Sprintf("codexsdk: websocket 升级失败: %v", e.Err)
}

func (e *DialError) Unwrap() error { return e.Err }

// options 是 Dial / NewHTTPClient 的共享配置（无关字段按形态忽略）：WS 形态
// 字段与 HTTP 专用字段分组相邻，"伪装层" 字段两面共用。调用方统一经 Option
// 配置；本结构为单结构而非按形态拆分（wsOptions / httpOptions）。
type options struct {
	compression  CompressionMode
	readLimit    int64
	pingInterval time.Duration // 仅 API 兼容（WithPingInterval）；内核不再读
	beta         string        // Responses WS beta 版本（默认 DefaultBetaWS=2026-02-06，现役唯一真实值）
	headers      http.Header

	// 伪装层（Send 帧 / 升级与请求头）。身份是请求数据，经 Identity 逐次传入，
	// 不落在 options 上。
	trace     *TraceContext // 外部注入（WS 帧 metadata，优先于自动生成）
	traceAuto bool          // 每帧自动生成 trace（默认关，WS 专用）

	// firstFrame 是 WithFirstFrame 的首帧（仅 Dial 读）：握手头的 routing-hint
	// 与 turn-metadata 取自它。
	firstFrame []byte
	// conversation 是 WithConversation 的参数（仅 Dial 读）：身份由会话簿合成。
	conversation *conversationOption

	// HTTP 专用。
	timeout     time.Duration
	maxLineSize int
	// transport 是 HTTP / WS 两面共用的出站传输层（自定义 RoundTripper，如代理）。
	transport http.RoundTripper
	// responsesTransport 是 HTTPClient 的 Responses 出站传输选择（零值 "" =
	// TransportHTTP；只有显式 TransportWS 才走上游 WS，见 WithResponsesTransport）。
	// 仅 HTTPClient 的 StreamBody / Responses 读该字段；Dial 与其它端点方法不看。
	responsesTransport Transport
	keepaliveInterval  time.Duration // GenerateImageStream 等待期间 keepalive 事件间隔

	// wsConnCache 是 Responses WS 臂的空闲连接容量（0 = 不复用，默认）。
	wsConnCache int
	// wsSweepInterval 是空闲表维护 ticker 的节拍（0 = defaultConnSweepInterval）。
	// 包内私有（测试 seam）：它只控制 sweep 频率，不影响任何探测行为（已无探测）。
	wsSweepInterval time.Duration
	// wsIdleTimeout 是一次"消费侧取帧等待"的预算（默认 300s；<=0 = 关闭）。
	// 它**不是**请求总超时：与 WithTimeout 互不影响，也不进 exchangeCtx。
	wsIdleTimeout time.Duration
}

func defaultOptions() options {
	return options{
		compression:  CompressionContextTakeover,
		readLimit:    defaultReadLimit,
		pingInterval: defaultPingInterval,
		beta:         DefaultBetaWS,
		maxLineSize:  defaultMaxLineSize,

		keepaliveInterval: defaultKeepaliveInterval,

		wsIdleTimeout: defaultWSIdleTimeout, // 读空闲上限默认开（显式 opt-out 见 WithWSIdleTimeout）
	}
}

// Option 配置 Dial / NewHTTPClient。
type Option func(*options)

// reservedIdentityHeaders 是保留头：身份 / turn-state 只能经 Identity 传入，
// 官方形态头（version、routing-hint、beta-features、turn-metadata、lite 标记、
// accept、content-encoding）只由 SDK 合成。WithHeader 遇到它们一律构造期
// panic——否则默认头就成了第二条注入通道。匹配大小写不敏感。
var reservedIdentityHeaders = [...]string{
	HeaderSessionID,
	HeaderThreadID,
	HeaderClientRequestID,
	HeaderWindowID,
	HeaderInstallationID,
	HeaderTurnState,
	HeaderVersion,
	HeaderRoutingHint,
	HeaderBetaFeatures,
	HeaderTurnMetadata,
	HeaderResponsesLite,
	"accept",
	"content-encoding",
}

// resolveOptions 是 Dial / NewHTTPClient 共用的 Option 应用阶段：默认值 → 选项
// → 保留键校验。两条构造入口都经过这里，过滤点不会因入口不同而漏掉。
func resolveOptions(opts []Option) options {
	cfg := defaultOptions()
	for _, o := range opts {
		o(&cfg)
	}
	rejectReservedHeaders(cfg.headers)
	return cfg
}

// rejectReservedHeaders 对调用方头做大小写不敏感的保留键检查。
func rejectReservedHeaders(h http.Header) {
	for k := range h {
		for _, reserved := range reservedIdentityHeaders {
			if strings.EqualFold(k, reserved) {
				panic("codexsdk: WithHeader 不得设置保留头 " + reserved + "（身份经 Identity 传入，形态头由 SDK 合成）")
			}
		}
	}
}

// WithCompression 设置 WS 压缩协商模式（默认 CompressionContextTakeover）。
// 出站帧恒不压缩（见 CompressionMode）。
func WithCompression(mode CompressionMode) Option {
	return func(o *options) { o.compression = mode }
}

// WithReadLimit 设置 WS 单帧最大字节数（默认 16MiB；-1 表示不设限）。
func WithReadLimit(n int64) Option {
	return func(o *options) { o.readLimit = n }
}

// WithPingInterval 是**保留的 API 兼容面**：主动心跳探测已删除。
//
// 这里只记下该值，连接内核不再读它——SDK 不会主动发 WS 层 ping。依据是真实
// codex 客户端：它的 Responses WS pump 只有"收到 Ping 就地回 Pong"，没有 ticker、
// 没有 pong deadline；空闲连接的 EOF/Close 由常驻读者观测，复用前由 releasable
// 复查。公开的 Client.Ping 仍在（调用方可自行决定何时探活）。
//
// 保留而不删的原因：现有调用方（如网关的 WithPingInterval(0)）要照旧编译。
//
// Deprecated: 无效果；传任何值都不再改变行为。
func WithPingInterval(d time.Duration) Option {
	return func(o *options) { o.pingInterval = d }
}

// WithBeta 设置 Responses WS 的 beta 版本（默认 DefaultBetaWS=2026-02-06，
// 现役唯一真实值），注入 "OpenAI-Beta: responses_websockets=<version>"。
// 仅作用于 WS 握手——HTTP 默认不发 OpenAI-Beta，
// 需要时以 WithHeader("OpenAI-Beta", ...) 显式注入。
func WithBeta(version string) Option {
	return func(o *options) { o.beta = version }
}

// WithHeader 注入附加请求头（WS 升级 / HTTP 请求通用），覆盖默认头
// （如 WithHeader("User-Agent", ...) 覆盖默认 codex UA）；同名多次调用为扩展。
// 发送时头名一律小写（User-Agent 除外），覆盖时规范写法与小写写法都删除。
//
// 保留键（身份：session-id / thread-id / x-client-request-id / x-codex-window-id /
// x-codex-installation-id / x-codex-turn-state；形态：version /
// x-codex-routing-hint / x-codex-beta-features / x-codex-turn-metadata /
// x-openai-internal-codex-responses-lite / accept / content-encoding）在构造期
// panic。
func WithHeader(key, value string) Option {
	return func(o *options) {
		if o.headers == nil {
			o.headers = make(http.Header)
		}
		o.headers.Add(key, value)
	}
}

// WithTraceContext 注入外部 trace context（WS 帧 client_metadata 的
// ws_request_header_traceparent / tracestate；优先于自动生成）。仅作用于 WS——
// 官方客户端握手头与 HTTP 请求均不发 trace。
func WithTraceContext(tc TraceContext) Option {
	return func(o *options) { o.trace = &tc }
}

// WithTraceAuto 开关每帧 trace 自动生成（默认关：官方客户端只在进程开启
// 链路追踪时才带 trace 键）。开启后每个 WS 帧生成新的 W3C traceparent。
func WithTraceAuto(enabled bool) Option {
	return func(o *options) { o.traceAuto = enabled }
}

// WithFirstFrame 只作用于 Dial（WS→WS 静态身份）：first 必须与随后第一次 Send
// 的帧逐字节相同。Dial 用它整形一次，握手带 x-codex-routing-hint 与
// x-codex-turn-metadata（first 是 generate:false 时为 prewarm 形态，否则 turn
// 形态）；第一次 Send 复用这次整形（同一个 turn_id）。未设置时握手不带这两个头。
// first 不是合法请求体时 Dial 返回 ErrInvalidRequestBody（不拨号）。
//
// 契约：Dial 不复制 first（整形结果直接引用它），传入的切片在第一次 Send 之前
// 不得被修改（与 io.Writer 等 Go 惯例一致）；第一次 Send 之后 SDK 不再引用它。
func WithFirstFrame(first []byte) Option {
	return func(o *options) { o.firstFrame = first }
}

// WithTimeout 设置 HTTP 客户端总超时（http.Client.Timeout；默认 0 不设限）。
func WithTimeout(d time.Duration) Option {
	return func(o *options) { o.timeout = d }
}

// WithWSIdleTimeout 设置 WS 读空闲上限（默认 300s，对齐真实客户端的
// DEFAULT_STREAM_IDLE_TIMEOUT_MS；d<=0 = 关闭，显式 opt-out）。
//
// 它是"**等一帧**"的预算，不是请求总超时：计时单位是一次消费侧取帧等待
// （pumped：wsExchange.Next 里队列为空、真正阻塞的那段；pull：一次 Client.Recv）。
// 队里已有帧 ⇒ 立即交付、不消耗预算；连接躺在缓存里、或消费者没在等帧时
// 没有计时器在跑。与 WithTimeout 互不影响——后者只作用于 HTTP 客户端的请求，
// 且 >0 会按墙钟砍断长流，不能当 idle 用。
//
// 超时 ⇒ 返回可 `errors.Is(err, ErrWSIdleTimeout)` 的错误，连接经所有权裁决后
// 退役（不可归还、不可复用），**不重试**（不重拨、不重发 create）。
//
// 两条读路径都覆盖：pumped（StreamBody/Responses 的 WS 臂）与 pull
// （Dial → Client.Recv）。**pull 的契约变更**：`Recv` 由"无期限读 socket"变为
// "每次调用有上限"——一次调用内没有完整数据消息就会关闭该连接；网关 relay 的读
// 循环跨轮常驻，因此**轮间静默超过预算会关闭整条 WS 会话**。长驻留场景若要保留
// 旧行为，用本选项显式传 0。
func WithWSIdleTimeout(d time.Duration) Option {
	return func(o *options) { o.wsIdleTimeout = d }
}

// WithMaxLineSize 设置 HTTP SSE 单行上限（默认 16MiB）。
func WithMaxLineSize(n int) Option {
	return func(o *options) { o.maxLineSize = n }
}

// WithTransport 注入 HTTP 传输层（自定义 RoundTripper，如代理）；默认使用
// http.DefaultTransport（连接池复用）。
//
// 官方客户端不发 Accept-Encoding；SDK 自己不设这个头，但 Go 的 *http.Transport
// 在 DisableCompression=false 时会自动加 "Accept-Encoding: gzip"。要与官方形态
// 一致，调用方传入的 Transport 需设置 DisableCompression（SDK 拿到的可能是包装型
// RoundTripper，不能替调用方改）。
func WithTransport(rt http.RoundTripper) Option {
	return func(o *options) { o.transport = rt }
}

// withKeepaliveInterval 设置 GenerateImageStream 等待期间的 keepalive 事件
// 间隔（默认 60s；仅对合成流式生图生效）。包内私有（测试 seam，不作为导出 API）。
func withKeepaliveInterval(d time.Duration) Option {
	return func(o *options) { o.keepaliveInterval = d }
}

// withConnSweepInterval 设置空闲表维护 ticker 的节拍（仅测试 seam，不导出）。
// 它**只**影响 TTL/maxAge 的清扫频率：没有探针、没有 ping。
func withConnSweepInterval(d time.Duration) Option {
	return func(o *options) { o.wsSweepInterval = d }
}

// Client 是 Responses WebSocket 连接（Dial 创建），是连接内核的 pull 模式门面。
//
// 读模型：**纯拉**——Recv 直读 socket（无常驻读协程、无队列）。因此
//
//   - pong 由调用方的常驻 Recv 循环处理（coder/websocket：Ping 必须与 Reader
//     并发，否则等不到 pong）。网关透传编排天然常驻 Recv 循环，满足该前提；
//   - Recv 的 ctx 不是"可取消的一次取帧"：按 coder 语义，取消会**关闭连接**
//     并返回 ctx 错误；
//   - 每次 Recv 有读空闲上限（默认 300s，WithWSIdleTimeout 可配）：一次调用内
//     无完整数据消息 ⇒ ErrWSIdleTimeout 并关闭连接（**契约变更**，见 Recv 注释）。
//
// 心跳：**没有主动探测**（对齐真实 codex 客户端）。连接只在对端发 Ping 时就地
// 回 Pong，这个服务由编码器/解码器（coder）在读循环里完成，不依赖调用方读频率
// 以外的任何定时器。半开连接只能靠读写错误或调用方 ctx 兜底——需要主动活体检测
// 的调用方自己发 `Client.Ping`（沿用调用方 ctx）。
//
// 并发语义：至多一个 goroutine 同时执行 Recv；Send / Ping 由连接内核的写闸
// 串行化（coder/websocket 禁止并发写）。关闭握手时延上界约 10s（coder 写关闭帧
// 5s + 等对端关闭帧 5s），等不起用 CloseNow（立即返回）。
type Client struct {
	cn    *conn
	shape shapeConfig
	id    Identity

	// first 是 WithFirstFrame 的首帧（调用方切片，不复制）与 Dial 时对它的整形。
	// 第一次 Send 用 Swap(nil) 取出：帧与首帧逐字节相同则复用这次整形（同一个
	// turn_id），否则重新整形。取出后不再有任何引用，首帧不随连接常驻。
	first atomic.Pointer[firstFrameState]

	// lastInject 是本连接上次实际插入的指令/工具项的 injectHash（0 = 无）。
	// 带 previous_response_id 的增量帧哈希相同则不再插入（上游已在上下文里），
	// 不同则插入并计数。
	lastInject atomic.Uint64

	// observer 只在 Recv 里使用（至多一个 goroutine 同时 Recv）：把属于本次
	// response 的 response.metadata turn-state 交给 id.Sink。
	observer turnStateObserver

	// conv 是 WithConversation 的会话状态（nil = 静态身份）。
	conv *wsConversation

	// closed 标记本地已发起关闭（Close/CloseNow）。对端关闭由常驻读者观测。
	closed atomic.Bool
}

// firstFrameState 是 WithFirstFrame 的首帧与 Dial 时对它的整形（整形结果的
// 切片指向 frame，二者同寿命）。
type firstFrameState struct {
	frame []byte
	sh    shapedRequest
}

// Closed 报告连接是否已被本地关闭（Close / CloseNow 置位）。
// 对端关闭由常驻读者（Recv）观测，不在此标志内。
func (c *Client) Closed() bool { return c.closed.Load() }

// Dial 建立到内置上游的 Responses WebSocket 连接：URL 由 SDK 固定维护——
// DefaultResponsesWSURL（wss 完整端点直用，不再从 HTTP URL 派生）。
//
// auth 注入升级请求的 Authorization 头（PAT 静态 / OAuth 每次升级取新 token）。
//
// WS 升级 401 自动轮转（OAuthWithRotation 专属）：401（有升级响应）→ SDK 内
// 单飞 refresh → 自动重连一次；仍 401 → 返回 DialError 且 Refreshed=true（网关
// 据此知道已轮转过）。PAT/oauthAuth 不轮转（401 直接返回 DialError）；refresh
// 失败（fatal / RefreshError）经本方法透传，不被 DialError 包层吞掉
// （errors.As 可区分）。无升级响应的失败（连接拒绝 / 拨号中取消）返回原始
// 错误链，**不**构造 DialError——errors.Is(ctx) 保持成立。
//
// ctx 只约束握手：连接寿命与调用方 ctx 解耦（今日语义不变）。
func Dial(ctx context.Context, auth Auth, id Identity, opts ...Option) (*Client, error) {
	if auth == nil {
		return nil, errors.New("codexsdk: auth 不能为 nil（用 PAT 或 OAuth）")
	}
	cfg := resolveOptions(opts)
	c := &Client{shape: shapeConfigOf(&cfg), id: id}
	if co := cfg.conversation; co != nil {
		switch {
		case cfg.firstFrame != nil:
			panic("codexsdk: WithConversation 与 WithFirstFrame 不能同时设置")
		case !isZeroIdentity(id):
			panic("codexsdk: 设置 WithConversation 时 Dial 的 id 必须为零值（身份由会话簿合成）")
		case co.book == nil:
			panic("codexsdk: WithConversation 的 book 不能为 nil")
		}
	}
	spec := dialSpec{
		auth:      auth,
		opts:      cfg,
		readLimit: cfg.readLimit,
		mode:      modePull,
		headers:   cfg.headers,
		identity:  id,
	}
	if cfg.firstFrame != nil {
		// 不复制：整形结果是指向首帧的零拷贝切片，契约见 WithFirstFrame。
		st := &firstFrameState{frame: cfg.firstFrame}
		spec.opts.firstFrame = nil // 首帧只经 c.first 引用，首次 Send 后释放
		var err error
		if st.sh, err = shapeBody(st.frame, surfaceWSNative, id, time.Now(), c.shape); err != nil {
			return nil, err
		}
		spec.shaped = &st.sh
		c.first.Store(st)
	}
	if co := cfg.conversation; co != nil {
		cv, cid, st, err := dialConversation(ctx, co, c.shape)
		if err != nil {
			return nil, err
		}
		spec.opts.conversation = nil // 首帧只经 c.first 引用，首次 Send 后释放
		spec.identity, spec.shaped = cid, &st.sh
		c.conv = cv
		c.first.Store(st)
	}
	cn, _, err := dialConn(ctx, spec)
	if err != nil {
		return nil, err
	}
	c.cn = cn
	c.observer = turnStateObserver{sink: id.Sink}
	return c, nil
}

// Send 发送一帧 response.create（文本帧）。
//
// 每帧按官方形态整形（两段式：白名单、同名键首现、官方键序、client_metadata
// 由 Identity 合成——客户端自带的整个丢弃）；previous_response_id 与 generate
// 原样保留（由上游判断）。generate:false 的帧按预热形态写 client_metadata
// （turn_id 为空串、prewarm 形态 turn-metadata、不带 turn-state）。顶层
// instructions / tools 生成的插入项按连接去重：带 previous_response_id 的增量帧
// 与本连接上次插入的内容相同时不再插入（顶层字段照样删除）。
// 请求体无效返回 ErrInvalidRequestBody（不入网）。WithConversation 连接的身份由
// 会话簿逐帧合成（见 WithConversation）。
// x-codex-ws-stream-request-start-ms 在取得写闸之后写入发送时刻。Write 同步
// 消费——Send 返回前不得复用 frame。
func (c *Client) Send(ctx context.Context, frame []byte) error {
	if c.conv != nil {
		return c.sendConversation(ctx, frame)
	}
	var sh shapedRequest
	if p := c.first.Swap(nil); p != nil && bytes.Equal(p.frame, frame) {
		sh = p.sh
	} else {
		var err error
		if sh, err = shapeBody(frame, surfaceWSNative, c.id, time.Now(), c.shape); err != nil {
			return err
		}
	}
	// 指令/工具插入的连接级去重：全量帧（不带 previous_response_id）照常插入并
	// 记下哈希；增量帧的插入项与本连接上次相同则只删顶层字段、不再插入，不同则
	// 插入；只有本连接此前已记录过插入项（与上次不同）才计入
	// IncrementalReinjects——连接上的第一次插入不算"重新插入"。
	hash, record := sh.injectHash(), true
	if sh.clientPrevID != "" {
		switch last := c.lastInject.Load(); {
		case hash == 0:
			record = false
		case hash == last:
			sh.inputPrefix, record = nil, false
		default:
			if last != 0 && c.shape.stats != nil {
				c.shape.stats.incrementalReinjects.Add(1)
			}
		}
	}
	a := wsAssembly{prevID: sh.clientPrevID, generate: sh.clientGenerate}
	if !sh.nativePrewarm {
		a.turnState = sh.turnState(c.id)
	}
	wire, off := assembleWS(&sh, a)
	if err := c.cn.writeStamped(ctx, wire, off); err != nil {
		return err
	}
	if record {
		c.lastInject.Store(hash)
	}
	return nil
}

// Recv 读取下一帧字节（文本/二进制帧原样透传，不改写）。
//
// turn-state：帧是 response.metadata 且其 response_id 等于最近一次
// response.created 的 id 时，把其中的 x-codex-turn-state 交给 Identity.Sink
// （只读顶层 type，其余帧零解析）。WithConversation 连接按 response.created 的
// 顺序把 ts 归给对应帧的轮次（见 WithConversation）。握手响应头里的 turn-state
// 不取（官方只认事件）。
//
// 直读 socket（pull 模式无 pump、无队列）。data 为 coder/websocket 每次 Read
// 独立分配的缓冲，跨次调用有效。对端关闭/网络断开/超 ReadLimit 时返回错误
// （如 coder/websocket.CloseError、ErrMessageTooBig，网关自行分类处置）。
//
// **读空闲上限（契约变更，默认 300s，WithWSIdleTimeout 可配）**：一次 Recv 调用
// 内没有收到任何完整数据消息（一次 Read 返回）⇒ 返回可
// `errors.Is(err, ErrWSIdleTimeout)` 的错误，并**关闭该连接**（不可复用；后续调用
// 失败）。计时范围就是"本次调用的持续时间"——没有 Recv 在飞时没有任何计时器，
// 所以它不保证诊断半开连接；而跨轮常驻的读循环（如网关 relay）在轮间静默超过
// 预算时会关掉整条 WS 会话。`WithWSIdleTimeout(0)` 恢复"无期限读 socket"。
//
// 父 ctx 的取消/期限优先：那种情况原样返回 ctx 错误，不算 idle 超时。
//
// 注意：ctx 取消按 coder 语义**关闭连接**并返回 ctx 错误——它不是"只影响本次
// 取帧"的开关。
func (c *Client) Recv(ctx context.Context) ([]byte, error) {
	budget := c.cn.cfg.idleTimeout
	if budget <= 0 {
		data, err := c.cn.readFrame(ctx) // 既有行为（显式 opt-out）
		if err == nil {
			c.observe(data)
		}
		return data, err
	}
	rctx, cancel := context.WithDeadline(ctx, time.Now().Add(budget))
	defer cancel()
	data, err := c.cn.readFrame(rctx)
	if err == nil {
		c.observe(data)
		return data, nil
	}
	if ctx.Err() != nil {
		return nil, ctx.Err() // 调用方取消/父 deadline：原样（显式，不靠编译器）
	}
	if errors.Is(rctx.Err(), context.DeadlineExceeded) {
		c.cn.terminate(ErrWSIdleTimeout) // 退役：不可复用；后续调用失败
		return nil, fmt.Errorf("%w（%s 内未收到任何帧）", ErrWSIdleTimeout, budget)
	}
	return nil, err
}

// observe 是 Recv 的 turn-state 观测：会话连接按帧身份归属，静态身份交给
// Identity.Sink。
func (c *Client) observe(data []byte) {
	if c.conv != nil {
		c.conv.observe(data)
		return
	}
	c.observer.observe(data)
}

// Ping 发送 WS 层 ping 并等待 pong（对端库自动回 pong；ctx 控制等待上限）。
// 注意：pong 由常驻 Recv 循环处理——Ping 必须在调用方同时执行 Recv 时使用。
// ctx 完全沿用调用方（不加内部封顶）。
func (c *Client) Ping(ctx context.Context) error {
	return c.cn.probe(ctx)
}

// Close 发起关闭握手（关闭码透传，如 StatusNormalClosure；reason ≤125 字节）。
// 注意：关闭握手可能阻塞最多 ~10s（coder 写关闭帧 5s + 等对端关闭帧 5s），
// 等不起时用 CloseNow。重复/并发调用返回首次结果（幂等；对端已断开时返回 nil）。
func (c *Client) Close(status StatusCode, reason string) error {
	c.closed.Store(true)
	return c.cn.closeGracefully(status, reason)
}

// CloseNow 立即终止连接（跳过关闭握手，不等待对端），用于等不起 Close 阻塞的
// 场景——如网关快速 failover、调用方自判死后的兜底强断。幂等；与 Close 互斥，
// 先调用者生效。
// 若 Close 的关闭握手已在飞，本调用只登记首因并立即返回，socket 由握手收尾
// （时延上界同 Close，~10s；与基线行为等价）。
// 之后 Close 返回既有结果（通常 nil，与今天一致）。
func (c *Client) CloseNow() error {
	c.closed.Store(true)
	c.cn.terminate(errConnClosing)
	return nil
}
