package codexsdk

import (
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
	// defaultCompressionThreshold 压缩开启时仅压缩超过该大小的消息。
	defaultCompressionThreshold = 512
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

// CompressionMode 是消息压缩模式（默认不压缩：性能优先，LLM 流式小消息
// 压缩收益低、耗 CPU）。
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

	// 伪装层（Send 帧 / 升级与请求头）。身份（Session/Meta/turn-state）是请求
	// 数据，经 Identity 逐次传入，不落在 options 上。
	trace        *TraceContext // 外部注入（WS 帧 metadata，优先于自动生成）
	traceAuto    bool          // 每帧自动生成 trace（默认开，WS 专用）
	turnAuto     bool          // 每帧自动生成 turn_id（默认开）
	turnProvider func(turn uint64) string

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
}

func defaultOptions() options {
	return options{
		compression:  CompressionDisabled,
		readLimit:    defaultReadLimit,
		pingInterval: defaultPingInterval,
		beta:         DefaultBetaWS,
		traceAuto:    true,
		turnAuto:     true,
		maxLineSize:  defaultMaxLineSize,

		keepaliveInterval: defaultKeepaliveInterval,
	}
}

// Option 配置 Dial / NewHTTPClient。
type Option func(*options)

// reservedIdentityHeaders 是身份/turn-state 保留头：身份只能经 Identity 传入，
// WithHeader（含默认头）遇到它们一律构造期 panic——否则删掉身份 Option 之后
// 仍可从默认头注入身份，边界就白划了。匹配大小写不敏感。
var reservedIdentityHeaders = [...]string{
	HeaderSessionID,
	HeaderThreadID,
	HeaderClientRequestID,
	HeaderWindowID,
	HeaderInstallationID,
	HeaderTurnState,
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
				panic("codexsdk: WithHeader 不得设置身份保留头 " + reserved + "（身份只能经 Identity 传入）")
			}
		}
	}
}

// WithCompression 设置 WS 压缩模式（默认 CompressionDisabled）。
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
//
// 身份/turn-state 保留键（session-id / thread-id / x-client-request-id /
// x-codex-window-id / x-codex-installation-id / x-codex-turn-state）在构造期
// panic：身份只能经 Identity 传入，默认头不得成为第二条注入通道。
func WithHeader(key, value string) Option {
	return func(o *options) {
		if o.headers == nil {
			o.headers = make(http.Header)
		}
		o.headers.Add(key, value)
	}
}

// WithTraceContext 注入外部 trace context（每帧注入 Send 帧
// client_metadata 的 trace key；禁用自动生成）。仅作用于 WS——
// 真实客户端握手头与 HTTP 请求均不发 trace。
func WithTraceContext(tc TraceContext) Option {
	return func(o *options) { o.trace = &tc }
}

// WithTraceAuto 开关每帧 trace 自动生成（默认开：每个 Send 帧生成新的
// W3C traceparent 并注入 client_metadata，与真实客户端每请求新 span 一致）。
// 仅作用于 WS。
func WithTraceAuto(enabled bool) Option {
	return func(o *options) { o.traceAuto = enabled }
}

// WithTurnAuto 开关 turn_id 自动生成（默认开：每个 Send 帧生成新的 UUIDv7
// 注入 client_metadata.turn_id；CodexMeta.TurnID 静态值优先）。
// 关闭后（且无静态值）帧内不带 turn_id。
func WithTurnAuto(enabled bool) Option {
	return func(o *options) { o.turnAuto = enabled }
}

// WithTurnMetadata 设置 turn_metadata 内容提供回调：每次 Send 计一轮 turn
// （从 1 起自增），以当前 turn 序号调用回调，返回值（协议约定格式的字符串，
// 可为空=不注入）组装进 client_metadata."x-codex-turn-metadata"。
// 优先级：帧内已存在 > CodexMeta.TurnMetadata > 本回调。
func WithTurnMetadata(provider func(turn uint64) string) Option {
	return func(o *options) { o.turnProvider = provider }
}

// WithTimeout 设置 HTTP 客户端总超时（http.Client.Timeout；默认 0 不设限）。
func WithTimeout(d time.Duration) Option {
	return func(o *options) { o.timeout = d }
}

// WithMaxLineSize 设置 HTTP SSE 单行上限（默认 16MiB）。
func WithMaxLineSize(n int) Option {
	return func(o *options) { o.maxLineSize = n }
}

// WithTransport 注入 HTTP 传输层（自定义 RoundTripper，如代理）；默认使用
// http.DefaultTransport（连接池复用）。
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
//     并返回 ctx 错误。
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
	codec *frameCodec
	id    Identity

	turn atomic.Uint64 // turn_metadata 计数（仅 WithTurnMetadata 时自增）

	// closed 标记本地已发起关闭（Close/CloseNow）。对端关闭由常驻读者观测。
	closed atomic.Bool
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
	cn, hs, err := dialConn(ctx, dialSpec{
		auth:      auth,
		opts:      cfg,
		readLimit: cfg.readLimit,
		mode:      modePull,
		headers:   cfg.headers,
		identity:  id,
	})
	if err != nil {
		return nil, err
	}
	// 握手响应头签发的 ts 只走 Sink（消息身份边界：SDK 不保留状态）。
	id.deliverTurnState(hs.turnState)
	return &Client{cn: cn, codec: newFrameCodec(&cfg), id: id}, nil
}

// Send 发送一帧字节（文本帧）。
//
// WS 归一无条件生效：顶层白名单过滤（FilterCodexWsPayload，过滤后为空返回
// ErrEmptyFrame 不入网）+ 强制 store:false；其后 client_metadata 组装照旧
// （CodexMeta 静态值 / trace / turn_metadata；整体替换帧内 client_metadata——
// 客户端自带的永不透传）。Write 同步消费——Send 返回前不得复用 frame。
func (c *Client) Send(ctx context.Context, frame []byte) error {
	wire, err := c.encodeFrame(frame)
	if err != nil {
		return err
	}
	return c.cn.write(ctx, wire)
}

// encodeFrame 把原生帧编码成 wire 字节（请求级快照：Identity + turn 计数）。
func (c *Client) encodeFrame(frame []byte) ([]byte, error) {
	turn := uint64(0)
	if c.codec.turnProvider != nil {
		turn = c.turn.Add(1)
	}
	return c.codec.encode(frame, encodeNativeWS, frameInput{
		Identity: c.id,
		Turn:     turn,
		Now:      time.Now(),
	})
}

// Recv 读取下一帧字节（文本/二进制帧原样透传，不区分帧类型不做解析）。
//
// 直读 socket（pull 模式无 pump、无队列）。data 为 coder/websocket 每次 Read
// 独立分配的缓冲，跨次调用有效。对端关闭/网络断开/超 ReadLimit 时返回错误
// （如 coder/websocket.CloseError、ErrMessageTooBig，网关自行分类处置）。
//
// 注意：ctx 取消按 coder 语义**关闭连接**并返回 ctx 错误——它不是"只影响本次
// 取帧"的开关。
func (c *Client) Recv(ctx context.Context) ([]byte, error) {
	return c.cn.readFrame(ctx)
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
