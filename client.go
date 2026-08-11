package codexsdk

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"sync"
	"sync/atomic"
	"time"

	coderws "github.com/coder/websocket"
)

// 传输常量（对齐参考实现 sub2api openai_ws_*）。
const (
	// defaultReadLimit 单帧读取上限。coder/websocket 默认 32KB 过小——
	// Codex 事件（rate_limits / 大 delta）可超阈值，对齐 sub2api 的 16MiB。
	defaultReadLimit = 16 * 1024 * 1024
	// defaultPingInterval 心跳间隔。Responses WS 空闲连接会被上游断链，
	// 需要 keepalive：上游客户端同时发送 type:"ping" 应用事件，本库选择
	// WS 层 ping 以不进入业务事件流。
	defaultPingInterval = 30 * time.Second
	// defaultPingTimeout 心跳 ping 超时（对齐 sub2api 连接健康检查超时）。
	defaultPingTimeout = 2 * time.Second
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

// options 是 Dial / NewHTTPClient 的共享配置（无关字段按形态忽略）。
type options struct {
	baseURL string     // 上游 responses 端点（默认 DefaultResponsesURL；WithBaseURL 覆盖）
	query   url.Values // WithQuery 注入的 URL query 参数（HTTP/WS 双形态）

	compression  CompressionMode
	readLimit    int64
	pingInterval time.Duration
	beta         string // Responses WS beta 版本（默认 DefaultBetaWS=2026-02-06，现役唯一真实值）
	headers      http.Header

	// 伪装层（Send 帧 / 升级与请求头）。
	filtering       bool // Send 白名单过滤（默认开）
	meta            *CodexMeta
	metaPassthrough []metadataEntry // WithClientMetadata 透传项（WS client_metadata 任意键，只透传不解析）
	session         *Session        // 会话标识（握手头 + 帧内 metadata）
	trace           *TraceContext   // 外部注入（WS 帧 metadata，优先于自动生成）
	traceAuto       bool            // 每帧自动生成 trace（默认开，WS 专用）
	turnAuto        bool            // 每帧自动生成 turn_id（默认开）
	turnProvider    func(turn uint64) string

	// HTTP 专用。
	timeout     time.Duration
	maxLineSize int
	transport   http.RoundTripper
}

func defaultOptions() options {
	return options{
		baseURL:      DefaultResponsesURL,
		compression:  CompressionDisabled,
		readLimit:    defaultReadLimit,
		pingInterval: defaultPingInterval,
		beta:         DefaultBetaWS,
		filtering:    true,
		traceAuto:    true,
		turnAuto:     true,
		maxLineSize:  defaultMaxLineSize,
	}
}

// Option 配置 Dial / NewHTTPClient。
type Option func(*options)

// WithBaseURL 覆盖内置上游 URL（默认 DefaultResponsesURL）——覆盖值按完整
// responses 端点语义直用：SDK 不再自动拼接 /responses（与旧版 baseURL 参数
// 语义不同，显式行为变更——自建上游传 https://selfhost/v1 将打 /v1 而非
// /v1/responses，避免静默 404 请传完整端点）。HTTP 与 WS（Dial 由该值派生
// scheme）双形态生效。
func WithBaseURL(u string) Option {
	return func(o *options) { o.baseURL = u }
}

// WithQuery 注入 URL query 参数（HTTP 请求与 WS 升级 URL 双形态生效；
// 与 WithBaseURL 同用时拼接到覆盖 URL；base URL 自带 query 时追加而非覆盖）。
// 保留面非对齐面——真实客户端 Responses 本无 query 版本参数
// （IMPERSONATION.md:130），本选项为既有调用方注入能力（beta 等）的延续。
func WithQuery(key, value string) Option {
	return func(o *options) {
		if o.query == nil {
			o.query = make(url.Values)
		}
		o.query.Add(key, value)
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

// WithPingInterval 设置 WS 心跳间隔（默认 30s；0 或负值禁用心跳）。
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
func WithHeader(key, value string) Option {
	return func(o *options) {
		if o.headers == nil {
			o.headers = make(http.Header)
		}
		o.headers.Add(key, value)
	}
}

// WithPayloadFiltering 开关 Send 帧的顶层 key 白名单过滤（默认开；
// 关闭后帧原样直写，零分配快速路径）。
func WithPayloadFiltering(enabled bool) Option {
	return func(o *options) { o.filtering = enabled }
}

// WithCodexMeta 设置 client_metadata 静态载体（值由调用方提供，
// SDK 只组装不生成；帧内已存在的 key 不覆盖）。
func WithCodexMeta(meta CodexMeta) Option {
	return func(o *options) { o.meta = &meta }
}

// WithClientMetadata 注入 client_metadata 任意键值（透传面：SDK 只透传不解析，
// 键名由调用方自定或引用 Meta* 常量，如 MetaResponsesLiteKey="true"）。
// 与其余注入同优先级——帧内已存在的 key 不覆盖；多次调用为多键注入。
// 仅作用于 WS——HTTP 无 client_metadata，对应请求头透传用 WithHeader。
func WithClientMetadata(key, value string) Option {
	return func(o *options) {
		o.metaPassthrough = append(o.metaPassthrough, metadataEntry{key, value})
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

// WithSession 设置会话级标识（真实客户端 WS 握手恒带
// x-client-request-id / session-id / thread-id / x-codex-window-id）：
// 注入握手头，并补齐帧内 client_metadata 的 session_id / thread_id /
// x-codex-window-id（CodexMeta 中同 key 优先）。会话内稳定。
func WithSession(s Session) Option {
	return func(o *options) { o.session = &s }
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

// Client 是 Responses WebSocket 连接（Dial 创建）。
//
// 并发语义：至多一个 goroutine 同时执行 Recv；Send / Ping 内部串行化
// （coder/websocket 禁止并发写）。连接建立后的帧收发路径零额外分配。
//
// 常驻读循环是硬性要求：Ping 与心跳依赖 Recv 处理 pong 控制帧
// （coder/websocket：Ping 必须与 Reader 并发，否则等不到 pong）——
// 网关透传编排天然常驻 Recv 循环，满足该前提。
type Client struct {
	conn    *coderws.Conn
	writeMu sync.Mutex // 串行化写路径（Send / Ping / 心跳共用）

	pingInterval time.Duration

	// 伪装层（Send 帧组装）。
	filtering       bool
	meta            *CodexMeta
	metaPassthrough []metadataEntry // WithClientMetadata 透传项（只透传不解析）
	session         *Session        // 会话标识（握手头 + 帧内 metadata）
	trace           *TraceContext   // 外部注入（每帧静态，优先于自动生成）
	traceAuto       bool            // 每帧自动生成 trace
	turnAuto        bool            // 每帧自动生成 turn_id
	turnProvider    func(turn uint64) string
	turn            atomic.Uint64
	turnState       atomic.Value // string；x-codex-turn-state 回传值

	ctx       context.Context
	cancel    context.CancelFunc
	closeOnce sync.Once
	closeErr  error
}

// Dial 建立到内置上游的 Responses WebSocket 连接：URL 由 SDK 维护——
// 默认 DefaultResponsesURL 派生（http→ws / https→wss 换 scheme，path/query
// 保留，对齐真实客户端 provider.rs:92-103），WithBaseURL / WithQuery 可覆盖
// 与追加（query 与 WithBaseURL 同用时拼接到覆盖 URL）。
//
// auth 注入升级请求的 Authorization 头（PAT 静态 / OAuth 每次升级取新 token）。
//
// WS 升级 401 自动轮转（OAuthWithRotation 专属）：DialError{401}（升级无
// 响应体不可判死）→ SDK 内单飞 refresh → 自动重连一次；仍 401 → 返回
// DialError 且 Refreshed=true（网关据此知道已轮转过）。PAT/oauthAuth 不轮转
// （401 直接返回 DialError，现状保持）；refresh 失败（fatal / RefreshError）
// 经本方法透传，不被 DialError 包层吞掉（errors.As 可区分）。
//
// 连接建立后启动心跳：每 pingInterval 发送 WS 层 ping（默认 30s）。心跳依赖
// 调用方的常驻 Recv 循环处理 pong（见 Client 并发语义）。心跳失败视为连接
// 已死：立即 CloseNow 解除阻塞中的 Recv（返回网络错误），网关据此触发重连。
func Dial(ctx context.Context, auth Auth, opts ...Option) (*Client, error) {
	if auth == nil {
		return nil, errors.New("codexsdk: auth 不能为 nil（用 PAT 或 OAuth）")
	}
	cfg := defaultOptions()
	for _, o := range opts {
		o(&cfg)
	}
	wsURL, err := wsURLOf(cfg.baseURL, cfg.query)
	if err != nil {
		return nil, err
	}

	// 升级头组装（重连时以新 at 重取）。
	buildHeaders := func(ctx context.Context) (http.Header, error) {
		hdr := make(http.Header, 10)
		token, err := auth.Authorization(ctx)
		if err != nil {
			return nil, fmt.Errorf("codexsdk: 获取鉴权信息失败: %w", err)
		}
		hdr.Set("Authorization", token)
		// 伪装层默认头（WithHeader 可覆盖）。
		hdr.Set("User-Agent", DefaultCodexUserAgent)
		hdr.Set("Originator", DefaultOriginator)
		hdr.Set("OpenAI-Beta", "responses_websockets="+cfg.beta)
		// 会话标识握手头（真实客户端恒带；trace 不进握手头，只进帧内 metadata）。
		if s := cfg.session; s != nil {
			if s.SessionID != "" {
				hdr.Set(HeaderSessionID, s.SessionID)
			}
			if s.ThreadID != "" {
				hdr.Set(HeaderThreadID, s.ThreadID)
			}
			clientRequestID := s.ClientRequestID
			if clientRequestID == "" {
				clientRequestID = s.ThreadID
			}
			if clientRequestID != "" {
				hdr.Set(HeaderClientRequestID, clientRequestID)
			}
			if s.WindowID != "" {
				hdr.Set(HeaderWindowID, s.WindowID)
			}
		}
		// 调用方 WithHeader：覆盖默认头（先删后加），同名多次调用为扩展。
		for k, vals := range cfg.headers {
			hdr.Del(k)
			for _, v := range vals {
				hdr.Add(k, v)
			}
		}
		return hdr, nil
	}

	dialWS := func(ctx context.Context) (*coderws.Conn, *http.Response, error) {
		hdr, err := buildHeaders(ctx)
		if err != nil {
			return nil, nil, err
		}
		dopts := &coderws.DialOptions{HTTPHeader: hdr}
		if cfg.transport != nil {
			dopts.HTTPClient = &http.Client{Transport: cfg.transport}
		}
		if cfg.compression != CompressionDisabled {
			dopts.CompressionMode = cfg.compression.coder()
			dopts.CompressionThreshold = defaultCompressionThreshold
		}
		return coderws.Dial(ctx, wsURL, dopts)
	}

	conn, resp, err := dialWS(ctx)
	if err != nil {
		status := dialStatus(resp)
		if status == http.StatusUnauthorized {
			// WS 升级 401（无响应体不可判死）→ 可轮转：单飞 refresh → 自动重连一次。
			if tr, ok := auth.(refreshTrigger); ok {
				if rerr := tr.refresh(ctx); rerr != nil {
					return nil, rerr // refresh 失败（fatal / RefreshError）透传
				}
				conn, resp, err = dialWS(ctx)
				if err != nil {
					return nil, &DialError{StatusCode: dialStatus(resp), Err: err, Refreshed: true}
				}
			} else {
				return nil, &DialError{StatusCode: status, Err: err} // PAT/oauthAuth：原样返回
			}
		} else {
			return nil, &DialError{StatusCode: status, Err: err}
		}
	}
	conn.SetReadLimit(cfg.readLimit)

	cctx, cancel := context.WithCancel(context.Background())
	c := &Client{
		conn:            conn,
		pingInterval:    cfg.pingInterval,
		filtering:       cfg.filtering,
		meta:            cfg.meta,
		metaPassthrough: cfg.metaPassthrough,
		session:         cfg.session,
		trace:           cfg.trace,
		traceAuto:       cfg.traceAuto,
		turnAuto:        cfg.turnAuto,
		turnProvider:    cfg.turnProvider,
		ctx:             cctx,
		cancel:          cancel,
	}
	// x-codex-turn-state 回传：上游在握手响应头签发 → SDK 缓存 →
	// 后续 Send 帧 client_metadata 自动回传（跨轮不得回传，网关在轮结束时
	// SetTurnState("") 清除）。
	if resp != nil {
		if v := resp.Header.Get(HeaderTurnState); v != "" {
			c.SetTurnState(v)
		}
	}
	if cfg.pingInterval > 0 {
		go c.heartbeat(cctx)
	}
	return c, nil
}

// buildURL 解析 base + 追加 WithQuery 参数（base URL 自带 query 时追加而非覆盖）。
func buildURL(base string, query url.Values) (string, error) {
	u, err := url.Parse(base)
	if err != nil {
		return "", fmt.Errorf("codexsdk: 解析上游 URL %q 失败: %w", base, err)
	}
	appendQuery(u, query)
	return u.String(), nil
}

// wsURLOf 由 responses HTTP 端点派生 WS 端点：scheme 替换（http→ws /
// https→wss；ws/wss 原样保留），path/query 保留 + 追加 WithQuery 参数
// （对齐真实客户端 provider.rs:92-103 url_for_path 的 scheme 替换与
// query 双形态拼接）。
func wsURLOf(base string, query url.Values) (string, error) {
	u, err := url.Parse(base)
	if err != nil {
		return "", fmt.Errorf("codexsdk: 解析上游 URL %q 失败: %w", base, err)
	}
	switch u.Scheme {
	case "http":
		u.Scheme = "ws"
	case "https":
		u.Scheme = "wss"
	case "ws", "wss":
		// 原样保留
	default:
		return "", fmt.Errorf("codexsdk: 不支持的上游 scheme %q（期望 http/https）", u.Scheme)
	}
	appendQuery(u, query)
	return u.String(), nil
}

// appendQuery 把 WithQuery 参数追加到 URL（已存在同 key 时为多值追加）。
func appendQuery(u *url.URL, query url.Values) {
	if len(query) == 0 {
		return
	}
	q := u.Query()
	for k, vs := range query {
		for _, v := range vs {
			q.Add(k, v)
		}
	}
	u.RawQuery = q.Encode()
}

// dialStatus 提取升级失败响应状态码（coderws 非 101 时返回响应）。
func dialStatus(resp *http.Response) int {
	if resp != nil {
		return resp.StatusCode
	}
	return 0
}

// Send 发送一帧字节（文本帧）。
//
// 伪装层默认生效（Options 可关）：白名单过滤（FilterCodexPayload，过滤后为空
// 返回 ErrEmptyFrame 不入网）+ client_metadata 组装（CodexMeta 静态值 /
// trace / turn_metadata；浅合并，帧内已存在的 key 不覆盖）。
// 关闭过滤且无任何注入时为零拷贝零分配快速路径；Write 同步消费——
// Send 返回前不得复用 frame。
func (c *Client) Send(ctx context.Context, frame []byte) error {
	frame, err := c.prepareFrame(frame)
	if err != nil {
		return err
	}
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	return c.conn.Write(ctx, coderws.MessageText, frame)
}

// prepareFrame 应用伪装层：白名单过滤 + client_metadata 组装。
// 优先级：帧内已存在 > CodexMeta 静态值 > WithSession > turn-state 回传 >
// WithClientMetadata 透传 > 自动机制（turn_id / trace）> turn_metadata 回调。
// 全部禁用时零拷贝零分配原样返回。
func (c *Client) prepareFrame(frame []byte) ([]byte, error) {
	if c.filtering {
		var err error
		frame, err = FilterCodexPayload(frame)
		if err != nil {
			return nil, err
		}
	}
	// 惰性组装 entries：无任何注入时零分配。
	var entries []metadataEntry
	appendEntry := func(key, value string) {
		if value == "" {
			return
		}
		if entries == nil {
			entries = make([]metadataEntry, 0, 8)
		}
		entries = append(entries, metadataEntry{key, value})
	}
	if m := c.meta; m != nil {
		appendEntry(codexMetaInstallationKey, m.InstallationID)
		appendEntry(codexMetaSessionKey, m.SessionID)
		appendEntry(codexMetaThreadKey, m.ThreadID)
		appendEntry(codexMetaTurnKey, m.TurnID)
		appendEntry(codexMetaWindowKey, m.WindowID)
		appendEntry(codexMetaSubagentKey, m.Subagent)
		appendEntry(codexMetaTurnMetadataKey, m.TurnMetadata)
		appendEntry(codexMetaTraceparentKey, m.Traceparent)
		appendEntry(codexMetaTracestateKey, m.Tracestate)
	}
	if s := c.session; s != nil {
		appendEntry(codexMetaSessionKey, s.SessionID)
		appendEntry(codexMetaThreadKey, s.ThreadID)
		appendEntry(codexMetaWindowKey, s.WindowID)
	}
	if ts := c.TurnState(); ts != "" {
		appendEntry(codexMetaTurnStateKey, ts)
	}
	// 调用方透传键（WithClientMetadata）：只透传不解析，帧内已有同 key 不覆盖
	// （注入顺序在自动机制之前，同 key 不会覆盖静态/会话值）。
	for _, e := range c.metaPassthrough {
		appendEntry(e.key, e.value)
	}
	// turn_id 透传优先：帧内已有 client_metadata.turn_id 时原值透传（零生成——
	// turn_id 有身份语义，透传保持 turn 链一致；常态路径省掉每帧 UUIDv7 生成）；
	// 无则自动生成 UUIDv7 兜底（CodexMeta.TurnID 静态值优先）。
	// 与 turn 计数联动不变：计数仍每帧自增，仅 id 值优先透传。
	if c.turnAuto && (c.meta == nil || c.meta.TurnID == "") && !hasClientMetadataKey(frame, codexMetaTurnKey) {
		appendEntry(codexMetaTurnKey, NewUUIDv7())
	}
	// 每帧 trace：外部 WithTraceContext 静态值优先，否则每帧自动生成
	// （真实客户端每请求新 span，同轮多请求 traceparent 不同；
	// 帧内已有 traceparent 时同样透传零生成）。
	if c.trace != nil {
		appendEntry(codexMetaTraceparentKey, c.trace.Traceparent)
		appendEntry(codexMetaTracestateKey, c.trace.Tracestate)
	} else if c.traceAuto && !hasClientMetadataKey(frame, codexMetaTraceparentKey) {
		tc := NewTraceContext()
		appendEntry(codexMetaTraceparentKey, tc.Traceparent)
		appendEntry(codexMetaTracestateKey, tc.Tracestate)
	}
	if c.turnProvider != nil {
		turn := c.turn.Add(1)
		appendEntry(codexMetaTurnMetadataKey, c.turnProvider(turn))
	}
	return injectClientMetadataKeys(frame, entries), nil
}

// SetTurnState 设置 x-codex-turn-state 回传值（服务端签发 → 后续 Send 帧
// client_metadata 自动回传）。轮结束时传空串清除——跨轮不得回传。
// 网关也可在别处观测到该值时显式设置。
func (c *Client) SetTurnState(state string) {
	c.turnState.Store(state)
}

// TurnState 返回当前缓存的 x-codex-turn-state（Dial 时自动捕获握手响应头）。
func (c *Client) TurnState() string {
	v, _ := c.turnState.Load().(string)
	return v
}

// Recv 读取下一帧字节（文本/二进制帧原样透传，不区分帧类型不做解析）。
//
// data 为 coder/websocket 每次 Read 独立分配的缓冲，跨次调用有效。
// 对端关闭/网络断开/超 ReadLimit 时返回错误（如 coder/websocket.CloseError、
// ErrMessageTooBig，网关自行分类处置）。
func (c *Client) Recv(ctx context.Context) ([]byte, error) {
	_, data, err := c.conn.Read(ctx)
	return data, err
}

// Ping 发送 WS 层 ping 并等待 pong（对端库自动回 pong；ctx 控制等待上限）。
// 注意：pong 由常驻 Recv 循环处理——Ping 必须在调用方同时执行 Recv 时使用。
func (c *Client) Ping(ctx context.Context) error {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	return c.conn.Ping(ctx)
}

// Close 发起关闭握手（关闭码透传，如 StatusNormalClosure；reason ≤125 字节）。
// 注意：关闭握手可能阻塞最多 ~10s（coder 写关闭帧 5s + 等对端关闭帧 5s），
// 等不起时用 CloseNow。停用心跳。重复调用返回首次结果（幂等；
// 对端已断开时返回 nil）。
func (c *Client) Close(status StatusCode, reason string) error {
	c.closeOnce.Do(func() {
		c.cancel()
		c.closeErr = c.conn.Close(status, reason)
	})
	return c.closeErr
}

// CloseNow 立即断开连接（跳过关闭握手，不等待对端），用于等不起 Close 阻塞的
// 场景——如网关快速 failover、心跳死亡兜底。幂等；与 Close 互斥，先调用者生效。
func (c *Client) CloseNow() {
	c.closeOnce.Do(func() {
		c.cancel()
		c.closeErr = c.conn.CloseNow()
	})
}

// heartbeat 周期性发送 WS 层 ping。Ping 失败（对端无 pong/连接已死）时
// CloseNow 解除阻塞中的 Recv 并退出。
func (c *Client) heartbeat(ctx context.Context) {
	t := time.NewTicker(c.pingInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			pingCtx, cancel := context.WithTimeout(ctx, defaultPingTimeout)
			err := c.Ping(pingCtx)
			cancel()
			if err != nil {
				if ctx.Err() != nil {
					return // 本地关闭竞态：不再处理
				}
				c.CloseNow()
				return
			}
		}
	}
}
