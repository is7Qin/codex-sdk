package codexsdk

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sync"
	"sync/atomic"

	"github.com/tidwall/gjson"
)

// defaultMaxLineSize HTTP SSE 单行上限（与 WS ReadLimit 同量级）。
const defaultMaxLineSize = 16 * 1024 * 1024

// DefaultResponsesURL 是内置官方上游 responses HTTP 完整端点（固定）。
const DefaultResponsesURL = "https://chatgpt.com/backend-api/codex/responses"

// DefaultResponsesWSURL 是内置官方上游 responses WebSocket 完整端点（固定）。
const DefaultResponsesWSURL = "wss://chatgpt.com/backend-api/codex/responses"

// HTTPClient 是统一端点方法 HTTP 客户端（懒构建：NewHTTPClient 零开销，
// 首次 Do/Stream 才创建 http.Client，连接池复用）。端点方法族：
// Do/Stream（responses 固定端点）、GenerateImage / Search / GetUsage（各自固定官方端点）、Responses（合成非流式）。
//
// x-codex-turn-state 仅响应侧（真实 codex 客户端行为）：响应头/SSE 中的
// turn-state 自动捕获，经 TurnState() / HTTPResponse.TurnState 暴露给网关；
// HTTP 请求不携带 x-codex-turn-state 头（头回传仅 WS 路径，见 Client）。
type HTTPClient struct {
	auth Auth
	opts options

	mu        sync.Mutex // 保护 client 懒构建与 CloseIdleConnections 并发访问
	client    *http.Client
	turnState atomic.Value // string；x-codex-turn-state 响应侧捕获值
}

// NewHTTPClient 创建 Responses HTTP 客户端（端点方法族客户端，所有数据面 URL 由 SDK 固定维护，不可覆盖）。
func NewHTTPClient(auth Auth, opts ...Option) *HTTPClient {
	cfg := defaultOptions()
	for _, o := range opts {
		o(&cfg)
	}
	return &HTTPClient{auth: auth, opts: cfg}
}

// CloseIdleConnections 关闭底层空闲连接（懒构建未使用时为零开销调用；
// 与首个请求并发调用安全——内部加锁）。
func (c *HTTPClient) CloseIdleConnections() {
	c.mu.Lock()
	client := c.client
	c.mu.Unlock()
	if client != nil && client.Transport != nil {
		if tc, ok := client.Transport.(*http.Transport); ok {
			tc.CloseIdleConnections()
		}
	}
}

// HTTPResponse 是 HTTP 非流式响应（原样交付，不做字段解析——字段由网关
// 在完整字节上自行解析；Do / Search 方法共用）。
type HTTPResponse struct {
	StatusCode int
	Raw        []byte // 完整响应体
	TurnState  string // 响应头 x-codex-turn-state（服务端签发，仅响应侧暴露）
}

// HTTPError 是上游非 2xx 响应（状态码 + 错误体原样交付，error 信封由网关解析）。
type HTTPError struct {
	StatusCode int
	Raw        []byte
}

func (e *HTTPError) Error() string {
	return fmt.Sprintf("codexsdk: upstream HTTP %d", e.StatusCode)
}

// Do 发送 POST <responses 端点> 请求（端点方法族成员——search 端点经
// Search 方法派生；payload 为完整 JSON 请求体，调用方决定 stream 等字段），
// 原样返回非流式响应。非 2xx 返回 *HTTPError。
//
// 401 自动轮转（OAuthWithRotation 专属）：每次 401 先做判死分类（响应体
// error_code/error_type/detail，大小写不敏感）——判死码 → Fatal 态 +
// 返回 *AuthPermanentlyRevokedError（不重试）；非判死 → 单飞 refresh →
// 自动重试一次；二次 401 同样过判死分类后原样返回。PAT/oauthAuth 不轮转
// （401 原样返回 HTTPError，现状保持）。fatal 类错误（refresh 路径
// RefreshOAuthError / AccountDisabledError 等）经本方法透传，不被 HTTPError
// 包层吞掉（errors.As 可区分）。
func (c *HTTPClient) Do(ctx context.Context, payload []byte) (*HTTPResponse, error) {
	resp, err := c.do(ctx, payload)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("codexsdk: 读取响应失败: %w", err)
	}
	if resp.StatusCode >= 400 {
		return nil, &HTTPError{StatusCode: resp.StatusCode, Raw: body}
	}
	c.captureTurnState(resp)
	return &HTTPResponse{
		StatusCode: resp.StatusCode,
		Raw:        body,
		TurnState:  resp.Header.Get(HeaderTurnState),
	}, nil
}

// Stream 发送 POST 请求并提取 SSE 事件帧：逐 data: 行交付原始字节（零拷贝
// 行切片——回调内的字节引用 scanner 复用缓冲，仅在回调执行期间有效；
// 跨回调保留需自行拷贝），[DONE] 标记流正常终止。
//
// 发送前统一注入 client_metadata（对齐真实 codex client_metadata()——
// responses_metadata.rs:255-288；见 injectResponsesClientMetadata）。
// Responses 内部调 Stream 走同一注入点，避免两路径重复；Do 是通用非流式
// POST 不注入（GenerateImage/Search 各自端点不受影响）。
//
// fn 返回错误立即终止读取并透传该错误。非 2xx 返回 *HTTPError；
// 401 自动轮转语义同 Do。
func (c *HTTPClient) Stream(ctx context.Context, payload []byte, fn func(raw []byte) error) error {
	payload = c.injectResponsesClientMetadata(payload)
	resp, err := c.do(ctx, payload)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode >= 400 {
		body, _ := io.ReadAll(resp.Body)
		return &HTTPError{StatusCode: resp.StatusCode, Raw: body}
	}
	c.captureTurnState(resp)

	maxLine := c.opts.maxLineSize
	if maxLine <= 0 {
		maxLine = defaultMaxLineSize
	}
	scanner := bufio.NewScanner(resp.Body)
	initSize := 64 * 1024
	if maxLine < initSize {
		initSize = maxLine
	}
	scanner.Buffer(make([]byte, initSize), maxLine)
	for scanner.Scan() {
		data, ok := extractSSEDataLine(scanner.Bytes())
		if !ok {
			continue
		}
		trimmed := bytes.TrimSpace(data)
		if len(trimmed) == 0 {
			continue
		}
		if bytes.Equal(trimmed, sseDone) {
			// [DONE]：流正常终止。排空残余 body——http.Transport 对未读完的
			// body 连接不回池（残余未读使连接不可复用），读尽后连接回池，
			// 下一轮请求复用同连接（消除重拨风暴）。排空失败（网络中断）
			// 不影响语义：连接本就不复用，行为与现状等价（defer Close 兜底）。
			// 病态上游边界：[DONE] 后不关闭 body 的病态上游——本读阻塞至
			// ctx 取消兜底（连接随 ctx 取消释放，与现状等价）。
			_, _ = io.Copy(io.Discard, resp.Body)
			return nil
		}
		if err := fn(trimmed); err != nil {
			return err
		}
	}
	if err := scanner.Err(); err != nil {
		return fmt.Errorf("codexsdk: 读取 SSE 流失败: %w", err)
	}
	return nil // EOF 结束（是否截断由网关按终态事件判定）
}

// injectResponsesClientMetadata 在发送前注入 HTTP /responses 面的
// client_metadata（键集严格对齐真实 codex client_metadata()——
// responses_metadata.rs:255-288；生产模板 turn_metadata.rs:260-278
// turn_id 恒带）：
//   - 恒 4 key：x-codex-installation-id / session_id / thread_id /
//     x-codex-window-id（CodexMeta 与 WithSession 同 key 时 CodexMeta 优先
//     ——对齐 WS 组装优先级 client.go:522-578；空值跳过）；
//   - 恒带 turn_id：payload 已含 → 原值透传（不覆盖）；CodexMeta.TurnID
//     静态值其次；否则每请求自动 UUIDv7（网关每请求即一轮，对齐真实
//     "每轮新 sub_id"语义）；
//   - 条件键（仅配置了才带）：x-openai-subagent / x-codex-parent-thread-id /
//     parent_turn_id / x-codex-turn-metadata；
//   - 不注入：traceparent/tracestate（HTTP 体面真实不带——trace 仅 WS
//     帧面）、turn-state（体键不存在，其请求头属 HTTP 头面不在本方法）；
//   - 不做 metaPassthrough / turn_metadata 回调（WS 帧面扩展——真实 HTTP
//     client_metadata() 是静态键集）。
// 预筛短路：payload 已含 client_metadata 键 → 零注入原样返回（真实客户端
// 自带完整 metadata 才自己组装——透传优先语义，缺键不补齐；bytes.Contains
// memchr 级，免 ValidBytes 全量 JSON 扫描，常见场景注入成本归零；判据为
// 带引号 token "client_metadata"——键名恒带引号，字符串值里的裸词不误判）。
// 非法 JSON payload 放弃注入保持原样（对齐 responses.go:37 先例——sjson
// SetBytes 对非法 JSON 静默产出损坏字节，须前置 gjson.ValidBytes）。
func (c *HTTPClient) injectResponsesClientMetadata(payload []byte) []byte {
	// 预筛短路：payload 已含 JSON 键 "client_metadata"（真实 codex 客户端
	// 自带完整 metadata 的请求体）→ 透传优先语义，零注入直接返回（省掉
	// ValidBytes 全量 JSON 扫描——常见场景注入成本归零）。判据为带引号
	// token（JSON 键名恒带引号）：字符串值里的裸词不误判（评审 P2-2）。
	// 判据局限（评审 P3-B'）：值恰为 "client_metadata" 的字符串值（如
	// prompt 值）仍命中短路——概率低且无害：仅跳过注入、请求合法，
	// 退化为缺 metadata 而非错误；假阴性（非标准写法）无害——走注入链
	// 补全更完整。
	if bytes.Contains(payload, []byte(`"client_metadata"`)) {
		return payload
	}
	if !gjson.ValidBytes(payload) {
		return payload
	}
	// 惰性组装 entries（同 WS prepareFrame 键序与优先级）；零配置时仍带
	// turn_id（真实恒发）。
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
	if m := c.opts.meta; m != nil {
		appendEntry(codexMetaInstallationKey, m.InstallationID)
		appendEntry(codexMetaSessionKey, m.SessionID)
		appendEntry(codexMetaThreadKey, m.ThreadID)
		appendEntry(codexMetaTurnKey, m.TurnID)
		appendEntry(codexMetaWindowKey, m.WindowID)
		appendEntry(codexMetaSubagentKey, m.Subagent)
		appendEntry(codexMetaParentThreadKey, m.ParentThreadID)
		appendEntry(codexMetaParentTurnKey, m.ParentTurnID)
		appendEntry(codexMetaTurnMetadataKey, m.TurnMetadata)
		// 注意：HTTP 体面不含 trace 键（真实 client_metadata() 无
		// ws_request_header_trace* 注入——trace 仅 WS 帧面扩展）。
	}
	if s := c.opts.session; s != nil {
		appendEntry(codexMetaSessionKey, s.SessionID)
		appendEntry(codexMetaThreadKey, s.ThreadID)
		appendEntry(codexMetaWindowKey, s.WindowID)
	}
	// turn_id 恒带：payload 已含 → injectClientMetadataKeys 不覆盖（透传）；
	// 无静态值才自动 UUIDv7（对齐 WS 组装条件 client.go:560）。
	if (c.opts.meta == nil || c.opts.meta.TurnID == "") && !hasClientMetadataKey(payload, codexMetaTurnKey) {
		appendEntry(codexMetaTurnKey, NewUUIDv7())
	}
	return injectClientMetadataKeys(payload, entries)
}

func (c *HTTPClient) do(ctx context.Context, payload []byte) (*http.Response, error) {
	if c.auth == nil {
		return nil, errors.New("codexsdk: auth 不能为 nil（用 PAT 或 OAuth）")
	}
	return c.doURL(ctx, DefaultResponsesURL, http.MethodPost, payload)
}

// doURL 发送指定 method 请求到指定完整端点并应用 401 自动轮转（do 的 URL
// 已构建形态；GenerateImage / Search / GetUsage 等非 responses 端点复用
// 同一传输层：判死分类 + 单飞 refresh + 自动重试一次，语义与 do 完全一致）。
func (c *HTTPClient) doURL(ctx context.Context, targetURL string, method string, payload []byte) (*http.Response, error) {
	if c.auth == nil {
		return nil, errors.New("codexsdk: auth 不能为 nil（用 PAT 或 OAuth）")
	}
	resp, err := c.sendRequest(ctx, targetURL, method, payload)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusUnauthorized {
		return resp, nil
	}
	// 401：判死分类 + 自动轮转（鉴权面，SDK 边界内——判死分类与轮转解耦，
	// 每次 401 均先判死分类，全部 Auth 类型生效）。判死码命中 → Fatal 态 +
	// OnAuthFatal 至多一次（私有接口 authFatalTrigger）+ 透传，不重试；
	// 仅非判死才进入轮转分支（refreshTrigger 实现者）。
	body, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if fatal := classifyAT401(body); fatal != nil {
		if f, ok := c.auth.(authFatalTrigger); ok {
			f.authFatal(fatal)
		}
		return nil, fatal
	}
	trigger, ok := c.auth.(refreshTrigger)
	if !ok {
		return respWithBody(resp, http.StatusUnauthorized, body), nil
	}
	if err := trigger.refresh(ctx); err != nil {
		return nil, err // refresh 失败（fatal / RefreshError）透传
	}
	// 自动重试一次（新 at）。
	resp2, err := c.sendRequest(ctx, targetURL, method, payload)
	if err != nil {
		return nil, err
	}
	if resp2.StatusCode == http.StatusUnauthorized {
		body2, _ := io.ReadAll(resp2.Body)
		_ = resp2.Body.Close()
		// 二次 401：同样过判死分类（并发吊销不错过判死），非判死则原样返回
		// （防重试风暴）。
		if fatal := classifyAT401(body2); fatal != nil {
			if f, ok := c.auth.(authFatalTrigger); ok {
				f.authFatal(fatal)
			}
			return nil, fatal
		}
		return respWithBody(resp2, http.StatusUnauthorized, body2), nil
	}
	return resp2, nil
}

// sendRequest 构造指定 method 请求并执行（伪装层默认头 + 调用方 WithHeader 覆盖）。
func (c *HTTPClient) sendRequest(ctx context.Context, targetURL string, method string, payload []byte) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, method, targetURL, bytes.NewReader(payload))
	if err != nil {
		return nil, fmt.Errorf("codexsdk: 构造请求失败: %w", err)
	}
	token, err := c.auth.Authorization(ctx)
	if err != nil {
		return nil, fmt.Errorf("codexsdk: 获取鉴权信息失败: %w", err)
	}
	req.Header.Set("Authorization", token)
	req.Header.Set("Content-Type", "application/json")
	// 伪装层默认头（WithHeader 可覆盖）。HTTP 不发 OpenAI-Beta 与 trace 头
	// （真实客户端行为）；需要 beta 时调用方以 WithHeader 显式注入。
	req.Header.Set("User-Agent", DefaultCodexUserAgent)
	req.Header.Set("Originator", DefaultOriginator)
	// 注意：HTTP 请求不携带 x-codex-turn-state 头（真实客户端行为——
	// turn-state 仅响应侧，WS 路径才回传，见 Client）。
	// 调用方 WithHeader：覆盖默认头（先删后加），同名多次调用为扩展。
	for k, vals := range c.opts.headers {
		req.Header.Del(k)
		for _, v := range vals {
			req.Header.Add(k, v)
		}
	}
	resp, err := c.httpClient().Do(req)
	if err != nil {
		return nil, fmt.Errorf("codexsdk: 上游请求失败: %w", err)
	}
	return resp, nil
}

// respWithBody 用已读出的 body 构造可重读响应（401 分类后 body 已消费，
// Do/Stream 按原逻辑重读并组装 HTTPError）。
func respWithBody(orig *http.Response, status int, body []byte) *http.Response {
	return &http.Response{
		StatusCode: status,
		Header:     orig.Header,
		Body:       io.NopCloser(bytes.NewReader(body)),
		Request:    orig.Request,
	}
}

// TurnState 返回最近一次响应捕获的 x-codex-turn-state（仅响应侧暴露——
// HTTP 请求不携带该头，头回传仅 WS 路径）。
func (c *HTTPClient) TurnState() string {
	v, _ := c.turnState.Load().(string)
	return v
}

// captureTurnState 从响应头捕获 x-codex-turn-state（非空才覆盖）。
func (c *HTTPClient) captureTurnState(resp *http.Response) {
	if resp != nil {
		if v := resp.Header.Get(HeaderTurnState); v != "" {
			c.turnState.Store(v)
		}
	}
}

// httpClient 懒构建：首次请求才创建（连接池复用；timeout/transport 可配）。
// 加锁保证与 CloseIdleConnections 并发安全。
func (c *HTTPClient) httpClient() *http.Client {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.client == nil {
		c.client = &http.Client{}
		if c.opts.timeout > 0 {
			c.client.Timeout = c.opts.timeout
		}
		if c.opts.transport != nil {
			c.client.Transport = c.opts.transport
		}
	}
	return c.client
}

// SSE 帧提取（传输层 framing，非协议解析）。
var (
	sseDataPrefix = []byte("data:")
	sseDone       = []byte("[DONE]")
)

// extractSSEDataLine 提取 SSE data: 行内容（兼容 "data: xxx" 与 "data:xxx"），
// 零拷贝行切片。
func extractSSEDataLine(line []byte) ([]byte, bool) {
	if !bytes.HasPrefix(line, sseDataPrefix) {
		return nil, false
	}
	return bytes.TrimLeft(line[len(sseDataPrefix):], " \t"), true
}
