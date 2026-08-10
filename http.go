package codexsdk

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
)

// defaultMaxLineSize HTTP SSE 单行上限（与 WS ReadLimit 同量级）。
const defaultMaxLineSize = 16 * 1024 * 1024

// HTTPClient 是 Responses HTTP 客户端（懒构建：NewHTTPClient 零开销，
// 首次 Do/Stream 才创建 http.Client，连接池复用）。
//
// 会话级状态：x-codex-turn-state 由响应头自动捕获并缓存，下一个请求自动
// 以请求头回传（同轮回传）；轮结束时网关 SetTurnState("") 清除——跨轮不得
// 回传。同会话请求串行使用（并发请求共享状态，最后写入者生效）。
type HTTPClient struct {
	baseURL string // 上游 base URL
	auth    Auth
	opts    options

	mu        sync.Mutex // 保护 client 懒构建与 CloseIdleConnections 并发访问
	client    *http.Client
	turnState atomic.Value // string；x-codex-turn-state 回传值
}

// NewHTTPClient 创建 Responses HTTP 客户端。baseURL 为上游 base URL
// （如 https://api.openai.com/v1），不含 /responses 后缀时自动拼接，已含则
// 原样使用。上游版本参数可由 WithBeta 注入或拼入 baseURL。
func NewHTTPClient(baseURL string, auth Auth, opts ...Option) *HTTPClient {
	cfg := defaultOptions()
	for _, o := range opts {
		o(&cfg)
	}
	return &HTTPClient{baseURL: baseURL, auth: auth, opts: cfg}
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

// HTTPResponse 是 Responses HTTP 非流式响应（原样交付，不做字段解析——
// id/type/usage 由网关在完整字节上自行解析）。
type HTTPResponse struct {
	StatusCode int
	Raw        []byte // 完整响应体
	TurnState  string // 响应头 x-codex-turn-state（服务端签发，同轮回传）
}

// HTTPError 是上游非 2xx 响应（状态码 + 错误体原样交付，error 信封由网关解析）。
type HTTPError struct {
	StatusCode int
	Raw        []byte
}

func (e *HTTPError) Error() string {
	return fmt.Sprintf("codexsdk: upstream HTTP %d", e.StatusCode)
}

// Do 发送 POST <base>/responses 请求（payload 为完整 JSON 请求体，调用方
// 决定 stream 等字段），原样返回非流式响应。非 2xx 返回 *HTTPError。
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
// fn 返回错误立即终止读取并透传该错误。非 2xx 返回 *HTTPError。
func (c *HTTPClient) Stream(ctx context.Context, payload []byte, fn func(raw []byte) error) error {
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
			return nil // [DONE]：流正常终止
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

func (c *HTTPClient) do(ctx context.Context, payload []byte) (*http.Response, error) {
	if c.auth == nil {
		return nil, errors.New("codexsdk: auth 不能为 nil（用 PAT 或 OAuth）")
	}
	targetURL := strings.TrimRight(c.baseURL, "/")
	if !strings.HasSuffix(targetURL, "/responses") {
		targetURL += "/responses"
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, targetURL, bytes.NewReader(payload))
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
	// x-codex-turn-state 同轮回传（响应头签发 → 缓存 → 下一请求回传）。
	if ts := c.TurnState(); ts != "" {
		req.Header.Set(HeaderTurnState, ts)
	}
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

// SetTurnState 设置 x-codex-turn-state 回传值（响应头自动捕获后也可显式
// 覆盖）。轮结束时传空串清除——跨轮不得回传。
func (c *HTTPClient) SetTurnState(state string) {
	c.turnState.Store(state)
}

// TurnState 返回当前缓存的 x-codex-turn-state（由最近一次响应头自动捕获）。
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
