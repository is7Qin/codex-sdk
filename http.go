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
	"time"

	"github.com/tidwall/gjson"
)

// defaultMaxLineSize HTTP SSE 单行上限（与 WS ReadLimit 同量级）。
const defaultMaxLineSize = 16 * 1024 * 1024

// DefaultResponsesURL 是内置官方上游 responses HTTP 完整端点（固定）。
const DefaultResponsesURL = "https://chatgpt.com/backend-api/codex/responses"

// DefaultResponsesWSURL 是内置官方上游 responses WebSocket 完整端点（固定）。
const DefaultResponsesWSURL = "wss://chatgpt.com/backend-api/codex/responses"

// HTTPClient 是统一端点方法 HTTP 客户端（懒构建：NewHTTPClient 不创建
// http.Client——只做选项快照——首次 Do/Stream 才创建，连接池复用）。端点方法族：
// Do/Stream（responses 固定端点）、GenerateImage / Search / GetUsage（各自固定官方端点）、Responses（合成非流式）。
//
// 身份（session/thread/window/turn/turn-state）是**请求数据**：Stream /
// StreamBody / Responses 逐次接收 Identity；捕获到上游签发的 ts 时只回调
// Identity.Sink，SDK 不保留任何身份状态。
type HTTPClient struct {
	auth Auth
	opts options

	// codec 是帧编码器（无可变状态，可并发使用）。
	codec *frameCodec

	mu               sync.Mutex // 保护 client 懒构建与 CloseIdleConnections 并发访问
	client           *http.Client
	noRedirectClient *http.Client // 仅 StreamBody：禁跟随重定向（3xx 原样返回）

	// turnStateDropped 是事件级 ts 捕获里"归属判定丢弃"的计数（S3 观测面）：
	// 只统计被丢掉的 ts 候选，不是帧计数。跨 goroutine 读，必须原子。
	turnStateDropped atomic.Int64

	// wsCache 是 WS 臂的空闲连接表：构造后不可变（nil = 复用关闭），
	// 所以并发读它不需要加锁。
	wsCache *connCache
}

// NewHTTPClient 创建 Responses HTTP 客户端（端点方法族客户端，所有数据面 URL 由 SDK 固定维护，不可覆盖）。
func NewHTTPClient(auth Auth, opts ...Option) *HTTPClient {
	cfg := resolveOptions(opts)
	return &HTTPClient{
		auth:    auth,
		opts:    cfg,
		codec:   newFrameCodec(&cfg),
		wsCache: newConnCacheIf(cfg.wsConnCache, cfg.wsSweepInterval),
	}
}

// CloseWSIdleConnections **关闭** WS 臂的连接缓存（含它自己的维护 ticker），
// **不碰**底层 http.Transport。
//
// 语义是"这个 client 要被丢弃了"：清空并终止现有 idle，此后归还一律拒收（由
// Release 终止），**不可逆**——没有重新打开的入口，要复用只能新建 HTTPClient。
// 与 CloseIdleConnections 的区别：后者只清空 idle（缓存仍可用，之后新建的连接
// 照常复用），且会对 `*http.Transport` 断言并清空其空闲池；网关出口通常是包装型
// transport（如分片 h2 池），断言失败即静默 no-op，而一旦哪天是裸
// `*http.Transport` 又会打掉全站 h2 连接。丢弃一个 HTTPClient 时只需要关掉它
// 自己的 WS 连接。
func (c *HTTPClient) CloseWSIdleConnections() {
	if c.wsCache != nil {
		c.wsCache.Close()
	}
}

// RetireThread 退役一个线程的空闲 WS 连接：缓存锁内摘出该 threadKey 的
// idle，锁外终止。前置条件 = 该 key 无在飞 exchange（会话独占租借保证）；
// 不设 retired 集合/墓碑。Close 之后为 no-op。
func (c *HTTPClient) RetireThread(threadKey string) {
	if c.wsCache != nil {
		c.wsCache.RetireThread(threadKey)
	}
}

// CloseIdleConnections 关闭底层空闲连接（懒构建未使用时为零开销调用；
// 与首个请求并发调用安全——内部加锁）。同时清空 WS 臂的空闲连接缓存并终止其中
// 连接，不中断在飞请求；缓存本身不关闭，后续新建连接仍可复用。
func (c *HTTPClient) CloseIdleConnections() {
	if c.wsCache != nil {
		c.wsCache.clear()
	}
	c.mu.Lock()
	clients := [...]*http.Client{c.client, c.noRedirectClient}
	c.mu.Unlock()
	// 两个 client 共用同一 Transport（或均为 nil→DefaultTransport）：逐个清理，
	// 对共享 Transport 的重复清理幂等无害；仅初始化了 noRedirectClient（只调
	// StreamBody）时也不遗漏。
	for _, client := range clients {
		if client != nil && client.Transport != nil {
			if tc, ok := client.Transport.(*http.Transport); ok {
				tc.CloseIdleConnections()
			}
		}
	}
}

// HTTPResponse 是 HTTP 非流式响应（原样交付，不做字段解析——字段由网关
// 在完整字节上自行解析；Do / Search 方法共用）。
type HTTPResponse struct {
	StatusCode int
	Raw        []byte // 完整响应体
}

// HTTPError 是上游非 2xx 响应（状态码 + 错误体原样交付，error 信封由网关解析）。
//
// WS 臂的升级失败也归为同一形状（StatusCode + ≤1KiB 的截断 body），底层原因
// 挂在未导出的 cause 上：errors.As 可回溯到 *DialError，errors.Is 可回溯到
// context.Canceled 等（网关的判死/重试分类不必区分两臂）。
type HTTPError struct {
	StatusCode int
	Raw        []byte

	cause error // 底层原因（可空）；仅供 errors.Is/As 回溯，不参与文案
}

func (e *HTTPError) Error() string {
	return fmt.Sprintf("codexsdk: upstream HTTP %d", e.StatusCode)
}

// Unwrap 暴露底层原因（有则回溯，无则 nil）。
func (e *HTTPError) Unwrap() error { return e.cause }

// Do 发送 POST <responses 端点> 请求（端点方法族成员——固定官方端点
// DefaultResponsesURL；payload 为完整 JSON 请求体，调用方决定 stream 等字段），
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
	resp, err := c.do(ctx, payload, Identity{})
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := readResponseBody(resp)
	if err != nil {
		return nil, err
	}
	return &HTTPResponse{StatusCode: resp.StatusCode, Raw: body}, nil
}

// Stream 发送 POST 请求并提取 SSE 事件帧：逐 data: 行交付原始字节（零拷贝
// 行切片——回调内的字节引用 scanner 复用缓冲，仅在回调执行期间有效；
// 跨回调保留需自行拷贝），[DONE] 标记流正常终止。身份与 ts 都来自 id；响应头
// 签发的 ts 只回调 id.Sink。
//
// 发送前无条件归一（HTTP 白名单过滤 FilterCodexHTTPPayload + 强制 store:false），
// 再统一注入 client_metadata（对齐真实 codex client_metadata()——
// responses_metadata.rs:255-288；见 injectResponsesClientMetadata）。
// Responses 内部调 Stream 走同一归一+注入点，避免两路径重复；Do 是通用非流式
// POST 不归一也不注入（GenerateImage/Search 各自端点不受影响）。
//
// fn 返回错误立即终止读取并透传该错误。非 2xx 返回 *HTTPError；
// 401 自动轮转语义同 Do。
func (c *HTTPClient) Stream(ctx context.Context, payload []byte, id Identity, fn func(raw []byte) error) error {
	return c.streamIssued(ctx, payload, id, fn)
}

// streamIssued 是 Stream 的实现体：响应头签发的 ts 只回调 id.Sink。
func (c *HTTPClient) streamIssued(ctx context.Context, payload []byte, id Identity, fn func(raw []byte) error) error {
	payload, err := normalizeCodexHTTPPayload(payload)
	if err != nil {
		return err
	}
	payload = injectResponsesClientMetadata(payload, id)
	resp, err := c.do(ctx, payload, id)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode >= 400 {
		body, _ := io.ReadAll(resp.Body)
		return &HTTPError{StatusCode: resp.StatusCode, Raw: body}
	}
	id.deliverTurnState(resp.Header.Get(HeaderTurnState))

	done, err := scanSSE(resp.Body, c.opts.maxLineSize, fn)
	if err != nil {
		return err
	}
	if done {
		// [DONE]：流正常终止。排空残余 body——http.Transport 对未读完的
		// body 连接不回池（残余未读使连接不可复用），读尽后连接回池，
		// 下一轮请求复用同连接（消除重拨风暴）。排空失败（网络中断）
		// 不影响语义：连接本就不复用，行为与现状等价（defer Close 兜底）。
		// 病态上游边界：[DONE] 后不关闭 body 的病态上游——本读阻塞至
		// ctx 取消兜底（连接随 ctx 取消释放，与现状等价）。
		_, _ = io.Copy(io.Discard, resp.Body)
	}
	return nil // EOF 结束（是否截断由网关按终态事件判定）
}

// scanSSE 逐 `data:` 行扫描 SSE 字节流并交付事件帧（原 Stream 内联循环的抽出）。
//
// 只有 HTTP 臂用它：WS 臂的 `Responses` 直接取帧喂聚合器（`responsesStreamWS`），
// 不再把帧渲染成 SSE 字节再扫回来；WS 臂 `StreamBody` 产出的 SSE 字节是给调用方
// 的，SDK 内部不扫描。
//
// 返回 done=true 表示遇到 [DONE] 终止标记（[DONE] 不进 feed）；done=false 且
// err=nil 表示读到 EOF——是否截断由调用方按终态事件判定（本函数不解释事件）。
// feed 返回错误立即终止扫描并原样透传。
//
// ctx 不入本函数：取消语义由 body 承担——HTTP body 绑定请求 ctx。此处再插一个
// 取消检查点会改变"ctx 已取消但 body 仍有缓冲数据"时的错误链与文案（现状是
// scanner 读到传输层错误、包成"读取响应流失败"），违反"HTTP 路径零变化"。
func scanSSE(body io.Reader, maxLine int, feed func(raw []byte) error) (bool, error) {
	if maxLine <= 0 {
		maxLine = defaultMaxLineSize
	}
	scanner := bufio.NewScanner(body)
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
			return true, nil
		}
		if err := feed(trimmed); err != nil {
			return false, err
		}
	}
	if err := scanner.Err(); err != nil {
		return false, fmt.Errorf("codexsdk: 读取响应流失败: %w", err)
	}
	return false, nil
}

// StreamBody 发送 POST <responses 端点> 请求并返回**未读**的原始流式响应体，
// 与 Stream 同源：发送前无条件归一（normalizeCodexHTTPPayload）+ client_metadata
// 注入（injectResponsesClientMetadata），再走 do（含 401 判死/单飞 refresh/自动
// 重试一次）。
//
// 接受状态 = **仅 200**（比 Stream 的 `>= 400` 判据更严，此为有意声明的行为）：
//   - 200 → id.deliverTurnState（turn-state 取自**本次响应头**，无响应体变换）
//     后返回**未关闭**的 *http.Response，body 归调用方、须**恰好关闭一次**；
//   - 非 200（含 201/204/3xx/4xx/5xx）→ 在 SDK 内读尽并 Close body 后返回
//     *HTTPError{StatusCode, Raw}（不泄漏未关闭的连接）。
//
// 读取沿用调用方 ctx（http 传输层）：调用方取消能终止后续对返回 body 的读取。
//
// TransportWS 时改走上游 Responses WS（见 streamBodyWS）：对外形状一致
// （StatusCode=200 / Body=SSE 字节流），ts 不进合成响应头——只走 Identity.Sink。
// WithWSConnCache(n) 可开启默认关闭的连接复用：连接身份 = (threadKey, readLimit)，
// 同一 thread 的连续请求——含跨轮跨窗口——都可命中；匿名不复用；读到终态后必须
// 关闭 Body 才能归还。与 Responses 的聚合路径不共享连接，收益边界见 TRANSPORT.md。
func (c *HTTPClient) StreamBody(ctx context.Context, payload []byte, id Identity) (*http.Response, error) {
	if c.opts.responsesTransport == TransportWS {
		return c.streamBodyWS(ctx, payload, id, wsStreamReadLimit)
	}
	wire, err := normalizeCodexHTTPPayload(payload)
	if err != nil {
		return nil, err
	}
	wire = injectResponsesClientMetadata(wire, id)
	// 仅本方法禁跟随重定向：3xx 原样返回 → 归为非 200 → *HTTPError（避免
	// 自动跟随到 Location 目标后按 200 当成功）。
	resp, err := c.doURL(ctx, c.clientNoRedirect(), DefaultResponsesURL, http.MethodPost, wire, id)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		return nil, &HTTPError{StatusCode: resp.StatusCode, Raw: body}
	}
	id.deliverTurnState(resp.Header.Get(HeaderTurnState))
	return resp, nil
}

// normalizeCodexHTTPPayload 对 HTTP /responses 请求体做 codex 形状归一：
// HTTP 白名单过滤（FilterCodexHTTPPayload）后强制 store:false。空/非法 JSON
// 原样返回；过滤后无任何白名单字段 → ErrEmptyFrame（空结果体不入网）。
func normalizeCodexHTTPPayload(raw []byte) ([]byte, error) {
	filtered, err := FilterCodexHTTPPayload(raw)
	if err != nil {
		return nil, err
	}
	return forceCodexStoreFalse(filtered), nil
}

// injectResponsesClientMetadata 在发送前注入 HTTP /responses 面的
// client_metadata（键集严格对齐真实 codex client_metadata()——
// responses_metadata.rs:255-288；生产模板 turn_metadata.rs:260-278
// turn_id 恒带）：
//   - 恒 4 key：x-codex-installation-id / session_id / thread_id /
//     x-codex-window-id（Identity.Meta 与 Identity.Session 同 key 时 Meta 优先
//     ——对齐 WS 组装优先级（frameCodec.metaEntries；空值跳过）；
//   - 恒带 turn_id：CodexMeta.TurnID 静态值优先；否则每请求自动 UUIDv7
//     （网关每请求即一轮，对齐真实"每轮新 sub_id"语义）；客户端自带 turn_id
//     不参与透传；
//   - 条件键（仅配置了才带）：x-openai-subagent / x-codex-parent-thread-id /
//     parent_turn_id / x-codex-turn-metadata；
//   - 不注入：traceparent/tracestate（HTTP 体面真实不带——trace 仅 WS
//     帧面）、turn-state（体键不存在，其请求头属 HTTP 头面不在本方法）；
//   - 不做 turn_metadata 回调（WS 帧面扩展——真实 HTTP client_metadata() 是
//     静态键集）。
//
// 整体替换：payload 顶层 client_metadata 由网关身份**整体重建**（单次
// sjson.SetRawBytes）——codex 面客户端自带的 client_metadata **永不透传**，
// 连同非网关键一并丢弃（网关身份恒为准）。实测整体替换优于逐键覆盖：逐键
// N 次重序列化整份 body，整体替换 1 次（256KB 体 ~0.73ms/25 allocs →
// ~0.29ms/11 allocs，内存 ~1.4MB → ~0.29MB）。非法 JSON payload 放弃注入
// 保持原样（对齐 responses.go Responses() 的 gjson.ValidBytes 前置校验先例——
// 实测 sjson.SetRawBytes 对非法 JSON 静默产出损坏字节（err=nil），故须前置
// gjson.ValidBytes）。
func injectResponsesClientMetadata(payload []byte, id Identity) []byte {
	payload = RewriteEnvironmentContextTime(payload, time.Now())
	// codex 面客户端自带的 client_metadata **永不透传**：不做对象级短路、也不做
	// 键级透传——下方 entries 由网关伪装身份构造，注入时覆盖请求体内已有同 key
	//（injectClientMetadataKeys 覆盖语义）。非法 JSON 放弃注入、保持原样由上游
	// 400 透传。
	if !gjson.ValidBytes(payload) {
		return payload
	}
	// 惰性组装 entries（与 WS 帧编码器共用 metaEntries 累积器与键序，
	// 面特有键各自补充）；先在栈上预留 8 项，超出才转堆；零配置时仍带
	// turn_id（真实恒发）。
	var entryBuf [8]metadataEntry
	entries := metaEntries(entryBuf[:0])
	entries = entries.withCodexMetaIdentity(id.Meta)
	entries = entries.add(codexMetaParentThreadKey, id.Meta.ParentThreadID)
	entries = entries.add(codexMetaParentTurnKey, id.Meta.ParentTurnID)
	entries = entries.add(codexMetaTurnMetadataKey, id.Meta.TurnMetadata)
	// 注意：HTTP 体面不含 trace 键（真实 client_metadata() 无
	// ws_request_header_trace* 注入——trace 仅 WS 帧面扩展）。
	entries = entries.withSessionIdentity(id.Session)
	// turn_id 恒带且覆盖：无静态值 → 每请求自动 UUIDv7（客户端自带 turn_id
	// 不参与透传——网关值恒为准，对齐 WS 组装条件）。
	if id.Meta.TurnID == "" {
		entries = entries.add(codexMetaTurnKey, NewUUIDv7())
	}
	return injectClientMetadataKeys(payload, entries)
}

func (c *HTTPClient) do(ctx context.Context, payload []byte, id Identity) (*http.Response, error) {
	if c.auth == nil {
		return nil, errors.New("codexsdk: auth 不能为 nil（用 PAT 或 OAuth）")
	}
	return c.doURL(ctx, c.httpClient(), DefaultResponsesURL, http.MethodPost, payload, id)
}

// doURL 发送指定 method 请求到指定完整端点并应用 401 自动轮转（do 的 URL
// 已构建形态；GenerateImage / Search / GetUsage 等非 responses 端点复用
// 同一传输层：判死分类 + 单飞 refresh + 自动重试一次，语义与 do 完全一致）。
// hc 为本次使用的 http.Client——默认 httpClient()；StreamBody 传 no-redirect
// client（仅影响重定向跟随，401 轮转语义不变）。
func (c *HTTPClient) doURL(ctx context.Context, hc *http.Client, targetURL string, method string, payload []byte, id Identity) (*http.Response, error) {
	if c.auth == nil {
		return nil, errors.New("codexsdk: auth 不能为 nil（用 PAT 或 OAuth）")
	}
	resp, err := c.sendRequest(ctx, hc, targetURL, method, payload, id)
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
		notifyAuthFatal(c.auth, fatal)
		return nil, fatal
	}
	rotated, rerr := rotateOn401(ctx, c.auth)
	if rerr != nil {
		return nil, rerr // refresh 失败（fatal / RefreshError）透传
	}
	if !rotated {
		return respWithBody(resp, http.StatusUnauthorized, body), nil
	}
	// 自动重试一次（新 at）。
	resp2, err := c.sendRequest(ctx, hc, targetURL, method, payload, id)
	if err != nil {
		return nil, err
	}
	if resp2.StatusCode == http.StatusUnauthorized {
		body2, _ := io.ReadAll(resp2.Body)
		_ = resp2.Body.Close()
		// 二次 401：同样过判死分类（并发吊销不错过判死），非判死则原样返回
		// （防重试风暴）。
		if fatal := classifyAT401(body2); fatal != nil {
			notifyAuthFatal(c.auth, fatal)
			return nil, fatal
		}
		return respWithBody(resp2, http.StatusUnauthorized, body2), nil
	}
	return resp2, nil
}

// sendRequest 构造指定 method 请求并执行（伪装层默认头 + 调用方 WithHeader 覆盖）。
// hc 由调用方选定（默认/禁跟随重定向）。
func (c *HTTPClient) sendRequest(ctx context.Context, hc *http.Client, targetURL string, method string, payload []byte, id Identity) (*http.Response, error) {
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
	applyDefaultHeaders(req.Header)
	// 账号标识头（ChatGPT 系 auth 恒发；空 = 不发，向后兼容）。
	applyAccountID(req.Header, c.auth)
	// 调用方 WithHeader：逐请求应用（先删后加，同名多次调用为扩展）。
	applyHeaderOverrides(req.Header, c.opts.headers)
	// 逐请求 turn-state 只来自 Identity（保留键已禁止经 WithHeader 注入）。
	if id.TurnState != "" {
		req.Header.Set(HeaderTurnState, id.TurnState)
	}
	resp, err := hc.Do(req)
	if err != nil {
		return nil, fmt.Errorf("codexsdk: 上游请求失败: %w", err)
	}
	return resp, nil
}

// readResponseBody 读取响应体（不关闭，调用方负责 defer Close），统一非流式
// 响应信封：读取失败 → 包装错误（"读取响应失败"）；状态码 >= 400 →
// *HTTPError{StatusCode, Raw}。Do / Search / GetUsage / GenerateImage 共用
// （Stream 走流式路径，不适用）。
func readResponseBody(resp *http.Response) ([]byte, error) {
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("codexsdk: 读取响应失败: %w", err)
	}
	if resp.StatusCode >= 400 {
		return nil, &HTTPError{StatusCode: resp.StatusCode, Raw: body}
	}
	return body, nil
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

// httpClient 懒构建：首次请求才创建（连接池复用；timeout/transport 可配）。
// 加锁保证与 CloseIdleConnections 并发安全。
func (c *HTTPClient) httpClient() *http.Client {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.client == nil {
		c.client = c.newClient(nil)
	}
	return c.client
}

// clientNoRedirect 懒构建**禁跟随重定向**的 client（仅 StreamBody 使用）：与
// httpClient 同 timeout/transport，额外 CheckRedirect 返回 http.ErrUseLastResponse
// ——3xx 原样作为最终响应返回（不访问 Location 目标），由 StreamBody 归为非 200 →
// *HTTPError。Do/Stream/其它方法仍用 httpClient（既有跟随行为不变）。
func (c *HTTPClient) clientNoRedirect() *http.Client {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.noRedirectClient == nil {
		c.noRedirectClient = c.newClient(func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		})
	}
	return c.noRedirectClient
}

// newClient 构造 http.Client（同 timeout/transport；checkRedirect 可选）。须持 mu。
func (c *HTTPClient) newClient(checkRedirect func(*http.Request, []*http.Request) error) *http.Client {
	hc := &http.Client{CheckRedirect: checkRedirect}
	if c.opts.timeout > 0 {
		hc.Timeout = c.opts.timeout
	}
	if c.opts.transport != nil {
		hc.Transport = c.opts.transport
	}
	return hc
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
