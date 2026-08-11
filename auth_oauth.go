package codexsdk

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/tidwall/gjson"
)

// OAuth 构造动态令牌鉴权：每次建连/请求前调用 tokenProvider 取最新 token
// （返回裸 token，SDK 补 "Bearer " 前缀），刷新与缓存逻辑由调用方实现。
func OAuth(tokenProvider func(ctx context.Context) (string, error)) Auth {
	return oauthAuth{provider: tokenProvider}
}

// oauthAuth 是 OAuth 鉴权实现（值类型，零分配）。
type oauthAuth struct {
	provider func(ctx context.Context) (string, error)
}

// Authorization 取最新 token 并组装 "Bearer <token>"。
func (a oauthAuth) Authorization(ctx context.Context) (string, error) {
	if a.provider == nil {
		return "", fmt.Errorf("codexsdk: OAuth tokenProvider 不能为 nil")
	}
	token, err := a.provider(ctx)
	if err != nil {
		return "", err
	}
	return "Bearer " + token, nil
}

// Invalidate：oauthAuth 无轮转状态（token 由调用方 provider 提供），no-op。
func (a oauthAuth) Invalidate() {}

// Fatal：oauthAuth 无轮转状态（终止判定归调用方），no-op。
func (a oauthAuth) Fatal(error) {}

// OAuth 轮转常量（对齐真实 codex 客户端 login/src/auth/manager.rs:192-198）。
const (
	// RefreshTokenURL 是 OpenAI OAuth refresh_token 端点。
	// 环境变量 CODEX_REFRESH_TOKEN_URL_OVERRIDE 可覆盖（同名对齐真实客户端）。
	RefreshTokenURL = "https://auth.openai.com/oauth/token"
	// RevokeTokenURL 是 OAuth revoke 端点（真实客户端同名常量；SDK 暂未使用）。
	RevokeTokenURL = "https://auth.openai.com/oauth/revoke"

	// defaultOAuthClientID 是 OpenAI app 默认 client_id（manager.rs:1618）。
	// 环境变量 CODEX_APP_SERVER_LOGIN_CLIENT_ID 可覆盖（同名对齐真实客户端）。
	defaultOAuthClientID = "app_EMoamEEZ73f0CkXaXp7hrann"
)

// OAuthWithRotation 构造 OAuth 轮转鉴权：SDK 内部维护 refresh_token 轮转协议
// （refresh 请求 / oauth 响应解析 / rt 轮换 / 判死分类 / 指数退避），调用方只
// 提供 refresh_token 材料，经 WithOnTokenRotated 接收新 at+rt、经
// WithOnAuthFatal 接收账号级终止通知。
//
// refreshToken 必传（构造期即拒绝空值——防空 rt 永久失败）；WithInitialAccessToken
// 可预置初始 at（传了直接用、401/失效才轮转，不传则首请求前先用 rt 换取）。
// 构造器直接返回 Auth 接口（具体类型不外露，防 copylocks：接口拷贝共享同一
// 装箱数据，单飞锁随指针共享）；多 client 共享同一 Auth 时状态天然共享
// （并发 401 单飞恰一次 refresh）。
//
// 与 OAuth(tokenProvider) 低层回调的关系：后者保留（自定义 token 源），
// 前者覆盖 RT 轮转协议面。PAT(token) 无轮转语义，不受影响。
func OAuthWithRotation(refreshToken string, opts ...OAuthOption) Auth {
	if refreshToken == "" {
		panic("codexsdk: OAuthWithRotation refreshToken 不能为空")
	}
	cfg := defaultOAuthConfig()
	for _, o := range opts {
		o(&cfg)
	}
	r := &rotationAuth{
		rt:                refreshToken,
		refreshTimeout:    cfg.refreshTimeout,
		backoffBase:       cfg.backoffBase,
		backoffCap:        cfg.backoffCap,
		maxAttempts:       cfg.maxAttempts,
		tokenRotatedRetry: cfg.tokenRotatedRetry,
		onTokenRotated:    cfg.onTokenRotated,
		onAuthFatal:       cfg.onAuthFatal,
		refreshURL:        resolveRefreshTokenURL(),
		clientID:          resolveOAuthClientID(),
	}
	if r.maxAttempts <= 0 {
		r.maxAttempts = 1 // 关闭退避：单次尝试
	}
	if r.tokenRotatedRetry <= 0 {
		r.tokenRotatedRetry = 1
	}
	if cfg.initialAT != "" {
		v := "Bearer " + cfg.initialAT // 缓存完整头值（每轮转计算一次）
		r.at.Store(&v)
	}
	return r
}

// rotationAuth 是 OAuthWithRotation 的实现（指针状态，构造后共享）。
//
// 状态：at 缓存完整头值 "Bearer <at>"（atomic 指针，Authorization 仅 Load——
// 热路径零分配零锁）；fatal 终止态（atomic）；refreshMu 保护单飞与 rt 轮换
// （refresh_token 仅单飞 leader 读写）；D4 回调重试状态（pending 未交付的
// 轮转结果 + 连续失败计数）。
type rotationAuth struct {
	// at 缓存完整头值；nil 表示需刷新（首请求 / Invalidate / 轮转中）。
	at    atomic.Pointer[string]
	fatal atomic.Pointer[fatalState]

	refreshMu sync.Mutex
	inflight  *refreshRun // 进行中的单飞 refresh（nil 表示无）

	rt string // 当前 refresh_token（仅单飞 leader 在锁内读写）

	// D4 回调重试状态（仅单飞 leader 在锁内读写）。
	pendingAt, pendingRt string
	pendingSet           bool
	callbackFails        int

	// 配置（构造时固定，只读）。
	refreshTimeout    time.Duration
	backoffBase       time.Duration
	backoffCap        time.Duration
	maxAttempts       int
	tokenRotatedRetry int
	onTokenRotated    func(at, rt string)
	onAuthFatal       func(err error)
	refreshURL        string
	clientID          string
}

// fatalState 是终止态容器（atomic 指针，避免 atomic.Value 存不同类型错误 panic）。
type fatalState struct {
	err error
}

// refreshRun 是一次单飞 refresh 的结果容器：leader 置 err 后 close(done)，
// waiters 在 done 上等待后读 err（close 同步建立 happens-before）。
type refreshRun struct {
	done chan struct{}
	err  error
}

// Authorization 返回 Authorization 头值：热路径仅 Load 缓存（零分配零锁）；
// 无缓存（首请求 / Invalidate）或终止态时走单飞 refresh。
func (r *rotationAuth) Authorization(ctx context.Context) (string, error) {
	if f := r.fatal.Load(); f != nil {
		return "", f.err
	}
	if at := r.at.Load(); at != nil {
		return *at, nil // 热路径：两次原子 Load + 解引用，零分配
	}
	if err := r.refresh(ctx); err != nil {
		return "", err
	}
	if f := r.fatal.Load(); f != nil {
		return "", f.err
	}
	at := r.at.Load()
	if at == nil {
		return "", &RefreshError{Attempts: 0, Err: errors.New("codexsdk: refresh 完成但未获得 access_token")}
	}
	return *at, nil
}

// Invalidate 显式失效：置空 at 缓存，下次 Authorization 前 refresh。
// 与进行中的单飞 refresh 并发安全（原子指针覆盖；refresh 完成后的 Store
// 与 Invalidate 的先后由调用方时序决定）。
func (r *rotationAuth) Invalidate() {
	r.at.Store(nil)
}

// Fatal 显式终止（网关解析到 WS 判死事件帧时调用）：置账号级终止状态，
// 后续 Authorization 恒返回该错误。不触发 OnAuthFatal（调用方已获知）。
// nil 忽略。
func (r *rotationAuth) Fatal(err error) {
	if err == nil {
		return
	}
	r.fatal.CompareAndSwap(nil, &fatalState{err: err})
}

// setFatal 置终止态并回调 OnAuthFatal（至多一次：CAS 胜者回调；败者并发
// 调用不重复通知）。
func (r *rotationAuth) setFatal(err error) {
	if r.fatal.CompareAndSwap(nil, &fatalState{err: err}) {
		if r.onAuthFatal != nil {
			r.onAuthFatal(err)
		}
	}
}

// refresh 是单飞入口（refreshTrigger 接口）：并发调用共享一次 refresh，
// 结果一致返回；调用方 ctx 取消只退出本调用（不污染单飞——下次可再试）。
func (r *rotationAuth) refresh(ctx context.Context) error {
	r.refreshMu.Lock()
	inf := r.inflight
	leader := inf == nil
	if leader {
		inf = &refreshRun{done: make(chan struct{})}
		r.inflight = inf
	}
	r.refreshMu.Unlock()

	if !leader {
		select {
		case <-inf.done:
			return inf.err
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	err := r.doRefresh(ctx)
	inf.err = err
	close(inf.done)
	r.refreshMu.Lock()
	if r.inflight == inf {
		r.inflight = nil
	}
	r.refreshMu.Unlock()
	return err
}

// doRefresh 执行一轮完整刷新（单飞 leader 专用）：
// D4 回调重试 → refresh 网络流程（退避）→ 缓存新 at / rt 轮换 → OnTokenRotated。
func (r *rotationAuth) doRefresh(ctx context.Context) error {
	// D4：上次轮转回调未交付时先重试（幂等 upsert 语义——同一 (at, rt) 可重复投递）。
	if err := r.deliverPendingRotate(); err != nil {
		return err
	}
	at, rt, err := r.refreshWithRetry(ctx)
	if err != nil {
		return err
	}
	// 仅非空覆盖（响应缺 refresh_token 时保留内存旧 rt——防空 rt 永久失败，
	// 对齐 codex manager.rs:1496-1498）。
	if rt != "" {
		r.rt = rt
	}
	v := "Bearer " + at // 缓存完整头值（每轮转计算一次）
	r.at.Store(&v)
	if r.onTokenRotated != nil {
		if err := r.callRotate(at, rt); err != nil {
			// D4：回调失败不阻塞请求——本次 at 放行，记 pending 下次 refresh 前重试。
			r.pendingAt, r.pendingRt = at, rt
			r.pendingSet = true
			r.callbackFails++
			if r.callbackFails >= r.tokenRotatedRetry {
				fatalErr := fmt.Errorf("codexsdk: 轮转回调连续失败 %d 次（%v），令牌持久化中断", r.callbackFails, err)
				r.setFatal(fatalErr)
				return fatalErr
			}
		}
	}
	return nil
}

// deliverPendingRotate 重试未交付的轮转回调（D4）。返回 nil 表示已交付或
// 无待交付或未达阈值（refresh 继续）；连续失败达阈值时置 Fatal 态并返回错误。
func (r *rotationAuth) deliverPendingRotate() error {
	if !r.pendingSet {
		return nil
	}
	if r.onTokenRotated == nil {
		r.pendingSet = false
		return nil // 无回调可交付：清 pending
	}
	if err := r.callRotate(r.pendingAt, r.pendingRt); err == nil {
		r.pendingSet = false
		r.callbackFails = 0
		return nil
	}
	r.callbackFails++
	if r.callbackFails >= r.tokenRotatedRetry {
		fatalErr := fmt.Errorf("codexsdk: 轮转回调连续失败 %d 次，令牌持久化中断", r.callbackFails)
		r.setFatal(fatalErr)
		return fatalErr
	}
	return nil // 未达阈值：本次 refresh 继续
}

// callRotate 调用 OnTokenRotated 回调并恢复 panic——回调失败（panic）按 D4
// 语义处理。回调在单飞内执行（阻塞并发等待者——网关回调应快，本地 upsert
// 毫秒级）；幂等 upsert 由调用方保证。
func (r *rotationAuth) callRotate(at, rt string) (err error) {
	defer func() {
		if p := recover(); p != nil {
			err = fmt.Errorf("codexsdk: OnTokenRotated 回调 panic: %v", p)
		}
	}()
	r.onTokenRotated(at, rt)
	return nil
}

// refreshWithRetry 执行 refresh 网络流程：POST token 端点 + 响应解析 +
// 判死分类 + 指数退避重试。成功返回 (accessToken, refreshToken)。
func (r *rotationAuth) refreshWithRetry(ctx context.Context) (at, rt string, err error) {
	attempts := 0
	for {
		attempts++
		resp, rerr := r.refreshOnce(ctx)
		if rerr == nil {
			return resp.AccessToken, resp.RefreshToken, nil
		}
		if isFatalAuthError(rerr) {
			r.setFatal(rerr)
			return "", "", rerr
		}
		// 可重试类（网络 / 5xx / 429 / RT 端点其他非 2xx / 响应缺 access_token）。
		if attempts >= r.maxAttempts {
			return "", "", &RefreshError{Attempts: attempts, Err: rerr}
		}
		delay := backoffDelay(attempts, r.backoffBase, r.backoffCap)
		select {
		case <-ctx.Done():
			return "", "", ctx.Err() // 取消/超时：单飞释放，下次可再试
		case <-time.After(delay):
		}
	}
}

// refreshOnce 单次 refresh 请求。成功返回解析后的响应；错误为分类结果：
// fatal 类（*RefreshOAuthError / *AccountDisabledError）或可重试类（普通错误）。
func (r *rotationAuth) refreshOnce(ctx context.Context) (*refreshResponse, error) {
	rctx, cancel := context.WithTimeout(ctx, r.refreshTimeout)
	defer cancel()

	payload, err := json.Marshal(refreshRequest{
		ClientID:     r.clientID,
		GrantType:    "refresh_token",
		RefreshToken: r.rt,
	})
	if err != nil {
		return nil, fmt.Errorf("codexsdk: 构造 refresh 请求体失败: %w", err)
	}
	req, err := http.NewRequestWithContext(rctx, http.MethodPost, r.refreshURL, bytes.NewReader(payload))
	if err != nil {
		return nil, fmt.Errorf("codexsdk: 构造 refresh 请求失败: %w", err)
	}
	// 伪装层默认头（对齐默认 UA/Originator 形态）。
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", DefaultCodexUserAgent)
	req.Header.Set("Originator", DefaultOriginator)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("codexsdk: refresh 请求失败: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("codexsdk: 读取 refresh 响应失败: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		return nil, classifyRefreshError(resp.StatusCode, body)
	}
	var rr refreshResponse
	if err := json.Unmarshal(body, &rr); err != nil {
		return nil, fmt.Errorf("codexsdk: refresh 响应解析失败: %w", err)
	}
	if rr.AccessToken == "" {
		return nil, errors.New("codexsdk: refresh 响应缺少 access_token")
	}
	return &rr, nil
}

// refreshRequest 是 OAuth refresh 请求体（JSON 形态，对齐 codex
// manager.rs:1506-1524——真实客户端用 JSON，非 form-urlencoded）。
type refreshRequest struct {
	ClientID     string `json:"client_id"`
	GrantType    string `json:"grant_type"`
	RefreshToken string `json:"refresh_token"`
}

// refreshResponse 是 OAuth refresh 响应（字段均可选——仅非空覆盖，对齐
// codex manager.rs:1610-1615）。
type refreshResponse struct {
	IDToken      string `json:"id_token"`
	AccessToken  string `json:"access_token"`
	RefreshToken string `json:"refresh_token"`
}

// refreshFatalCodes 是 RT 判死码集（10 码，大小写不敏感）——账号级终止。
// 实证：sub2api token_refresh_service.go:437-502（9 码）+ codex manager.rs:1553
// refresh_token_expired（真实客户端判永久）。不含 sub2api 本地生成的
// "no refresh token available"（SDK 构造期 rt 必传，该码不可能出现在响应体）。
var refreshFatalCodes = []string{
	"invalid_grant", "invalid_refresh_token", "refresh_token_expired",
	"refresh_token_reused", "refresh_token_invalidated", "app_session_terminated",
	"token_expired", "invalid_client", "unauthorized_client", "access_denied",
}

func isRefreshFatalCode(code string) bool {
	for _, c := range refreshFatalCodes {
		if strings.EqualFold(code, c) {
			return true
		}
	}
	return false
}

// classifyRefreshError 分类 refresh 端点非 2xx 响应（鉴权面错误分类，SDK 边界内；
// responses 业务协议零解析纪律不受影响）：
//   - token 端点 401 → 无条件判死（对齐 codex manager.rs:1537-1538，无论错误码）
//   - RT 判死码（10 码，大小写不敏感）→ RefreshOAuthError
//   - 账号禁用类（402 泛化 / 400 org disabled / KYC 文案）→ AccountDisabledError
//   - 其余（429/529/5xx/403/未知码）→ 可重试（两权威源默认，评审 R1）
func classifyRefreshError(status int, body []byte) error {
	if status == http.StatusUnauthorized {
		return &RefreshOAuthError{Code: strings.ToLower(firstNonEmpty(extractErrorCode(body), "unauthorized")), Raw: body}
	}
	if code := extractErrorCode(body); isRefreshFatalCode(code) {
		return &RefreshOAuthError{Code: strings.ToLower(code), Raw: body}
	}
	if status == http.StatusPaymentRequired {
		// 402 泛化判死（sub2api B7：余额不足/计费问题）；detail.code/error.code 取作依据。
		return &AccountDisabledError{StatusCode: status, Detail: firstNonEmpty(extractNestedCode(body), "payment required"), Raw: body}
	}
	if status == http.StatusBadRequest {
		if msg := extractErrorMessage(body); isAccountDisabledMessage(msg) {
			return &AccountDisabledError{StatusCode: status, Detail: msg, Raw: body}
		}
	}
	return fmt.Errorf("codexsdk: refresh 端点 HTTP %d: %s", status, body)
}

// extractErrorCode 提取响应体错误码：优先 error.code（OpenAI 形态
// {"error":{"code":"x"}}），其次顶层 error 字符串（OAuth 标准形态
// {"error":"invalid_grant"}）。
func extractErrorCode(body []byte) string {
	if v := gjson.GetBytes(body, "error.code"); v.Exists() {
		return v.String()
	}
	return gjson.GetBytes(body, "error").String()
}

// extractNestedCode 提取 402 形态错误码（{"detail":{"code":"deactivated_workspace"}}）。
func extractNestedCode(body []byte) string {
	if v := gjson.GetBytes(body, "detail.code"); v.Exists() {
		return v.String()
	}
	return gjson.GetBytes(body, "error.code").String()
}

// extractErrorMessage 提取 error.message 文案。
func extractErrorMessage(body []byte) string {
	return gjson.GetBytes(body, "error.message").String()
}

// isAccountDisabledMessage 匹配账号禁用文案（400 形态；大小写不敏感）：
// organization has been disabled（sub2api B4）/ identity verification is
// required（KYC，sub2api B5）。
func isAccountDisabledMessage(msg string) bool {
	m := strings.ToLower(msg)
	return strings.Contains(m, "organization has been disabled") ||
		strings.Contains(m, "identity verification is required")
}

// classifyAT401 分类 responses 请求路径 401（判死码，大小写不敏感）：
// error.code/error.type ∈ {token_invalidated, token_revoked}（token 永久作废，
// sub2api B1），或顶层 detail == "Unauthorized"（ChatGPT 内部 API 风格，
// token 完全无效，sub2api B2）。非判死 401（过期/无效 AT）→ nil：走自动轮转。
func classifyAT401(body []byte) *AuthPermanentlyRevokedError {
	for _, p := range []string{"error.code", "error.type"} {
		if code := gjson.GetBytes(body, p).String(); isATFatalCode(code) {
			return &AuthPermanentlyRevokedError{Code: strings.ToLower(code), Raw: body}
		}
	}
	if detail := gjson.GetBytes(body, "detail").String(); strings.EqualFold(strings.TrimSpace(detail), "unauthorized") {
		return &AuthPermanentlyRevokedError{Code: "unauthorized", Raw: body}
	}
	return nil
}

func isATFatalCode(code string) bool {
	return strings.EqualFold(code, "token_invalidated") || strings.EqualFold(code, "token_revoked")
}

// isFatalAuthError 判断错误是否为账号级终止类（refresh 路径）。
func isFatalAuthError(err error) bool {
	var re *RefreshOAuthError
	var ad *AccountDisabledError
	return errors.As(err, &re) || errors.As(err, &ad)
}

// backoffDelay 计算第 attempt 次（从 1 起）失败后的退避延迟：
// base * 2^(attempt-1)，cap 封顶。base 为 SDK 自有默认 200ms。
func backoffDelay(attempt int, base, cap time.Duration) time.Duration {
	if base <= 0 {
		base = 200 * time.Millisecond
	}
	d := base
	for i := 1; i < attempt; i++ {
		d *= 2
	}
	if cap > 0 && d > cap {
		d = cap
	}
	return d
}

func firstNonEmpty(a, b string) string {
	if a != "" {
		return a
	}
	return b
}

// resolveRefreshTokenURL 解析 refresh 端点 URL（CODEX_REFRESH_TOKEN_URL_OVERRIDE
// 覆盖默认，同名对齐真实客户端 env，构造期解析）。
func resolveRefreshTokenURL() string {
	if v := os.Getenv("CODEX_REFRESH_TOKEN_URL_OVERRIDE"); v != "" {
		return v
	}
	return RefreshTokenURL
}

// resolveOAuthClientID 解析 OAuth client_id（CODEX_APP_SERVER_LOGIN_CLIENT_ID
// 覆盖默认 app client id，同名对齐真实客户端 env，构造期解析）。
func resolveOAuthClientID() string {
	if v := os.Getenv("CODEX_APP_SERVER_LOGIN_CLIENT_ID"); v != "" {
		return v
	}
	return defaultOAuthClientID
}

// OAuthOption 配置 OAuthWithRotation。
type OAuthOption func(*oauthConfig)

type oauthConfig struct {
	initialAT         string
	onTokenRotated    func(at, rt string)
	onAuthFatal       func(err error)
	refreshTimeout    time.Duration
	backoffBase       time.Duration
	backoffCap        time.Duration
	maxAttempts       int
	tokenRotatedRetry int
}

// defaultOAuthConfig 返回 OAuthWithRotation 默认配置：
// refresh 超时 10s；退避 base 200ms / cap 30s / 上限 3 次（总尝试数，含首次）；
// 轮转回调失败重试阈值 3 次（评审 D2/D8 定值）。
func defaultOAuthConfig() oauthConfig {
	return oauthConfig{
		refreshTimeout:    10 * time.Second,
		backoffBase:       200 * time.Millisecond,
		backoffCap:        30 * time.Second,
		maxAttempts:       3,
		tokenRotatedRetry: 3,
	}
}

// WithInitialAccessToken 预置初始 access token（裸 token）：传了直接用，
// 401/失效才轮转；不传则首请求前先用 refresh_token 换取。
func WithInitialAccessToken(at string) OAuthOption {
	return func(c *oauthConfig) { c.initialAT = at }
}

// WithOnTokenRotated 设置轮转回调：每次 refresh 成功产出新 at+rt 时同步调用
// （成功投递时每轮转至多一次；回调在单飞内执行，阻塞并发等待者——应快速
// 返回，本地 upsert 毫秒级）。回调失败（panic）不阻塞请求：本次 at 放行，
// 下次 refresh 前重试投递（幂等 upsert 由调用方保证），连续失败达
// WithTokenRotatedRetry 阈值 → OnAuthFatal。
func WithOnTokenRotated(fn func(at, rt string)) OAuthOption {
	return func(c *oauthConfig) { c.onTokenRotated = fn }
}

// WithOnAuthFatal 设置账号级终止回调（至多一次）：SDK 判定账号级不可重试
// （RT 判死码 / token 端点 401 / 账号禁用 / 回调连续失败达阈值）时通知网关
// 标记账号失效。网关自行调用 Auth.Fatal 的显式终止不触发本回调。
func WithOnAuthFatal(fn func(err error)) OAuthOption {
	return func(c *oauthConfig) { c.onAuthFatal = fn }
}

// WithRefreshTimeout 设置单次 refresh 超时（默认 10s）。超时/取消不污染单飞
// ——等待者收到错误，下次可重试。
func WithRefreshTimeout(d time.Duration) OAuthOption {
	return func(c *oauthConfig) { c.refreshTimeout = d }
}

// WithBackoff 设置 refresh 退避：cap 为延迟封顶（默认 30s；0 表示不封顶），
// maxAttempts 为总尝试次数上限（默认 3；<=0 关闭重试，单次尝试后即返回）。
// base 固定 200ms（指数翻倍）。
func WithBackoff(cap time.Duration, maxAttempts int) OAuthOption {
	return func(c *oauthConfig) {
		c.backoffCap = cap
		c.maxAttempts = maxAttempts
	}
}

// WithTokenRotatedRetry 设置 OnTokenRotated 回调失败重试阈值（默认 3）：
// 连续失败达阈值 → OnAuthFatal。
func WithTokenRotatedRetry(max int) OAuthOption {
	return func(c *oauthConfig) { c.tokenRotatedRetry = max }
}

// refreshTrigger 是 401 自动轮转的触达接口（私有）：实现了 refresh 的 Auth
// 在 Do/Stream/Dial 遇到 401 时触发单飞刷新。oauthAuth/patAuth 不实现 →
// 无自动轮转（PAT 场景 401 原样返回，现状保持）。
type refreshTrigger interface {
	refresh(ctx context.Context) error
}
