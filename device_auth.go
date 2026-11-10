package codexsdk

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/tidwall/gjson"
)

// —— Codex 设备码登录（ChatGPT OAuth device authorization grant） ——
//
// 协议逐点对齐真客户端 openai/codex codex-rs/login/src/device_code_auth.rs +
// server.rs::exchange_code_for_tokens（非第三方实现转述）：
//
//	1. POST {issuer}/api/accounts/deviceauth/usercode  JSON {"client_id"}
//	   → {device_auth_id, user_code, interval[, expires_in]}
//	2. 轮询 POST {issuer}/api/accounts/deviceauth/token  JSON {device_auth_id, user_code}
//	   → 403/404 用户未授权；200 {authorization_code, code_challenge, code_verifier}
//	   （真客户端对 403/404 按 interval 睡眠后重试，15 分钟上限）
//	3. POST {issuer}/oauth/token  form-urlencoded（TokenEncoding::Form）
//	   grant_type=authorization_code / code / redirect_uri={issuer}/deviceauth/callback /
//	   client_id / code_verifier
//	   → {access_token, refresh_token, id_token, expires_in}
//
// 用户在第 1、3 步之间打开 {issuer}/codex/device 输入 user_code 完成授权。
//
// 边界：本文件只做协议与错误分类，**不持有登录状态**（待授权会话由调用方
// 持久化——网关侧按 Redis TTL 管理、多实例共享）、不落盘、不做多账号。刷新
// 沿用既有 OAuthWithRotation（同一 client_id / refresh 端点语义）。
// issuer 无 env 覆盖：真客户端经配置项（ServerOptions.issuer）传入而非 env，
// SDK 以 WithIssuer 对齐，默认 DefaultIssuer。

// DefaultIssuer 是 ChatGPT 认证 issuer 默认值（真客户端 ServerOptions 缺省同值）。
const DefaultIssuer = "https://auth.openai.com"

// 设备码协议路径（相对 issuer；与真客户端拼接方式一致）。
const (
	deviceUserCodePath     = "/api/accounts/deviceauth/usercode"
	deviceTokenPath        = "/api/accounts/deviceauth/token"
	deviceVerificationPath = "/codex/device"
	deviceCallbackPath     = "/deviceauth/callback"
	oauthTokenPath         = "/oauth/token"
)

const (
	// defaultDeviceAuthTimeout 是单个认证请求超时（镜像 refresh 的 10s：
	// 认证端点不是大模型流式面，卡住应尽快失败）。
	defaultDeviceAuthTimeout = 10 * time.Second
	// defaultDeviceLifetime 是上游未下发 expires_in 时的有效期（真客户端以 15 分钟
	// 为设备码登录上限——device_code_auth.rs 的 max_wait）。对外恒为正数，客户端
	// 才能维护本地截止时间，而不是把「未下发」当成「永不过期」。
	defaultDeviceLifetime = 15 * time.Minute
	// defaultDeviceInterval 是服务端未下发 interval 时的轮询间隔（真客户端不下发
	// 即 0，直接死循环；SDK 取保守缺省，服务端下发时以服务端为准）。
	defaultDeviceInterval = 5 * time.Second
	// maxDeviceAuthBodyBytes 是认证响应体上限（防护性；正常响应 <2KB）。
	maxDeviceAuthBodyBytes = 1 << 20
)

// 设备码登录的哨兵错误（errors.Is 判定；网关据此区分「继续轮询 / 过期 / 拒绝」）。
var (
	// ErrDeviceAuthorizationPending 用户尚未完成授权（轮询端点 403/404）。
	ErrDeviceAuthorizationPending = errors.New("codexsdk: 等待用户完成设备码授权")
	// ErrDeviceCodeExpired 设备码已过期（轮询端点 410）。
	ErrDeviceCodeExpired = errors.New("codexsdk: 设备码已过期")
	// ErrDeviceCodeDenied 用户拒绝授权（轮询端点 400 + access_denied 类错误体）。
	ErrDeviceCodeDenied = errors.New("codexsdk: 用户拒绝设备码授权")
)

// DeviceAuthError 是设备码流程的非预期响应（状态码非 2xx 且不属于上述哨兵）；
// 错误体原样保留供诊断（不含凭据）。
type DeviceAuthError struct {
	Op         string // usercode / token / exchange
	StatusCode int
	Body       []byte
}

func (e *DeviceAuthError) Error() string {
	body := strings.TrimSpace(string(e.Body))
	if len(body) > 256 {
		body = body[:256] + "..."
	}
	if body == "" {
		return fmt.Sprintf("codexsdk: 设备码 %s 端点 HTTP %d", e.Op, e.StatusCode)
	}
	return fmt.Sprintf("codexsdk: 设备码 %s 端点 HTTP %d: %s", e.Op, e.StatusCode, body)
}

// DeviceCode 是一次待授权的设备码会话（StartDeviceFlow 返回）。
//
// AuthID（device_auth_id）是轮询凭据：调用方持久化时须按敏感值处理，不回显给
// 最终用户（用户只需要 UserCode + VerificationURL）。
type DeviceCode struct {
	AuthID          string        // device_auth_id
	UserCode        string        // 展示给用户的一次性码（形如 ABCD-EFGHI）
	VerificationURL string        // 用户打开并输入 UserCode 的页面
	Interval        time.Duration // 轮询间隔（服务端下发；缺失按 5s）
	ExpiresIn       time.Duration // 服务端下发的有效期；0 = 未下发（用 MaxWait 兜底）
}

// DeviceTokens 是设备码登录换到的凭据束（字段语义对齐 auth.json tokens）。
//
// RefreshToken 上游正常必下发，但 SDK **不强制**（协议字段可缺；是否接受只有
// access token 的凭据束属消费方策略——如 gateway 要求 oauth 账号必须可续期）。
type DeviceTokens struct {
	AccessToken  string
	RefreshToken string
	IDToken      string
	ExpiresIn    time.Duration // 响应 expires_in；0 = 未下发
	ExpiresAt    time.Time     // 0 值 = 未下发 expires_in
	AccountID    string        // chatgpt_account_id（id_token 优先，access_token 兜底；空 = 未解析出）
	Email        string        // 登录邮箱（id_token claims；空 = 未解析出）
}

// DeviceAuthClient 执行设备码登录协议。零值不可用（clientID/issuer 为空），
// 用 NewDeviceAuthClient 构造。并发安全（无可变状态；每次调用自带 ctx）。
type DeviceAuthClient struct {
	issuer     string
	clientID   string
	httpClient *http.Client
	timeout    time.Duration
	now        func() time.Time
}

// DeviceAuthOption 配置 DeviceAuthClient。
type DeviceAuthOption func(*DeviceAuthClient)

// WithIssuer 覆盖认证 issuer（默认 DefaultIssuer；测试/自托管端点用）。
// TrimSpace + 去尾斜杠；空值忽略。
func WithIssuer(issuer string) DeviceAuthOption {
	return func(c *DeviceAuthClient) {
		v := strings.TrimRight(strings.TrimSpace(issuer), "/")
		if v != "" {
			c.issuer = v
		}
	}
}

// WithDeviceAuthClientID 覆盖 OAuth client_id（默认 defaultOAuthClientID——
// 与 refresh 路径同源 app client；空值忽略）。
func WithDeviceAuthClientID(id string) DeviceAuthOption {
	return func(c *DeviceAuthClient) {
		if v := strings.TrimSpace(id); v != "" {
			c.clientID = v
		}
	}
}

// WithDeviceAuthHTTPClient 覆盖 HTTP 客户端（默认 http.DefaultClient，
// 与 refresh / PAT whoami 同口径；nil 忽略）。
func WithDeviceAuthHTTPClient(hc *http.Client) DeviceAuthOption {
	return func(c *DeviceAuthClient) {
		if hc != nil {
			c.httpClient = hc
		}
	}
}

// WithDeviceAuthTimeout 覆盖单请求超时（<=0 忽略）。
func WithDeviceAuthTimeout(d time.Duration) DeviceAuthOption {
	return func(c *DeviceAuthClient) {
		if d > 0 {
			c.timeout = d
		}
	}
}

// NewDeviceAuthClient 构建设备码登录客户端（默认 issuer / client_id / 超时）。
func NewDeviceAuthClient(opts ...DeviceAuthOption) *DeviceAuthClient {
	c := &DeviceAuthClient{
		issuer: DefaultIssuer,
		// client_id 与 refresh 路径**同源**（CODEX_APP_SERVER_LOGIN_CLIENT_ID 覆盖）：
		// 设备码签发的 refresh token 绑定 client_id，刷新时必须用同一个值，否则
		// 登录成功、刷新必然 invalid_client。
		clientID:   resolveOAuthClientID(),
		httpClient: http.DefaultClient,
		timeout:    defaultDeviceAuthTimeout,
		now:        time.Now,
	}
	for _, o := range opts {
		o(c)
	}
	return c
}

// StartDeviceFlow 申请用户码与设备码（协议第 1 步）。端点 404 表示该 issuer 未
// 开启设备码登录（真客户端同语义：改用浏览器登录或检查服务端地址）。
func (c *DeviceAuthClient) StartDeviceFlow(ctx context.Context) (*DeviceCode, error) {
	payload, err := json.Marshal(struct {
		ClientID string `json:"client_id"`
	}{ClientID: c.clientID})
	if err != nil {
		return nil, fmt.Errorf("codexsdk: 构造设备码请求失败: %w", err)
	}
	resp, raw, err := c.post(ctx, c.issuer+deviceUserCodePath, "application/json", payload)
	if err != nil {
		return nil, fmt.Errorf("codexsdk: 设备码请求失败: %w", err)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, &DeviceAuthError{Op: "usercode", StatusCode: resp.StatusCode, Body: raw}
	}

	expiresIn := secondsDuration(gjson.GetBytes(raw, "expires_in"))
	if expiresIn <= 0 {
		expiresIn = defaultDeviceLifetime
	}
	code := &DeviceCode{
		AuthID:          strings.TrimSpace(gjson.GetBytes(raw, "device_auth_id").String()),
		UserCode:        strings.TrimSpace(firstNonEmpty(gjson.GetBytes(raw, "user_code").String(), gjson.GetBytes(raw, "usercode").String())),
		VerificationURL: c.issuer + deviceVerificationPath,
		Interval:        deviceInterval(gjson.GetBytes(raw, "interval")),
		ExpiresIn:       expiresIn,
	}
	if code.AuthID == "" || code.UserCode == "" {
		return nil, fmt.Errorf("codexsdk: 设备码响应缺少 device_auth_id/user_code")
	}
	return code, nil
}

// PollDeviceFlow 轮询一次并（授权完成后）换码拿凭据：协议第 2、3 步合一
// （真客户端 poll_for_token 后必然紧跟 exchange_code_for_tokens）。
//
// 返回：
//   - ErrDeviceAuthorizationPending：用户尚未授权，调用方保留会话稍后再轮询
//   - ErrDeviceCodeExpired：会话已过期，调用方删除会话
//   - ErrDeviceCodeDenied：用户拒绝
//   - *DeviceAuthError：其它非预期响应（限流/服务端故障等）
//   - *DeviceTokens：授权完成
//
// 不睡眠、不重试——轮询节奏由调用方（网关 HTTP 端点）决定。
func (c *DeviceAuthClient) PollDeviceFlow(ctx context.Context, code *DeviceCode) (*DeviceTokens, error) {
	if code == nil || code.AuthID == "" || code.UserCode == "" {
		return nil, errors.New("codexsdk: 设备码会话缺少 device_auth_id/user_code")
	}
	payload, err := json.Marshal(struct {
		DeviceAuthID string `json:"device_auth_id"`
		UserCode     string `json:"user_code"`
	}{DeviceAuthID: code.AuthID, UserCode: code.UserCode})
	if err != nil {
		return nil, fmt.Errorf("codexsdk: 构造轮询请求失败: %w", err)
	}
	resp, raw, err := c.post(ctx, c.issuer+deviceTokenPath, "application/json", payload)
	if err != nil {
		return nil, fmt.Errorf("codexsdk: 设备码轮询失败: %w", err)
	}
	switch {
	case resp.StatusCode >= 200 && resp.StatusCode < 300:
		// 继续解析。
	case resp.StatusCode == http.StatusForbidden || resp.StatusCode == http.StatusNotFound:
		return nil, ErrDeviceAuthorizationPending
	case resp.StatusCode == http.StatusGone:
		return nil, ErrDeviceCodeExpired
	case deviceDeniedBody(raw):
		// 拒权不是固定 400：真客户端测试用 401 authorization_declined
		// （codex-rs/login/tests/suite/device_code_login.rs）。403/404 已在上面
		// 按「未授权」处理（状态优先，与真客户端只判状态一致）。
		return nil, ErrDeviceCodeDenied
	default:
		return nil, &DeviceAuthError{Op: "token", StatusCode: resp.StatusCode, Body: raw}
	}

	authorizationCode := strings.TrimSpace(gjson.GetBytes(raw, "authorization_code").String())
	codeVerifier := strings.TrimSpace(gjson.GetBytes(raw, "code_verifier").String())
	if authorizationCode == "" || codeVerifier == "" {
		// 2xx 但缺换码材料：真客户端同样失败（必然无法换 token），按协议错误上报。
		return nil, fmt.Errorf("codexsdk: 设备码轮询响应缺少 authorization_code/code_verifier")
	}
	return c.exchangeCode(ctx, authorizationCode, codeVerifier)
}

// exchangeCode 用授权码 + PKCE verifier 换取 token（协议第 3 步；表单编码，
// 对齐真客户端 TokenEncoding::Form）。不导出：调用方只经 PollDeviceFlow 进入
// （协议保证轮询成功后必然换码），避免公开一条半程路径。
func (c *DeviceAuthClient) exchangeCode(ctx context.Context, authorizationCode, codeVerifier string) (*DeviceTokens, error) {
	if strings.TrimSpace(authorizationCode) == "" || strings.TrimSpace(codeVerifier) == "" {
		return nil, errors.New("codexsdk: 换码缺少 authorization_code/code_verifier")
	}
	form := url.Values{
		"grant_type":    {"authorization_code"},
		"code":          {authorizationCode},
		"redirect_uri":  {c.issuer + deviceCallbackPath},
		"client_id":     {c.clientID},
		"code_verifier": {codeVerifier},
	}
	resp, raw, err := c.post(ctx, c.issuer+oauthTokenPath, "application/x-www-form-urlencoded", []byte(form.Encode()))
	if err != nil {
		return nil, fmt.Errorf("codexsdk: 设备码换 token 失败: %w", err)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, &DeviceAuthError{Op: "exchange", StatusCode: resp.StatusCode, Body: raw}
	}
	return c.parseDeviceTokens(raw)
}

// post 单请求执行：ctx 超时 + Content-Type + 伪装层默认头（UA/Originator，
// 与 refresh / whoami 同口径），响应体按上限读完并关闭。
func (c *DeviceAuthClient) post(ctx context.Context, endpoint, contentType string, body []byte) (*http.Response, []byte, error) {
	reqCtx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	req, err := http.NewRequestWithContext(reqCtx, http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		return nil, nil, err
	}
	req.Header.Set("Content-Type", contentType)
	req.Header.Set("Accept", "application/json")
	applyDefaultHeaders(req.Header)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, maxDeviceAuthBodyBytes))
	if err != nil {
		return nil, nil, err
	}
	return resp, raw, nil
}

// parseDeviceTokens 解析 /oauth/token 响应；账号标识/id_token 缺失不视为错误
// （字段为可选——真客户端亦按可选处理），但 access_token 必填。
func (c *DeviceAuthClient) parseDeviceTokens(raw []byte) (*DeviceTokens, error) {
	tok := &DeviceTokens{
		AccessToken:  strings.TrimSpace(gjson.GetBytes(raw, "access_token").String()),
		RefreshToken: strings.TrimSpace(gjson.GetBytes(raw, "refresh_token").String()),
		IDToken:      strings.TrimSpace(gjson.GetBytes(raw, "id_token").String()),
		ExpiresIn:    secondsDuration(gjson.GetBytes(raw, "expires_in")),
	}
	if tok.AccessToken == "" {
		return nil, errors.New("codexsdk: token 响应缺少 access_token")
	}
	if tok.ExpiresIn > 0 {
		tok.ExpiresAt = c.now().Add(tok.ExpiresIn)
	}
	tok.AccountID, tok.Email = deviceLoginIdentity(tok.IDToken, tok.AccessToken)
	return tok, nil
}

// deviceLoginIdentity 解析 (chatgpt_account_id, email)：id_token 优先（真客户端
// 从 id_token 的 https://api.openai.com/auth 取 account id），account id 缺省时
// 用 access_token claims 兜底（两者共享同一 claims 命名空间）。解析失败 → 空串。
func deviceLoginIdentity(idToken, accessToken string) (accountID, email string) {
	if idToken != "" {
		accountID, email = deviceTokenClaims(idToken)
	}
	if accountID == "" && accessToken != "" {
		id, mail := deviceTokenClaims(accessToken)
		accountID = id
		if email == "" {
			email = mail
		}
	}
	return accountID, email
}

// deviceTokenClaims 解出 ID token 关心字段（不验签、不校验 exp——与
// AccountIDFromToken 同语义：本地提取而非鉴权）。workSpace 级 account id 在
// 嵌套 auth claim 内，部分 issuer 也会在顶层复述，两处都取。
func deviceTokenClaims(token string) (accountID, email string) {
	payload, ok := jwtPayload(token)
	if !ok {
		return "", ""
	}
	var claims struct {
		AccountID string `json:"chatgpt_account_id"`
		Email     string `json:"email"`
		Auth      struct {
			AccountID string `json:"chatgpt_account_id"`
			Email     string `json:"email"`
		} `json:"https://api.openai.com/auth"`
	}
	if err := json.Unmarshal(payload, &claims); err != nil {
		return "", ""
	}
	accountID = strings.TrimSpace(firstNonEmpty(claims.AccountID, claims.Auth.AccountID))
	email = strings.TrimSpace(firstNonEmpty(claims.Email, claims.Auth.Email))
	return accountID, email
}

// deviceDeniedBody 轮询 400 的拒权判据：真客户端不细分 400，但 OAuth 设备流
// 惯例以 access_denied 表达用户拒绝（cc-switch 同判据），据此给出可区分的哨兵。
func deviceDeniedBody(body []byte) bool {
	lower := strings.ToLower(string(body))
	return strings.Contains(lower, "access_denied") ||
		strings.Contains(lower, "authorization_denied") ||
		strings.Contains(lower, "authorization_declined")
}

// deviceInterval 解析服务端 interval：真客户端按**字符串**反序列化
// （device_code_auth.rs deserialize_interval），实际响应亦有数字形态，两种都接受；
// 缺失/非法/<=0 → defaultDeviceInterval。
func deviceInterval(raw gjson.Result) time.Duration {
	d := secondsDuration(raw)
	if d <= 0 {
		return defaultDeviceInterval
	}
	return d
}

// secondsDuration 解析「秒」字段（数字或字符串）；缺失/非法 → 0。
func secondsDuration(raw gjson.Result) time.Duration {
	if !raw.Exists() {
		return 0
	}
	s := strings.TrimSpace(raw.String())
	if s == "" {
		return 0
	}
	secs, err := time.ParseDuration(s + "s")
	if err != nil || secs <= 0 {
		return 0
	}
	return secs
}
