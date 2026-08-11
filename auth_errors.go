package codexsdk

import "fmt"

// 账号级终止错误类型（网关 W6 接线以 errors.As 区分类别）。
// fatal 类经 Do / Stream / Dial 返回时透传（不被 HTTPError / DialError 包层
// 吞掉），errors.As 可直接命中。

// RefreshOAuthError 是 refresh 端点判死错误（RT 路径账号级终止，需重新授权）。
// 触发：响应体错误码 ∈ RT 判死码集（invalid_grant / invalid_refresh_token /
// refresh_token_expired / refresh_token_reused / refresh_token_invalidated /
// app_session_terminated / token_expired / invalid_client /
// unauthorized_client / access_denied，大小写不敏感），或 token 端点响应状态
// 401（无条件判死，无论错误码——对齐 codex manager.rs:1537-1538）。
type RefreshOAuthError struct {
	Code string // 响应体 error.code；端点 401 无错误码时为 "unauthorized"
	Raw  []byte // 响应体（诊断用）
}

func (e *RefreshOAuthError) Error() string {
	return fmt.Sprintf("codexsdk: refresh 被拒绝（%s），账号需重新授权", e.Code)
}

// AuthPermanentlyRevokedError 是 AT 路径判死错误：HTTP 401 响应体
// error.code/error.type ∈ {token_invalidated, token_revoked}（token 永久作废，
// 非过期），或顶层 detail == "Unauthorized"（ChatGPT 内部 API 风格，
// token 完全无效）。匹配大小写不敏感。
type AuthPermanentlyRevokedError struct {
	Code string // 判死依据（token_invalidated / token_revoked / unauthorized）
	Raw  []byte // 401 响应体（诊断用）
}

func (e *AuthPermanentlyRevokedError) Error() string {
	return fmt.Sprintf("codexsdk: 访问令牌已永久作废（%s），账号需重新授权", e.Code)
}

// AccountDisabledError 是账号/组织禁用错误（账号级终止）：
// 400 + "organization has been disabled" / "identity verification is required"
// （KYC），或 402（deactivated_workspace / payment required，402 泛化判死）。
type AccountDisabledError struct {
	StatusCode int
	Detail     string // 判死依据（错误码或 message 文案）
	Raw        []byte // 响应体（诊断用）
}

func (e *AccountDisabledError) Error() string {
	if e.Detail != "" {
		return fmt.Sprintf("codexsdk: 账号已禁用（HTTP %d）: %s", e.StatusCode, e.Detail)
	}
	return fmt.Sprintf("codexsdk: 账号已禁用（HTTP %d）", e.StatusCode)
}

// RefreshError 是 refresh 可重试类失败（网络错误 / 5xx / 429 / RT 端点其他
// 非 2xx / 响应缺 access_token）——非 fatal，下次调用可再试，不触发
// OnAuthFatal。指数退避耗尽后返回。
type RefreshError struct {
	Attempts int   // 已尝试次数（耗尽时 = 上限）
	Err      error // 最后一次尝试的错误
}

func (e *RefreshError) Error() string {
	return fmt.Sprintf("codexsdk: refresh 失败（已尝试 %d 次）: %v", e.Attempts, e.Err)
}

func (e *RefreshError) Unwrap() error { return e.Err }

// CallbackDeliveryError 是 OnTokenRotated 回调连续失败达阈值（D4，
// WithTokenRotatedRetry 可配，默认 3）触发的账号级终止错误——网关无法持久化
// 新令牌（at/rt 落库中断）。errors.As 可与协议级判死类型（RefreshOAuthError /
// AccountDisabledError / AuthPermanentlyRevokedError）区分。
type CallbackDeliveryError struct {
	Attempts int   // 连续失败次数（达阈值）
	Err      error // 最后一次回调失败的原因
}

func (e *CallbackDeliveryError) Error() string {
	return fmt.Sprintf("codexsdk: 轮转回调连续失败 %d 次，令牌持久化中断", e.Attempts)
}

func (e *CallbackDeliveryError) Unwrap() error { return e.Err }
