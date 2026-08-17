package codexsdk

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
)

// DefaultUsageURL 是默认上游 usage 完整端点形态（与 DefaultResponsesURL
// 同源派生：末尾 /responses 路径段 → /wham/usage——ChatGPT 面
// base=https://chatgpt.com/backend-api/codex → .../backend-api/wham/usage）。
// 实证出处：codex-rs backend-client/src/client/rate_limit_resets.rs:80-85
// GET {base}/wham/usage（ChatGPT 面）+ client.rs:118-125 PathStyle 判定
// （base_url 含 /backend-api → /wham/*）。仅支持 ChatGPT 面（用户裁决
// 2026-08-17）：API-key 面端点 /api/codex/usage 无实际调用者——codex-rs
// tui/chatwidget/rate_limits.rs:330 should_prefetch_rate_limits 仅在 ChatGPT
// 账号登录态触发（has_chatgpt_account），API-key/PAT 模式从不调用。
// 本常量在派生路径下仅文档/测试引用：默认 c.baseURL=DefaultResponsesURL →
// GetUsage 方法内 usageEndpointFrom 派生结果即本值。
const DefaultUsageURL = "https://chatgpt.com/backend-api/wham/usage"

// GetUsage 查看账号额度（非流式 GET）：GET {usageEndpoint}，无请求体；
// 响应解码为 *UsageStatus（白名单解码，模型外字段忽略）。状态码 >= 400
// 返回 *HTTPError；401 自动轮转语义同 Search。
//
// 端点：由 c.baseURL（responses 完整端点）尾段派生——默认
// DefaultResponsesURL → DefaultUsageURL；WithBaseURL 覆盖值同样按
// responses 端点语义派生（网关 cred.BaseURL 直传即用；URL 派生逻辑留
// SDK——网关零拼装）。仅支持 ChatGPT 面形态（.../backend-api/codex/
// responses）：API-key 面形态（.../v1/responses）派生报错——其端点无
// 实际调用者（见 DefaultUsageURL 实证出处）。
//
// 传输层复用 doURL：鉴权头注入 / 懒构建 / 401 判死分类 + 单飞 refresh +
// 自动重试一次 / fatal 类错误透传（不被 HTTPError 吞掉，errors.As 可区分）/
// 状态码 >= 400 返回 *HTTPError 原样交付。GET 无请求体（payload nil →
// sendRequest 内空 reader，不特判；GET 语义由 method 参数决定）。turn-state
// 不捕获（对齐 GenerateImage/Search——doURL 路径不捕获，网关不消费）。
func (c *HTTPClient) GetUsage(ctx context.Context) (*UsageStatus, error) {
	usageURL, err := usageEndpointFrom(c.baseURL)
	if err != nil {
		return nil, err
	}
	targetURL, err := buildURL(usageURL, c.opts.query)
	if err != nil {
		return nil, err
	}
	resp, err := c.doURL(ctx, targetURL, http.MethodGet, nil)
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
	var usage UsageStatus
	if err := json.Unmarshal(body, &usage); err != nil {
		return nil, fmt.Errorf("codexsdk: 解析 usage 响应失败: %w", err)
	}
	return &usage, nil
}

// usageEndpointFrom 由 responses 完整端点派生 usage 端点（对齐
// searchEndpointFrom 防呆——非 /responses 结尾 → 错误，不静默产生错误 URL）：
// 仅支持 ChatGPT 面形态（尾段 /responses 且前一段为 codex，
// .../backend-api/codex/responses）→ 切尾两段得 base → + /wham/usage；
// 尾段非 /responses 或前一段非 codex → 错误（API-key 面形态 .../v1/responses
// 直接拒绝——其端点 /api/codex/usage 无实际调用者，见 DefaultUsageURL 实证
// 出处；用户裁决 2026-08-17 仅 ChatGPT 面）。包内函数不导出——URL 派生全
// 在 SDK。
func usageEndpointFrom(responsesURL string) (string, error) {
	u, err := url.Parse(responsesURL)
	if err != nil {
		return "", fmt.Errorf("codexsdk: 解析 responses 端点 %q 失败: %w", responsesURL, err)
	}
	path := u.Path
	i := strings.LastIndex(path, "/")
	if i < 0 || path[i+1:] != "responses" {
		return "", fmt.Errorf("codexsdk: usage 端点派生失败：%q 尾段非 /codex/responses", responsesURL)
	}
	prefix := path[:i]
	j := strings.LastIndex(prefix, "/")
	if j < 0 || prefix[j+1:] != "codex" {
		return "", fmt.Errorf("codexsdk: usage 端点派生失败：%q 尾段非 /codex/responses", responsesURL)
	}
	u.Path = prefix[:j] + "/wham/usage"
	return u.String(), nil
}

// UsageStatus 账号额度（GET {usageEndpoint} 响应白名单解码——字段集合外的
// 键自动丢弃，零成本）。
type UsageStatus struct {
	PlanType             string                `json:"plan_type"`
	RateLimit            *RateLimitStatus      `json:"rate_limit,omitempty"`
	Credits              *UsageCredits         `json:"credits,omitempty"`
	SpendControl         *SpendControlStatus   `json:"spend_control,omitempty"`
	RateLimitReachedType *RateLimitReachedType `json:"rate_limit_reached_type,omitempty"`
}

type RateLimitStatus struct {
	Allowed       bool             `json:"allowed"`
	LimitReached  bool             `json:"limit_reached"`
	PrimaryWindow *RateLimitWindow `json:"primary_window,omitempty"`
}

// RateLimitWindow 主窗口：used_percent 用量百分比；reset_at 重置时刻（Unix 秒）。
type RateLimitWindow struct {
	UsedPercent        int `json:"used_percent"`
	LimitWindowSeconds int `json:"limit_window_seconds"`
	ResetAfterSeconds  int `json:"reset_after_seconds"`
	ResetAt            int `json:"reset_at"`
}

type UsageCredits struct {
	HasCredits          bool    `json:"has_credits"`
	Unlimited           bool    `json:"unlimited"`
	OverageLimitReached bool    `json:"overage_limit_reached"` // 实测存在，codex 模型无
	Balance             *string `json:"balance,omitempty"`     // 金额字符串（如 "12.50"），不解析
	ApproxLocalMessages []any   `json:"approx_local_messages,omitempty"`
	ApproxCloudMessages []any   `json:"approx_cloud_messages,omitempty"`
}

type SpendControlStatus struct {
	Reached         bool               `json:"reached"`
	IndividualLimit *SpendControlLimit `json:"individual_limit,omitempty"`
}

// SpendControlLimit 消费控制额度（limit/used/remaining 为金额字符串）。
type SpendControlLimit struct {
	Limit            string `json:"limit"`
	Used             string `json:"used"`
	Remaining        string `json:"remaining"`
	UsedPercent      int    `json:"used_percent"`
	RemainingPercent int    `json:"remaining_percent"`
}

// RateLimitReachedType 限流触达类型（如 {type:"rate_limit_reached", details:"default"}）。
type RateLimitReachedType struct {
	Type    string `json:"type"`
	Details string `json:"details"`
}
