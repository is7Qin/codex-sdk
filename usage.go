package codexsdk

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
)

// DefaultUsageURL 是官方上游 usage 完整端点（固定）。
const DefaultUsageURL = "https://chatgpt.com/backend-api/wham/usage"

// GetUsage 查看账号额度（非流式 GET）：GET DefaultUsageURL，无请求体；
// 响应解码为 *UsageStatus（白名单解码，模型外字段忽略）。状态码 >= 400
// 返回 *HTTPError；401 自动轮转语义同 Search。
//
// 传输层复用 doURL：鉴权头注入 / 懒构建 / 401 判死分类 + 单飞 refresh +
// 自动重试一次 / fatal 类错误透传（不被 HTTPError 吞掉，errors.As 可区分）/
// 状态码 >= 400 返回 *HTTPError 原样交付。GET 无请求体（payload nil →
// sendRequest 内空 reader，不特判；GET 语义由 method 参数决定）。turn-state
// 不捕获（对齐 GenerateImage/Search——doURL 路径不捕获，网关不消费）。
func (c *HTTPClient) GetUsage(ctx context.Context) (*UsageStatus, error) {
	resp, err := c.doURL(ctx, DefaultUsageURL, http.MethodGet, nil)
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
