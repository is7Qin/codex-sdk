package codexsdk

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"

	"github.com/tidwall/gjson"
)

// DefaultAuthAPIBaseURL 是 PAT whoami 端点 base 默认值（对齐真客户端）。
const DefaultAuthAPIBaseURL = "https://auth.openai.com/api/accounts"

// PATMetadata 是 PAT whoami 响应（字段对齐真客户端 PersonalAccessTokenMetadata；
// 缺失字段宽松，不报错）。
type PATMetadata struct {
	Email     string // 可空
	UserID    string
	AccountID string
	PlanType  string
	FedRAMP   bool
}

// resolveAuthAPIBase 解析 whoami 端点 base（env CODEX_AUTHAPI_BASE_URL
// 覆盖默认，与真客户端同名）。
func resolveAuthAPIBase() string {
	if v := strings.TrimSpace(os.Getenv("CODEX_AUTHAPI_BASE_URL")); v != "" {
		return strings.TrimRight(v, "/")
	}
	return DefaultAuthAPIBaseURL
}

// FetchPATMetadata 调用 PAT whoami 端点获取账号元数据。请求形态与 SDK 既有
// refresh 请求一致（Authorization: Bearer <token> + UA/Originator；UA 用
// DefaultCodexUserAgent）。端点 base 支持 env CODEX_AUTHAPI_BASE_URL 覆盖
// （与真客户端同名）；非 2xx → 带状态码错误。
//
// 不自动触发：PAT() 构造保持零网络（静态契约不变，向后兼容）；调用时机由
// 消费方（管理面导入/保存）决定。错误语义：非 2xx / 解码失败 → error
// （不掩盖状态码）；调用方 best-effort（whoami 失败不阻塞账号导入——
// account id 仍可由人工提供）。
func FetchPATMetadata(ctx context.Context, token string, opts ...PATOption) (*PATMetadata, error) {
	url := resolveAuthAPIBase() + "/v1/user-auth-credential/whoami"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("codexsdk: 构造 PAT whoami 请求失败: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("User-Agent", DefaultCodexUserAgent)
	req.Header.Set("Originator", DefaultOriginator)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("codexsdk: PAT whoami 请求失败: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("codexsdk: 读取 PAT whoami 响应失败: %w", err)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("codexsdk: PAT whoami 端点 HTTP %d: %s", resp.StatusCode, body)
	}
	// 宽松映射：缺失字段留零值，不报错。
	return &PATMetadata{
		Email:     gjson.GetBytes(body, "email").String(),
		UserID:    gjson.GetBytes(body, "chatgpt_user_id").String(),
		AccountID: gjson.GetBytes(body, "chatgpt_account_id").String(),
		PlanType:  gjson.GetBytes(body, "chatgpt_plan_type").String(),
		FedRAMP:   gjson.GetBytes(body, "chatgpt_account_is_fedramp").Bool(),
	}, nil
}
