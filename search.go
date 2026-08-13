package codexsdk

import (
	"context"
	"fmt"
	"io"
	"net/url"
	"strings"
)

// DefaultSearchURL 是默认上游 search 完整端点形态（与 DefaultResponsesURL
// 同源派生：末尾 /responses 路径段 → /alpha/search——chatgpt.com 登录模式
// base=https://chatgpt.com/backend-api/codex → .../codex/alpha/search）。
// 实证出处：codex-rs codex-api/src/endpoint/search.rs:32-34 path 常量
// "alpha/search"（无前导 /v1）+ provider.rs:50-59 {base_url}/{path} 拼接
// （/v1 来自 base_url）+ lib.rs:37,243-257；API key 模式派生形态 =
// https://api.openai.com/v1/alpha/search 一并成立。非流式、请求/响应体
// opaque（SDK 零解析——alpha 端点实验性，上游变更网关免疫）。
// 本常量在派生路径下仅文档/测试引用：默认 c.baseURL=DefaultResponsesURL →
// Search 方法内 searchEndpointFrom 派生结果即本值。
const DefaultSearchURL = "https://chatgpt.com/backend-api/codex/alpha/search"

// Search 非流式搜索（codex 凭据直连 alpha/search 端点）：POST
// {searchEndpoint}，请求/响应体 opaque（SDK 零解析——alpha 端点实验性，
// 上游变更网关免疫；HTTPResponse.Raw 原样交付）。
//
// 端点：由 c.baseURL（responses 完整端点）尾段派生——默认
// DefaultResponsesURL → DefaultSearchURL；WithBaseURL 覆盖值同样按
// responses 端点语义派生（网关 cred.BaseURL 直传即用；URL 派生逻辑留
// SDK——网关零拼装）。
//
// 传输层复用 doURL：鉴权头注入 / 懒构建 / 401 判死分类 + 单飞 refresh +
// 自动重试一次 / fatal 类错误透传（不被 HTTPError 吞掉，errors.As 可区分）/
// 非 2xx 返回 *HTTPError 原样交付。请求头与 HTTPClient 既有默认一致
// （Authorization + Content-Type: application/json + codex UA/Originator）；
// 不发 OpenAI-Beta 与 x-codex-turn-metadata（网关不转发，与 resp HTTP 路径
// 现状一致；需要时 WithHeader 注入）。
// turn-state 不捕获（对齐 GenerateImage——doURL 路径不捕获，
// HTTPResponse.TurnState 恒空，网关不消费）。
func (c *HTTPClient) Search(ctx context.Context, payload []byte) (*HTTPResponse, error) {
	searchURL, err := searchEndpointFrom(c.baseURL)
	if err != nil {
		return nil, err
	}
	targetURL, err := buildURL(searchURL, c.opts.query)
	if err != nil {
		return nil, err
	}
	resp, err := c.doURL(ctx, targetURL, payload)
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
	return &HTTPResponse{StatusCode: resp.StatusCode, Raw: body}, nil
}

// searchEndpointFrom 由 responses 完整端点派生 search 端点：末尾 /responses
// 路径段 → /alpha/search（默认 c.baseURL=DefaultResponsesURL → 派生结果即
// DefaultSearchURL；WithBaseURL 覆盖值同样按 responses 端点语义派生）。
// 非 /responses 结尾 → 错误（尾斜杠 / query 形态健壮——不静默产生错误
// URL；实态输入已归一，纯防御性）。包内函数不导出——URL 派生全在 SDK。
func searchEndpointFrom(responsesURL string) (string, error) {
	u, err := url.Parse(responsesURL)
	if err != nil {
		return "", fmt.Errorf("codexsdk: 解析 responses 端点 %q 失败: %w", responsesURL, err)
	}
	path := u.Path
	i := strings.LastIndex(path, "/")
	if i < 0 || path[i+1:] != "responses" {
		return "", fmt.Errorf("codexsdk: search 端点派生失败：%q 尾段非 /responses", responsesURL)
	}
	u.Path = path[:i] + "/alpha/search"
	return u.String(), nil
}
