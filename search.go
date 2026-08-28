package codexsdk

import (
	"context"
	"fmt"
	"io"
	"net/http"
)

// DefaultSearchURL 是官方上游 search 完整端点（固定）。
const DefaultSearchURL = "https://chatgpt.com/backend-api/codex/alpha/search"

// Search 非流式搜索（codex 凭据直连 alpha/search 端点）：POST
// DefaultSearchURL，请求/响应体 opaque（SDK 零解析——alpha 端点实验性，
// 上游变更网关免疫；HTTPResponse.Raw 原样交付）。
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
	resp, err := c.doURL(ctx, DefaultSearchURL, http.MethodPost, payload)
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
