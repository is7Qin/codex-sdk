package codexsdk

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"
)

// DefaultImagesURL 是官方上游 images generations 完整端点（固定）。
const DefaultImagesURL = "https://chatgpt.com/backend-api/codex/images/generations"

// DefaultImagesEditsURL 是官方上游 images edits 完整端点（固定）。
const DefaultImagesEditsURL = "https://chatgpt.com/backend-api/codex/images/edits"

// maxEditImages 是单次 edits 输入图上限（codex-rs MAX_EDIT_IMAGES=5，
// ext/image-generation/src/tool.rs:58）。
const maxEditImages = 5

// ImageGenParams 生图参数（generations/edits 共参；网关从 HTTP 请求
// JSON body / multipart form 解析后传入——SDK 不做 HTTP 协议解析）。
// 参数集 = codex-rs 实证收敛（prompt/model/size/quality/background/n）；
// moderation/output_format/output_compression/partial_images/style 零实证——不映射。
type ImageGenParams struct {
	Model      string  // 生图模型（gpt-image-2 等；必填）
	Prompt     string  // 必填
	N          *int    // nil = 不发 n 字段（上游默认 1；codex 客户端恒 None，SDK 按需透传）
	Size       *string // nil = 不发 size 字段（上游默认 "auto"）
	Quality    *string // nil = 不发 quality 字段（上游默认 "auto"）；枚举 low|medium|high|auto
	Background *string // nil = 不发 background 字段（上游默认 "auto"）；枚举 transparent|opaque|auto
	// edits 专属：
	Images []ImageRef // 输入图片（≤5，codex-rs MAX_EDIT_IMAGES）；generations 恒空
}

// ImageRef 是 edits 单张输入图（两者取一；generations 不使用）。
type ImageRef struct {
	ImageURL *string // 完整 URL 或 base64 data URL（优先于 Raw）
	Raw      []byte  // 原始文件字节 → SDK 内部转 data URL（into_data_url 同款）
}

// ImageResponse 是标准 ImagesResponse（对齐上游 images 端点响应）——
// 网关直接据此计费（data 长 = 张数、usage 提取 image_tokens）与序列化转发。
// 与 API-key 直连响应同一口径（网关统一计费逻辑）。
type ImageResponse struct {
	Created      int64       `json:"created"`
	Background   *string     `json:"background"`
	Data         []Image     `json:"data"`
	OutputFormat *string     `json:"output_format"`
	Quality      *string     `json:"quality"`
	Size         *string     `json:"size"`
	Usage        *ImageUsage `json:"usage"` // 上游未提供 → nil（网关 per-image 分量兜底）
}

// Image 是单张生成结果（实证：b64_json 为原始 PNG base64，无 data URL 前缀）。
type Image struct {
	B64JSON *string `json:"b64_json"`
}

// ImageUsage 是生图 token 用量（由上游嵌套 input/output_tokens_details.
// image_tokens 提取为平铺四字段，与网关 API-key 直连同一计费口径）。
type ImageUsage struct {
	InputTokens       int64 `json:"input_tokens"`
	InputImageTokens  int64 `json:"input_image_tokens"` // input_tokens_details.image_tokens
	OutputTokens      int64 `json:"output_tokens"`
	OutputImageTokens int64 `json:"output_image_tokens"` // output_tokens_details.image_tokens
}

// ImageStreamEvent 事件类型常量（GenerateImageStream 合成事件）。
const (
	// ImageStreamEventCompleted 对齐上游 image_generation_call 会话 item 的
	// completed 终态（每张图一个）。
	ImageStreamEventCompleted = "image_generation.completed"
	// ImageStreamEventKeepalive 保活事件（等待期间每 60s 一个；B64JSON/Usage
	// 恒 nil）——网关收到首个事件即发 SSE 响应头，keepalive 保证 120s 响应头
	// 超时门槛内必有字节流（CF 524 免疫）。
	ImageStreamEventKeepalive = "keepalive"
)

// ImageStreamEvent 是 GenerateImageStream 的合成流式事件（用户裁决：codex
// 专属合成归 SDK，网关统一透传）。completed：每张图一个（带 b64_json；
// usage 仅最后一个事件携带——对齐上游 completed 事件语义）；keepalive：
// 等待期间保活（B64JSON/Usage 恒 nil）。partial_image 不合成——无 wire 来源。
type ImageStreamEvent struct {
	Type    string      // "image_generation.completed" | "keepalive"
	B64JSON *string     // completed：原始 PNG base64（无 data URL 前缀）；keepalive 恒 nil
	Usage   *ImageUsage // 仅最后一个 completed 事件携带；keepalive 恒 nil
}

// keepaliveInterval 是 GenerateImageStream 等待期间 keepalive 事件间隔
// （默认 60s；测试可改小加速）。
var keepaliveInterval = 60 * time.Second

// GenerateImage 非流式生图（codex 凭据直连 images 端点）：POST
// DefaultImagesURL 或 DefaultImagesEditsURL（JSON 非流式——上游无流式路径，
// 流式语义见 GenerateImageStream——合成 completed 事件包装本方法）。
// edits 由 Images 非空判定；输入图 Raw 字节经 data URL 直嵌
// （MIME 魔数检测 PNG/JPEG，默认 image/png——codex-rs 恒 PNG 先例）。
//
// 端点：固定官方端点（DefaultImagesURL / DefaultImagesEditsURL）。
//
// 传输层复用 Do：鉴权头注入 / 懒构建 / 401 判死分类 + 单飞 refresh + 自动
// 重试一次 / fatal 类错误透传（不被 HTTPError 吞掉，errors.As 可区分）/
// 非 2xx 返回 *HTTPError 原样交付（403 = 账号无生图权限，网关透传映射）。
// 请求头与 HTTPClient 既有默认一致（Authorization + Content-Type:
// application/json + codex UA/Originator）；不发 OpenAI-Beta 与
// x-codex-image-turn-id（实证不需要，不影响功能；需要时 WithHeader 注入）。
// turn-state 不捕获（doURL 路径无捕获调用——对齐 Search 方法语义，
// 响应头 turn-state 不读取；网关不消费）。
func (c *HTTPClient) GenerateImage(ctx context.Context, p *ImageGenParams) (*ImageResponse, error) {
	payload, err := buildImageRequest(p)
	if err != nil {
		return nil, err
	}
	targetURL := DefaultImagesURL
	if len(p.Images) > 0 {
		targetURL = DefaultImagesEditsURL
	}
	resp, err := c.doURL(ctx, targetURL, http.MethodPost, payload)
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
	img, err := parseImageResponse(body)
	if err != nil {
		return nil, fmt.Errorf("codexsdk: 解析生图响应失败: %w", err)
	}
	return img, nil
}

// GenerateImageStream 流式语义（合成——上游 images 端点无流式路径）：内部调
// 非流式 GenerateImage，等待期间（请求发出后、响应返回前）每 60s 回调一次
// keepalive 事件（网关收到首个事件即发 SSE 响应头——keepalive 保证 120s
// 响应头超时门槛内必有字节流，CF 524 免疫）；响应返回后停 ticker，为每张图
// 合成一个 "image_generation.completed" 事件回调（B64JSON 各自；Usage 仅
// 最后一个事件携带——对齐上游 completed 事件语义）→ 结束（发完即止，
// 无会话维持）。
//
// 回调式对齐既有 Stream 风格：fn 返回错误立即终止并透传该错误（keepalive
// 回调错误取消在途请求且优先返回）。错误路径同 GenerateImage：生成失败 →
// completed 回调不调用、错误原样透传（HTTPError / 鉴权 fatal 五类 /
// refresh 失败均不包装）。Data 为空 → 无 completed 事件直接返回。无
// partial_image 合成（无 wire 来源）。
func (c *HTTPClient) GenerateImageStream(ctx context.Context, p *ImageGenParams, fn func(ImageStreamEvent) error) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// 保活 goroutine：GenerateImage 等待期间每 keepaliveInterval 回调一次
	// keepalive 事件；回调错误 → 取消在途请求并优先返回该错误。
	keepaliveErr := make(chan error, 1)
	done := make(chan struct{})
	go func() {
		defer close(done)
		t := time.NewTicker(keepaliveInterval)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				if err := fn(ImageStreamEvent{Type: ImageStreamEventKeepalive}); err != nil {
					keepaliveErr <- err
					cancel()
					return
				}
			}
		}
	}()

	resp, err := c.GenerateImage(ctx, p)
	cancel()
	<-done // 等保活 goroutine 退出（避免与 completed 事件并发回调）
	select {
	case ke := <-keepaliveErr:
		return ke // keepalive 回调错误优先（调用方 sentinel）
	default:
	}
	if err != nil {
		return err
	}
	for i := range resp.Data {
		ev := ImageStreamEvent{Type: ImageStreamEventCompleted, B64JSON: resp.Data[i].B64JSON}
		if i == len(resp.Data)-1 {
			ev.Usage = resp.Usage
		}
		if err := fn(ev); err != nil {
			return err
		}
	}
	return nil
}

// imageGenerationRequest 是上游 images 端点 JSON 请求体（对齐 codex-rs
// ImageGenerationRequest：prompt/background?/model/n?/quality?/size?，
// codex-api/src/images.rs:4-16；edits 追加 images:[{image_url}]——
// data URL 直嵌，非 multipart/File API）。
type imageGenerationRequest struct {
	Prompt     string          `json:"prompt"`
	Background *string         `json:"background,omitempty"`
	Model      string          `json:"model"`
	N          *int            `json:"n,omitempty"`
	Quality    *string         `json:"quality,omitempty"`
	Size       *string         `json:"size,omitempty"`
	Images     []imageInputRef `json:"images,omitempty"`
}

type imageInputRef struct {
	ImageURL string `json:"image_url"`
}

// buildImageRequest 校验参数并构造 images 请求体（nil 可选字段不发——
// 上游默认值兜底：n=1 / size/quality/background="auto"）。
func buildImageRequest(p *ImageGenParams) ([]byte, error) {
	if p == nil {
		return nil, errors.New("codexsdk: ImageGenParams 不能为 nil")
	}
	if p.Model == "" {
		return nil, errors.New("codexsdk: ImageGenParams.Model 必填")
	}
	if p.Prompt == "" {
		return nil, errors.New("codexsdk: ImageGenParams.Prompt 必填")
	}
	if len(p.Images) > maxEditImages {
		return nil, fmt.Errorf("codexsdk: 编辑输入图 %d 张超过上限 %d", len(p.Images), maxEditImages)
	}
	req := imageGenerationRequest{
		Prompt:     p.Prompt,
		Background: p.Background,
		Model:      p.Model,
		N:          p.N,
		Quality:    p.Quality,
		Size:       p.Size,
	}
	for i := range p.Images {
		imageURL, err := imageRefURL(&p.Images[i])
		if err != nil {
			return nil, fmt.Errorf("codexsdk: Images[%d]: %w", i, err)
		}
		req.Images = append(req.Images, imageInputRef{ImageURL: imageURL})
	}
	return json.Marshal(req)
}

// imageRefURL 把 ImageRef 转为 wire 形态：ImageURL 优先直用；否则 Raw 字节
// → data URL（MIME 魔数检测 PNG/JPEG，默认 image/png）。
func imageRefURL(ref *ImageRef) (string, error) {
	if ref.ImageURL != nil && *ref.ImageURL != "" {
		return *ref.ImageURL, nil
	}
	if len(ref.Raw) > 0 {
		return "data:" + detectImageMIME(ref.Raw) + ";base64," + base64.StdEncoding.EncodeToString(ref.Raw), nil
	}
	return "", errors.New("ImageRef 需提供 ImageURL 或 Raw")
}

// detectImageMIME 魔数检测图片 MIME（PNG/JPEG；未知默认 image/png——
// codex-rs 恒 PNG 先例）。
func detectImageMIME(raw []byte) string {
	if len(raw) >= 8 && raw[0] == 0x89 && raw[1] == 'P' && raw[2] == 'N' && raw[3] == 'G' &&
		raw[4] == 0x0D && raw[5] == 0x0A && raw[6] == 0x1A && raw[7] == 0x0A {
		return "image/png"
	}
	if len(raw) >= 3 && raw[0] == 0xFF && raw[1] == 0xD8 && raw[2] == 0xFF {
		return "image/jpeg"
	}
	return "image/png"
}

// imageResponseWire 是上游 ImagesResponse 的 wire 形态（usage 为嵌套
// input/output_tokens_details.image_tokens，endpoint/images.rs 测试 148-171）。
type imageResponseWire struct {
	Created      int64           `json:"created"`
	Background   *string         `json:"background"`
	Data         []imageDataWire `json:"data"`
	OutputFormat *string         `json:"output_format"`
	Quality      *string         `json:"quality"`
	Size         *string         `json:"size"`
	Usage        *imageUsageWire `json:"usage"`
}

type imageDataWire struct {
	B64JSON *string `json:"b64_json"`
}

type imageUsageWire struct {
	InputTokens   int64            `json:"input_tokens"`
	OutputTokens  int64            `json:"output_tokens"`
	InputDetails  *imageTokensWire `json:"input_tokens_details"`
	OutputDetails *imageTokensWire `json:"output_tokens_details"`
}

type imageTokensWire struct {
	ImageTokens int64 `json:"image_tokens"`
}

// parseImageResponse 解析上游 JSON 响应（未知字段忽略；缺 usage → nil——
// 网关 per-image 分量兜底）。
func parseImageResponse(body []byte) (*ImageResponse, error) {
	var w imageResponseWire
	if err := json.Unmarshal(body, &w); err != nil {
		return nil, err
	}
	out := &ImageResponse{
		Created:      w.Created,
		Background:   w.Background,
		OutputFormat: w.OutputFormat,
		Quality:      w.Quality,
		Size:         w.Size,
	}
	for _, d := range w.Data {
		out.Data = append(out.Data, Image{B64JSON: d.B64JSON})
	}
	if w.Usage != nil {
		out.Usage = &ImageUsage{
			InputTokens:  w.Usage.InputTokens,
			OutputTokens: w.Usage.OutputTokens,
		}
		if w.Usage.InputDetails != nil {
			out.Usage.InputImageTokens = w.Usage.InputDetails.ImageTokens
		}
		if w.Usage.OutputDetails != nil {
			out.Usage.OutputImageTokens = w.Usage.OutputDetails.ImageTokens
		}
	}
	return out, nil
}
