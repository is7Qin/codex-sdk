package codexsdk

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

func strPtr(s string) *string { return &s }
func intPtr(n int) *int       { return &n }

// imageResponseBody 是上游完整 ImagesResponse 样本（usage 含嵌套
// input/output_tokens_details.image_tokens，对齐 endpoint/images.rs 测试形态）。
const imageResponseBody = `{"created":1755000000,` +
	`"data":[{"b64_json":"iVBORw0KGgoAAAANSUhEUg=="}],` +
	`"background":"auto","quality":"auto","size":"1024x1024","output_format":"png",` +
	`"usage":{"input_tokens":100,"input_tokens_details":{"image_tokens":80,"text_tokens":20},` +
	`"output_tokens":200,"output_tokens_details":{"image_tokens":150,"text_tokens":50},"total_tokens":300}}`

// ---- 请求构造单测 ----

// TestBuildImageRequestGenerations：generations 形态——model/prompt 必填、
// nil 可选字段不发（上游默认值兜底：n=1 / size/quality/background="auto"）。
func TestBuildImageRequestGenerations(t *testing.T) {
	body, err := buildImageRequest(&ImageGenParams{Model: "gpt-image-2", Prompt: "a red fox"})
	if err != nil {
		t.Fatalf("buildImageRequest: %v", err)
	}
	if string(body) != `{"prompt":"a red fox","model":"gpt-image-2"}` {
		t.Fatalf("body = %s（nil 可选字段应不发）", body)
	}

	// 全字段
	n := 2
	body, err = buildImageRequest(&ImageGenParams{
		Model:      "gpt-image-2",
		Prompt:     "a red fox",
		N:          &n,
		Size:       strPtr("1024x1024"),
		Quality:    strPtr("high"),
		Background: strPtr("transparent"),
	})
	if err != nil {
		t.Fatalf("buildImageRequest: %v", err)
	}
	var got map[string]any
	if err := json.Unmarshal(body, &got); err != nil {
		t.Fatalf("body 非法 JSON: %v (%s)", err, body)
	}
	if got["prompt"] != "a red fox" || got["model"] != "gpt-image-2" {
		t.Fatalf("prompt/model = %v/%v", got["prompt"], got["model"])
	}
	if got["n"] != float64(2) || got["size"] != "1024x1024" || got["quality"] != "high" || got["background"] != "transparent" {
		t.Fatalf("全字段 = %v", got)
	}
	if _, ok := got["images"]; ok {
		t.Fatalf("generations 不应含 images 字段: %v", got)
	}
}

// TestBuildImageRequestEdits：edits 形态——images:[{image_url}] 直嵌；
// ImageURL 优先于 Raw。
func TestBuildImageRequestEdits(t *testing.T) {
	body, err := buildImageRequest(&ImageGenParams{
		Model:  "gpt-image-1.5",
		Prompt: "add a red hat",
		Images: []ImageRef{{ImageURL: strPtr("https://cdn.example.com/a.png")}},
	})
	if err != nil {
		t.Fatalf("buildImageRequest: %v", err)
	}
	if string(body) != `{"prompt":"add a red hat","model":"gpt-image-1.5","images":[{"image_url":"https://cdn.example.com/a.png"}]}` {
		t.Fatalf("body = %s", body)
	}
}

// TestBuildImageRequestRawDataURL：Raw 字节 → data URL 直嵌——PNG/JPEG 魔数
// 检测、未知默认 image/png（codex-rs 恒 PNG 先例）。
func TestBuildImageRequestRawDataURL(t *testing.T) {
	png := []byte{0x89, 'P', 'N', 'G', 0x0D, 0x0A, 0x1A, 0x0A, 1, 2, 3}
	body, err := buildImageRequest(&ImageGenParams{
		Model:  "gpt-image-1.5",
		Prompt: "edit",
		Images: []ImageRef{{Raw: png}},
	})
	if err != nil {
		t.Fatalf("buildImageRequest: %v", err)
	}
	want := `{"prompt":"edit","model":"gpt-image-1.5","images":[{"image_url":"data:image/png;base64,` +
		base64.StdEncoding.EncodeToString(png) + `"}]}`
	if string(body) != want {
		t.Fatalf("PNG Raw body = %s, 期望 %s", body, want)
	}

	jpeg := []byte{0xFF, 0xD8, 0xFF, 0xE0, 9, 9}
	body, err = buildImageRequest(&ImageGenParams{
		Model:  "gpt-image-1.5",
		Prompt: "edit",
		Images: []ImageRef{{Raw: jpeg}},
	})
	if err != nil {
		t.Fatalf("buildImageRequest: %v", err)
	}
	want = `{"prompt":"edit","model":"gpt-image-1.5","images":[{"image_url":"data:image/jpeg;base64,` +
		base64.StdEncoding.EncodeToString(jpeg) + `"}]}`
	if string(body) != want {
		t.Fatalf("JPEG Raw body = %s, 期望 %s", body, want)
	}

	unknown := []byte{1, 2, 3}
	body, err = buildImageRequest(&ImageGenParams{
		Model:  "gpt-image-1.5",
		Prompt: "edit",
		Images: []ImageRef{{Raw: unknown}},
	})
	if err != nil {
		t.Fatalf("buildImageRequest: %v", err)
	}
	want = `{"prompt":"edit","model":"gpt-image-1.5","images":[{"image_url":"data:image/png;base64,` +
		base64.StdEncoding.EncodeToString(unknown) + `"}]}`
	if string(body) != want {
		t.Fatalf("未知类型默认 image/png: body = %s, 期望 %s", body, want)
	}
}

// TestBuildImageRequestValidation：参数校验——nil / 缺 model / 缺 prompt /
// 超 5 张 / 空引用。
func TestBuildImageRequestValidation(t *testing.T) {
	if _, err := buildImageRequest(nil); err == nil {
		t.Fatal("nil params 应报错")
	}
	if _, err := buildImageRequest(&ImageGenParams{Prompt: "x"}); err == nil {
		t.Fatal("缺 Model 应报错")
	}
	if _, err := buildImageRequest(&ImageGenParams{Model: "gpt-image-2"}); err == nil {
		t.Fatal("缺 Prompt 应报错")
	}
	refs := make([]ImageRef, 6)
	for i := range refs {
		refs[i] = ImageRef{ImageURL: strPtr("https://x/1.png")}
	}
	if _, err := buildImageRequest(&ImageGenParams{Model: "m", Prompt: "p", Images: refs}); err == nil {
		t.Fatal("超过 5 张输入图应报错")
	}
	if _, err := buildImageRequest(&ImageGenParams{Model: "m", Prompt: "p", Images: []ImageRef{{}}}); err == nil {
		t.Fatal("空引用（无 ImageURL 无 Raw）应报错")
	}
}

// ---- 响应解析单测 ----

// TestParseImageResponse：上游 JSON → ImageResponse——created/data 数组/
// background/quality/size/output_format + usage image_tokens 提取（嵌套 →
// 平铺四字段）。
func TestParseImageResponse(t *testing.T) {
	img, err := parseImageResponse([]byte(imageResponseBody))
	if err != nil {
		t.Fatalf("parseImageResponse: %v", err)
	}
	if img.Created != 1755000000 {
		t.Fatalf("Created = %d", img.Created)
	}
	if len(img.Data) != 1 {
		t.Fatalf("Data 长度 = %d, 期望 1", len(img.Data))
	}
	if img.Data[0].B64JSON == nil || *img.Data[0].B64JSON != "iVBORw0KGgoAAAANSUhEUg==" {
		t.Fatalf("B64JSON = %v", img.Data[0].B64JSON)
	}
	if img.Background == nil || *img.Background != "auto" {
		t.Fatalf("Background = %v", img.Background)
	}
	if img.OutputFormat == nil || *img.OutputFormat != "png" {
		t.Fatalf("OutputFormat = %v", img.OutputFormat)
	}
	if img.Quality == nil || *img.Quality != "auto" {
		t.Fatalf("Quality = %v", img.Quality)
	}
	if img.Size == nil || *img.Size != "1024x1024" {
		t.Fatalf("Size = %v", img.Size)
	}
	if img.Usage == nil {
		t.Fatal("Usage 应为非 nil")
	}
	if img.Usage.InputTokens != 100 || img.Usage.InputImageTokens != 80 ||
		img.Usage.OutputTokens != 200 || img.Usage.OutputImageTokens != 150 {
		t.Fatalf("Usage = %+v（期望 100/80/200/150——image_tokens 提取）", img.Usage)
	}
}

// TestParseImageResponseMissingUsage：缺 usage → nil；data 缺失 → nil 数组。
func TestParseImageResponseMissingUsage(t *testing.T) {
	img, err := parseImageResponse([]byte(`{"created":1,"data":[{"b64_json":"AAA="}]}`))
	if err != nil {
		t.Fatalf("parseImageResponse: %v", err)
	}
	if img.Usage != nil {
		t.Fatalf("缺 usage 应兜底 nil, got %+v", img.Usage)
	}
	if len(img.Data) != 1 {
		t.Fatalf("Data = %+v", img.Data)
	}
	img, err = parseImageResponse([]byte(`{"created":1}`))
	if err != nil {
		t.Fatalf("parseImageResponse: %v", err)
	}
	if img.Data != nil {
		t.Fatalf("缺 data 应为 nil, got %+v", img.Data)
	}

	// 非法 JSON
	if _, err := parseImageResponse([]byte(`{`)); err == nil {
		t.Fatal("非法 JSON 应报错")
	}
}

// ---- 端点派生单测 ----

// TestImagesEndpoint：无覆盖 = DefaultImagesURL 同源派生；WithBaseURL 覆盖值
// = 完整 generations 端点直用；edits = 尾段 /images/generations → /images/edits。
func TestImagesEndpoint(t *testing.T) {
	hc := NewHTTPClient(PAT("t"))
	gen, err := hc.imagesEndpoint(false)
	if err != nil || gen != DefaultImagesURL {
		t.Fatalf("默认 generations = %q, %v", gen, err)
	}
	ed, err := hc.imagesEndpoint(true)
	if err != nil || ed != "https://chatgpt.com/backend-api/codex/images/edits" {
		t.Fatalf("默认 edits 派生 = %q, %v", ed, err)
	}

	hc2 := NewHTTPClient(PAT("t"), WithBaseURL("https://selfhost/v1/images/generations"))
	gen, err = hc2.imagesEndpoint(false)
	if err != nil || gen != "https://selfhost/v1/images/generations" {
		t.Fatalf("覆盖 generations 直用 = %q, %v", gen, err)
	}
	ed, err = hc2.imagesEndpoint(true)
	if err != nil || ed != "https://selfhost/v1/images/edits" {
		t.Fatalf("覆盖 edits 派生 = %q, %v", ed, err)
	}

	// 覆盖值尾段非 /generations：edits 派生报错（不静默打错误 URL）
	hc3 := NewHTTPClient(PAT("t"), WithBaseURL("https://selfhost/v1/responses"))
	if _, err := hc3.imagesEndpoint(true); err == nil {
		t.Fatal("尾段非 /generations 的 edits 派生应报错")
	}
}

// ---- 集成（httptest mock 上游）----

// TestGenerateImageGenerations：全链路——WithBaseURL 完整端点直用（路径
// /images/generations）、鉴权头注入、默认头（Content-Type/UA/Originator、
// 不发 OpenAI-Beta 与 x-codex-image-turn-id）、请求体、响应解析。
func TestGenerateImageGenerations(t *testing.T) {
	var gotAuth, gotBeta, gotTurnID, gotContentType, gotUA, gotOriginator string
	var gotBody []byte
	var gotPath string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		gotAuth = r.Header.Get("Authorization")
		gotBeta = r.Header.Get("OpenAI-Beta")
		gotTurnID = r.Header.Get("x-codex-image-turn-id")
		gotContentType = r.Header.Get("Content-Type")
		gotUA = r.Header.Get("User-Agent")
		gotOriginator = r.Header.Get("Originator")
		gotBody, _ = io.ReadAll(r.Body)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(imageResponseBody))
	}))
	t.Cleanup(srv.Close)

	hc := NewHTTPClient(PAT("pat-img"), WithBaseURL(srv.URL+"/images/generations"))
	n := 1
	resp, err := hc.GenerateImage(context.Background(), &ImageGenParams{
		Model:  "gpt-image-2",
		Prompt: "a red fox",
		N:      &n,
		Size:   strPtr("1024x1024"),
	})
	if err != nil {
		t.Fatalf("GenerateImage: %v", err)
	}
	if gotPath != "/images/generations" {
		t.Fatalf("路径 = %q（覆盖值按完整 generations 端点直用）", gotPath)
	}
	if gotAuth != "Bearer pat-img" {
		t.Fatalf("Authorization = %q", gotAuth)
	}
	if gotContentType != "application/json" {
		t.Fatalf("Content-Type = %q", gotContentType)
	}
	if gotUA != DefaultCodexUserAgent || gotOriginator != DefaultOriginator {
		t.Fatalf("UA/originator = %q/%q", gotUA, gotOriginator)
	}
	if gotBeta != "" {
		t.Fatalf("默认不应发 OpenAI-Beta, got %q", gotBeta)
	}
	if gotTurnID != "" {
		t.Fatalf("默认不应发 x-codex-image-turn-id, got %q", gotTurnID)
	}
	if string(gotBody) != `{"prompt":"a red fox","model":"gpt-image-2","n":1,"size":"1024x1024"}` {
		t.Fatalf("请求体 = %s", gotBody)
	}
	if resp.Created != 1755000000 || resp.Usage == nil || resp.Usage.InputImageTokens != 80 {
		t.Fatalf("响应解析失败: %+v", resp)
	}
}

// TestGenerateImageEdits：edits 端点派生（/images/edits）+ Raw 字节 →
// data URL 直嵌。
func TestGenerateImageEdits(t *testing.T) {
	png := []byte{0x89, 'P', 'N', 'G', 0x0D, 0x0A, 0x1A, 0x0A, 7, 7, 7}
	var gotPath string
	var gotBody []byte
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		gotBody, _ = io.ReadAll(r.Body)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"created":1,"data":[{"b64_json":"AAA="}]}`))
	}))
	t.Cleanup(srv.Close)

	hc := NewHTTPClient(PAT("pat-img"), WithBaseURL(srv.URL+"/images/generations"))
	resp, err := hc.GenerateImage(context.Background(), &ImageGenParams{
		Model:  "gpt-image-1.5",
		Prompt: "add a red hat",
		Images: []ImageRef{{Raw: png}},
	})
	if err != nil {
		t.Fatalf("GenerateImage: %v", err)
	}
	if gotPath != "/images/edits" {
		t.Fatalf("edits 路径 = %q（尾段 /images/generations → /images/edits）", gotPath)
	}
	wantBody := `{"prompt":"add a red hat","model":"gpt-image-1.5","images":[{"image_url":"data:image/png;base64,` +
		base64.StdEncoding.EncodeToString(png) + `"}]}`
	if string(gotBody) != wantBody {
		t.Fatalf("请求体 = %s, 期望 %s", gotBody, wantBody)
	}
	if resp.Data[0].B64JSON == nil || *resp.Data[0].B64JSON != "AAA=" {
		t.Fatalf("响应解析失败: %+v", resp)
	}
}

// TestGenerateImage401Rotate：401 自动轮转经 GenerateImage 全链路生效——
// 非判死 401 → 单飞 refresh → 自动重试一次成功（复用 Do 传输层）。
func TestGenerateImage401Rotate(t *testing.T) {
	m := newMockRefresh(t, refreshStep{status: 200, body: `{"access_token":"at-new","refresh_token":"rt-new"}`})
	var mu sync.Mutex
	var auths []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		auths = append(auths, r.Header.Get("Authorization"))
		mu.Unlock()
		if r.Header.Get("Authorization") == "Bearer at-old" {
			w.WriteHeader(http.StatusUnauthorized)
			_, _ = w.Write([]byte(`{"error":{"type":"invalid_request_error"}}`))
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(imageResponseBody))
	}))
	t.Cleanup(srv.Close)

	auth := OAuthWithRotation("rt-0", WithInitialAccessToken("at-old"))
	hc := NewHTTPClient(auth, WithBaseURL(srv.URL+"/images/generations"))
	resp, err := hc.GenerateImage(context.Background(), &ImageGenParams{Model: "gpt-image-2", Prompt: "hi"})
	if err != nil {
		t.Fatalf("GenerateImage: %v", err)
	}
	if resp.Created != 1755000000 {
		t.Fatalf("轮转后应成功解析, Created = %d", resp.Created)
	}
	if m.callCount() != 1 {
		t.Fatalf("refresh 次数 = %d, 期望 1", m.callCount())
	}
	mu.Lock()
	defer mu.Unlock()
	if len(auths) != 2 || auths[0] != "Bearer at-old" || auths[1] != "Bearer at-new" {
		t.Fatalf("请求序列 = %v, 期望 [Bearer at-old, Bearer at-new]", auths)
	}
}

// TestGenerateImage401Fatal：401 判死码 → *AuthPermanentlyRevokedError 透传
// （不重试，Fatal 态后不再发请求）。
func TestGenerateImage401Fatal(t *testing.T) {
	var reqs int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reqs++
		w.WriteHeader(http.StatusUnauthorized)
		_, _ = w.Write([]byte(`{"error":{"code":"token_revoked"}}`))
	}))
	t.Cleanup(srv.Close)

	auth := OAuthWithRotation("rt-0", WithInitialAccessToken("at-old"))
	hc := NewHTTPClient(auth, WithBaseURL(srv.URL+"/images/generations"))
	_, err := hc.GenerateImage(context.Background(), &ImageGenParams{Model: "m", Prompt: "p"})
	var are *AuthPermanentlyRevokedError
	if !errors.As(err, &are) {
		t.Fatalf("期望 *AuthPermanentlyRevokedError 透传, got %T: %v", err, err)
	}
	if are.Code != "token_revoked" {
		t.Fatalf("Code = %q, 期望 token_revoked", are.Code)
	}
	if reqs != 1 {
		t.Fatalf("判死不重试, 请求数 = %d", reqs)
	}
	// Fatal 态：后续 GenerateImage 不再发请求（Authorization 直接报错）
	_, err = hc.GenerateImage(context.Background(), &ImageGenParams{Model: "m", Prompt: "p"})
	if !errors.As(err, &are) {
		t.Fatalf("Fatal 态后应恒报错, got %T: %v", err, err)
	}
	if reqs != 1 {
		t.Fatalf("Fatal 态后不应发请求, reqs = %d", reqs)
	}
}

// TestGenerateImageRefreshFatalPassthrough：401 → refresh 判死
// （invalid_grant）→ *RefreshOAuthError 透传（不被 HTTPError 吞掉）。
func TestGenerateImageRefreshFatalPassthrough(t *testing.T) {
	m := newMockRefresh(t, refreshStep{status: 400, body: `{"error":{"code":"invalid_grant"}}`})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
		_, _ = w.Write([]byte(`{"error":{"type":"invalid_request_error"}}`))
	}))
	t.Cleanup(srv.Close)

	auth := OAuthWithRotation("rt-0", WithInitialAccessToken("at-old"))
	hc := NewHTTPClient(auth, WithBaseURL(srv.URL+"/images/generations"))
	_, err := hc.GenerateImage(context.Background(), &ImageGenParams{Model: "m", Prompt: "p"})
	var re *RefreshOAuthError
	if !errors.As(err, &re) {
		t.Fatalf("refresh 判死应透传, got %T: %v", err, err)
	}
	if re.Code != "invalid_grant" {
		t.Fatalf("Code = %q, 期望 invalid_grant", re.Code)
	}
	if m.callCount() != 1 {
		t.Fatalf("refresh 次数 = %d, 期望 1（判死不重试）", m.callCount())
	}
}

// TestGenerateImage403Passthrough：403（账号无生图权限）→ *HTTPError 原样
// 交付（网关透传映射）。
func TestGenerateImage403Passthrough(t *testing.T) {
	errorBody := []byte(`{"detail":"Forbidden"}`)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusForbidden)
		_, _ = w.Write([]byte(errorBody))
	}))
	t.Cleanup(srv.Close)

	hc := NewHTTPClient(PAT("p"), WithBaseURL(srv.URL+"/images/generations"))
	_, err := hc.GenerateImage(context.Background(), &ImageGenParams{Model: "m", Prompt: "p"})
	var he *HTTPError
	if !errors.As(err, &he) {
		t.Fatalf("期望 *HTTPError, got %T: %v", err, err)
	}
	if he.StatusCode != http.StatusForbidden {
		t.Fatalf("StatusCode = %d, 期望 403", he.StatusCode)
	}
	if !bytes.Equal(he.Raw, errorBody) {
		t.Fatalf("Raw 应原样交付错误体: %s", he.Raw)
	}
}

// ---- 流式合成单测（GenerateImageStream）----

// imageStreamBody 是双图上游响应样本（usage 嵌套 details）。
const imageStreamBody = `{"created":1,` +
	`"data":[{"b64_json":"AAA="},{"b64_json":"BBB="}],` +
	`"usage":{"input_tokens":10,"input_tokens_details":{"image_tokens":8,"text_tokens":2},` +
	`"output_tokens":20,"output_tokens_details":{"image_tokens":15,"text_tokens":5},"total_tokens":30}}`

// startImageMock 起 images 端点 mock：固定响应体返回。
func startImageMock(t *testing.T, body string) string {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(body))
	}))
	t.Cleanup(srv.Close)
	return srv.URL + "/images/generations"
}

// TestGenerateImageStreamSuccess：非流式生成成功 → 每张图一个 completed 事件
// （B64JSON 各自）+ usage 仅最后一个事件携带。
func TestGenerateImageStreamSuccess(t *testing.T) {
	hc := NewHTTPClient(PAT("p"), WithBaseURL(startImageMock(t, imageStreamBody)))
	var events []ImageStreamEvent
	err := hc.GenerateImageStream(context.Background(), &ImageGenParams{Model: "m", Prompt: "p"},
		func(ev ImageStreamEvent) error {
			events = append(events, ev)
			return nil
		})
	if err != nil {
		t.Fatalf("GenerateImageStream: %v", err)
	}
	if len(events) != 2 {
		t.Fatalf("事件数 = %d, 期望 2（每图一个）", len(events))
	}
	if events[0].Type != ImageStreamEventCompleted {
		t.Fatalf("Type = %q, 期望 %q", events[0].Type, ImageStreamEventCompleted)
	}
	if events[0].B64JSON == nil || *events[0].B64JSON != "AAA=" {
		t.Fatalf("事件0 B64JSON = %v, 期望 AAA=", events[0].B64JSON)
	}
	if events[0].Usage != nil {
		t.Fatalf("usage 不应出现在非最后一个事件, got %+v", events[0].Usage)
	}
	if events[1].B64JSON == nil || *events[1].B64JSON != "BBB=" {
		t.Fatalf("事件1 B64JSON = %v, 期望 BBB=", events[1].B64JSON)
	}
	if events[1].Usage == nil {
		t.Fatal("usage 应仅最后一个事件携带")
	}
	if events[1].Usage.InputTokens != 10 || events[1].Usage.InputImageTokens != 8 ||
		events[1].Usage.OutputTokens != 20 || events[1].Usage.OutputImageTokens != 15 {
		t.Fatalf("Usage = %+v（期望 10/8/20/15——image_tokens 提取）", events[1].Usage)
	}
}

// TestGenerateImageStreamSingleEvent：单图 + usage → 恰好一个 completed 事件
// （usage 即携带于该事件）——spec 验收：流式合成单事件。
func TestGenerateImageStreamSingleEvent(t *testing.T) {
	hc := NewHTTPClient(PAT("p"), WithBaseURL(startImageMock(t, imageResponseBody)))
	var calls int
	var last ImageStreamEvent
	err := hc.GenerateImageStream(context.Background(), &ImageGenParams{Model: "m", Prompt: "p"},
		func(ev ImageStreamEvent) error {
			calls++
			last = ev
			return nil
		})
	if err != nil {
		t.Fatalf("GenerateImageStream: %v", err)
	}
	if calls != 1 {
		t.Fatalf("事件数 = %d, 期望 1", calls)
	}
	if last.Usage == nil || last.Usage.InputImageTokens != 80 {
		t.Fatalf("单事件应携带 usage, got %+v", last.Usage)
	}
}

// TestGenerateImageStreamErrorNoEvents：上游 403 → 回调不调用 + *HTTPError
// 原样透传。
func TestGenerateImageStreamErrorNoEvents(t *testing.T) {
	errorBody := []byte(`{"detail":"Forbidden"}`)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusForbidden)
		_, _ = w.Write([]byte(errorBody))
	}))
	t.Cleanup(srv.Close)

	hc := NewHTTPClient(PAT("p"), WithBaseURL(srv.URL+"/images/generations"))
	var calls int
	err := hc.GenerateImageStream(context.Background(), &ImageGenParams{Model: "m", Prompt: "p"},
		func(ev ImageStreamEvent) error {
			calls++
			return nil
		})
	var he *HTTPError
	if !errors.As(err, &he) {
		t.Fatalf("期望 *HTTPError 透传, got %T: %v", err, err)
	}
	if he.StatusCode != http.StatusForbidden {
		t.Fatalf("StatusCode = %d, 期望 403", he.StatusCode)
	}
	if calls != 0 {
		t.Fatalf("错误路径回调不应调用, calls = %d", calls)
	}
}

// TestGenerateImageStreamEmptyData：Data 为空 → 不调回调直接返回（无事件）。
func TestGenerateImageStreamEmptyData(t *testing.T) {
	hc := NewHTTPClient(PAT("p"), WithBaseURL(startImageMock(t, `{"created":1}`)))
	var calls int
	err := hc.GenerateImageStream(context.Background(), &ImageGenParams{Model: "m", Prompt: "p"},
		func(ev ImageStreamEvent) error {
			calls++
			return nil
		})
	if err != nil {
		t.Fatalf("Data 空应无错误返回, got %v", err)
	}
	if calls != 0 {
		t.Fatalf("Data 空不应有事件, calls = %d", calls)
	}
}

// TestGenerateImageStreamCallbackError：completed 回调返回错误 → 立即终止并
// 透传（对齐既有 Stream 回调语义——后续事件不再调用）。
func TestGenerateImageStreamCallbackError(t *testing.T) {
	hc := NewHTTPClient(PAT("p"), WithBaseURL(startImageMock(t, imageStreamBody)))
	sentinel := errors.New("stop")
	var calls int
	err := hc.GenerateImageStream(context.Background(), &ImageGenParams{Model: "m", Prompt: "p"},
		func(ev ImageStreamEvent) error {
			calls++
			return sentinel
		})
	if !errors.Is(err, sentinel) {
		t.Fatalf("回调错误应透传, got %v", err)
	}
	if calls != 1 {
		t.Fatalf("回调错误应立即终止, calls = %d, 期望 1", calls)
	}
}

// TestGenerateImageStreamKeepalive：等待期间（mock 上游延迟响应）收到
// keepalive 事件（B64JSON/Usage 恒 nil）；响应返回后停 ticker → 每图一个
// completed 事件 + usage 仅最后一个携带。
func TestGenerateImageStreamKeepalive(t *testing.T) {
	old := keepaliveInterval
	keepaliveInterval = 50 * time.Millisecond
	t.Cleanup(func() { keepaliveInterval = old })

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(150 * time.Millisecond) // 延迟响应：等待期 > 2 个 keepalive 周期
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(imageStreamBody))
	}))
	t.Cleanup(srv.Close)

	hc := NewHTTPClient(PAT("p"), WithBaseURL(srv.URL+"/images/generations"))
	var keepalives int
	var keepaliveNilViolations int
	var events []ImageStreamEvent
	err := hc.GenerateImageStream(context.Background(), &ImageGenParams{Model: "m", Prompt: "p"},
		func(ev ImageStreamEvent) error {
			if ev.Type == ImageStreamEventKeepalive {
				keepalives++
				if ev.B64JSON != nil || ev.Usage != nil {
					keepaliveNilViolations++
				}
				return nil
			}
			events = append(events, ev)
			return nil
		})
	if err != nil {
		t.Fatalf("GenerateImageStream: %v", err)
	}
	if keepalives < 1 {
		t.Fatalf("等待期间应收到 keepalive 事件, got %d", keepalives)
	}
	if keepaliveNilViolations != 0 {
		t.Fatalf("keepalive 事件 B64JSON/Usage 应恒 nil, 违规 %d 次", keepaliveNilViolations)
	}
	if len(events) != 2 {
		t.Fatalf("completed 事件数 = %d, 期望 2（每图一个）", len(events))
	}
	if events[1].Usage == nil || events[0].Usage != nil {
		t.Fatalf("usage 应仅最后一个 completed 事件携带, 0=%+v 1=%+v", events[0].Usage, events[1].Usage)
	}
}

// TestGenerateImageStreamKeepaliveCallbackError：keepalive 回调错误 → 取消
// 在途请求 + 回调错误优先返回（completed 回调不调用）。
func TestGenerateImageStreamKeepaliveCallbackError(t *testing.T) {
	old := keepaliveInterval
	keepaliveInterval = 30 * time.Millisecond
	t.Cleanup(func() { keepaliveInterval = old })

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(300 * time.Millisecond) // 延迟足够长：keepalive 先触发
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(imageStreamBody))
	}))
	t.Cleanup(srv.Close)

	hc := NewHTTPClient(PAT("p"), WithBaseURL(srv.URL+"/images/generations"))
	sentinel := errors.New("stop")
	var completedCalls int
	err := hc.GenerateImageStream(context.Background(), &ImageGenParams{Model: "m", Prompt: "p"},
		func(ev ImageStreamEvent) error {
			if ev.Type == ImageStreamEventKeepalive {
				return sentinel
			}
			completedCalls++
			return nil
		})
	if !errors.Is(err, sentinel) {
		t.Fatalf("keepalive 回调错误应优先透传, got %v", err)
	}
	if completedCalls != 0 {
		t.Fatalf("错误路径 completed 回调不应调用, calls = %d", completedCalls)
	}
}
