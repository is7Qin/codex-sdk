package codexsdk

import (
	"context"
	"encoding/base64"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/coder/websocket"
)

// accountIDCanonicalKey 是 ChatGPT-Account-ID 的 Go 规范化形态（Header.Set
// 经 textproto 规范化后上线为 Chatgpt-Account-Id；头名大小写不敏感）。
// 测试断言一律用 Header.Get + 本键的槽位级存在检查（防 Get 空串假绿）。
var accountIDCanonicalKey = http.CanonicalHeaderKey("ChatGPT-Account-ID")

// headerHasAccountID 槽位级检查头是否存在（map 级，不经 Get 空串语义）。
func headerHasAccountID(h http.Header) bool {
	_, ok := h[accountIDCanonicalKey]
	return ok
}

// bareTestAuth 是未实现 AccountIDProvider 的自定义 Auth（向后兼容断言用）。
type bareTestAuth struct{ token string }

func (a bareTestAuth) Authorization(context.Context) (string, error) {
	return "Bearer " + a.token, nil
}

func (a bareTestAuth) Invalidate() {}
func (a bareTestAuth) Fatal(error) {}

// accountIDWSGate 起 WS 升级门：按 Authorization 值决定 401/接受，逐次记录
// 升级头（含 401 重拨场景的多次握手）。
type accountIDWSGate struct {
	mu      sync.Mutex
	headers []http.Header
}

func startAccountIDWSGate(t *testing.T, accept func(auth string) bool) (string, *accountIDWSGate) {
	t.Helper()
	g := &accountIDWSGate{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		g.mu.Lock()
		g.headers = append(g.headers, r.Header.Clone())
		g.mu.Unlock()
		if !accept(r.Header.Get("Authorization")) {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		c, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer c.CloseNow()
		for {
			typ, data, err := c.Read(r.Context())
			if err != nil {
				return
			}
			if err := c.Write(r.Context(), typ, data); err != nil {
				return
			}
		}
	}))
	t.Cleanup(srv.Close)
	return srv.URL, g
}

func (g *accountIDWSGate) snapshot() []http.Header {
	g.mu.Lock()
	defer g.mu.Unlock()
	out := make([]http.Header, len(g.headers))
	copy(out, g.headers)
	return out
}

// accountIDCaptureTransport 是任意 URL 通用捕获传输层（多端点单测用——
// fixedTransport 只映射单 URL，T3 四端点改用本传输层；仅记录请求头）。
type accountIDCaptureTransport struct {
	mu      sync.Mutex
	headers []http.Header
	body    string
}

func (tr *accountIDCaptureTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	tr.mu.Lock()
	tr.headers = append(tr.headers, req.Header.Clone())
	body := tr.body
	tr.mu.Unlock()
	return &http.Response{
		StatusCode: http.StatusOK,
		Header:     make(http.Header),
		Body:       io.NopCloser(strings.NewReader(body)),
		Request:    req,
	}, nil
}

func (tr *accountIDCaptureTransport) snapshot() []http.Header {
	tr.mu.Lock()
	defer tr.mu.Unlock()
	out := make([]http.Header, len(tr.headers))
	copy(out, tr.headers)
	return out
}

// T1：WS 握手带 WithOAuthAccountID → 握手头存在且值一致；无该 option 时
// 槽位不存在（map 级，防 Get 假绿）。上线形态为 Chatgpt-Account-Id（§2.4）。
func TestAccountIDWSHandshake(t *testing.T) {
	url, st := startEchoServer(t, "")
	ctx := context.Background()
	c, err := Dial(ctx,
		OAuthWithRotation("rt-0", WithInitialAccessToken("at-1"), WithOAuthAccountID("acc-1")),
		WithTransport(newFixedTransport(t, "https://chatgpt.com/backend-api/codex/responses", url)))
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer c.Close(StatusGoingAway, "")
	waitFor(t, func() bool {
		st.mu.Lock()
		defer st.mu.Unlock()
		return st.hasAccountID
	})
	st.mu.Lock()
	got, present := st.accountID, st.hasAccountID
	st.mu.Unlock()
	if !present || got != "acc-1" {
		t.Fatalf("握手头 ChatGPT-Account-ID = %q present=%v, 期望 acc-1/true", got, present)
	}

	// 无 option：槽位不存在
	url2, st2 := startEchoServer(t, "")
	c2, err := Dial(ctx, PAT("t"),
		WithTransport(newFixedTransport(t, "https://chatgpt.com/backend-api/codex/responses", url2)))
	if err != nil {
		t.Fatalf("Dial #2: %v", err)
	}
	defer c2.Close(StatusGoingAway, "")
	waitFor(t, func() bool {
		st2.mu.Lock()
		defer st2.mu.Unlock()
		return st2.authHeader != ""
	})
	st2.mu.Lock()
	got2, present2 := st2.accountID, st2.hasAccountID
	st2.mu.Unlock()
	if present2 || got2 != "" {
		t.Fatalf("无 account id 时头不应出现: %q present=%v", got2, present2)
	}
}

// T2：HTTP Do + Stream 带 id → 头存在且值一致；无 id → 槽位不存在。
func TestAccountIDHTTPDoAndStream(t *testing.T) {
	newDoServer := func(t *testing.T, got *string, present *bool, mu *sync.Mutex) string {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			mu.Lock()
			*got = r.Header.Get("ChatGPT-Account-ID")
			_, *present = r.Header[accountIDCanonicalKey]
			mu.Unlock()
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{}`))
		}))
		t.Cleanup(srv.Close)
		return srv.URL
	}
	ctx := context.Background()

	t.Run("Do 带 id", func(t *testing.T) {
		var got string
		var present bool
		var mu sync.Mutex
		srvURL := newDoServer(t, &got, &present, &mu)
		hc := NewHTTPClient(
			OAuthWithRotation("rt-0", WithInitialAccessToken("at-1"), WithOAuthAccountID("acc-1")),
			WithTransport(newFixedTransport(t, "https://chatgpt.com/backend-api/codex/responses", srvURL)))
		if _, err := hc.Do(ctx, []byte(`{}`)); err != nil {
			t.Fatalf("Do: %v", err)
		}
		mu.Lock()
		defer mu.Unlock()
		if !present || got != "acc-1" {
			t.Fatalf("Do 头 = %q present=%v, 期望 acc-1/true", got, present)
		}
	})

	t.Run("Do 无 id", func(t *testing.T) {
		var got string
		var present bool
		var mu sync.Mutex
		srvURL := newDoServer(t, &got, &present, &mu)
		hc := NewHTTPClient(PAT("t"),
			WithTransport(newFixedTransport(t, "https://chatgpt.com/backend-api/codex/responses", srvURL)))
		if _, err := hc.Do(ctx, []byte(`{}`)); err != nil {
			t.Fatalf("Do: %v", err)
		}
		mu.Lock()
		defer mu.Unlock()
		if present || got != "" {
			t.Fatalf("无 account id 时 Do 头不应出现: %q present=%v", got, present)
		}
	})

	t.Run("Stream 带 id", func(t *testing.T) {
		var got string
		var present bool
		var mu sync.Mutex
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			mu.Lock()
			got = r.Header.Get("ChatGPT-Account-ID")
			_, present = r.Header[accountIDCanonicalKey]
			mu.Unlock()
			w.Header().Set("Content-Type", "text/event-stream")
			_, _ = io.WriteString(w, "data: {\"type\":\"x\"}\n\ndata: [DONE]\n\n")
		}))
		t.Cleanup(srv.Close)
		hc := NewHTTPClient(PAT("t", WithPATAccountID("acc-1")),
			WithTransport(newFixedTransport(t, "https://chatgpt.com/backend-api/codex/responses", srv.URL)))
		if err := hc.Stream(ctx, []byte(`{}`), func(raw []byte) error { return nil }); err != nil {
			t.Fatalf("Stream: %v", err)
		}
		mu.Lock()
		defer mu.Unlock()
		if !present || got != "acc-1" {
			t.Fatalf("Stream 头 = %q present=%v, 期望 acc-1/true", got, present)
		}
	})
}

// T3：GenerateImage（generations+edits 两 URL）+ Search + GetUsage 各面头
// 存在且值一致（全走 doURL → sendRequest 单点）。
func TestAccountIDAllHTTPFaces(t *testing.T) {
	tr := &accountIDCaptureTransport{body: `{}`}
	hc := NewHTTPClient(
		OAuthWithRotation("rt-0", WithInitialAccessToken("at-1"), WithOAuthAccountID("acc-3")),
		WithTransport(tr))
	ctx := context.Background()

	if _, err := hc.GenerateImage(ctx, &ImageGenParams{Model: "m", Prompt: "p"}); err != nil {
		t.Fatalf("GenerateImage generations: %v", err)
	}
	if _, err := hc.GenerateImage(ctx, &ImageGenParams{
		Model: "m", Prompt: "p",
		Images: []ImageRef{{ImageURL: strPtr("https://cdn.example.com/a.png")}},
	}); err != nil {
		t.Fatalf("GenerateImage edits: %v", err)
	}
	if _, err := hc.Search(ctx, []byte(`{"query":"q"}`)); err != nil {
		t.Fatalf("Search: %v", err)
	}
	if _, err := hc.GetUsage(ctx); err != nil {
		t.Fatalf("GetUsage: %v", err)
	}

	got := tr.snapshot()
	if len(got) != 4 {
		t.Fatalf("请求数 = %d, 期望 4（generations/edits/search/usage）", len(got))
	}
	for i, h := range got {
		if v := h.Get("ChatGPT-Account-ID"); v != "acc-3" {
			t.Fatalf("第 %d 面头 = %q, 期望 acc-3", i, v)
		}
	}
}

// T4：向后兼容——PAT 未给 id、自定义 Auth 未实现 AccountIDProvider → 头不
// 出现、无 panic。
func TestAccountIDBackwardCompat(t *testing.T) {
	tr := &accountIDCaptureTransport{body: `{}`}
	ctx := context.Background()

	hc := NewHTTPClient(PAT("t"), WithTransport(tr))
	if _, err := hc.Do(ctx, []byte(`{}`)); err != nil {
		t.Fatalf("Do PAT 无 id: %v", err)
	}
	hc2 := NewHTTPClient(bareTestAuth{token: "t"}, WithTransport(tr))
	if _, err := hc2.Do(ctx, []byte(`{}`)); err != nil {
		t.Fatalf("Do 自定义 Auth: %v", err)
	}

	got := tr.snapshot()
	if len(got) != 2 {
		t.Fatalf("请求数 = %d, 期望 2", len(got))
	}
	for i, h := range got {
		if headerHasAccountID(h) {
			t.Fatalf("第 %d 个请求不应带 ChatGPT-Account-ID（向后兼容）", i)
		}
	}

	// WS 面：自定义 Auth 同样无头无 panic
	url, st := startEchoServer(t, "")
	c, err := Dial(ctx, bareTestAuth{token: "t"},
		WithTransport(newFixedTransport(t, "https://chatgpt.com/backend-api/codex/responses", url)))
	if err != nil {
		t.Fatalf("Dial 自定义 Auth: %v", err)
	}
	defer c.Close(StatusGoingAway, "")
	waitFor(t, func() bool {
		st.mu.Lock()
		defer st.mu.Unlock()
		return st.authHeader != ""
	})
	st.mu.Lock()
	present := st.hasAccountID
	st.mu.Unlock()
	if present {
		t.Fatal("自定义 Auth 的 WS 握手不应带 ChatGPT-Account-ID")
	}
}

// T5：旋转——401 → refresh → 重拨，重拨握手头仍带同一 id（不可变）。
func TestAccountIDRotationRedial(t *testing.T) {
	m := newMockRefresh(t, refreshStep{status: 200, body: `{"access_token":"at-new","refresh_token":"rt-new"}`})
	url, gate := startAccountIDWSGate(t, func(auth string) bool { return auth == "Bearer at-new" })
	auth := OAuthWithRotation("rt-0",
		WithInitialAccessToken("at-old"), WithOAuthAccountID("acc-5"))
	c, err := Dial(context.Background(), auth,
		WithTransport(newFixedTransport(t, "https://chatgpt.com/backend-api/codex/responses", url)))
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer c.Close(StatusGoingAway, "")

	if m.callCount() != 1 {
		t.Fatalf("refresh 次数 = %d, 期望 1", m.callCount())
	}
	got := gate.snapshot()
	if len(got) != 2 {
		t.Fatalf("握手次数 = %d, 期望 2（首次 401 + 重拨）", len(got))
	}
	for i, h := range got {
		if v := h.Get("ChatGPT-Account-ID"); v != "acc-5" {
			t.Fatalf("第 %d 次握手头 = %q, 期望 acc-5（account id 不可变）", i, v)
		}
	}
}

// T6：token 刷新端点不带 ChatGPT-Account-ID（钉住排除项）。
func TestAccountIDRefreshEndpointExcluded(t *testing.T) {
	m := newMockRefresh(t, refreshStep{status: 200, body: `{"access_token":"at-1"}`})
	auth := OAuthWithRotation("rt-0", WithOAuthAccountID("acc-6"))
	h, err := auth.Authorization(context.Background())
	if err != nil || h != "Bearer at-1" {
		t.Fatalf("Authorization: %q %v", h, err)
	}
	hdr := m.header(0)
	if headerHasAccountID(hdr) {
		t.Fatal("refresh 请求不应带 ChatGPT-Account-ID（真客户端刷新端点仅 UA/Originator）")
	}
	if hdr.Get("User-Agent") != DefaultCodexUserAgent || hdr.Get("Originator") != DefaultOriginator {
		t.Fatal("refresh 请求应保持既有 UA/Originator 形态")
	}
}

// T7：值卫生——含 \r\n / 纯空白的 id 被跳过（不注入脏值）。
func TestAccountIDValueHygiene(t *testing.T) {
	ids := []string{"a\rb", "a\nb", "a\r\nb", "a\x00b", "   ", ""}
	for _, id := range ids {
		t.Run("oauth:"+strings.ReplaceAll(strings.ReplaceAll(id, "\r", "\\r"), "\n", "\\n"), func(t *testing.T) {
			tr := &accountIDCaptureTransport{body: `{}`}
			hc := NewHTTPClient(
				OAuthWithRotation("rt-0", WithInitialAccessToken("at-1"), WithOAuthAccountID(id)),
				WithTransport(tr))
			if _, err := hc.Do(context.Background(), []byte(`{}`)); err != nil {
				t.Fatalf("Do: %v", err)
			}
			for _, h := range tr.snapshot() {
				if headerHasAccountID(h) {
					t.Fatalf("脏值 %q 不应注入", id)
				}
			}
		})
		t.Run("pat:"+strings.ReplaceAll(strings.ReplaceAll(id, "\r", "\\r"), "\n", "\\n"), func(t *testing.T) {
			tr := &accountIDCaptureTransport{body: `{}`}
			hc := NewHTTPClient(PAT("t", WithPATAccountID(id)), WithTransport(tr))
			if _, err := hc.Do(context.Background(), []byte(`{}`)); err != nil {
				t.Fatalf("Do: %v", err)
			}
			for _, h := range tr.snapshot() {
				if headerHasAccountID(h) {
					t.Fatalf("脏值 %q 不应注入", id)
				}
			}
		})
	}
}

// T8：覆盖语义——调用方 WithHeader 覆盖默认注入值（循环后写赢）。
func TestAccountIDHeaderOverride(t *testing.T) {
	tr := &accountIDCaptureTransport{body: `{}`}
	hc := NewHTTPClient(
		OAuthWithRotation("rt-0", WithInitialAccessToken("at-1"), WithOAuthAccountID("acc-default")),
		WithTransport(tr),
		WithHeader("ChatGPT-Account-ID", "override"))
	if _, err := hc.Do(context.Background(), []byte(`{}`)); err != nil {
		t.Fatalf("Do: %v", err)
	}
	got := tr.snapshot()
	if len(got) != 1 {
		t.Fatalf("请求数 = %d, 期望 1", len(got))
	}
	if v := got[0].Get("ChatGPT-Account-ID"); v != "override" {
		t.Fatalf("覆盖后头 = %q, 期望 override（WithHeader 后写赢）", v)
	}
}

// T9：版本号——DefaultCodexUserAgent 全串相等断言（常量级，防单点漏改）。
func TestAccountIDUserAgentVersion(t *testing.T) {
	const want = "codex-tui/0.154.0 (Ubuntu 24.4.0; x86_64) xterm-256color (codex-tui; 0.154.0)"
	if DefaultCodexUserAgent != want {
		t.Fatalf("DefaultCodexUserAgent = %q, 期望 %q", DefaultCodexUserAgent, want)
	}
	if !strings.Contains(DefaultCodexUserAgent, "codex-tui/0.154.0") ||
		!strings.Contains(DefaultCodexUserAgent, "(codex-tui; 0.154.0)") {
		t.Fatalf("版本号两处应同步为 0.154.0: %q", DefaultCodexUserAgent)
	}
}

// makeAccountIDTestJWT 组装未签名测试 JWT（头.payload.sig；解析只读
// payload，签名内容无关）。
func makeAccountIDTestJWT(t *testing.T, payload string) string {
	t.Helper()
	enc := base64.RawURLEncoding.EncodeToString
	return enc([]byte(`{"alg":"RS256","typ":"JWT"}`)) + "." + enc([]byte(payload)) + ".sig"
}

// T10：AccountIDFromToken table-driven（离线 claims 提取）。
func TestAccountIDFromToken(t *testing.T) {
	cases := []struct {
		name   string
		token  func(t *testing.T) string
		wantID string
		wantOK bool
	}{
		{"命中", func(t *testing.T) string {
			return makeAccountIDTestJWT(t, `{"https://api.openai.com/auth":{"chatgpt_account_id":"acc-xyz","chatgpt_plan_type":"free"}}`)
		}, "acc-xyz", true},
		{"值两侧空白被 trim", func(t *testing.T) string {
			return makeAccountIDTestJWT(t, `{"https://api.openai.com/auth":{"chatgpt_account_id":"  acc-1  "}}`)
		}, "acc-1", true},
		{"claim 缺失", func(t *testing.T) string {
			return makeAccountIDTestJWT(t, `{"sub":"user-1"}`)
		}, "", false},
		{"命名空间缺失但顶层有同名键", func(t *testing.T) string {
			return makeAccountIDTestJWT(t, `{"chatgpt_account_id":"acc-x"}`)
		}, "", false},
		{"claim 非字符串", func(t *testing.T) string {
			return makeAccountIDTestJWT(t, `{"https://api.openai.com/auth":{"chatgpt_account_id":123}}`)
		}, "", false},
		{"claim 纯空白", func(t *testing.T) string {
			return makeAccountIDTestJWT(t, `{"https://api.openai.com/auth":{"chatgpt_account_id":"   "}}`)
		}, "", false},
		{"非 JWT", func(t *testing.T) string { return "not-a-jwt" }, "", false},
		{"空串", func(t *testing.T) string { return "" }, "", false},
		{"两段", func(t *testing.T) string { return "a.b" }, "", false},
		{"四段", func(t *testing.T) string { return "a.b.c.d" }, "", false},
		{"refreshToken 不透明串", func(t *testing.T) string { return "rt.1.abc" }, "", false},
		{"payload 非 base64", func(t *testing.T) string { return "h.%%%.s" }, "", false},
		{"payload 非 JSON", func(t *testing.T) string {
			enc := base64.RawURLEncoding.EncodeToString
			return enc([]byte(`{}`)) + "." + enc([]byte("not json")) + ".s"
		}, "", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := AccountIDFromToken(tc.token(t))
			if got != tc.wantID || ok != tc.wantOK {
				t.Fatalf("AccountIDFromToken = (%q, %v), 期望 (%q, %v)",
					got, ok, tc.wantID, tc.wantOK)
			}
		})
	}
}

// T11：零隐式派生——仅给 WithInitialAccessToken（含 claim 的 JWT）不给
// WithOAuthAccountID → AccountID() == ""（SDK 不自行解析）；显式 option 才生效。
func TestAccountIDNoImplicitDerivation(t *testing.T) {
	jwt := makeAccountIDTestJWT(t, `{"https://api.openai.com/auth":{"chatgpt_account_id":"acc-derived"}}`)
	if _, ok := AccountIDFromToken(jwt); !ok {
		t.Fatal("测试 JWT 应可解析（用例前置条件）")
	}

	auth := OAuthWithRotation("rt-0", WithInitialAccessToken(jwt))
	p, ok := auth.(AccountIDProvider)
	if !ok {
		t.Fatal("rotationAuth 应实现 AccountIDProvider")
	}
	if got := p.AccountID(); got != "" {
		t.Fatalf("未显式配置时 AccountID() = %q, 期望空（零隐式派生）", got)
	}

	auth2 := OAuthWithRotation("rt-0",
		WithInitialAccessToken(jwt), WithOAuthAccountID("acc-explicit"))
	p2, ok := auth2.(AccountIDProvider)
	if !ok {
		t.Fatal("rotationAuth 应实现 AccountIDProvider")
	}
	if got := p2.AccountID(); got != "acc-explicit" {
		t.Fatalf("显式配置后 AccountID() = %q, 期望 acc-explicit", got)
	}
}

// T12：FetchPATMetadata（httptest + CODEX_AUTHAPI_BASE_URL 指向 mock）。
func TestFetchPATMetadata(t *testing.T) {
	ctx := context.Background()

	t.Run("200 字段映射", func(t *testing.T) {
		var gotAuth, gotUA, gotOriginator string
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			gotAuth = r.Header.Get("Authorization")
			gotUA = r.Header.Get("User-Agent")
			gotOriginator = r.Header.Get("Originator")
			if r.URL.Path != "/v1/user-auth-credential/whoami" {
				http.Error(w, "bad path: "+r.URL.Path, http.StatusNotFound)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"email":"u@x.com","chatgpt_user_id":"user-1",` +
				`"chatgpt_account_id":"acc-9","chatgpt_plan_type":"plus",` +
				`"chatgpt_account_is_fedramp":true}`))
		}))
		t.Cleanup(srv.Close)
		t.Setenv("CODEX_AUTHAPI_BASE_URL", srv.URL)

		md, err := FetchPATMetadata(ctx, "pat-123")
		if err != nil {
			t.Fatalf("FetchPATMetadata: %v", err)
		}
		if md.Email != "u@x.com" || md.UserID != "user-1" || md.AccountID != "acc-9" ||
			md.PlanType != "plus" || !md.FedRAMP {
			t.Fatalf("字段映射错误: %+v", md)
		}
		if gotAuth != "Bearer pat-123" {
			t.Fatalf("Authorization = %q, 期望 Bearer pat-123", gotAuth)
		}
		if gotUA != DefaultCodexUserAgent {
			t.Fatalf("User-Agent = %q, 期望默认 codex UA", gotUA)
		}
		if gotOriginator != DefaultOriginator {
			t.Fatalf("Originator = %q, 期望 %q", gotOriginator, DefaultOriginator)
		}
	})

	t.Run("缺失字段宽松", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"chatgpt_account_id":"acc-only"}`))
		}))
		t.Cleanup(srv.Close)
		t.Setenv("CODEX_AUTHAPI_BASE_URL", srv.URL)

		md, err := FetchPATMetadata(ctx, "pat-123")
		if err != nil {
			t.Fatalf("缺失字段不应报错: %v", err)
		}
		if md.AccountID != "acc-only" || md.Email != "" || md.UserID != "" ||
			md.PlanType != "" || md.FedRAMP {
			t.Fatalf("宽松映射错误: %+v", md)
		}
	})

	t.Run("非 2xx 带状态码错误", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusUnauthorized)
			_, _ = w.Write([]byte(`{"error":"bad token"}`))
		}))
		t.Cleanup(srv.Close)
		t.Setenv("CODEX_AUTHAPI_BASE_URL", srv.URL)

		if _, err := FetchPATMetadata(ctx, "bad"); err == nil ||
			!strings.Contains(err.Error(), "401") {
			t.Fatalf("非 2xx 应返回带状态码错误, got %v", err)
		}
	})
}

// T13：无网络契约——PAT() 与 OAuthWithRotation 构造期零出站；whoami 仅在
// 显式 FetchPATMetadata 调用时发生。
func TestAccountIDNoNetworkAtConstruction(t *testing.T) {
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{}`))
	}))
	t.Cleanup(srv.Close)
	t.Setenv("CODEX_AUTHAPI_BASE_URL", srv.URL)

	_ = PAT("tok", WithPATAccountID("acc-t13"))
	_ = OAuthWithRotation("rt-0", WithInitialAccessToken("at-1"), WithOAuthAccountID("acc-t13"))
	if n := calls.Load(); n != 0 {
		t.Fatalf("构造期出站 = %d, 期望 0（PAT/OAuthWithRotation 零网络）", n)
	}

	if _, err := FetchPATMetadata(context.Background(), "tok"); err != nil {
		t.Fatalf("FetchPATMetadata: %v", err)
	}
	if n := calls.Load(); n != 1 {
		t.Fatalf("显式 whoami 后出站 = %d, 期望 1", n)
	}
}
