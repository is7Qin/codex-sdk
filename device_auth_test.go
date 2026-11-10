package codexsdk

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"
)

// deviceTestJWT 构造三段式 JWT（payload 为 base64url 无填充 JSON；签名段占位，
// SDK 不验签）。
func deviceTestJWT(t *testing.T, claims map[string]any) string {
	t.Helper()
	payload, err := json.Marshal(claims)
	if err != nil {
		t.Fatalf("marshal claims: %v", err)
	}
	enc := base64.RawURLEncoding.EncodeToString(payload)
	return "eyJhbGciOiJub25lIn0." + enc + ".sig"
}

func TestDeviceAuthStartFlow(t *testing.T) {
	var gotBody map[string]any
	var gotCT, gotUA, gotOriginator string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("method = %s, 期望 POST", r.Method)
		}
		if r.URL.Path != deviceUserCodePath {
			t.Errorf("path = %s, 期望 %s", r.URL.Path, deviceUserCodePath)
		}
		gotCT = r.Header.Get("Content-Type")
		gotUA = r.Header.Get("User-Agent")
		gotOriginator = r.Header.Get("Originator")
		if err := json.NewDecoder(r.Body).Decode(&gotBody); err != nil {
			t.Errorf("decode body: %v", err)
		}
		_, _ = w.Write([]byte(`{"device_auth_id":"auth-1","user_code":"ABCD-EFGHI","interval":"7","expires_in":900}`))
	}))
	defer srv.Close()

	c := NewDeviceAuthClient(WithIssuer(srv.URL+"/"), WithDeviceAuthClientID("client-x"))
	code, err := c.StartDeviceFlow(context.Background())
	if err != nil {
		t.Fatalf("StartDeviceFlow: %v", err)
	}
	if gotBody["client_id"] != "client-x" {
		t.Fatalf("client_id = %v, 期望 client-x", gotBody["client_id"])
	}
	if gotCT != "application/json" {
		t.Fatalf("Content-Type = %q", gotCT)
	}
	if gotUA != DefaultCodexUserAgent || gotOriginator != DefaultOriginator {
		t.Fatalf("伪装层默认头缺失: UA=%q originator=%q", gotUA, gotOriginator)
	}
	if code.AuthID != "auth-1" || code.UserCode != "ABCD-EFGHI" {
		t.Fatalf("code = %+v", code)
	}
	if code.VerificationURL != srv.URL+deviceVerificationPath {
		t.Fatalf("VerificationURL = %q", code.VerificationURL)
	}
	if code.Interval != 7*time.Second {
		t.Fatalf("Interval = %v, 期望 7s（字符串 interval 需解析）", code.Interval)
	}
	if code.ExpiresIn != 900*time.Second {
		t.Fatalf("ExpiresIn = %v, 期望 900s", code.ExpiresIn)
	}
}

func TestDeviceAuthStartFlowAliasesAndDefaults(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		// usercode 别名 + 数字 interval（两种形态都要接受）。
		_, _ = w.Write([]byte(`{"device_auth_id":"auth-2","usercode":"ZZZZ-ZZZZ","interval":3}`))
	}))
	defer srv.Close()

	c := NewDeviceAuthClient(WithIssuer(srv.URL))
	code, err := c.StartDeviceFlow(context.Background())
	if err != nil {
		t.Fatalf("StartDeviceFlow: %v", err)
	}
	if code.UserCode != "ZZZZ-ZZZZ" {
		t.Fatalf("UserCode = %q, 期望别名 usercode 生效", code.UserCode)
	}
	if code.Interval != 3*time.Second {
		t.Fatalf("Interval = %v, 期望数字 3s", code.Interval)
	}
	if code.ExpiresIn != defaultDeviceLifetime {
		t.Fatalf("ExpiresIn = %v, 未下发应按 %v（客户端需要本地截止时间）", code.ExpiresIn, defaultDeviceLifetime)
	}

	// interval 缺失 → 缺省 5s。
	srv2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"device_auth_id":"auth-3","user_code":"YYYY-YYYY"}`))
	}))
	defer srv2.Close()
	code, err = NewDeviceAuthClient(WithIssuer(srv2.URL)).StartDeviceFlow(context.Background())
	if err != nil {
		t.Fatalf("StartDeviceFlow(default): %v", err)
	}
	if code.Interval != defaultDeviceInterval {
		t.Fatalf("Interval = %v, 缺省应为 %v", code.Interval, defaultDeviceInterval)
	}
}

func TestDeviceAuthStartFlowErrors(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte(`{"error":"not enabled"}`))
	}))
	defer srv.Close()

	_, err := NewDeviceAuthClient(WithIssuer(srv.URL)).StartDeviceFlow(context.Background())
	var de *DeviceAuthError
	if !errors.As(err, &de) {
		t.Fatalf("err = %v, 期望 *DeviceAuthError", err)
	}
	if de.StatusCode != http.StatusNotFound || de.Op != "usercode" {
		t.Fatalf("DeviceAuthError = %+v", de)
	}

	// 缺 device_auth_id / user_code → 协议错误（快速失败，不返回半成品会话）。
	bad := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"device_auth_id":""}`))
	}))
	defer bad.Close()
	if _, err := NewDeviceAuthClient(WithIssuer(bad.URL)).StartDeviceFlow(context.Background()); err == nil {
		t.Fatal("缺字段应报错")
	}
}

func TestDeviceAuthPollPendingAndExpired(t *testing.T) {
	var mu sync.Mutex
	var exchangeHits int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case deviceTokenPath:
			// 403 与 404 都是「用户未授权」（真客户端同判据：两者都睡眠重试）。
			w.WriteHeader(http.StatusForbidden)
		case oauthTokenPath:
			mu.Lock()
			exchangeHits++
			mu.Unlock()
		}
	}))
	defer srv.Close()

	c := NewDeviceAuthClient(WithIssuer(srv.URL))
	code := &DeviceCode{AuthID: "auth-1", UserCode: "ABCD-EFGHI", Interval: time.Second}
	if _, err := c.PollDeviceFlow(context.Background(), code); !errors.Is(err, ErrDeviceAuthorizationPending) {
		t.Fatalf("403 → %v, 期望 ErrDeviceAuthorizationPending", err)
	}
	mu.Lock()
	if exchangeHits != 0 {
		t.Fatalf("未授权时不应换码，exchangeHits=%d", exchangeHits)
	}
	mu.Unlock()

	cases := []struct {
		name   string
		status int
		body   string
		check  func(t *testing.T, err error)
	}{
		{"notfound", http.StatusNotFound, "", func(t *testing.T, err error) {
			if !errors.Is(err, ErrDeviceAuthorizationPending) {
				t.Fatalf("404 → %v, 期望 ErrDeviceAuthorizationPending", err)
			}
		}},
		{"expired", http.StatusGone, "", func(t *testing.T, err error) {
			if !errors.Is(err, ErrDeviceCodeExpired) {
				t.Fatalf("410 → %v, 期望 ErrDeviceCodeExpired", err)
			}
		}},
		{"denied", http.StatusBadRequest, `{"error":"access_denied"}`, func(t *testing.T, err error) {
			if !errors.Is(err, ErrDeviceCodeDenied) {
				t.Fatalf("400 access_denied → %v, 期望 ErrDeviceCodeDenied", err)
			}
		}},
		{"declined", http.StatusUnauthorized, `{"error":"authorization_declined"}`, func(t *testing.T, err error) {
			if !errors.Is(err, ErrDeviceCodeDenied) {
				t.Fatalf("401 authorization_declined（真客户端实测形态）→ %v, 期望 ErrDeviceCodeDenied", err)
			}
		}},
		{"slow_down", http.StatusTooManyRequests, `{"error":"slow_down"}`, func(t *testing.T, err error) {
			var de *DeviceAuthError
			if !errors.As(err, &de) || de.StatusCode != http.StatusTooManyRequests || de.Op != "token" {
				t.Fatalf("429 → %v, 期望 *DeviceAuthError{token,429}", err)
			}
		}},
	}
	for _, tc := range cases {
		status, body := tc.status, tc.body
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(status)
			_, _ = w.Write([]byte(body))
		}))
		_, err := NewDeviceAuthClient(WithIssuer(srv.URL)).PollDeviceFlow(context.Background(), code)
		tc.check(t, err)
		srv.Close()
	}
}

func TestDeviceAuthPollSuccessExchange(t *testing.T) {
	idToken := deviceTestJWT(t, map[string]any{
		"email": "codex@example.com",
		"https://api.openai.com/auth": map[string]any{
			"chatgpt_account_id": "acct-123",
		},
	})
	var gotForm url.Values
	var gotPath, gotCT string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case deviceTokenPath:
			var body map[string]string
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Errorf("poll body: %v", err)
			}
			if body["device_auth_id"] != "auth-1" || body["user_code"] != "ABCD-EFGHI" {
				t.Errorf("poll body = %v", body)
			}
			_, _ = w.Write([]byte(`{"authorization_code":"auth-code","code_challenge":"challenge","code_verifier":"verifier"}`))
		case oauthTokenPath:
			gotPath = r.URL.Path
			gotCT = r.Header.Get("Content-Type")
			if err := r.ParseForm(); err != nil {
				t.Errorf("parse form: %v", err)
			}
			gotForm = r.PostForm
			resp := map[string]any{
				"access_token":  "at-1",
				"refresh_token": "rt-1",
				"id_token":      idToken,
				"expires_in":    3600,
			}
			_ = json.NewEncoder(w).Encode(resp)
		default:
			t.Errorf("unexpected path %s", r.URL.Path)
		}
	}))
	defer srv.Close()

	fixedNow := time.Date(2026, 11, 10, 12, 0, 0, 0, time.UTC)
	c := NewDeviceAuthClient(WithIssuer(srv.URL), WithDeviceAuthClientID("client-x"))
	c.now = func() time.Time { return fixedNow }

	tokens, err := c.PollDeviceFlow(context.Background(), &DeviceCode{AuthID: "auth-1", UserCode: "ABCD-EFGHI"})
	if err != nil {
		t.Fatalf("PollDeviceFlow: %v", err)
	}
	if gotPath != oauthTokenPath || gotCT != "application/x-www-form-urlencoded" {
		t.Fatalf("换码请求 path=%q ct=%q（上游 TokenEncoding::Form）", gotPath, gotCT)
	}
	wantForm := map[string]string{
		"grant_type":    "authorization_code",
		"code":          "auth-code",
		"redirect_uri":  srv.URL + deviceCallbackPath,
		"client_id":     "client-x",
		"code_verifier": "verifier",
	}
	for k, want := range wantForm {
		if got := gotForm.Get(k); got != want {
			t.Fatalf("form[%s] = %q, 期望 %q（全量 form=%v）", k, got, want, gotForm)
		}
	}
	if tokens.AccessToken != "at-1" || tokens.RefreshToken != "rt-1" || tokens.IDToken != idToken {
		t.Fatalf("tokens = %+v", tokens)
	}
	if tokens.AccountID != "acct-123" || tokens.Email != "codex@example.com" {
		t.Fatalf("claims = account=%q email=%q", tokens.AccountID, tokens.Email)
	}
	if !tokens.ExpiresAt.Equal(fixedNow.Add(time.Hour)) || tokens.ExpiresIn != time.Hour {
		t.Fatalf("expiry = %v / %v", tokens.ExpiresAt, tokens.ExpiresIn)
	}
}

func TestDeviceAuthTokenClaimsFallback(t *testing.T) {
	// id_token 只有 email；account id 落 access_token（cc-switch 同判据）。
	idToken := deviceTestJWT(t, map[string]any{"email": "a@example.com"})
	atToken := deviceTestJWT(t, map[string]any{
		"https://api.openai.com/auth": map[string]any{"chatgpt_account_id": "acct-at"},
	})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case deviceTokenPath:
			_, _ = w.Write([]byte(`{"authorization_code":"c","code_verifier":"v"}`))
		case oauthTokenPath:
			_ = json.NewEncoder(w).Encode(map[string]any{
				"access_token": atToken, "refresh_token": "rt", "id_token": idToken,
			})
		}
	}))
	defer srv.Close()

	tokens, err := NewDeviceAuthClient(WithIssuer(srv.URL)).PollDeviceFlow(context.Background(), &DeviceCode{AuthID: "a", UserCode: "u"})
	if err != nil {
		t.Fatalf("PollDeviceFlow: %v", err)
	}
	if tokens.AccountID != "acct-at" {
		t.Fatalf("AccountID = %q, 期望 access_token 兜底 acct-at", tokens.AccountID)
	}
	if tokens.Email != "a@example.com" {
		t.Fatalf("Email = %q", tokens.Email)
	}
	if !tokens.ExpiresAt.IsZero() || tokens.ExpiresIn != 0 {
		t.Fatalf("未下发 expires_in 应为零值: %v/%v", tokens.ExpiresAt, tokens.ExpiresIn)
	}

	// 畸形 JWT 不 panic，字段留空。
	if id, mail := deviceLoginIdentity("not-a-jwt", "also-bad"); id != "" || mail != "" {
		t.Fatalf("畸形 token 解析 = %q/%q", id, mail)
	}
	// 顶层 chatgpt_account_id 也接受。
	top := deviceTestJWT(t, map[string]any{"chatgpt_account_id": "acct-top"})
	if id, _ := deviceLoginIdentity(top, ""); id != "acct-top" {
		t.Fatalf("顶层 claim = %q", id)
	}
}

func TestDeviceAuthExchangeValidation(t *testing.T) {
	c := NewDeviceAuthClient()
	if _, err := c.exchangeCode(context.Background(), "", "v"); err == nil {
		t.Fatal("空 authorization_code 应报错")
	}
	if _, err := c.exchangeCode(context.Background(), "c", ""); err == nil {
		t.Fatal("空 code_verifier 应报错")
	}
	if _, err := c.PollDeviceFlow(context.Background(), nil); err == nil {
		t.Fatal("nil 会话应报错")
	}

	// 换码端点非 2xx → *DeviceAuthError。
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{"error":"invalid_grant"}`))
	}))
	defer srv.Close()
	_, err := NewDeviceAuthClient(WithIssuer(srv.URL)).exchangeCode(context.Background(), "c", "v")
	var de *DeviceAuthError
	if !errors.As(err, &de) || de.StatusCode != http.StatusBadRequest || de.Op != "exchange" {
		t.Fatalf("err = %v", err)
	}
	if !strings.Contains(de.Error(), "invalid_grant") {
		t.Fatalf("错误体应保留诊断信息: %v", de)
	}
}

// TestDeviceAuthClientIDMatchesRefresh 设备码登录与 refresh 必须使用同一 client_id
// 来源：设备码签发的 refresh token 绑定 client_id，不同源会让「登录成功、刷新
// invalid_client」。
func TestDeviceAuthClientIDMatchesRefresh(t *testing.T) {
	if got := NewDeviceAuthClient().clientID; got != resolveOAuthClientID() {
		t.Fatalf("default clientID = %q, 期望与 refresh 同源 %q", got, resolveOAuthClientID())
	}
	t.Setenv("CODEX_APP_SERVER_LOGIN_CLIENT_ID", "client-env")
	if got := NewDeviceAuthClient().clientID; got != "client-env" {
		t.Fatalf("env 覆盖后 clientID = %q", got)
	}
}
