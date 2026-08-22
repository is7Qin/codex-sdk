package codexsdk

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"
)

// refreshStep 是 refresh mock 的一步响应。
type refreshStep struct {
	status int
	body   string
}

// mockRefresh 是 refresh 端点 mock：记录请求（次数/请求体/头），按序弹出
// 响应序列（耗尽后重复最后一步）；block 非空时每次请求先等待其关闭。
// 构造即设置 CODEX_REFRESH_TOKEN_URL_OVERRIDE 指向 mock。
type mockRefresh struct {
	mu      sync.Mutex
	srv     *httptest.Server
	calls   int
	bodies  []string
	headers []http.Header
	steps   []refreshStep
	last    refreshStep
	block   chan struct{}
}

func newMockRefresh(t *testing.T, steps ...refreshStep) *mockRefresh {
	t.Helper()
	m := &mockRefresh{steps: steps, last: refreshStep{status: 500, body: `{}`}}
	m.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		m.mu.Lock()
		m.calls++
		m.bodies = append(m.bodies, string(body))
		m.headers = append(m.headers, r.Header.Clone())
		step := m.last
		if len(m.steps) > 0 {
			step = m.steps[0]
			m.steps = m.steps[1:]
			m.last = step
		}
		block := m.block
		m.mu.Unlock()
		if block != nil {
			<-block
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(step.status)
		_, _ = w.Write([]byte(step.body))
	}))
	t.Cleanup(m.srv.Close)
	t.Setenv("CODEX_REFRESH_TOKEN_URL_OVERRIDE", m.srv.URL)
	return m
}

func (m *mockRefresh) callCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.calls
}

func (m *mockRefresh) body(i int) string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.bodies[i]
}

func (m *mockRefresh) header(i int) http.Header {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.headers[i].Clone()
}

// reqJSON 解析第 i 次 refresh 请求体。
func (m *mockRefresh) reqJSON(t *testing.T, i int) map[string]string {
	t.Helper()
	var got map[string]string
	if err := json.Unmarshal([]byte(m.body(i)), &got); err != nil {
		t.Fatalf("refresh 请求体非法 JSON: %v (%s)", err, m.body(i))
	}
	return got
}

// TestRotationRefreshRequest：refresh 请求形态断言——端点 / client_id /
// grant_type / refresh_token / 伪装头；响应解析 + at/rt 轮换 →
// OnTokenRotated 收到新 at+rt；缓存命中不重复请求。
func TestRotationRefreshRequest(t *testing.T) {
	var rotated atomic.Int32
	var gotAt, gotRt string
	m := newMockRefresh(t, refreshStep{
		status: 200,
		body:   `{"id_token":"id-1","access_token":"at-1","refresh_token":"rt-1"}`,
	})
	auth := OAuthWithRotation("rt-0", WithOnTokenRotated(func(at, rt string) {
		rotated.Add(1)
		gotAt, gotRt = at, rt
	}))

	h, err := auth.Authorization(context.Background())
	if err != nil {
		t.Fatalf("Authorization: %v", err)
	}
	if h != "Bearer at-1" {
		t.Fatalf("头值 = %q, 期望 Bearer at-1", h)
	}
	if m.callCount() != 1 {
		t.Fatalf("refresh 次数 = %d, 期望 1", m.callCount())
	}
	req := m.reqJSON(t, 0)
	if req["client_id"] != defaultOAuthClientID {
		t.Fatalf("client_id = %q, 期望默认 %q", req["client_id"], defaultOAuthClientID)
	}
	if req["grant_type"] != "refresh_token" {
		t.Fatalf("grant_type = %q", req["grant_type"])
	}
	if req["refresh_token"] != "rt-0" {
		t.Fatalf("refresh_token = %q, 期望 rt-0", req["refresh_token"])
	}
	hdr := m.header(0)
	if hdr.Get("Content-Type") != "application/json" {
		t.Fatalf("Content-Type = %q, 期望 application/json（JSON 形态对齐真实客户端）", hdr.Get("Content-Type"))
	}
	if hdr.Get("User-Agent") != DefaultCodexUserAgent {
		t.Fatalf("User-Agent = %q, 期望默认伪装 UA", hdr.Get("User-Agent"))
	}
	if hdr.Get("Originator") != DefaultOriginator {
		t.Fatalf("Originator = %q, 期望默认", hdr.Get("Originator"))
	}
	if rotated.Load() != 1 {
		t.Fatalf("OnTokenRotated 次数 = %d, 期望 1", rotated.Load())
	}
	if gotAt != "at-1" || gotRt != "rt-1" {
		t.Fatalf("回调收到 (%q, %q), 期望 (at-1, rt-1)", gotAt, gotRt)
	}

	// 缓存命中：再次 Authorization 不触发 refresh
	if h, err := auth.Authorization(context.Background()); err != nil || h != "Bearer at-1" {
		t.Fatalf("缓存命中失败: %q %v", h, err)
	}
	if m.callCount() != 1 {
		t.Fatalf("缓存命中不应 refresh, 次数 = %d", m.callCount())
	}

	// Invalidate 后轮转使用新 rt（rt 轮换生效）
	auth.Invalidate()
	if h, err := auth.Authorization(context.Background()); err != nil || h != "Bearer at-1" {
		t.Fatalf("Invalidate 后 Authorization: %q %v", h, err)
	}
	if m.callCount() != 2 {
		t.Fatalf("Invalidate 后应重新 refresh, 次数 = %d", m.callCount())
	}
	req2 := m.reqJSON(t, 1)
	if req2["refresh_token"] != "rt-1" {
		t.Fatalf("轮换后 refresh_token = %q, 期望 rt-1（rt 轮换生效）", req2["refresh_token"])
	}
	if rotated.Load() != 2 {
		t.Fatalf("OnTokenRotated 次数 = %d, 期望 2（每轮转一次）", rotated.Load())
	}
}

// TestRotationEnvOverrides：CODEX_APP_SERVER_LOGIN_CLIENT_ID 覆盖 client_id。
func TestRotationEnvOverrides(t *testing.T) {
	m := newMockRefresh(t, refreshStep{status: 200, body: `{"access_token":"at-1"}`})
	t.Setenv("CODEX_APP_SERVER_LOGIN_CLIENT_ID", "client-x")
	auth := OAuthWithRotation("rt-0")
	if _, err := auth.Authorization(context.Background()); err != nil {
		t.Fatalf("Authorization: %v", err)
	}
	req := m.reqJSON(t, 0)
	if req["client_id"] != "client-x" {
		t.Fatalf("client_id = %q, 期望 env 覆盖 client-x", req["client_id"])
	}
}

// TestRotationInitialAccessToken：WithInitialAccessToken 预置直接用，
// 不传则首请求前用 rt 换取。
func TestRotationInitialAccessToken(t *testing.T) {
	m := newMockRefresh(t, refreshStep{status: 200, body: `{"access_token":"at-1"}`})
	auth := OAuthWithRotation("rt-0", WithInitialAccessToken("at-0"))
	h, err := auth.Authorization(context.Background())
	if err != nil || h != "Bearer at-0" {
		t.Fatalf("初始 at 应直接用: %q %v", h, err)
	}
	if m.callCount() != 0 {
		t.Fatalf("有初始 at 不应 refresh, 次数 = %d", m.callCount())
	}
	auth.Invalidate()
	if h, err := auth.Authorization(context.Background()); err != nil || h != "Bearer at-1" {
		t.Fatalf("Invalidate 后轮转: %q %v", h, err)
	}
	if m.callCount() != 1 {
		t.Fatalf("Invalidate 后应 refresh, 次数 = %d", m.callCount())
	}
}

// TestRotationSingleFlight：N 并发 Invalidate/Authorization 共享恰一次 refresh。
func TestRotationSingleFlight(t *testing.T) {
	m := newMockRefresh(t, refreshStep{status: 200, body: `{"access_token":"at-1","refresh_token":"rt-1"}`})
	m.block = make(chan struct{})
	auth := OAuthWithRotation("rt-0")

	const n = 16
	invalidators := (n + 2) / 3 // i%3==0 的个数
	var invalidated atomic.Int32
	start := make(chan struct{})
	errs := make([]error, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			if i%3 == 0 {
				auth.Invalidate()
				invalidated.Add(1)
			}
			_, errs[i] = auth.Authorization(context.Background())
		}(i)
	}
	close(start)
	// 等全部 Invalidate 完成（refresh 被 leader 阻塞中），再放行——保证
	// at.Store 晚于全部 Invalidate，单飞恰好一次不被打断。
	waitFor(t, func() bool { return invalidated.Load() == int32(invalidators) })
	waitFor(t, func() bool { return m.callCount() >= 1 }) // 单飞已开始
	close(m.block)
	wg.Wait()

	if m.callCount() != 1 {
		t.Fatalf("refresh 次数 = %d, 期望恰一次（单飞）", m.callCount())
	}
	for i, err := range errs {
		if err != nil {
			t.Fatalf("goroutine %d: %v", i, err)
		}
	}
}

// TestRotationSharedAuthMultiClient401：共享同一 Auth 的多 HTTPClient
// 并发 401 → 仍恰一次 refresh（状态跨 client 共享）。
func TestRotationSharedAuthMultiClient401(t *testing.T) {
	const n = 4
	m := newMockRefresh(t, refreshStep{status: 200, body: `{"access_token":"at-new","refresh_token":"rt-new"}`})
	var saw401 atomic.Int32
	all401 := make(chan struct{})
	var auths atomic.Int32
	respSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") == "Bearer at-old" {
			if saw401.Add(1) == n {
				close(all401)
			}
			w.WriteHeader(http.StatusUnauthorized)
			_, _ = w.Write([]byte(`{"error":{"type":"rate_limit_exceeded"}}`))
			return
		}
		auths.Add(1)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"id":"x"}`))
	}))
	t.Cleanup(respSrv.Close)
	// refresh mock 阻塞直到全部 N 个 401 已观测（保证 N 个并发请求都先 401）
	m.block = all401

	auth := OAuthWithRotation("rt-0", WithInitialAccessToken("at-old"))
	clients := make([]*HTTPClient, n)
	for i := range clients {
		clients[i] = NewHTTPClient(auth, WithBaseURL(respSrv.URL))
	}
	var wg sync.WaitGroup
	for _, c := range clients {
		wg.Add(1)
		go func(hc *HTTPClient) {
			defer wg.Done()
			if _, err := hc.Do(context.Background(), []byte(`{}`)); err != nil {
				t.Errorf("Do: %v", err)
			}
		}(c)
	}
	wg.Wait()

	if m.callCount() != 1 {
		t.Fatalf("refresh 次数 = %d, 期望恰一次（共享 Auth 单飞）", m.callCount())
	}
	if auths.Load() != n {
		t.Fatalf("重试成功请求数 = %d, 期望 %d", auths.Load(), n)
	}
}

// TestRotationBackoff：退避重试（429 可重试）、上限生效、耗尽 →
// RefreshError 非 fatal 且下次可再试。
func TestRotationBackoff(t *testing.T) {
	// 429, 429, 200 → 3 次尝试后成功（cap 1ms 加速）
	m := newMockRefresh(t,
		refreshStep{status: 429, body: `{"error":{"type":"rate_limit_exceeded"}}`},
		refreshStep{status: 429, body: `{"error":{"type":"rate_limit_exceeded"}}`},
		refreshStep{status: 200, body: `{"access_token":"at-1"}`})
	auth := OAuthWithRotation("rt-0", WithBackoff(time.Millisecond, 3))
	h, err := auth.Authorization(context.Background())
	if err != nil || h != "Bearer at-1" {
		t.Fatalf("退避重试后应成功: %q %v", h, err)
	}
	if m.callCount() != 3 {
		t.Fatalf("refresh 次数 = %d, 期望 3（429 重试 2 次）", m.callCount())
	}

	// 耗尽：恒 5xx → RefreshError（非 fatal），OnAuthFatal 不触发
	var fatalCalls atomic.Int32
	m2 := newMockRefresh(t, refreshStep{status: 500, body: `{}`})
	auth2 := OAuthWithRotation("rt-0", WithBackoff(time.Millisecond, 3),
		WithOnAuthFatal(func(err error) { fatalCalls.Add(1) }))
	_, err = auth2.Authorization(context.Background())
	var re *RefreshError
	if !errors.As(err, &re) {
		t.Fatalf("期望 *RefreshError, got %T: %v", err, err)
	}
	if re.Attempts != 3 {
		t.Fatalf("Attempts = %d, 期望 3（上限生效）", re.Attempts)
	}
	if fatalCalls.Load() != 0 {
		t.Fatal("退避耗尽不应触发 OnAuthFatal（非 fatal）")
	}

	// 下次可再试：再次 Authorization 重新完整退避
	_, err = auth2.Authorization(context.Background())
	if !errors.As(err, &re) {
		t.Fatalf("下次可再试: 期望 *RefreshError, got %T: %v", err, err)
	}
	if m2.callCount() != 6 {
		t.Fatalf("第二次尝试 refresh 次数 = %d, 期望 6", m2.callCount())
	}
}

// TestRotationBackoffBaseDelay：默认 base 200ms 生效（首失败后等待 ≥200ms）。
func TestRotationBackoffBaseDelay(t *testing.T) {
	m := newMockRefresh(t,
		refreshStep{status: 500, body: `{}`},
		refreshStep{status: 200, body: `{"access_token":"at-1"}`})
	auth := OAuthWithRotation("rt-0", WithBackoff(0, 2)) // cap 0=不封顶，2 次尝试
	start := time.Now()
	if _, err := auth.Authorization(context.Background()); err != nil {
		t.Fatalf("Authorization: %v", err)
	}
	if d := time.Since(start); d < 190*time.Millisecond {
		t.Fatalf("退避延迟 = %v, 期望 ≥ 200ms base", d)
	}
	if m.callCount() != 2 {
		t.Fatalf("refresh 次数 = %d, 期望 2", m.callCount())
	}
}

// TestRotationRefreshFatalCodes：RT 判死码集（10 码）→ RefreshOAuthError +
// OnAuthFatal 至多一次 + 后续 Authorization 恒报错（不重试）。
func TestRotationRefreshFatalCodes(t *testing.T) {
	codes := []string{
		"invalid_grant", "invalid_refresh_token", "refresh_token_expired",
		"refresh_token_reused", "refresh_token_invalidated", "app_session_terminated",
		"token_expired", "invalid_client", "unauthorized_client", "access_denied",
	}
	for _, code := range codes {
		t.Run(code, func(t *testing.T) {
			var fatalCalls atomic.Int32
			m := newMockRefresh(t, refreshStep{status: 400, body: fmt.Sprintf(`{"error":{"code":%q}}`, code)})
			auth := OAuthWithRotation("rt-0", WithOnAuthFatal(func(err error) { fatalCalls.Add(1) }))
			_, err := auth.Authorization(context.Background())
			var re *RefreshOAuthError
			if !errors.As(err, &re) {
				t.Fatalf("期望 *RefreshOAuthError, got %T: %v", err, err)
			}
			if re.Code != code {
				t.Fatalf("Code = %q, 期望 %q", re.Code, code)
			}
			if fatalCalls.Load() != 1 {
				t.Fatalf("OnAuthFatal 次数 = %d, 期望 1", fatalCalls.Load())
			}
			if m.callCount() != 1 {
				t.Fatalf("判死不重试, refresh 次数 = %d", m.callCount())
			}
			// 后续 Authorization 恒报错（不重试、不再通知）
			_, err = auth.Authorization(context.Background())
			if !errors.As(err, &re) {
				t.Fatalf("后续 Authorization 应恒报错, got %T: %v", err, err)
			}
			if m.callCount() != 1 || fatalCalls.Load() != 1 {
				t.Fatalf("Fatal 态后不应 refresh/通知: calls=%d fatal=%d", m.callCount(), fatalCalls.Load())
			}
		})
	}
}

// TestRotationFatalCodeCaseInsensitive：错误码大小写不敏感（EqualFold 语义）。
func TestRotationFatalCodeCaseInsensitive(t *testing.T) {
	for _, code := range []string{"INVALID_GRANT", "Refresh_Token_Expired", "TOKEN_EXPIRED"} {
		t.Run(code, func(t *testing.T) {
			m := newMockRefresh(t, refreshStep{status: 400, body: fmt.Sprintf(`{"error":{"code":%q}}`, code)})
			auth := OAuthWithRotation("rt-0")
			_, err := auth.Authorization(context.Background())
			var re *RefreshOAuthError
			if !errors.As(err, &re) {
				t.Fatalf("大小写不敏感判死失败, got %T: %v", err, err)
			}
			if m.callCount() != 1 {
				t.Fatalf("refresh 次数 = %d, 期望 1", m.callCount())
			}
		})
	}
	// OAuth 标准形态 {"error":"invalid_grant"}（顶层字符串）
	m2 := newMockRefresh(t, refreshStep{status: 400, body: `{"error":"invalid_grant"}`})
	auth := OAuthWithRotation("rt-0")
	_, err := auth.Authorization(context.Background())
	var re *RefreshOAuthError
	if !errors.As(err, &re) || re.Code != "invalid_grant" {
		t.Fatalf("OAuth 标准形态应判死, got %T %v", err, err)
	}
	if m2.callCount() != 1 {
		t.Fatalf("refresh 次数 = %d, 期望 1", m2.callCount())
	}
}

// TestRotationEndpoint401UnconditionalFatal：token 端点 401 无条件判死
// （无论错误码，对齐 codex manager.rs:1537-1538）。
func TestRotationEndpoint401UnconditionalFatal(t *testing.T) {
	for _, body := range []string{`{"error":"something_unknown"}`, `{}`, ``} {
		m := newMockRefresh(t, refreshStep{status: 401, body: body})
		auth := OAuthWithRotation("rt-0")
		_, err := auth.Authorization(context.Background())
		var re *RefreshOAuthError
		if !errors.As(err, &re) {
			t.Fatalf("端点 401 应无条件判死, got %T: %v", err, err)
		}
		if body == "" && re.Code != "unauthorized" {
			t.Fatalf("空 body 401 Code = %q, 期望 unauthorized", re.Code)
		}
		if m.callCount() != 1 {
			t.Fatalf("判死不重试, refresh 次数 = %d", m.callCount())
		}
	}
}

// TestRotationAccountDisabled：账号禁用类（400 org disabled / KYC、
// 402 deactivated_workspace / payment required 泛化）→ AccountDisabledError。
func TestRotationAccountDisabled(t *testing.T) {
	cases := []struct {
		name   string
		status int
		body   string
		detail string
	}{
		{"org disabled", 400, `{"error":{"message":"organization has been disabled. contact support"}}`, "organization has been disabled. contact support"},
		{"KYC", 400, `{"error":{"message":"identity verification is required"}}`, "identity verification is required"},
		{"deactivated workspace", 402, `{"detail":{"code":"deactivated_workspace"}}`, "deactivated_workspace"},
		{"payment required 泛化", 402, `{"error":{"code":"payment required"}}`, "payment required"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			m := newMockRefresh(t, refreshStep{status: tc.status, body: tc.body})
			auth := OAuthWithRotation("rt-0")
			_, err := auth.Authorization(context.Background())
			var ad *AccountDisabledError
			if !errors.As(err, &ad) {
				t.Fatalf("期望 *AccountDisabledError, got %T: %v", err, err)
			}
			if ad.StatusCode != tc.status {
				t.Fatalf("StatusCode = %d, 期望 %d", ad.StatusCode, tc.status)
			}
			if m.callCount() != 1 {
				t.Fatalf("判死不重试, refresh 次数 = %d", m.callCount())
			}
		})
	}
}

// TestHTTP401AutoRotate：HTTP 401 自动轮转——判死分类与重试策略解耦：
// 非判死 → 单飞 refresh → 自动重试一次；判死码 → Fatal 态 + 不重试；
// 二次 401 含判死码 → 仍判死（并发吊销不错过）。
func TestHTTP401AutoRotate(t *testing.T) {
	t.Run("非判死轮转重试一次", func(t *testing.T) {
		m := newMockRefresh(t, refreshStep{status: 200, body: `{"access_token":"at-new","refresh_token":"rt-new"}`})
		var auths []string
		var mu sync.Mutex
		respSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			mu.Lock()
			auths = append(auths, r.Header.Get("Authorization"))
			mu.Unlock()
			if r.Header.Get("Authorization") == "Bearer at-old" {
				w.WriteHeader(http.StatusUnauthorized)
				_, _ = w.Write([]byte(`{"error":{"type":"invalid_request_error"}}`))
				return
			}
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"id":"x"}`))
		}))
		t.Cleanup(respSrv.Close)

		auth := OAuthWithRotation("rt-0", WithInitialAccessToken("at-old"))
		hc := NewHTTPClient(auth, WithBaseURL(respSrv.URL))
		resp, err := hc.Do(context.Background(), []byte(`{}`))
		if err != nil {
			t.Fatalf("Do: %v", err)
		}
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("StatusCode = %d, 期望 200", resp.StatusCode)
		}
		mu.Lock()
		defer mu.Unlock()
		if m.callCount() != 1 {
			t.Fatalf("refresh 次数 = %d, 期望 1", m.callCount())
		}
		if len(auths) != 2 || auths[0] != "Bearer at-old" || auths[1] != "Bearer at-new" {
			t.Fatalf("请求序列 = %v, 期望 [Bearer at-old, Bearer at-new]", auths)
		}
	})

	t.Run("首次 401 判死码不重试且 OnAuthFatal 至多一次", func(t *testing.T) {
		var fatalCalls atomic.Int32
		m := newMockRefresh(t, refreshStep{status: 200, body: `{"access_token":"x"}`})
		var reqs atomic.Int32
		respSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			reqs.Add(1)
			w.WriteHeader(http.StatusUnauthorized)
			_, _ = w.Write([]byte(`{"error":{"code":"TOKEN_REVOKED"}}`)) // 大写验证大小写不敏感
		}))
		t.Cleanup(respSrv.Close)

		auth := OAuthWithRotation("rt-0", WithInitialAccessToken("at-old"),
			WithOnAuthFatal(func(err error) { fatalCalls.Add(1) }))
		hc := NewHTTPClient(auth, WithBaseURL(respSrv.URL))
		_, err := hc.Do(context.Background(), []byte(`{}`))
		var are *AuthPermanentlyRevokedError
		if !errors.As(err, &are) {
			t.Fatalf("期望 *AuthPermanentlyRevokedError, got %T: %v", err, err)
		}
		if are.Code != "token_revoked" {
			t.Fatalf("Code = %q, 期望 token_revoked", are.Code)
		}
		if m.callCount() != 0 || reqs.Load() != 1 {
			t.Fatalf("判死不 refresh 不重试: refresh=%d reqs=%d", m.callCount(), reqs.Load())
		}
		if fatalCalls.Load() != 1 {
			t.Fatalf("AT 判死应触发 OnAuthFatal 恰一次（与 RT 判死路径对称）, got %d", fatalCalls.Load())
		}
		// Fatal 态：第二次 Do 请求未发出（Authorization 直接报错）
		_, err = hc.Do(context.Background(), []byte(`{}`))
		if !errors.As(err, &are) {
			t.Fatalf("Fatal 态后应恒报错, got %T: %v", err, err)
		}
		if reqs.Load() != 1 {
			t.Fatalf("Fatal 态后不应发请求, reqs = %d", reqs.Load())
		}
		if fatalCalls.Load() != 1 {
			t.Fatalf("OnAuthFatal 应至多一次, got %d", fatalCalls.Load())
		}
	})

	t.Run("二次 401 判死码仍判死且 OnAuthFatal 至多一次", func(t *testing.T) {
		var fatalCalls atomic.Int32
		m := newMockRefresh(t, refreshStep{status: 200, body: `{"access_token":"at-new"}`})
		var reqs atomic.Int32
		respSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			reqs.Add(1)
			if r.Header.Get("Authorization") == "Bearer at-old" {
				w.WriteHeader(http.StatusUnauthorized)
				_, _ = w.Write([]byte(`{"error":{"type":"invalid_request_error"}}`))
				return
			}
			w.WriteHeader(http.StatusUnauthorized)
			_, _ = w.Write([]byte(`{"error":{"code":"token_invalidated"}}`)) // 二次 401 判死
		}))
		t.Cleanup(respSrv.Close)

		auth := OAuthWithRotation("rt-0", WithInitialAccessToken("at-old"),
			WithOnAuthFatal(func(err error) { fatalCalls.Add(1) }))
		hc := NewHTTPClient(auth, WithBaseURL(respSrv.URL))
		_, err := hc.Do(context.Background(), []byte(`{}`))
		var are *AuthPermanentlyRevokedError
		if !errors.As(err, &are) {
			t.Fatalf("二次 401 判死码应判死, got %T: %v", err, err)
		}
		if are.Code != "token_invalidated" {
			t.Fatalf("Code = %q, 期望 token_invalidated", are.Code)
		}
		if m.callCount() != 1 || reqs.Load() != 2 {
			t.Fatalf("应恰一次 refresh + 两次请求: refresh=%d reqs=%d", m.callCount(), reqs.Load())
		}
		if fatalCalls.Load() != 1 {
			t.Fatalf("二次 401 判死应触发 OnAuthFatal 恰一次, got %d", fatalCalls.Load())
		}
	})

	t.Run("PAT 401 含判死码体触发判死", func(t *testing.T) {
		// PAT 已升级为可判死 Auth：401 先判死分类，命中致命码 → AuthPermanentlyRevokedError。
		respSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusUnauthorized)
			_, _ = w.Write([]byte(`{"error":{"code":"token_revoked"}}`))
		}))
		t.Cleanup(respSrv.Close)

		var fatalCalls atomic.Int32
		auth := PAT("p", WithPATOnAuthFatal(func(err error) { fatalCalls.Add(1) }))
		hc := NewHTTPClient(auth, WithBaseURL(respSrv.URL))
		_, err := hc.Do(context.Background(), []byte(`{}`))
		var ape *AuthPermanentlyRevokedError
		if !errors.As(err, &ape) {
			t.Fatalf("PAT 判死应返回 *AuthPermanentlyRevokedError, got %T: %v", err, err)
		}
		if ape.Code != "token_revoked" {
			t.Fatalf("Code = %q, 期望 token_revoked", ape.Code)
		}
		if fatalCalls.Load() != 1 {
			t.Fatalf("PAT 判死应触发 OnAuthFatal 恰一次, got %d", fatalCalls.Load())
		}
		// 毒化后后续 Authorization 恒错
		if _, err := auth.Authorization(context.Background()); !errors.As(err, &ape) {
			t.Fatalf("毒化后应恒返回致命错误, got %v", err)
		}
	})
}

// startWSAuthGate 起 WS 升级 mock：Authorization ∈ wantAuth 时升级回显，
// 否则返回 401。wantAuth 为空 = 恒 401（用于 Refreshed 标记与 PAT 不轮转测试）。
func startWSAuthGate(t *testing.T, wantAuth ...string) (string, *echoState) {
	t.Helper()
	st := &echoState{}
	accept := make(map[string]struct{}, len(wantAuth))
	for _, a := range wantAuth {
		accept[a] = struct{}{}
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		st.mu.Lock()
		st.authHeader = r.Header.Get("Authorization")
		st.mu.Unlock()
		if _, ok := accept[r.Header.Get("Authorization")]; !ok {
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
	return srv.URL, st
}

// TestWSDial401Rotation：WS 升级 401 → 单飞 refresh → 自动重连一次；
// 仍 401 → DialError.Refreshed=true；PAT/oauthAuth 不轮转（Refreshed=false）。
func TestWSDial401Rotation(t *testing.T) {
	t.Run("401 轮转重连成功", func(t *testing.T) {
		m := newMockRefresh(t, refreshStep{status: 200, body: `{"access_token":"at-new","refresh_token":"rt-new"}`})
		url, _ := startWSAuthGate(t, "Bearer at-new") // 第一次升级（旧 at）→ 401，第二次（新 at）→ 升级
		auth := OAuthWithRotation("rt-0", WithInitialAccessToken("at-old"))
		c, err := Dial(context.Background(), auth, WithBaseURL(url))
		if err != nil {
			t.Fatalf("Dial: %v", err)
		}
		defer c.Close(StatusGoingAway, "")
		if m.callCount() != 1 {
			t.Fatalf("refresh 次数 = %d, 期望 1", m.callCount())
		}
		// 重连后连接可用
		if err := c.Send(context.Background(), []byte(`{"type":"ping"}`)); err != nil {
			t.Fatalf("重连后 Send: %v", err)
		}
	})

	t.Run("仍 401 Refreshed=true", func(t *testing.T) {
		m := newMockRefresh(t, refreshStep{status: 200, body: `{"access_token":"at-new"}`})
		url, _ := startWSAuthGate(t) // 恒 401
		auth := OAuthWithRotation("rt-0", WithInitialAccessToken("at-old"))
		_, err := Dial(context.Background(), auth, WithBaseURL(url))
		var de *DialError
		if !errors.As(err, &de) {
			t.Fatalf("期望 *DialError, got %T: %v", err, err)
		}
		if de.StatusCode != http.StatusUnauthorized || !de.Refreshed {
			t.Fatalf("期望 DialError{401, Refreshed:true}, got %+v", de)
		}
		if m.callCount() != 1 {
			t.Fatalf("refresh 次数 = %d, 期望 1", m.callCount())
		}
	})

	t.Run("PAT 不轮转 Refreshed=false", func(t *testing.T) {
		url, _ := startWSAuthGate(t)
		_, err := Dial(context.Background(), PAT("wrong"), WithBaseURL(url))
		var de *DialError
		if !errors.As(err, &de) {
			t.Fatalf("期望 *DialError, got %T: %v", err, err)
		}
		if de.StatusCode != http.StatusUnauthorized || de.Refreshed {
			t.Fatalf("PAT 场景应原样返回 DialError{401}, got %+v", de)
		}
	})

	t.Run("oauthAuth 不轮转 Refreshed=false", func(t *testing.T) {
		url, _ := startWSAuthGate(t)
		_, err := Dial(context.Background(), OAuth(func(ctx context.Context) (string, error) {
			return "wrong", nil
		}), WithBaseURL(url))
		var de *DialError
		if !errors.As(err, &de) {
			t.Fatalf("期望 *DialError, got %T: %v", err, err)
		}
		if de.StatusCode != http.StatusUnauthorized || de.Refreshed {
			t.Fatalf("oauthAuth 场景应原样返回 DialError{401}, got %+v", de)
		}
	})

	t.Run("401 后 refresh 判死透传", func(t *testing.T) {
		m := newMockRefresh(t, refreshStep{status: 400, body: `{"error":{"code":"invalid_grant"}}`})
		url, _ := startWSAuthGate(t) // 恒 401 → 触发 refresh → 判死
		auth := OAuthWithRotation("rt-0", WithInitialAccessToken("at-old"))
		_, err := Dial(context.Background(), auth, WithBaseURL(url))
		var re *RefreshOAuthError
		if !errors.As(err, &re) {
			t.Fatalf("refresh 判死应透传（不被 DialError 吞掉）, got %T: %v", err, err)
		}
		if re.Code != "invalid_grant" {
			t.Fatalf("Code = %q, 期望 invalid_grant", re.Code)
		}
		if m.callCount() != 1 {
			t.Fatalf("refresh 次数 = %d, 期望 1（判死不重试）", m.callCount())
		}
	})
}

// TestRotationInvalidateFatal：Invalidate 下次取新 at；Fatal 后 Authorization
// 恒报错；Fatal 与并发 Authorization 竞态安全。
func TestRotationInvalidateFatal(t *testing.T) {
	m := newMockRefresh(t, refreshStep{status: 200, body: `{"access_token":"at-1","refresh_token":"rt-1"}`})
	auth := OAuthWithRotation("rt-0", WithInitialAccessToken("at-0"))
	if h, _ := auth.Authorization(context.Background()); h != "Bearer at-0" {
		t.Fatalf("初始 at = %q", h)
	}
	auth.Invalidate()
	if h, err := auth.Authorization(context.Background()); err != nil || h != "Bearer at-1" {
		t.Fatalf("Invalidate 后应取新 at: %q %v", h, err)
	}

	// Fatal 显式终止（网关解析 WS 判死事件时调用）
	boom := errors.New("account terminated by gateway")
	auth.Fatal(boom)
	_, err := auth.Authorization(context.Background())
	if !errors.Is(err, boom) {
		t.Fatalf("Fatal 后应返回该错误, got %v", err)
	}
	if m.callCount() != 1 {
		t.Fatalf("Fatal 后不应 refresh, 次数 = %d", m.callCount())
	}
	// Fatal(nil) 忽略
	auth.Fatal(nil)
	if _, err := auth.Authorization(context.Background()); !errors.Is(err, boom) {
		t.Fatalf("Fatal(nil) 应忽略: %v", err)
	}

	// 并发竞态安全：Fatal 与 N 并发 Authorization——结果均为 fatal 错误或成功头，
	// 无 panic；Fatal 完成后恒报错
	m2 := newMockRefresh(t, refreshStep{status: 200, body: `{"access_token":"at-1"}`})
	m2.block = make(chan struct{})
	auth2 := OAuthWithRotation("rt-0")
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _ = auth2.Authorization(context.Background())
		}()
	}
	waitFor(t, func() bool { return m2.callCount() >= 1 })
	close(m2.block) // 放行后 Fatal 与等待者并发
	auth2.Fatal(boom)
	wg.Wait()
	for i := 0; i < 16; i++ {
		if _, err := auth2.Authorization(context.Background()); !errors.Is(err, boom) {
			t.Fatalf("Fatal 完成后 Authorization 应恒报错, got %v", err)
		}
	}
}

// TestPATAndOAuthNoop：PAT 已升级为可判死（Fatal 毒化、Invalidate 仍 no-op），
// oauthAuth 保持 no-op。
func TestPATAndOAuthNoop(t *testing.T) {
	pat := PAT("p")
	pat.Invalidate()
	if h, err := pat.Authorization(context.Background()); err != nil || h != "Bearer p" {
		t.Fatalf("PAT Invalidate 应 no-op: %q %v", h, err)
	}
	// PAT Fatal 毒化（不再 no-op）
	pat2 := PAT("p")
	pat2.Fatal(errors.New("x"))
	if _, err := pat2.Authorization(context.Background()); err == nil {
		t.Fatal("PAT Fatal 后应毒化")
	}
	// Fatal(nil) 忽略
	pat3 := PAT("p")
	pat3.Fatal(nil)
	if h, err := pat3.Authorization(context.Background()); err != nil || h != "Bearer p" {
		t.Fatalf("PAT Fatal(nil) 应忽略: %q %v", h, err)
	}
	oa := OAuth(func(ctx context.Context) (string, error) { return "t", nil })
	oa.Invalidate()
	oa.Fatal(errors.New("x"))
	if h, err := oa.Authorization(context.Background()); err != nil || h != "Bearer t" {
		t.Fatalf("oauthAuth no-op 不破坏鉴权: %q %v", h, err)
	}
}

// TestRotationRTMissingTokenRetained：RefreshResponse 缺 refresh_token 时
// 内存旧 rt 保留（仅非空覆盖，防空 rt 永久失败）——回调与后续 refresh 均用
// 保留值（网关盲写 upsert 不落空）。
func TestRotationRTMissingTokenRetained(t *testing.T) {
	m := newMockRefresh(t,
		refreshStep{status: 200, body: `{"access_token":"at-1"}`}, // 无 refresh_token
		refreshStep{status: 200, body: `{"access_token":"at-2","refresh_token":"rt-2"}`},
	)
	var cbAt, cbRt string
	auth := OAuthWithRotation("rt-original", WithOnTokenRotated(func(at, rt string) {
		cbAt, cbRt = at, rt
	}))
	if h, err := auth.Authorization(context.Background()); err != nil || h != "Bearer at-1" {
		t.Fatalf("Authorization: %q %v", h, err)
	}
	// 回调应收到保留后的旧 rt（而非响应原始空值）
	if cbAt != "at-1" || cbRt != "rt-original" {
		t.Fatalf("回调收到 (%q, %q), 期望 (at-1, rt-original)——缺 rt 时应传保留值", cbAt, cbRt)
	}
	auth.Invalidate()
	if h, err := auth.Authorization(context.Background()); err != nil || h != "Bearer at-2" {
		t.Fatalf("Authorization #2: %q %v", h, err)
	}
	// 第二次 refresh 请求体应使用旧 rt（响应缺 refresh_token → 保留 rt-original）
	req := m.reqJSON(t, 1)
	if req["refresh_token"] != "rt-original" {
		t.Fatalf("缺省保留失败: refresh_token = %q, 期望 rt-original", req["refresh_token"])
	}
}

// TestRotationCtxCancel：refresh ctx 取消/超时 → 单飞释放、下次可重试。
func TestRotationCtxCancel(t *testing.T) {
	m := newMockRefresh(t, refreshStep{status: 200, body: `{"access_token":"at-1"}`})
	m.block = make(chan struct{})
	auth := OAuthWithRotation("rt-0")

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	_, err := auth.Authorization(ctx)
	cancel()
	if err == nil {
		t.Fatal("ctx 超时应返回错误")
	}
	close(m.block) // 释放被阻塞的 refresh handler
	if m.callCount() != 1 {
		t.Fatalf("首轮 refresh 次数 = %d, 期望 1", m.callCount())
	}
	// 单飞已释放：新 ctx 可再次 refresh 并成功
	h, err := auth.Authorization(context.Background())
	if err != nil || h != "Bearer at-1" {
		t.Fatalf("下次可重试: %q %v", h, err)
	}
	if m.callCount() != 2 {
		t.Fatalf("第二轮 refresh 次数 = %d, 期望 2（单飞释放后可重试）", m.callCount())
	}
}

// TestRotationCallbackRetry：OnTokenRotated 失败重试（D4）——失败未达阈值
// 本次 at 放行、下次 refresh 前重试；连续失败达阈值 → OnAuthFatal。
func TestRotationCallbackRetry(t *testing.T) {
	t.Run("连续失败达阈值 OnAuthFatal", func(t *testing.T) {
		var fatalCalls atomic.Int32
		var cbCalls atomic.Int32
		m := newMockRefresh(t,
			refreshStep{status: 200, body: `{"access_token":"at-1","refresh_token":"rt-1"}`},
			refreshStep{status: 200, body: `{"access_token":"at-2","refresh_token":"rt-2"}`})
		auth := OAuthWithRotation("rt-0",
			WithOnTokenRotated(func(at, rt string) {
				cbCalls.Add(1)
				panic("db down") // 回调失败：SDK 恢复 panic 按 D4 处理
			}),
			WithOnAuthFatal(func(err error) { fatalCalls.Add(1) }),
			WithTokenRotatedRetry(3))

		// 轮转 1：回调失败（第 1 次），at 放行
		h, err := auth.Authorization(context.Background())
		if err != nil || h != "Bearer at-1" {
			t.Fatalf("回调失败本次 at 应放行: %q %v", h, err)
		}
		if cbCalls.Load() != 1 || fatalCalls.Load() != 0 {
			t.Fatalf("cb=%d fatal=%d, 期望 cb=1 fatal=0", cbCalls.Load(), fatalCalls.Load())
		}

		// Invalidate → refresh 2：先重试 pending（第 2 次失败）→ 新轮转回调（第 3 次失败）
		// → 达阈值 → OnAuthFatal + Fatal 态（*CallbackDeliveryError，errors.As 可区分）
		auth.Invalidate()
		_, err = auth.Authorization(context.Background())
		var cde *CallbackDeliveryError
		if !errors.As(err, &cde) {
			t.Fatalf("回调达阈值应返回 *CallbackDeliveryError, got %T: %v", err, err)
		}
		if cde.Attempts != 3 {
			t.Fatalf("Attempts = %d, 期望 3", cde.Attempts)
		}
		if cbCalls.Load() != 3 || fatalCalls.Load() != 1 {
			t.Fatalf("cb=%d fatal=%d, 期望 cb=3 fatal=1", cbCalls.Load(), fatalCalls.Load())
		}
		if m.callCount() != 2 {
			t.Fatalf("refresh 次数 = %d, 期望 2", m.callCount())
		}
		// 后续恒报错
		if _, err := auth.Authorization(context.Background()); err == nil {
			t.Fatal("Fatal 态后应报错")
		}
		if fatalCalls.Load() != 1 {
			t.Fatalf("OnAuthFatal 应至多一次, got %d", fatalCalls.Load())
		}
	})

	t.Run("失败后成功不触发 OnAuthFatal", func(t *testing.T) {
		var fatalCalls atomic.Int32
		var cbCalls atomic.Int32
		failures := 2
		m := newMockRefresh(t,
			refreshStep{status: 200, body: `{"access_token":"a1","refresh_token":"r1"}`},
			refreshStep{status: 200, body: `{"access_token":"a2","refresh_token":"r2"}`})
		auth := OAuthWithRotation("rt-0",
			WithOnTokenRotated(func(at, rt string) {
				cbCalls.Add(1)
				if failures > 0 {
					failures--
					panic("db down")
				}
			}),
			WithOnAuthFatal(func(err error) { fatalCalls.Add(1) }))

		// 轮转 1：回调失败（1/2）
		if h, err := auth.Authorization(context.Background()); err != nil || h != "Bearer a1" {
			t.Fatalf("轮转 1: %q %v", h, err)
		}
		// refresh 2：先重试 pending（2/2 失败）→ 新轮转回调成功 → pending 清除
		auth.Invalidate()
		if h, err := auth.Authorization(context.Background()); err != nil || h != "Bearer a2" {
			t.Fatalf("轮转 2: %q %v", h, err)
		}
		if cbCalls.Load() != 3 || fatalCalls.Load() != 0 {
			t.Fatalf("cb=%d fatal=%d, 期望 cb=3 fatal=0（成功即清除）", cbCalls.Load(), fatalCalls.Load())
		}
		if m.callCount() != 2 {
			t.Fatalf("refresh 次数 = %d, 期望 2", m.callCount())
		}
	})
}

// TestRotationEmptyRefreshToken：构造器拒绝空 refresh_token（防空 rt 永久失败）。
func TestRotationEmptyRefreshToken(t *testing.T) {
	defer func() {
		if p := recover(); p == nil {
			t.Fatal("空 refreshToken 应 panic")
		}
	}()
	OAuthWithRotation("")
}

// TestRotationAuthorizationZeroAlloc：Authorization 热路径零分配
// （缓存完整头值，仅 Load——不拼接不分配）。
func TestRotationAuthorizationZeroAlloc(t *testing.T) {
	auth := OAuthWithRotation("rt-0", WithInitialAccessToken("at-1"))
	if h, err := auth.Authorization(context.Background()); err != nil || h != "Bearer at-1" {
		t.Fatalf("Authorization: %q %v", h, err)
	}
	allocs := testing.AllocsPerRun(1000, func() {
		if _, err := auth.Authorization(context.Background()); err != nil {
			t.Fatalf("Authorization: %v", err)
		}
	})
	if allocs != 0 {
		t.Fatalf("Authorization 热路径分配 = %.1f, 期望 0", allocs)
	}
}

// TestRotationRefreshEmptyAT：refresh 响应缺 access_token → RefreshError
// （可重试非 fatal），耗尽后下次可再试。
func TestRotationRefreshEmptyAT(t *testing.T) {
	var fatalCalls atomic.Int32
	m := newMockRefresh(t,
		refreshStep{status: 200, body: `{"refresh_token":"rt-1"}`}, // 200 但无 access_token
		refreshStep{status: 200, body: `{"access_token":"at-1"}`})
	auth := OAuthWithRotation("rt-0", WithBackoff(time.Millisecond, 2),
		WithOnAuthFatal(func(err error) { fatalCalls.Add(1) }))
	h, err := auth.Authorization(context.Background())
	if err != nil || h != "Bearer at-1" {
		t.Fatalf("缺 at 应重试后成功: %q %v", h, err)
	}
	if m.callCount() != 2 {
		t.Fatalf("refresh 次数 = %d, 期望 2", m.callCount())
	}
	if fatalCalls.Load() != 0 {
		t.Fatal("缺 access_token 不应判死")
	}
}
