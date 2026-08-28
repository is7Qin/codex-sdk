package codexsdk

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
)

// TestPAT401FatalViaClassify 401 携带致命码（双码 × 大小写变体）→
// *AuthPermanentlyRevokedError + OnAuthFatal 恰一次 + 后续 Authorization 恒错。
func TestPAT401FatalViaClassify(t *testing.T) {
	cases := []struct {
		name string
		body string
		want string
	}{
		{"revoked lower code", `{"error":{"code":"token_revoked"}}`, "token_revoked"},
		{"revoked upper code", `{"error":{"code":"TOKEN_REVOKED"}}`, "token_revoked"},
		{"revoked mixed type", `{"error":{"type":"Token_Revoked"}}`, "token_revoked"},
		{"invalidated lower code", `{"error":{"code":"token_invalidated"}}`, "token_invalidated"},
		{"invalidated upper type", `{"error":{"type":"TOKEN_INVALIDATED"}}`, "token_invalidated"},
		{"invalidated mixed code", `{"error":{"code":"Token_Invalidated"}}`, "token_invalidated"},
		// detail Unauthorized 路径（HTTP 面特有，帧面不含）
		{"detail unauthorized lower", `{"detail":"unauthorized"}`, "unauthorized"},
		{"detail unauthorized upper", `{"detail":"Unauthorized"}`, "unauthorized"},
		{"detail with spaces", `{"detail":"  UNAUTHORIZED  "}`, "unauthorized"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var fatalCalls atomic.Int32
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusUnauthorized)
				_, _ = w.Write([]byte(tc.body))
			}))
			t.Cleanup(srv.Close)

			auth := PAT("pat-1", WithPATOnAuthFatal(func(err error) { fatalCalls.Add(1) }))
			hc := NewHTTPClient(auth, WithTransport(newFixedTransport(t, "https://chatgpt.com/backend-api/codex/responses", srv.URL)))
			_, err := hc.Do(context.Background(), []byte(`{}`))
			var ape *AuthPermanentlyRevokedError
			if !errors.As(err, &ape) {
				t.Fatalf("期望 *AuthPermanentlyRevokedError, got %T: %v", err, err)
			}
			if ape.Code != tc.want {
				t.Fatalf("Code = %q, 期望 %q", ape.Code, tc.want)
			}
			if fatalCalls.Load() != 1 {
				t.Fatalf("OnAuthFatal 次数 = %d, 期望 1", fatalCalls.Load())
			}
			// 后续 Authorization 恒错（fail-closed）
			if _, err := auth.Authorization(context.Background()); !errors.As(err, &ape) {
				t.Fatalf("毒化后 Authorization 应恒返回致命错误, got %T: %v", err, err)
			}
			// 第二次 Do 直接走 Authorization 失败，不再触发二次回调
			_, err2 := hc.Do(context.Background(), []byte(`{}`))
			if !errors.As(err2, &ape) {
				t.Fatalf("第二次 Do 应恒返回致命错误, got %T: %v", err2, err2)
			}
			if fatalCalls.Load() != 1 {
				t.Fatalf("OnAuthFatal 应至多一次, got %d", fatalCalls.Load())
			}
		})
	}
}

// TestPAT401NonFatalPassthrough 非致命 401 原样返回 HTTPError{401}，不毒化不回调。
func TestPAT401NonFatalPassthrough(t *testing.T) {
	bodies := []string{
		`{"error":{"code":"invalid_request_error","message":"bad token"}}`,
		`{"error":{"type":"rate_limit_exceeded"}}`,
		`{}`,
		`{"error":{"code":"some_other"}}`,
	}
	for _, body := range bodies {
		var fatalCalls atomic.Int32
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusUnauthorized)
			_, _ = w.Write([]byte(body))
		}))
		// no t.Cleanup in loop: close explicitly
		auth := PAT("pat-1", WithPATOnAuthFatal(func(err error) { fatalCalls.Add(1) }))
		hc := NewHTTPClient(auth, WithTransport(newFixedTransport(t, "https://chatgpt.com/backend-api/codex/responses", srv.URL)))
		_, err := hc.Do(context.Background(), []byte(`{}`))
		var he *HTTPError
		if !errors.As(err, &he) {
			t.Fatalf("非致命 401 应返回 *HTTPError, body=%s got %T: %v", body, err, err)
		}
		if he.StatusCode != http.StatusUnauthorized {
			t.Fatalf("StatusCode = %d, 期望 401", he.StatusCode)
		}
		if fatalCalls.Load() != 0 {
			t.Fatalf("非致命不应触发 OnAuthFatal, body=%s", body)
		}
		if _, err := auth.Authorization(context.Background()); err != nil {
			t.Fatalf("非致命不应毒化, Authorization err=%v body=%s", err, body)
		}
		srv.Close()
	}
}

// TestPATFatalNilIgnored Fatal(nil) 忽略，不毒化。
func TestPATFatalNilIgnored(t *testing.T) {
	auth := PAT("pat-1", WithPATOnAuthFatal(func(err error) { t.Fatal("nil Fatal 不应回调") }))
	auth.Fatal(nil)
	if _, err := auth.Authorization(context.Background()); err != nil {
		t.Fatalf("Fatal(nil) 应忽略, got %v", err)
	}
	// 通过后仍可用
	if h, err := auth.Authorization(context.Background()); err != nil || h != "Bearer pat-1" {
		t.Fatalf("Authorization = %q %v, 期望 Bearer pat-1", h, err)
	}
}

// TestPATFatalPoisonNoCallback 公开 Fatal 毒化但不触发回调（rotationAuth 语义）。
func TestPATFatalPoisonNoCallback(t *testing.T) {
	var calls atomic.Int32
	auth := PAT("pat-1", WithPATOnAuthFatal(func(err error) { calls.Add(1) }))
	boom := errors.New("gateway explicit fatal")
	auth.Fatal(boom)
	if calls.Load() != 0 {
		t.Fatalf("公开 Fatal 不应触发 OnAuthFatal, got %d", calls.Load())
	}
	if _, err := auth.Authorization(context.Background()); !errors.Is(err, boom) {
		t.Fatalf("Fatal 后应毒化, got %v", err)
	}
	// double Fatal：CAS 幂等，仍不回调，仍为首次错误
	other := errors.New("other")
	auth.Fatal(other)
	if calls.Load() != 0 {
		t.Fatalf("二次 Fatal 不应回调, got %d", calls.Load())
	}
	if _, err := auth.Authorization(context.Background()); !errors.Is(err, boom) {
		t.Fatalf("二次 Fatal 应保持首次错误, got %v", err)
	}
}

// TestPATAuthFatalCallbackOnce 内部 authFatal 触发回调至多一次（CAS 胜者）。
func TestPATAuthFatalCallbackOnce(t *testing.T) {
	var calls atomic.Int32
	auth := PAT("pat-1", WithPATOnAuthFatal(func(err error) { calls.Add(1) }))
	// auth 接口为 Auth，需断言为 authFatalTrigger
	trigger, ok := auth.(authFatalTrigger)
	if !ok {
		t.Fatal("patAuth 应实现 authFatalTrigger")
	}
	boom := &AuthPermanentlyRevokedError{Code: "token_revoked", Raw: []byte(`{}`)}
	trigger.authFatal(boom)
	if calls.Load() != 1 {
		t.Fatalf("首次 authFatal 应回调一次, got %d", calls.Load())
	}
	// 二次不同错误：CAS 幂等，不再回调
	trigger.authFatal(&AuthPermanentlyRevokedError{Code: "token_invalidated"})
	if calls.Load() != 1 {
		t.Fatalf("二次 authFatal 不应重复回调, got %d", calls.Load())
	}
	if _, err := auth.Authorization(context.Background()); !errors.Is(err, boom) {
		t.Fatalf("毒化后应为首次错误, got %v", err)
	}
}

// TestPATDoesNotImplementRefreshTrigger PAT 不实现 refreshTrigger。
func TestPATDoesNotImplementRefreshTrigger(t *testing.T) {
	auth := PAT("pat-1")
	if _, ok := auth.(refreshTrigger); ok {
		t.Fatal("patAuth 不应实现 refreshTrigger")
	}
	if _, ok := auth.(authFatalTrigger); !ok {
		t.Fatal("patAuth 应实现 authFatalTrigger")
	}
}

// TestOAuthRotationRegressionReorderedGate 门控重排后 OAuthWithRotation 行为不变：
// 非致命 401 仍轮转重试一次；致命 401 仍判死 + OnAuthFatal。
func TestOAuthRotationRegressionReorderedGate(t *testing.T) {
	t.Run("非致命轮转重试一次", func(t *testing.T) {
		m := newMockRefresh(t, refreshStep{status: 200, body: `{"access_token":"at-new","refresh_token":"rt-new"}`})
		var auths []string
		respSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Header.Get("Authorization") == "Bearer at-old" {
				w.WriteHeader(http.StatusUnauthorized)
				_, _ = w.Write([]byte(`{"error":{"type":"invalid_request_error"}}`))
				return
			}
			auths = append(auths, r.Header.Get("Authorization"))
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"id":"x"}`))
		}))
		t.Cleanup(respSrv.Close)

		auth := OAuthWithRotation("rt-0", WithInitialAccessToken("at-old"))
		hc := NewHTTPClient(auth, WithTransport(newFixedTransport(t, "https://chatgpt.com/backend-api/codex/responses", respSrv.URL)))
		resp, err := hc.Do(context.Background(), []byte(`{}`))
		if err != nil {
			t.Fatalf("Do: %v", err)
		}
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("StatusCode = %d, 期望 200", resp.StatusCode)
		}
		if m.callCount() != 1 {
			t.Fatalf("refresh 次数 = %d, 期望 1", m.callCount())
		}
		if len(auths) != 1 || auths[0] != "Bearer at-new" {
			t.Fatalf("重试请求头 = %v, 期望 [Bearer at-new]", auths)
		}
	})

	t.Run("首次致命判死不重试", func(t *testing.T) {
		var fatalCalls atomic.Int32
		m := newMockRefresh(t, refreshStep{status: 200, body: `{"access_token":"x"}`})
		respSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusUnauthorized)
			_, _ = w.Write([]byte(`{"error":{"code":"token_revoked"}}`))
		}))
		t.Cleanup(respSrv.Close)

		auth := OAuthWithRotation("rt-0", WithInitialAccessToken("at-old"),
			WithOnAuthFatal(func(err error) { fatalCalls.Add(1) }))
		hc := NewHTTPClient(auth, WithTransport(newFixedTransport(t, "https://chatgpt.com/backend-api/codex/responses", respSrv.URL)))
		_, err := hc.Do(context.Background(), []byte(`{}`))
		var ape *AuthPermanentlyRevokedError
		if !errors.As(err, &ape) {
			t.Fatalf("期望 *AuthPermanentlyRevokedError, got %T: %v", err, err)
		}
		if m.callCount() != 0 {
			t.Fatalf("判死不应 refresh, got %d", m.callCount())
		}
		if fatalCalls.Load() != 1 {
			t.Fatalf("致命应触发 OnAuthFatal 恰一次, got %d", fatalCalls.Load())
		}
	})

	t.Run("二次致命仍判死", func(t *testing.T) {
		var fatalCalls atomic.Int32
		m := newMockRefresh(t, refreshStep{status: 200, body: `{"access_token":"at-new"}`})
		respSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Header.Get("Authorization") == "Bearer at-old" {
				w.WriteHeader(http.StatusUnauthorized)
				_, _ = w.Write([]byte(`{"error":{"type":"invalid_request_error"}}`))
				return
			}
			w.WriteHeader(http.StatusUnauthorized)
			_, _ = w.Write([]byte(`{"error":{"code":"token_invalidated"}}`))
		}))
		t.Cleanup(respSrv.Close)

		auth := OAuthWithRotation("rt-0", WithInitialAccessToken("at-old"),
			WithOnAuthFatal(func(err error) { fatalCalls.Add(1) }))
		hc := NewHTTPClient(auth, WithTransport(newFixedTransport(t, "https://chatgpt.com/backend-api/codex/responses", respSrv.URL)))
		_, err := hc.Do(context.Background(), []byte(`{}`))
		var ape *AuthPermanentlyRevokedError
		if !errors.As(err, &ape) {
			t.Fatalf("二次致命应判死, got %T: %v", err, err)
		}
		if ape.Code != "token_invalidated" {
			t.Fatalf("Code = %q, 期望 token_invalidated", ape.Code)
		}
		if m.callCount() != 1 {
			t.Fatalf("应恰一次 refresh, got %d", m.callCount())
		}
		if fatalCalls.Load() != 1 {
			t.Fatalf("OnAuthFatal 应恰一次, got %d", fatalCalls.Load())
		}
	})
}

// TestCodesetConsistencyAnchor 三面共享 isATFatalCode：同一码集通过
// ClassifyAuthFatalFrame 与 HTTP 401（PAT/OAuth）一致命中，防码集分裂。
func TestCodesetConsistencyAnchor(t *testing.T) {
	codes := []string{"token_invalidated", "token_revoked", "TOKEN_INVALIDATED", "TOKEN_REVOKED", "Token_Revoked"}
	for _, code := range codes {
		// WS 帧面：ClassifyAuthFatalFrame
		frame := `{"type":"error","error":{"code":"` + code + `"}}`
		if got := ClassifyAuthFatalFrame([]byte(frame)); got == nil {
			t.Fatalf("ClassifyAuthFatalFrame 未命中 code=%q", code)
		} else if got.Code != strings.ToLower(code) {
			t.Fatalf("帧面 Code = %q, 期望 %q", got.Code, strings.ToLower(code))
		}
		frame2 := `{"type":"error","error":{"type":"` + code + `"}}`
		if got := ClassifyAuthFatalFrame([]byte(frame2)); got == nil {
			t.Fatalf("ClassifyAuthFatalFrame 未命中 type=%q", code)
		}

		// HTTP 面：PAT 401 分类（doURL 先判死路径）
		body := `{"error":{"code":"` + code + `"}}`
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusUnauthorized)
			_, _ = w.Write([]byte(body))
		}))
		auth := PAT("pat-x", WithPATOnAuthFatal(func(err error) {}))
		hc := NewHTTPClient(auth, WithTransport(newFixedTransport(t, "https://chatgpt.com/backend-api/codex/responses", srv.URL)))
		_, err := hc.Do(context.Background(), []byte(`{}`))
		var ape *AuthPermanentlyRevokedError
		if !errors.As(err, &ape) {
			t.Fatalf("PAT HTTP 401 未命中 code=%q got %T: %v", code, err, err)
		}
		if ape.Code != strings.ToLower(code) {
			t.Fatalf("HTTP Code = %q, 期望 %q", ape.Code, strings.ToLower(code))
		}
		srv.Close()

		// HTTP 面：OAuth 同码同样命中（回归证明共享）
		srv2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusUnauthorized)
			_, _ = w.Write([]byte(body))
		}))
		auth2 := OAuthWithRotation("rt-0", WithInitialAccessToken("at-old"))
		hc2 := NewHTTPClient(auth2, WithTransport(newFixedTransport(t, "https://chatgpt.com/backend-api/codex/responses", srv2.URL)))
		_, err2 := hc2.Do(context.Background(), []byte(`{}`))
		var ape2 *AuthPermanentlyRevokedError
		if !errors.As(err2, &ape2) {
			t.Fatalf("OAuth HTTP 401 未命中 code=%q got %T: %v", code, err2, err2)
		}
		srv2.Close()
	}

	// 反例：非致命码三面均不命中
	nonFatal := `{"type":"error","error":{"code":"invalid_request_error"}}`
	if got := ClassifyAuthFatalFrame([]byte(nonFatal)); got != nil {
		t.Fatalf("非致命码帧面不应命中, got %v", got)
	}
	body := `{"error":{"code":"invalid_request_error"}}`
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
		_, _ = w.Write([]byte(body))
	}))
	t.Cleanup(srv.Close)
	auth := PAT("pat-x")
	hc := NewHTTPClient(auth, WithTransport(newFixedTransport(t, "https://chatgpt.com/backend-api/codex/responses", srv.URL)))
	_, err := hc.Do(context.Background(), []byte(`{}`))
	var he *HTTPError
	if !errors.As(err, &he) {
		t.Fatalf("非致命 HTTP 应返回 HTTPError, got %T: %v", err, err)
	}
}
