package codexsdk

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
)

// TestSearchDefaultURLConst：DefaultSearchURL 常量值断言（无网络）。
func TestSearchDefaultURLConst(t *testing.T) {
	if DefaultSearchURL != "https://chatgpt.com/backend-api/codex/alpha/search" {
		t.Fatalf("DefaultSearchURL 应为完整 search 端点, got %q", DefaultSearchURL)
	}
}

// TestSearchDerivedPathAndMethod：Search 方法固定官方端点验证（请求 method + path 全量断言）。
func TestSearchDerivedPathAndMethod(t *testing.T) {
	var gotMethod, gotPath string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotMethod = r.Method
		gotPath = r.URL.Path
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	t.Cleanup(srv.Close)

	hc := NewHTTPClient(PAT("pat-search"), WithTransport(newFixedTransport(t, "https://chatgpt.com/backend-api/codex/alpha/search", srv.URL)))
	if _, err := hc.Search(context.Background(), []byte(`{}`)); err != nil {
		t.Fatalf("Search: %v", err)
	}
	if gotMethod != http.MethodPost {
		t.Fatalf("method = %q, 期望 POST", gotMethod)
	}
	if gotPath != "/backend-api/codex/alpha/search" {
		t.Fatalf("路径 = %q, 期望 /backend-api/codex/alpha/search", gotPath)
	}
}

// TestSearchPayloadAndRaw：payload 原样送达 + 响应体原样交付（opaque
// 透传——SDK 零解析请求/响应体，HTTPResponse.Raw 交付完整字节）。
// TurnState 恒空断言：Search 不捕获 turn-state（对齐 GenerateImage）。
func TestSearchPayloadAndRaw(t *testing.T) {
	var gotBody []byte
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotBody, _ = io.ReadAll(r.Body)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"results":[{"id":"r1"}]}`))
	}))
	t.Cleanup(srv.Close)

	hc := NewHTTPClient(PAT("pat-search"), WithTransport(newFixedTransport(t, "https://chatgpt.com/backend-api/codex/alpha/search", srv.URL)))
	resp, err := hc.Search(context.Background(), []byte(`{"query":"codex","limit":10}`))
	if err != nil {
		t.Fatalf("Search: %v", err)
	}
	if string(gotBody) != `{"query":"codex","limit":10}` {
		t.Fatalf("payload 应原样送达, got %s", gotBody)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("StatusCode = %d, 期望 200", resp.StatusCode)
	}
	if !bytes.Equal(resp.Raw, []byte(`{"results":[{"id":"r1"}]}`)) {
		t.Fatalf("Raw 应原样交付完整响应体: %s", resp.Raw)
	}
	if resp.TurnState != "" {
		t.Fatalf("Search 不捕获 turn-state（对齐 GenerateImage）, got %q", resp.TurnState)
	}
}

// TestSearchErrorStatus：非 2xx → *HTTPError（状态码 + 错误体原样交付）。
func TestSearchErrorStatus(t *testing.T) {
	errorBody := []byte(`{"error":{"type":"invalid_request_error","message":"bad request"}}`)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write(errorBody)
	}))
	t.Cleanup(srv.Close)

	hc := NewHTTPClient(PAT("wrong"), WithTransport(newFixedTransport(t, "https://chatgpt.com/backend-api/codex/alpha/search", srv.URL)))
	_, err := hc.Search(context.Background(), []byte(`{}`))
	var he *HTTPError
	if !errors.As(err, &he) {
		t.Fatalf("期望 *HTTPError, got %T: %v", err, err)
	}
	if he.StatusCode != http.StatusBadRequest {
		t.Fatalf("StatusCode = %d, 期望 400", he.StatusCode)
	}
	if !bytes.Equal(he.Raw, errorBody) {
		t.Fatalf("Raw 应原样交付错误体: %s", he.Raw)
	}
}

// TestSearchAuthorizationForms：Authorization 注入两形态——PAT 静态 +
// OAuth 静态 token（每次调用注入）。
func TestSearchAuthorizationForms(t *testing.T) {
	t.Run("PAT 静态", func(t *testing.T) {
		var gotAuth string
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			gotAuth = r.Header.Get("Authorization")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{}`))
		}))
		t.Cleanup(srv.Close)

		hc := NewHTTPClient(PAT("pat-search"), WithTransport(newFixedTransport(t, "https://chatgpt.com/backend-api/codex/alpha/search", srv.URL)))
		if _, err := hc.Search(context.Background(), []byte(`{}`)); err != nil {
			t.Fatalf("Search: %v", err)
		}
		if gotAuth != "Bearer pat-search" {
			t.Fatalf("Authorization = %q", gotAuth)
		}
	})

	t.Run("OAuth 静态 token", func(t *testing.T) {
		var gotAuth string
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			gotAuth = r.Header.Get("Authorization")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{}`))
		}))
		t.Cleanup(srv.Close)

		var providerCalls atomic.Int32
		hc := NewHTTPClient(OAuth(func(ctx context.Context) (string, error) {
			providerCalls.Add(1)
			return "oauth-static", nil
		}), WithTransport(newFixedTransport(t, "https://chatgpt.com/backend-api/codex/alpha/search", srv.URL)))
		if _, err := hc.Search(context.Background(), []byte(`{}`)); err != nil {
			t.Fatalf("Search: %v", err)
		}
		if gotAuth != "Bearer oauth-static" {
			t.Fatalf("Authorization = %q", gotAuth)
		}
		if providerCalls.Load() != 1 {
			t.Fatalf("provider 调用次数 = %d, 期望 1", providerCalls.Load())
		}
	})
}

// TestSearch401Rotate：401 自动轮转经 Search 方法全链路生效——非判死 401 →
// 单飞 refresh → 自动重试一次成功（复用 doURL 通道既有测试语义）。
func TestSearch401Rotate(t *testing.T) {
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
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	t.Cleanup(srv.Close)

	auth := OAuthWithRotation("rt-0", WithInitialAccessToken("at-old"))
	hc := NewHTTPClient(auth, WithTransport(newFixedTransport(t, "https://chatgpt.com/backend-api/codex/alpha/search", srv.URL)))
	resp, err := hc.Search(context.Background(), []byte(`{}`))
	if err != nil {
		t.Fatalf("Search: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("StatusCode = %d, 期望 200（轮转后应成功）", resp.StatusCode)
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

