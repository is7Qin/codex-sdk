package codexsdk

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
)

// TestHTTPDoRaw：POST /v1/responses，鉴权/Beta/content-type 头注入 +
// 非流式响应原样交付（StatusCode + Raw，不做字段解析）。
func TestHTTPDoRaw(t *testing.T) {
	var gotAuth, gotBeta, gotContentType string
	var gotBody []byte
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		gotBeta = r.Header.Get("OpenAI-Beta")
		gotContentType = r.Header.Get("Content-Type")
		if r.URL.Path != "/v1/responses" {
			http.Error(w, "bad path: "+r.URL.Path, http.StatusNotFound)
			return
		}
		gotBody, _ = io.ReadAll(r.Body)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"id":"resp_123","type":"response","model":"gpt-5"}`))
	}))
	t.Cleanup(srv.Close)

	hc := NewHTTPClient(srv.URL+"/v1", PAT("pat-http"), WithBeta("2026-02-06"))
	resp, err := hc.Do(context.Background(), []byte(`{"model":"gpt-5","input":"hi"}`))
	if err != nil {
		t.Fatalf("Do: %v", err)
	}
	if gotAuth != "Bearer pat-http" {
		t.Fatalf("Authorization = %q", gotAuth)
	}
	if gotBeta != "responses_websockets=2026-02-06" {
		t.Fatalf("OpenAI-Beta = %q", gotBeta)
	}
	if gotContentType != "application/json" {
		t.Fatalf("Content-Type = %q", gotContentType)
	}
	if string(gotBody) != `{"model":"gpt-5","input":"hi"}` {
		t.Fatalf("body = %s", gotBody)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("StatusCode = %d, 期望 200", resp.StatusCode)
	}
	if !bytes.Equal(resp.Raw, []byte(`{"id":"resp_123","type":"response","model":"gpt-5"}`)) {
		t.Fatalf("Raw 应原样交付完整响应体: %s", resp.Raw)
	}
}

// TestHTTPDoErrorStatus：非 2xx 返回 *HTTPError（状态码 + 错误体原样交付）。
func TestHTTPDoErrorStatus(t *testing.T) {
	errorBody := []byte(`{"error":{"type":"invalid_request_error","message":"bad token"}}`)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusUnauthorized)
		_, _ = w.Write(errorBody)
	}))
	t.Cleanup(srv.Close)

	hc := NewHTTPClient(srv.URL, PAT("wrong"))
	_, err := hc.Do(context.Background(), []byte(`{}`))
	var he *HTTPError
	if !errors.As(err, &he) {
		t.Fatalf("期望 *HTTPError, got %T: %v", err, err)
	}
	if he.StatusCode != http.StatusUnauthorized {
		t.Fatalf("StatusCode = %d, 期望 401", he.StatusCode)
	}
	if !bytes.Equal(he.Raw, errorBody) {
		t.Fatalf("Raw 应原样交付错误体: %s", he.Raw)
	}
}

// TestHTTPStreamSSE：SSE 事件帧原样提取（data: 行 + [DONE] 终止），
// OAuth provider 恰好调用一次。
func TestHTTPStreamSSE(t *testing.T) {
	events := []string{
		`{"type":"response.output_text.delta","delta":"hel"}`,
		`{"type":"response.completed","id":"resp_1"}`,
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer oauth-1" {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		f := w.(http.Flusher)
		for _, ev := range events {
			fmt.Fprintf(w, "data: %s\n\n", ev)
			f.Flush()
		}
		_, _ = io.WriteString(w, "data: [DONE]\n\n")
		f.Flush()
	}))
	t.Cleanup(srv.Close)

	var providerCalls atomic.Int32
	hc := NewHTTPClient(srv.URL, OAuth(func(ctx context.Context) (string, error) {
		providerCalls.Add(1)
		return "oauth-1", nil
	}))

	// 回调内的原始字节引用 scanner 复用缓冲（仅在回调执行期间有效）——回调内拷贝。
	var got []string
	err := hc.Stream(context.Background(), []byte(`{"stream":true}`), func(raw []byte) error {
		got = append(got, string(raw))
		return nil
	})
	if err != nil {
		t.Fatalf("Stream: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("事件数 = %d, 期望 2", len(got))
	}
	if string(got[0]) != events[0] {
		t.Fatalf("事件0 = %s, 期望 %s", got[0], events[0])
	}
	if string(got[1]) != events[1] {
		t.Fatalf("事件1 = %s, 期望 %s", got[1], events[1])
	}
	if providerCalls.Load() != 1 {
		t.Fatalf("provider 调用次数 = %d, 期望 1", providerCalls.Load())
	}
}

// TestHTTPStreamErrorStatus：流式请求非 2xx 同样返回 *HTTPError。
func TestHTTPStreamErrorStatus(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusTooManyRequests)
		_, _ = w.Write([]byte(`{"error":{"type":"rate_limit_exceeded","message":"slow down"}}`))
	}))
	t.Cleanup(srv.Close)

	hc := NewHTTPClient(srv.URL, PAT("t"))
	err := hc.Stream(context.Background(), []byte(`{"stream":true}`), func(raw []byte) error { return nil })
	var he *HTTPError
	if !errors.As(err, &he) || he.StatusCode != http.StatusTooManyRequests {
		t.Fatalf("期望 429 *HTTPError, got %v", err)
	}
}

// TestHTTPCallbackErrorAbortsStream：回调错误立即终止读取并透传。
func TestHTTPCallbackErrorAbortsStream(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		f := w.(http.Flusher)
		_, _ = io.WriteString(w, "data: {\"type\":\"response.created\"}\n\n")
		f.Flush()
	}))
	t.Cleanup(srv.Close)

	hc := NewHTTPClient(srv.URL, PAT("t"))
	sentinel := errors.New("stop")
	err := hc.Stream(context.Background(), []byte(`{}`), func(raw []byte) error {
		return sentinel
	})
	if !errors.Is(err, sentinel) {
		t.Fatalf("回调错误应透传, got %v", err)
	}
}

// TestHTTPBaseURLResponsesSuffix：baseURL 已含 /responses 时不重复拼接。
func TestHTTPBaseURLResponsesSuffix(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/custom/responses" {
			http.Error(w, "bad path: "+r.URL.Path, http.StatusNotFound)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"id":"x"}`))
	}))
	t.Cleanup(srv.Close)

	hc := NewHTTPClient(srv.URL+"/custom/responses", PAT("t"))
	if _, err := hc.Do(context.Background(), []byte(`{}`)); err != nil {
		t.Fatalf("Do: %v", err)
	}
}

// TestHTTPNilAuth：auth 为 nil 直接报错。
func TestHTTPNilAuth(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(srv.Close)

	hc := NewHTTPClient(srv.URL, nil)
	if _, err := hc.Do(context.Background(), []byte(`{}`)); err == nil {
		t.Fatal("auth 为 nil 应报错")
	}
}
