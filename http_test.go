package codexsdk

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
)

// TestHTTPDoRaw：POST /v1/responses，鉴权/content-type 头注入 +
// 默认 codex UA/originator + HTTP beta（responses=v1，与 WS 不同）+
// 自动 trace 头 + 非流式响应原样交付（StatusCode + Raw，不做字段解析）。
func TestHTTPDoRaw(t *testing.T) {
	var gotAuth, gotBeta, gotContentType, gotUA, gotOriginator, gotTraceparent string
	var gotBody []byte
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		gotBeta = r.Header.Get("OpenAI-Beta")
		gotContentType = r.Header.Get("Content-Type")
		gotUA = r.Header.Get("User-Agent")
		gotOriginator = r.Header.Get("Originator")
		gotTraceparent = r.Header.Get("traceparent")
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

	hc := NewHTTPClient(srv.URL+"/v1", PAT("pat-http"))
	resp, err := hc.Do(context.Background(), []byte(`{"model":"gpt-5","input":"hi"}`))
	if err != nil {
		t.Fatalf("Do: %v", err)
	}
	if gotAuth != "Bearer pat-http" {
		t.Fatalf("Authorization = %q", gotAuth)
	}
	if gotBeta != HTTPBetaResponsesV1 {
		t.Fatalf("OpenAI-Beta = %q, 期望 %q（HTTP 与 WS 的 beta 值不同）", gotBeta, HTTPBetaResponsesV1)
	}
	if gotContentType != "application/json" {
		t.Fatalf("Content-Type = %q", gotContentType)
	}
	if gotUA != DefaultCodexUserAgent {
		t.Fatalf("User-Agent = %q, 期望默认 codex UA", gotUA)
	}
	if gotOriginator != DefaultOriginator {
		t.Fatalf("Originator = %q, 期望 %q", gotOriginator, DefaultOriginator)
	}
	if !traceparentRe.MatchString(gotTraceparent) {
		t.Fatalf("traceparent 格式不符: %q", gotTraceparent)
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

// TestHTTPBetaOverride：WithHeader 可覆盖 HTTP 默认 beta 头。
func TestHTTPBetaOverride(t *testing.T) {
	var gotBeta string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotBeta = r.Header.Get("OpenAI-Beta")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{}`))
	}))
	t.Cleanup(srv.Close)

	hc := NewHTTPClient(srv.URL, PAT("t"), WithHeader("OpenAI-Beta", "responses=v2"))
	if _, err := hc.Do(context.Background(), []byte(`{}`)); err != nil {
		t.Fatalf("Do: %v", err)
	}
	if gotBeta != "responses=v2" {
		t.Fatalf("OpenAI-Beta = %q, 期望覆盖为 responses=v2", gotBeta)
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

// countingListener 统计 TCP 连接数。
type countingListener struct {
	net.Listener
	accepts *atomic.Int32
}

func (l *countingListener) Accept() (net.Conn, error) {
	conn, err := l.Listener.Accept()
	if err == nil {
		l.accepts.Add(1)
	}
	return conn, err
}

// TestHTTPConnectionReuse：同一 HTTPClient 顺序请求复用连接（keep-alive）。
func TestHTTPConnectionReuse(t *testing.T) {
	var accepts atomic.Int32
	base, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"id":"x"}`))
	}))
	srv.Listener = &countingListener{Listener: base, accepts: &accepts}
	srv.Start()
	t.Cleanup(srv.Close)

	hc := NewHTTPClient(srv.URL, PAT("t"))
	for i := 0; i < 3; i++ {
		if _, err := hc.Do(context.Background(), []byte(`{}`)); err != nil {
			t.Fatalf("Do #%d: %v", i, err)
		}
	}
	if n := accepts.Load(); n != 1 {
		t.Fatalf("TCP 连接数 = %d, 期望 1（keep-alive 复用）", n)
	}
}
