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
// 默认 codex UA/originator + 不发 OpenAI-Beta 与 trace 头（真实客户端行为）
// + 非流式响应原样交付（StatusCode + Raw，不做字段解析）。
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

	// 内置上游 URL 下沉：WithBaseURL 覆盖值按完整 responses 端点语义直用
	// （不再自动拼接 /responses），故测试传完整端点 srv.URL+"/v1/responses"。
	hc := NewHTTPClient(PAT("pat-http"), WithBaseURL(srv.URL+"/v1/responses"))
	resp, err := hc.Do(context.Background(), []byte(`{"model":"gpt-5","input":"hi"}`))
	if err != nil {
		t.Fatalf("Do: %v", err)
	}
	if gotAuth != "Bearer pat-http" {
		t.Fatalf("Authorization = %q", gotAuth)
	}
	if gotBeta != "" {
		t.Fatalf("HTTP 默认不应发 OpenAI-Beta, got %q", gotBeta)
	}
	if gotTraceparent != "" {
		t.Fatalf("HTTP 默认不应发 traceparent, got %q", gotTraceparent)
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

// TestHTTPBetaOverride：WithHeader 显式注入 HTTP OpenAI-Beta（默认不发）。
func TestHTTPBetaOverride(t *testing.T) {
	var gotBeta string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotBeta = r.Header.Get("OpenAI-Beta")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{}`))
	}))
	t.Cleanup(srv.Close)

	hc := NewHTTPClient(PAT("t"), WithBaseURL(srv.URL), WithHeader("OpenAI-Beta", "responses=v2"))
	if _, err := hc.Do(context.Background(), []byte(`{}`)); err != nil {
		t.Fatalf("Do: %v", err)
	}
	if gotBeta != "responses=v2" {
		t.Fatalf("OpenAI-Beta = %q, 期望覆盖为 responses=v2", gotBeta)
	}
}

// TestHTTPResponsesLiteHeader：responses-lite internal 头透传（WithHeader）——
// 默认不带（非 lite 无此头），SDK 只透传不解析。
func TestHTTPResponsesLiteHeader(t *testing.T) {
	var gotLite string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotLite = r.Header.Get(HeaderResponsesLite)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{}`))
	}))
	t.Cleanup(srv.Close)

	// 默认：不带 lite 头
	hc := NewHTTPClient(PAT("t"), WithBaseURL(srv.URL))
	if _, err := hc.Do(context.Background(), []byte(`{}`)); err != nil {
		t.Fatalf("Do（默认）: %v", err)
	}
	if gotLite != "" {
		t.Fatalf("默认请求不应携带 %s, got %q", HeaderResponsesLite, gotLite)
	}

	// WithHeader 透传
	hc = NewHTTPClient(PAT("t"), WithBaseURL(srv.URL), WithHeader(HeaderResponsesLite, "true"))
	if _, err := hc.Do(context.Background(), []byte(`{}`)); err != nil {
		t.Fatalf("Do（lite）: %v", err)
	}
	if gotLite != "true" {
		t.Fatalf("lite 头透传失败, got %q", gotLite)
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

	hc := NewHTTPClient(PAT("wrong"), WithBaseURL(srv.URL))
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
	hc := NewHTTPClient(OAuth(func(ctx context.Context) (string, error) {
		providerCalls.Add(1)
		return "oauth-1", nil
	}), WithBaseURL(srv.URL))

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

	hc := NewHTTPClient(PAT("t"), WithBaseURL(srv.URL))
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

	hc := NewHTTPClient(PAT("t"), WithBaseURL(srv.URL))
	sentinel := errors.New("stop")
	err := hc.Stream(context.Background(), []byte(`{}`), func(raw []byte) error {
		return sentinel
	})
	if !errors.Is(err, sentinel) {
		t.Fatalf("回调错误应透传, got %v", err)
	}
}

// TestHTTPBaseURLFullEndpointSemantics：WithBaseURL 覆盖值按完整端点语义直用
// ——不再自动拼接 /responses（与旧版 baseURL 参数语义不同，显式行为变更）：
// 传 srv.URL+"/v1" 打 /v1 而非 /v1/responses；传完整端点原样命中。
func TestHTTPBaseURLFullEndpointSemantics(t *testing.T) {
	// 完整端点原样命中
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/custom/responses" {
			http.Error(w, "bad path: "+r.URL.Path, http.StatusNotFound)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"id":"x"}`))
	}))
	t.Cleanup(srv.Close)

	hc := NewHTTPClient(PAT("t"), WithBaseURL(srv.URL+"/custom/responses"))
	if _, err := hc.Do(context.Background(), []byte(`{}`)); err != nil {
		t.Fatalf("Do（完整端点）: %v", err)
	}

	// 不自动拼接 /responses：/v1 打 /v1
	srv2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1" {
			http.Error(w, "bad path: "+r.URL.Path, http.StatusNotFound)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"id":"x"}`))
	}))
	t.Cleanup(srv2.Close)

	hc2 := NewHTTPClient(PAT("t"), WithBaseURL(srv2.URL+"/v1"))
	if _, err := hc2.Do(context.Background(), []byte(`{}`)); err != nil {
		t.Fatalf("Do（/v1 应直用不拼接）: %v", err)
	}
}

// TestHTTPNilAuth：auth 为 nil 直接报错。
func TestHTTPNilAuth(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(srv.Close)

	hc := NewHTTPClient(nil, WithBaseURL(srv.URL))
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

	hc := NewHTTPClient(PAT("t"), WithBaseURL(srv.URL))
	for i := 0; i < 3; i++ {
		if _, err := hc.Do(context.Background(), []byte(`{}`)); err != nil {
			t.Fatalf("Do #%d: %v", i, err)
		}
	}
	if n := accepts.Load(); n != 1 {
		t.Fatalf("TCP 连接数 = %d, 期望 1（keep-alive 复用）", n)
	}
}

// TestHTTPTurnStateNoEcho：HTTP 请求绝不携带 x-codex-turn-state 头（真实
// codex 客户端行为——turn-state 仅响应侧）；响应头签发 → 自动捕获，经
// HTTPResponse.TurnState / TurnState() 暴露，不回传为请求头（头回传仅 WS 路径）。
func TestHTTPTurnStateNoEcho(t *testing.T) {
	var gotEcho string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotEcho = r.Header.Get(HeaderTurnState)
		w.Header().Set(HeaderTurnState, "st-http")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{}`))
	}))
	t.Cleanup(srv.Close)

	hc := NewHTTPClient(PAT("t"), WithBaseURL(srv.URL))
	for i := 0; i < 3; i++ {
		resp, err := hc.Do(context.Background(), []byte(`{}`))
		if err != nil {
			t.Fatalf("Do #%d: %v", i, err)
		}
		if gotEcho != "" {
			t.Fatalf("第 %d 次请求不应携带 x-codex-turn-state, got %q", i+1, gotEcho)
		}
		if resp.TurnState != "st-http" {
			t.Fatalf("HTTPResponse.TurnState = %q, 期望 st-http", resp.TurnState)
		}
		if hc.TurnState() != "st-http" {
			t.Fatalf("TurnState = %q, 期望 st-http（响应头自动捕获）", hc.TurnState())
		}
	}
}

// TestHTTPStreamDoneDrainsBodyForReuse：[DONE] 命中后排空残余 body——mock
// 上游发送 [DONE] + 尾部字节（不关闭连接），客户端返回后第二轮请求必须复用
// 同一连接（countingListener 断言 accepts==1）。排空完成是连接回池的前提：
// 残余未读使 http.Transport 判定连接不可复用，第二轮必重拨（accepts==2）。
func TestHTTPStreamDoneDrainsBodyForReuse(t *testing.T) {
	var accepts atomic.Int32
	var reqs atomic.Int32
	base, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reqs.Add(1)
		w.Header().Set("Content-Type", "text/event-stream")
		f := w.(http.Flusher)
		_, _ = io.WriteString(w, "data: {\"type\":\"response.completed\"}\n\n")
		f.Flush()
		_, _ = io.WriteString(w, "data: [DONE]\n\n")
		_, _ = io.WriteString(w, "trailing-bytes-after-done") // [DONE] 后残余字节
		f.Flush()
	}))
	srv.Listener = &countingListener{Listener: base, accepts: &accepts}
	srv.Start()
	t.Cleanup(srv.Close)

	hc := NewHTTPClient(PAT("t"), WithBaseURL(srv.URL))
	for i := 0; i < 2; i++ {
		if err := hc.Stream(context.Background(), []byte(`{}`), func(raw []byte) error { return nil }); err != nil {
			t.Fatalf("Stream #%d: %v", i+1, err)
		}
	}
	if n := reqs.Load(); n != 2 {
		t.Fatalf("服务端请求数 = %d, 期望 2（服务端收到第二轮请求 = 排空完成信号）", n)
	}
	if n := accepts.Load(); n != 1 {
		t.Fatalf("TCP 连接数 = %d, 期望 1（[DONE] 后排空 → 连接回池复用）", n)
	}
}

// TestHTTPStreamCapturesTurnState：Stream 响应头同样捕获 turn-state。
func TestHTTPStreamCapturesTurnState(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set(HeaderTurnState, "st-stream")
		w.Header().Set("Content-Type", "text/event-stream")
		f := w.(http.Flusher)
		_, _ = io.WriteString(w, "data: {\"type\":\"response.completed\"}\n\ndata: [DONE]\n\n")
		f.Flush()
	}))
	t.Cleanup(srv.Close)

	hc := NewHTTPClient(PAT("t"), WithBaseURL(srv.URL))
	if err := hc.Stream(context.Background(), []byte(`{"stream":true}`), func(raw []byte) error { return nil }); err != nil {
		t.Fatalf("Stream: %v", err)
	}
	if hc.TurnState() != "st-stream" {
		t.Fatalf("Stream 响应头应捕获 turn-state, got %q", hc.TurnState())
	}
}
