package codexsdk

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/tidwall/gjson"
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
		if r.URL.Path != "/backend-api/codex/responses" {
			http.Error(w, "bad path: "+r.URL.Path, http.StatusNotFound)
			return
		}
		gotBody, _ = io.ReadAll(r.Body)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"id":"resp_123","type":"response","model":"gpt-5"}`))
	}))
	t.Cleanup(srv.Close)

	hc := NewHTTPClient(PAT("pat-http"), WithTransport(newFixedTransport(t, "https://chatgpt.com/backend-api/codex/responses", srv.URL)))
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

	hc := NewHTTPClient(PAT("t"), WithTransport(newFixedTransport(t, "https://chatgpt.com/backend-api/codex/responses", srv.URL)), WithHeader("OpenAI-Beta", "responses=v2"))
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
	hc := NewHTTPClient(PAT("t"), WithTransport(newFixedTransport(t, "https://chatgpt.com/backend-api/codex/responses", srv.URL)))
	if _, err := hc.Do(context.Background(), []byte(`{}`)); err != nil {
		t.Fatalf("Do（默认）: %v", err)
	}
	if gotLite != "" {
		t.Fatalf("默认请求不应携带 %s, got %q", HeaderResponsesLite, gotLite)
	}

	// WithHeader 透传
	hc = NewHTTPClient(PAT("t"), WithTransport(newFixedTransport(t, "https://chatgpt.com/backend-api/codex/responses", srv.URL)), WithHeader(HeaderResponsesLite, "true"))
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

	hc := NewHTTPClient(PAT("wrong"), WithTransport(newFixedTransport(t, "https://chatgpt.com/backend-api/codex/responses", srv.URL)))
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
	}), WithTransport(newFixedTransport(t, "https://chatgpt.com/backend-api/codex/responses", srv.URL)))

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

	hc := NewHTTPClient(PAT("t"), WithTransport(newFixedTransport(t, "https://chatgpt.com/backend-api/codex/responses", srv.URL)))
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

	hc := NewHTTPClient(PAT("t"), WithTransport(newFixedTransport(t, "https://chatgpt.com/backend-api/codex/responses", srv.URL)))
	sentinel := errors.New("stop")
	err := hc.Stream(context.Background(), []byte(`{}`), func(raw []byte) error {
		return sentinel
	})
	if !errors.Is(err, sentinel) {
		t.Fatalf("回调错误应透传, got %v", err)
	}
}


// TestHTTPNilAuth：auth 为 nil 直接报错。
func TestHTTPNilAuth(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(srv.Close)

	hc := NewHTTPClient(nil, WithTransport(newFixedTransport(t, "https://chatgpt.com/backend-api/codex/responses", srv.URL)))
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

	hc := NewHTTPClient(PAT("t"), WithTransport(newFixedTransport(t, "https://chatgpt.com/backend-api/codex/responses", srv.URL)))
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

	hc := NewHTTPClient(PAT("t"), WithTransport(newFixedTransport(t, "https://chatgpt.com/backend-api/codex/responses", srv.URL)))
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

	hc := NewHTTPClient(PAT("t"), WithTransport(newFixedTransport(t, "https://chatgpt.com/backend-api/codex/responses", srv.URL)))
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

// ---- HTTP /responses client_metadata 注入（对齐真实 client_metadata()——
// responses_metadata.rs:255-288；Stream 发送前统一注入点）----

// TestHTTPStreamClientMetadataMinimal：未配置 meta/session → 请求体仅注入
// client_metadata.turn_id（UUIDv7 格式，真实恒发），无其他键。
// （兼证预筛判据：payload 无 "client_metadata" 键 → 仍注入 turn_id——
// 字符串值里的裸词 "client_metadata" 不误判为已含 metadata。）
func TestHTTPStreamClientMetadataMinimal(t *testing.T) {
	var gotBody []byte
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotBody, _ = io.ReadAll(r.Body)
		w.Header().Set("Content-Type", "text/event-stream")
		f := w.(http.Flusher)
		_, _ = io.WriteString(w, "data: {\"type\":\"response.completed\"}\n\ndata: [DONE]\n\n")
		f.Flush()
	}))
	t.Cleanup(srv.Close)

	hc := NewHTTPClient(PAT("t"), WithTransport(newFixedTransport(t, "https://chatgpt.com/backend-api/codex/responses", srv.URL)))
	if err := hc.Stream(context.Background(), []byte(`{"model":"m"}`), func(raw []byte) error { return nil }); err != nil {
		t.Fatalf("Stream: %v", err)
	}
	cm := gjson.GetBytes(gotBody, "client_metadata")
	if !cm.Exists() || len(cm.Map()) != 1 {
		t.Fatalf("client_metadata = %s（期望仅 turn_id）", cm.Raw)
	}
	if turn := cm.Get("turn_id").String(); !uuidv7Re.MatchString(turn) {
		t.Fatalf("turn_id = %q, 期望 UUIDv7 格式", turn)
	}
	if gjson.GetBytes(gotBody, "model").String() != "m" {
		t.Fatalf("注入不应动其余字段: %s", gotBody)
	}

	// 预筛判据收紧回归：instructions 字符串值含裸词 "client_metadata"
	// （无引号包裹）→ 不触发短路，仍注入 turn_id
	if err := hc.Stream(context.Background(), []byte(`{"model":"m","instructions":"see client_metadata docs"}`), func(raw []byte) error { return nil }); err != nil {
		t.Fatalf("Stream #2: %v", err)
	}
	cm2 := gjson.GetBytes(gotBody, "client_metadata")
	if !cm2.Exists() || len(cm2.Map()) != 1 {
		t.Fatalf("裸词不应触发短路（期望仍注入仅 turn_id）, client_metadata = %s", cm2.Raw)
	}
	if turn := cm2.Get("turn_id").String(); !uuidv7Re.MatchString(turn) {
		t.Fatalf("turn_id = %q, 期望 UUIDv7 格式", turn)
	}
	if got := gjson.GetBytes(gotBody, "instructions").String(); got != "see client_metadata docs" {
		t.Fatalf("instructions 应原样保留: %s", gotBody)
	}
}

// TestHTTPStreamClientMetadataFullKeys：配置 CodexMeta（全键）+ WithSession
// （不同值）→ 恒 4 key + turn_id（meta 静态值优先于自动生成）+ 条件键全带
// （subagent/parent-thread-id/parent_turn_id/turn-metadata）；meta 与 session
// 同 key 时 meta 优先（对齐 WS 组装优先级 client.go prepareFrame）。不注入 trace
// 与 turn-state 键（HTTP 体面真实不带）。
func TestHTTPStreamClientMetadataFullKeys(t *testing.T) {
	var gotBody []byte
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotBody, _ = io.ReadAll(r.Body)
		w.Header().Set("Content-Type", "text/event-stream")
		f := w.(http.Flusher)
		_, _ = io.WriteString(w, "data: {\"type\":\"response.completed\"}\n\ndata: [DONE]\n\n")
		f.Flush()
	}))
	t.Cleanup(srv.Close)

	hc := NewHTTPClient(PAT("t"), WithTransport(newFixedTransport(t, "https://chatgpt.com/backend-api/codex/responses", srv.URL)),
		WithCodexMeta(CodexMeta{
			InstallationID: "inst-meta",
			SessionID:      "sess-meta",
			ThreadID:       "thread-meta",
			TurnID:         "turn-meta",
			WindowID:       "win-meta:0",
			Subagent:       "sub-agent-1",
			ParentThreadID: "parent-thread-1",
			ParentTurnID:   "parent-turn-1",
			TurnMetadata:   `{"request_kind":"turn"}`,
		}),
		WithSession(Session{SessionID: "sess-s", ThreadID: "thread-s", WindowID: "win-s:1"}))
	if err := hc.Stream(context.Background(), []byte(`{"model":"m"}`), func(raw []byte) error { return nil }); err != nil {
		t.Fatalf("Stream: %v", err)
	}
	cm := gjson.GetBytes(gotBody, "client_metadata")
	if !cm.Exists() || len(cm.Map()) != 9 {
		t.Fatalf("client_metadata = %s（期望 4 恒键 + turn_id + 4 条件键 = 9）", cm.Raw)
	}
	// 恒 4 key：meta 值生效（session 同 key 值被 meta 覆盖）
	for key, want := range map[string]string{
		"x-codex-installation-id": "inst-meta",
		"session_id":              "sess-meta",
		"thread_id":               "thread-meta",
		"x-codex-window-id":       "win-meta:0",
	} {
		if got := cm.Get(key).String(); got != want {
			t.Fatalf("%s = %q, 期望 %q（CodexMeta 优先于 WithSession）", key, got, want)
		}
	}
	// turn_id：meta 静态值优先（不自动生成）
	if got := cm.Get("turn_id").String(); got != "turn-meta" {
		t.Fatalf("turn_id = %q, 期望 meta 静态值 turn-meta", got)
	}
	// 条件键：配置了才带
	for key, want := range map[string]string{
		"x-openai-subagent":       "sub-agent-1",
		"x-codex-parent-thread-id": "parent-thread-1",
		"parent_turn_id":           "parent-turn-1",
		"x-codex-turn-metadata":    `{"request_kind":"turn"}`,
	} {
		if got := cm.Get(key).String(); got != want {
			t.Fatalf("%s = %q, 期望 %q", key, got, want)
		}
	}
	// 不注入：trace 键（仅 WS 帧面）与 turn-state（体键不存在）
	if cm.Get("ws_request_header_traceparent").Exists() || cm.Get("ws_request_header_tracestate").Exists() {
		t.Fatalf("HTTP 体面不应注入 trace 键: %s", cm.Raw)
	}
	if cm.Get("x-codex-turn-state").Exists() {
		t.Fatalf("HTTP 体面不应注入 turn-state 键: %s", cm.Raw)
	}
}

// TestHTTPStreamClientMetadataOverride：payload 已含 client_metadata → 网关仍
// 注入并**覆盖**同 key（codex 面客户端自带的 client_metadata 永不透传）；
// CodexMeta 值恒为准，缺失键补齐。
func TestHTTPStreamClientMetadataOverride(t *testing.T) {
	var gotBody []byte
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotBody, _ = io.ReadAll(r.Body)
		w.Header().Set("Content-Type", "text/event-stream")
		f := w.(http.Flusher)
		_, _ = io.WriteString(w, "data: {\"type\":\"response.completed\"}\n\ndata: [DONE]\n\n")
		f.Flush()
	}))
	t.Cleanup(srv.Close)

	hc := NewHTTPClient(PAT("t"), WithTransport(newFixedTransport(t, "https://chatgpt.com/backend-api/codex/responses", srv.URL)),
		WithCodexMeta(CodexMeta{InstallationID: "inst-1", TurnID: "meta-turn", Subagent: "meta-sub"}))
	payload := []byte(`{"model":"m","client_metadata":{"turn_id":"payload-turn","x-openai-subagent":"keep"}}`)
	if err := hc.Stream(context.Background(), payload, func(raw []byte) error { return nil }); err != nil {
		t.Fatalf("Stream: %v", err)
	}
	// 覆盖：payload 内 turn_id/subagent 被 CodexMeta 值覆盖；缺失键补齐。
	if got := gjson.GetBytes(gotBody, "client_metadata.turn_id").String(); got != "meta-turn" {
		t.Fatalf("turn_id 应被网关覆盖为 meta-turn, got %q: %s", got, gotBody)
	}
	if got := gjson.GetBytes(gotBody, "client_metadata.x-openai-subagent").String(); got != "meta-sub" {
		t.Fatalf("subagent 应被网关覆盖为 meta-sub, got %q: %s", got, gotBody)
	}
	if got := gjson.GetBytes(gotBody, "client_metadata.x-codex-installation-id").String(); got != "inst-1" {
		t.Fatalf("缺失键应补齐 inst-1, got %q: %s", got, gotBody)
	}
	if got := gjson.GetBytes(gotBody, "model").String(); got != "m" {
		t.Fatalf("model 应保留, got %q", got)
	}
}

// TestHTTPStreamClientMetadataQuotedValueEdge：值恰为键名的字符串（如
// instructions="client_metadata"）不再触发任何短路——注入照常执行（原对象级
// 短路已移除）：instructions 值原样保留、client_metadata 正常注入。
func TestHTTPStreamClientMetadataQuotedValueEdge(t *testing.T) {
	var gotBody []byte
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotBody, _ = io.ReadAll(r.Body)
		w.Header().Set("Content-Type", "text/event-stream")
		f := w.(http.Flusher)
		_, _ = io.WriteString(w, "data: {\"type\":\"response.completed\"}\n\ndata: [DONE]\n\n")
		f.Flush()
	}))
	t.Cleanup(srv.Close)

	hc := NewHTTPClient(PAT("t"), WithTransport(newFixedTransport(t, "https://chatgpt.com/backend-api/codex/responses", srv.URL)))
	payload := []byte(`{"model":"m","instructions":"client_metadata"}`)
	if err := hc.Stream(context.Background(), payload, func(raw []byte) error { return nil }); err != nil {
		t.Fatalf("Stream: %v", err)
	}
	if got := gjson.GetBytes(gotBody, "instructions").String(); got != "client_metadata" {
		t.Fatalf("instructions 值应原样保留, got %q: %s", got, gotBody)
	}
	if v := gjson.GetBytes(gotBody, "client_metadata.turn_id").String(); !uuidv7Re.MatchString(v) {
		t.Fatalf("注入应照常执行（自动 turn_id UUIDv7）, got client_metadata=%s", gjson.GetBytes(gotBody, "client_metadata").Raw)
	}
}

// TestHTTPStreamClientMetadataSessionFallback：未配置 meta 仅 WithSession →
// session_id/thread_id/x-codex-window-id（session 值）+ 自动 turn_id（无静态
// 值）；installation_id 缺（仅 CodexMeta 可携带）。
func TestHTTPStreamClientMetadataSessionFallback(t *testing.T) {
	var gotBody []byte
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotBody, _ = io.ReadAll(r.Body)
		w.Header().Set("Content-Type", "text/event-stream")
		f := w.(http.Flusher)
		_, _ = io.WriteString(w, "data: {\"type\":\"response.completed\"}\n\ndata: [DONE]\n\n")
		f.Flush()
	}))
	t.Cleanup(srv.Close)

	hc := NewHTTPClient(PAT("t"), WithTransport(newFixedTransport(t, "https://chatgpt.com/backend-api/codex/responses", srv.URL)),
		WithSession(Session{SessionID: "sess-s", ThreadID: "thread-s", WindowID: "win-s:1"}))
	if err := hc.Stream(context.Background(), []byte(`{"model":"m"}`), func(raw []byte) error { return nil }); err != nil {
		t.Fatalf("Stream: %v", err)
	}
	cm := gjson.GetBytes(gotBody, "client_metadata")
	if !cm.Exists() || len(cm.Map()) != 4 {
		t.Fatalf("client_metadata = %s（期望 session 3 键 + turn_id）", cm.Raw)
	}
	for key, want := range map[string]string{
		"session_id":         "sess-s",
		"thread_id":          "thread-s",
		"x-codex-window-id":  "win-s:1",
	} {
		if got := cm.Get(key).String(); got != want {
			t.Fatalf("%s = %q, 期望 %q", key, got, want)
		}
	}
	if turn := cm.Get("turn_id").String(); !uuidv7Re.MatchString(turn) {
		t.Fatalf("turn_id = %q, 期望 UUIDv7 格式（无静态值时自动生成）", turn)
	}
	if cm.Get("x-codex-installation-id").Exists() {
		t.Fatalf("未配置 meta 不应注入 installation_id: %s", cm.Raw)
	}
}

// TestHTTPStreamClientMetadataInvalidJSON：非法 JSON payload → 放弃注入保持
// 原样（对齐 responses.go Responses() 的 gjson.ValidBytes 前置校验先例）→ 上游 400 原样透传。
func TestHTTPStreamClientMetadataInvalidJSON(t *testing.T) {
	errorBody := []byte(`{"error":{"code":"invalid_request","message":"bad json"}}`)
	var gotBody []byte
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotBody, _ = io.ReadAll(r.Body)
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write(errorBody)
	}))
	t.Cleanup(srv.Close)

	hc := NewHTTPClient(PAT("t"), WithTransport(newFixedTransport(t, "https://chatgpt.com/backend-api/codex/responses", srv.URL)),
		WithCodexMeta(CodexMeta{InstallationID: "inst-1"}))
	err := hc.Stream(context.Background(), []byte(`{"broken`), func(raw []byte) error { return nil })
	var he *HTTPError
	if !errors.As(err, &he) {
		t.Fatalf("期望 *HTTPError, got %T: %v", err, err)
	}
	if he.StatusCode != http.StatusBadRequest {
		t.Fatalf("StatusCode = %d, 期望 400", he.StatusCode)
	}
	if !bytes.Equal(he.Raw, errorBody) {
		t.Fatalf("Raw 应原样交付上游错误体: %s", he.Raw)
	}
	if string(gotBody) != `{"broken` {
		t.Fatalf("非法 payload 应原样上送（放弃注入）, got %s", gotBody)
	}
}

// ---- StreamBody：返回未关闭的 raw body（仅 200 接受） ----

// TestHTTPStreamBodyReturnsOnHeadersBeforeBodyEnds：收到响应头即返回（body
// 尚未结束）——handler 在首事件后阻塞，StreamBody 仍须先返回，且返回的 body
// 可被继续读取。
func TestHTTPStreamBodyReturnsOnHeadersBeforeBodyEnds(t *testing.T) {
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		f := w.(http.Flusher)
		_, _ = io.WriteString(w, "data: {\"type\":\"response.created\"}\n\n")
		f.Flush()
		select {
		case <-r.Context().Done():
		case <-release:
		}
		_, _ = io.WriteString(w, "data: [DONE]\n\n")
		f.Flush()
	}))
	t.Cleanup(srv.Close)
	t.Cleanup(func() { close(release) })

	hc := NewHTTPClient(PAT("t"), WithTransport(newFixedTransport(t, "https://chatgpt.com/backend-api/codex/responses", srv.URL)))

	type result struct {
		resp *http.Response
		err  error
	}
	got := make(chan result, 1)
	go func() {
		resp, err := hc.StreamBody(context.Background(), []byte(`{"stream":true}`))
		got <- result{resp, err}
	}()

	var resp *http.Response
	select {
	case r := <-got:
		if r.err != nil {
			t.Fatalf("StreamBody: %v", r.err)
		}
		resp = r.resp
	case <-time.After(3 * time.Second):
		t.Fatal("StreamBody 未在收到响应头后返回（body 尚在阻塞）")
	}
	t.Cleanup(func() { _ = resp.Body.Close() })

	// body 未结束即可读首事件（handler 仍阻塞在 release 上）。
	lineCh := make(chan string, 1)
	go func() {
		line, _ := bufio.NewReader(resp.Body).ReadString('\n')
		lineCh <- line
	}()
	select {
	case line := <-lineCh:
		if !strings.Contains(line, "response.created") {
			t.Fatalf("首事件读取 = %q", line)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("返回的 body 不可读首事件")
	}
}

// TestHTTPStreamBodySuccessDoesNotCloseBodyEarly：200 返回的 body 完整可读
// （未被 SDK 提前关闭/读尽）——调用方读到全部事件与末尾 [DONE]。
func TestHTTPStreamBodySuccessDoesNotCloseBodyEarly(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		f := w.(http.Flusher)
		_, _ = io.WriteString(w, "data: {\"type\":\"response.created\"}\n\n")
		f.Flush()
		_, _ = io.WriteString(w, "data: {\"type\":\"response.completed\"}\n\n")
		f.Flush()
		_, _ = io.WriteString(w, "data: [DONE]\n\n")
		f.Flush()
	}))
	t.Cleanup(srv.Close)

	hc := NewHTTPClient(PAT("t"), WithTransport(newFixedTransport(t, "https://chatgpt.com/backend-api/codex/responses", srv.URL)))
	resp, err := hc.StreamBody(context.Background(), []byte(`{"stream":true}`))
	if err != nil {
		t.Fatalf("StreamBody: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("StatusCode = %d, 期望 200", resp.StatusCode)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("读取 body：%v", err)
	}
	_ = resp.Body.Close()
	if !bytes.Contains(body, []byte("response.created")) || !bytes.Contains(body, []byte("response.completed")) {
		t.Fatalf("body 不完整: %s", body)
	}
	if !bytes.Contains(body, []byte("data: [DONE]")) {
		t.Fatalf("body 应原样含 [DONE]: %s", body)
	}
}

// TestHTTPStreamBodyRejectsNon200AndDrains：非 200（429）→ SDK 内读尽并关闭
// body 后返回 *HTTPError（含原始错误体）；连接可复用（排空+关闭完成，第二轮
// 复用同一 TCP 连接）。
func TestHTTPStreamBodyRejectsNon200AndDrains(t *testing.T) {
	var accepts atomic.Int32
	var reqs atomic.Int32
	errorBody := `{"error":{"type":"rate_limit_exceeded","message":"slow down"}}`
	base, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reqs.Add(1)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusTooManyRequests)
		_, _ = io.WriteString(w, errorBody)
	}))
	srv.Listener = &countingListener{Listener: base, accepts: &accepts}
	srv.Start()
	t.Cleanup(srv.Close)

	hc := NewHTTPClient(PAT("t"), WithTransport(newFixedTransport(t, "https://chatgpt.com/backend-api/codex/responses", srv.URL)))
	for i := 0; i < 2; i++ {
		resp, err := hc.StreamBody(context.Background(), []byte(`{"stream":true}`))
		if resp != nil {
			_ = resp.Body.Close()
			t.Fatalf("#%d 非 200 不应返回 resp", i+1)
		}
		var he *HTTPError
		if !errors.As(err, &he) {
			t.Fatalf("#%d 期望 *HTTPError, got %T: %v", i+1, err, err)
		}
		if he.StatusCode != http.StatusTooManyRequests {
			t.Fatalf("#%d StatusCode = %d, 期望 429", i+1, he.StatusCode)
		}
		if string(he.Raw) != errorBody {
			t.Fatalf("#%d Raw 应原样交付错误体: %s", i+1, he.Raw)
		}
	}
	if reqs.Load() != 2 {
		t.Fatalf("服务端请求数 = %d, 期望 2", reqs.Load())
	}
	if n := accepts.Load(); n != 1 {
		t.Fatalf("TCP 连接数 = %d, 期望 1（非 200 读尽+关闭 → 连接回池复用）", n)
	}
}

// TestHTTPStreamBody401Rotation：401 非判死 → 单飞 refresh → 自动重试一次成功；
// StreamBody 返回 200 的未关闭 body（新 at 序列）。
func TestHTTPStreamBody401Rotation(t *testing.T) {
	var mu sync.Mutex
	var auths []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		a := r.Header.Get("Authorization")
		mu.Lock()
		auths = append(auths, a)
		mu.Unlock()
		if a == "Bearer at-10" {
			w.WriteHeader(http.StatusUnauthorized)
			_, _ = io.WriteString(w, `{"error":{"type":"invalid_request_error"}}`)
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		f := w.(http.Flusher)
		_, _ = io.WriteString(w, "data: {\"type\":\"response.completed\"}\n\ndata: [DONE]\n\n")
		f.Flush()
	}))
	t.Cleanup(srv.Close)
	m := newMockRefresh(t, refreshStep{status: 200, body: `{"access_token":"at-new","refresh_token":"rt-new"}`})

	hc := NewHTTPClient(OAuthWithRotation("rt-0", WithInitialAccessToken("at-10")),
		WithTransport(newFixedTransport(t, "https://chatgpt.com/backend-api/codex/responses", srv.URL)))
	resp, err := hc.StreamBody(context.Background(), []byte(`{"stream":true}`))
	if err != nil {
		t.Fatalf("StreamBody: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("StatusCode = %d, 期望 200", resp.StatusCode)
	}
	if m.callCount() != 1 {
		t.Fatalf("refresh 次数 = %d, 期望 1（单飞）", m.callCount())
	}
	mu.Lock()
	defer mu.Unlock()
	if len(auths) != 2 || auths[0] != "Bearer at-10" || auths[1] != "Bearer at-new" {
		t.Fatalf("请求序列 = %v, 期望 [旧 at 401 → 新 at 重试]", auths)
	}
}

// TestHTTPStreamBodyCapturesTurnState：turn-state 取自**本次响应头**。
func TestHTTPStreamBodyCapturesTurnState(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set(HeaderTurnState, "st-body")
		w.Header().Set("Content-Type", "text/event-stream")
		f := w.(http.Flusher)
		_, _ = io.WriteString(w, "data: [DONE]\n\n")
		f.Flush()
	}))
	t.Cleanup(srv.Close)

	hc := NewHTTPClient(PAT("t"), WithTransport(newFixedTransport(t, "https://chatgpt.com/backend-api/codex/responses", srv.URL)))
	resp, err := hc.StreamBody(context.Background(), []byte(`{"stream":true}`))
	if err != nil {
		t.Fatalf("StreamBody: %v", err)
	}
	defer resp.Body.Close()
	if got := resp.Header.Get(HeaderTurnState); got != "st-body" {
		t.Fatalf("本次响应头 turn-state = %q, 期望 st-body", got)
	}
	if hc.TurnState() != "st-body" {
		t.Fatalf("TurnState = %q, 期望 st-body（本次响应头捕获）", hc.TurnState())
	}
}

// TestHTTPStreamBodyCancelTerminatesRead：调用方 ctx 取消能终止对返回 body 的
// 读取（读取沿用调用方 ctx）。
func TestHTTPStreamBodyCancelTerminatesRead(t *testing.T) {
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		f := w.(http.Flusher)
		_, _ = io.WriteString(w, "data: {\"type\":\"response.created\"}\n\n")
		f.Flush()
		select {
		case <-r.Context().Done():
		case <-release:
		}
	}))
	t.Cleanup(srv.Close)
	t.Cleanup(func() { close(release) })

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	hc := NewHTTPClient(PAT("t"), WithTransport(newFixedTransport(t, "https://chatgpt.com/backend-api/codex/responses", srv.URL)))
	resp, err := hc.StreamBody(ctx, []byte(`{"stream":true}`))
	if err != nil {
		t.Fatalf("StreamBody: %v", err)
	}
	defer resp.Body.Close()

	// 先读到首事件（确认 body 在读），再取消——后续读取须以错误返回。
	if _, err := bufio.NewReader(resp.Body).ReadString('\n'); err != nil {
		t.Fatalf("读首事件: %v", err)
	}

	readErr := make(chan error, 1)
	go func() { _, err := io.ReadAll(resp.Body); readErr <- err }()
	cancel()
	select {
	case err := <-readErr:
		if err == nil {
			t.Fatal("取消后读取应返回错误")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("取消未终止 body 读取")
	}
}

// redirectRewriteTransport 只把官方 responses URL 重写到 mock；其余 URL（如
// 重定向 Location 目标）按原样交给默认 transport——用于验证禁跟随（目标不被访问）。
type redirectRewriteTransport struct {
	official string
	srvBase  *url.URL
}

func (t *redirectRewriteTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	clone := req.Clone(req.Context())
	if req.URL.String() == t.official {
		u := *req.URL
		u.Scheme = t.srvBase.Scheme
		u.Host = t.srvBase.Host
		clone.URL = &u
	}
	return http.DefaultTransport.RoundTrip(clone)
}

// TestHTTPStreamBodyNoRedirect3xx StreamBody 禁跟随重定向：302/307 带有效
// Location → *HTTPError{302/307} 且不访问重定向目标（第二上游计数恒 0）。
// 仅 StreamBody 生效（do/Stream/其它方法既有跟随行为不变）。
func TestHTTPStreamBodyNoRedirect3xx(t *testing.T) {
	for _, code := range []int{http.StatusFound, http.StatusTemporaryRedirect} {
		code := code
		t.Run(http.StatusText(code), func(t *testing.T) {
			var target2Hits atomic.Int32
			target2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				target2Hits.Add(1)
				w.WriteHeader(http.StatusOK)
				_, _ = io.WriteString(w, "should-not-be-reached")
			}))
			t.Cleanup(target2.Close)

			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Location", target2.URL+"/next")
				w.WriteHeader(code)
			}))
			t.Cleanup(srv.Close)

			base, err := url.Parse(srv.URL)
			if err != nil {
				t.Fatalf("parse: %v", err)
			}
			tr := &redirectRewriteTransport{official: "https://chatgpt.com/backend-api/codex/responses", srvBase: base}
			hc := NewHTTPClient(PAT("t"), WithTransport(tr))

			resp, err := hc.StreamBody(context.Background(), []byte(`{"model":"m"}`))
			if resp != nil {
				_ = resp.Body.Close()
				t.Fatalf("3xx 不应返回未关闭 resp")
			}
			var he *HTTPError
			if !errors.As(err, &he) {
				t.Fatalf("期望 *HTTPError, got %T: %v", err, err)
			}
			if he.StatusCode != code {
				t.Fatalf("StatusCode = %d, 期望 %d（3xx 原样归非 200）", he.StatusCode, code)
			}
			if n := target2Hits.Load(); n != 0 {
				t.Fatalf("重定向目标被访问 %d 次（期望 0——StreamBody 禁跟随）", n)
			}
		})
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

	hc := NewHTTPClient(PAT("t"), WithTransport(newFixedTransport(t, "https://chatgpt.com/backend-api/codex/responses", srv.URL)))
	if err := hc.Stream(context.Background(), []byte(`{"stream":true}`), func(raw []byte) error { return nil }); err != nil {
		t.Fatalf("Stream: %v", err)
	}
	if hc.TurnState() != "st-stream" {
		t.Fatalf("Stream 响应头应捕获 turn-state, got %q", hc.TurnState())
	}
}
