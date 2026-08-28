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
// 字符串值里的裸词 "client_metadata" 不误判为已含 metadata，评审 P2-2。）
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

	// 预筛判据收紧回归（P2-2）：prompt 字符串值含裸词 "client_metadata"
	// （无引号包裹）→ 不触发短路，仍注入 turn_id
	if err := hc.Stream(context.Background(), []byte(`{"model":"m","prompt":"see client_metadata docs"}`), func(raw []byte) error { return nil }); err != nil {
		t.Fatalf("Stream #2: %v", err)
	}
	cm2 := gjson.GetBytes(gotBody, "client_metadata")
	if !cm2.Exists() || len(cm2.Map()) != 1 {
		t.Fatalf("裸词不应触发短路（期望仍注入仅 turn_id）, client_metadata = %s", cm2.Raw)
	}
	if turn := cm2.Get("turn_id").String(); !uuidv7Re.MatchString(turn) {
		t.Fatalf("turn_id = %q, 期望 UUIDv7 格式", turn)
	}
	if got := gjson.GetBytes(gotBody, "prompt").String(); got != "see client_metadata docs" {
		t.Fatalf("prompt 应原样保留: %s", gotBody)
	}
}

// TestHTTPStreamClientMetadataFullKeys：配置 CodexMeta（全键）+ WithSession
// （不同值）→ 恒 4 key + turn_id（meta 静态值优先于自动生成）+ 条件键全带
// （subagent/parent-thread-id/parent_turn_id/turn-metadata）；meta 与 session
// 同 key 时 meta 优先（对齐 WS 组装优先级 client.go:522-578）。不注入 trace
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

// TestHTTPStreamClientMetadataPassthrough：payload 已含 client_metadata →
// 预筛短路零注入（透传优先语义——真实客户端自带完整 metadata，注入仅面向
// 无 client_metadata 的组装请求体）：整包逐字节原样上送，不补键不覆盖
// （CodexMeta 配置不生效）。
func TestHTTPStreamClientMetadataPassthrough(t *testing.T) {
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
	// 预筛短路：整包零注入原样透传（含 client_metadata 即免注入，逐字节一致）
	if !bytes.Equal(gotBody, payload) {
		t.Fatalf("含 client_metadata 的 payload 应零注入原样上送（透传零改动）\n got: %s\nwant: %s", gotBody, payload)
	}
	// 缺 key 不补（评审 P3-D'）：payload 含 metadata 但缺 x-codex-installation-id
	// → 不补齐（透传优先语义——即使 CodexMeta 已配置）
	if gjson.GetBytes(gotBody, "client_metadata.x-codex-installation-id").Exists() {
		t.Fatalf("缺键不应补齐 x-codex-installation-id: %s", gotBody)
	}
}

// TestHTTPStreamClientMetadataQuotedValueEdge：判据边界（评审 P3-D'）——
// 字符串值恰为 "client_metadata"（JSON 值恒带引号，与键 token 不可区分）→
// 命中短路：零注入原样上送、不报错（仅跳过注入、请求合法——退化为缺
// metadata 而非错误）。
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
	payload := []byte(`{"model":"m","prompt":"client_metadata"}`)
	if err := hc.Stream(context.Background(), payload, func(raw []byte) error { return nil }); err != nil {
		t.Fatalf("Stream: %v", err)
	}
	if !bytes.Equal(gotBody, payload) {
		t.Fatalf("值恰为键名的字符串应短路零注入原样上送（不报错）\n got: %s\nwant: %s", gotBody, payload)
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
// 原样（对齐 responses.go:37 先例）→ 上游 400 原样透传。
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
