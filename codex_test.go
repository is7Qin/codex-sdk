package codexsdk

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/tidwall/gjson"
)

func BenchmarkRewriteEnvironmentContextMiss(b *testing.B) {
	raw := []byte(`{"type":"response.create","model":"gpt-5","input":[{"role":"user","content":"plain text without the environment block"}]}`)
	now := time.Date(2026, time.July, 4, 3, 30, 0, 0, time.UTC)
	b.ReportAllocs()
	b.SetBytes(int64(len(raw)))
	for b.Loop() {
		if out := RewriteEnvironmentContextTime(raw, now); len(out) != len(raw) {
			b.Fatal(len(out))
		}
	}
}

func BenchmarkRewriteEnvironmentContextHit(b *testing.B) {
	raw := []byte(`{"type":"response.create","model":"gpt-5","input":[{"role":"user","content":"<environment_context>\n  <current_date>2026-09-28</current_date>\n  <timezone>Asia/Shanghai</timezone>\n</environment_context>"}]}`)
	now := time.Date(2026, time.July, 4, 3, 30, 0, 0, time.UTC)
	b.ReportAllocs()
	b.SetBytes(int64(len(raw)))
	for b.Loop() {
		if out := RewriteEnvironmentContextTime(raw, now); len(out) == 0 {
			b.Fatal("empty")
		}
	}
}

func TestRewriteEnvironmentContextTime(t *testing.T) {
	now := time.Date(2026, time.July, 4, 3, 30, 0, 0, time.UTC)
	in := []byte(`{"input":[{"role":"user","content":"<environment_context>\n  <current_date>2026-09-28</current_date>\n  <timezone>Asia/Shanghai</timezone>\n</environment_context>"}]}`)
	out := RewriteEnvironmentContextTime(in, now)
	text := gjson.GetBytes(out, "input.0.content").String()
	if !strings.Contains(text, "<current_date>2026-07-03</current_date>") || !strings.Contains(text, "<timezone>America/New_York</timezone>") {
		t.Fatalf("环境时间未改成美东时间: %s", text)
	}
	if !strings.Contains(text, "<environment_context>") || !strings.Contains(text, "</environment_context>") {
		t.Fatalf("环境上下文边界被破坏: %s", text)
	}
	if got := RewriteEnvironmentContextTime([]byte(`{"input":"plain"}`), now); string(got) != `{"input":"plain"}` {
		t.Fatalf("无环境上下文应原样返回: %s", got)
	}
}

// TestFilterCodexWsPayload：WS 顶层 key 白名单过滤（纯函数）。
func TestFilterCodexWsPayload(t *testing.T) {
	in := []byte(`{"type":"response.create","model":"gpt-5","input":"hi","access_programs":["org-a"],"stream_options":{"reasoning_summary_delivery":"sequential_cutoff"},"max_output_tokens":4096,"evil":"x","foo":{"bar":1}}`)
	out, err := FilterCodexWsPayload(in)
	if err != nil {
		t.Fatalf("FilterCodexWsPayload: %v", err)
	}
	for _, k := range []string{"max_output_tokens", "evil", "foo"} {
		if gjson.GetBytes(out, k).Exists() {
			t.Fatalf("非白名单 key %q 应被删除: %s", k, out)
		}
	}
	if gjson.GetBytes(out, "type").String() != "response.create" ||
		gjson.GetBytes(out, "model").String() != "gpt-5" ||
		gjson.GetBytes(out, "input").String() != "hi" {
		t.Fatalf("白名单字段应保留: %s", out)
	}
	// access_programs 属真实 WS 字段（新补），应保留且值原样
	if v := gjson.GetBytes(out, "access_programs"); !v.Exists() || v.Raw != `["org-a"]` {
		t.Fatalf("access_programs 应保留且值原样: %s", out)
	}
	if v := gjson.GetBytes(out, "stream_options.reasoning_summary_delivery").String(); v != "sequential_cutoff" {
		t.Fatalf("stream_options 应保留: %s", out)
	}

	// 过滤后为空 → ErrEmptyFrame（空结果帧不入网）
	if _, err := FilterCodexWsPayload([]byte(`{"evil":1,"foo":2,"max_output_tokens":9}`)); !errors.Is(err, ErrEmptyFrame) {
		t.Fatalf("过滤后为空应返回 ErrEmptyFrame, got %v", err)
	}

	// 无需过滤时零拷贝原样返回
	clean := []byte(`{"type":"response.create"}`)
	got, err := FilterCodexWsPayload(clean)
	if err != nil {
		t.Fatalf("FilterCodexWsPayload: %v", err)
	}
	if len(got) == 0 || &got[0] != &clean[0] {
		t.Fatal("干净输入应零拷贝原样返回")
	}

	// 非法 JSON / 空输入原样返回
	bad := []byte("not json")
	if got, err := FilterCodexWsPayload(bad); err != nil || !bytes.Equal(got, bad) {
		t.Fatalf("非法 JSON 应原样返回: %v %s", err, got)
	}
	if got, err := FilterCodexWsPayload(nil); err != nil || got != nil {
		t.Fatalf("空输入应原样返回: %v", err)
	}
}

// TestFilterCodexHTTPPayload：HTTP 顶层 key 白名单过滤（纯函数）——
// type/previous_response_id/generate 被剥（HTTP 无此三键），access_programs 保留。
func TestFilterCodexHTTPPayload(t *testing.T) {
	in := []byte(`{"model":"gpt-5","input":"hi","access_programs":["org-a"],"type":"response.create","previous_response_id":"resp_1","generate":true,"max_output_tokens":4096,"evil":1}`)
	out, err := FilterCodexHTTPPayload(in)
	if err != nil {
		t.Fatalf("FilterCodexHTTPPayload: %v", err)
	}
	for _, k := range []string{"type", "previous_response_id", "generate", "max_output_tokens", "evil"} {
		if gjson.GetBytes(out, k).Exists() {
			t.Fatalf("HTTP 白名单外 key %q 应被删除: %s", k, out)
		}
	}
	if gjson.GetBytes(out, "model").String() != "gpt-5" || gjson.GetBytes(out, "input").String() != "hi" {
		t.Fatalf("白名单字段应保留: %s", out)
	}
	if v := gjson.GetBytes(out, "access_programs"); !v.Exists() || v.Raw != `["org-a"]` {
		t.Fatalf("access_programs 应保留且值原样: %s", out)
	}

	// 过滤后为空 → ErrEmptyFrame
	if _, err := FilterCodexHTTPPayload([]byte(`{"type":"response.create","generate":true}`)); !errors.Is(err, ErrEmptyFrame) {
		t.Fatalf("过滤后为空应返回 ErrEmptyFrame, got %v", err)
	}

	// 无需过滤时零拷贝原样返回
	clean := []byte(`{"model":"gpt-5"}`)
	got, err := FilterCodexHTTPPayload(clean)
	if err != nil {
		t.Fatalf("FilterCodexHTTPPayload: %v", err)
	}
	if len(got) == 0 || &got[0] != &clean[0] {
		t.Fatal("干净输入应零拷贝原样返回")
	}

	// 非法 JSON / 空输入原样返回
	bad := []byte("not json")
	if got, err := FilterCodexHTTPPayload(bad); err != nil || !bytes.Equal(got, bad) {
		t.Fatalf("非法 JSON 应原样返回: %v %s", err, got)
	}
	if got, err := FilterCodexHTTPPayload(nil); err != nil || got != nil {
		t.Fatalf("空输入应原样返回: %v", err)
	}
}

// TestCodexWhitelists：两张白名单逐键对齐真实 codex 结构。
func TestCodexWhitelists(t *testing.T) {
	if len(CodexWsPayloadFields) != 19 {
		t.Fatalf("CodexWsPayloadFields 键数 = %d, 期望 19", len(CodexWsPayloadFields))
	}
	wsSet := map[string]bool{}
	for _, f := range CodexWsPayloadFields {
		if wsSet[f] {
			t.Fatalf("CodexWsPayloadFields 含重复键 %q", f)
		}
		wsSet[f] = true
	}
	if !wsSet["access_programs"] {
		t.Fatal("CodexWsPayloadFields 应含 access_programs")
	}
	if wsSet["max_output_tokens"] {
		t.Fatal("CodexWsPayloadFields 不应含 max_output_tokens")
	}

	if len(CodexHTTPPayloadFields) != 16 {
		t.Fatalf("CodexHTTPPayloadFields 键数 = %d, 期望 16", len(CodexHTTPPayloadFields))
	}
	httpSet := map[string]bool{}
	for _, f := range CodexHTTPPayloadFields {
		if httpSet[f] {
			t.Fatalf("CodexHTTPPayloadFields 含重复键 %q", f)
		}
		httpSet[f] = true
	}
	if !httpSet["access_programs"] {
		t.Fatal("CodexHTTPPayloadFields 应含 access_programs")
	}
	if httpSet["max_output_tokens"] {
		t.Fatal("CodexHTTPPayloadFields 不应含 max_output_tokens")
	}

	// HTTP == WS − {type, previous_response_id, generate}
	wantHTTP := map[string]bool{}
	for _, f := range CodexWsPayloadFields {
		switch f {
		case "type", "previous_response_id", "generate":
			continue
		}
		wantHTTP[f] = true
	}
	if len(wantHTTP) != len(httpSet) {
		t.Fatalf("HTTP 键集大小 = %d, 期望 WS−3 = %d", len(httpSet), len(wantHTTP))
	}
	for f := range wantHTTP {
		if !httpSet[f] {
			t.Fatalf("HTTP 白名单缺 WS 键 %q", f)
		}
	}
	for f := range httpSet {
		if !wantHTTP[f] {
			t.Fatalf("HTTP 白名单含非 WS−3 键 %q", f)
		}
	}
}

// TestForceCodexStoreFalse：强制顶层 store:false。
func TestForceCodexStoreFalse(t *testing.T) {
	// store:true → false
	out := forceCodexStoreFalse([]byte(`{"model":"m","store":true}`))
	if gjson.GetBytes(out, "store").Bool() {
		t.Fatalf("store:true 应被强制为 false: %s", out)
	}
	if gjson.GetBytes(out, "model").String() != "m" {
		t.Fatalf("不应动其余字段: %s", out)
	}
	// 缺 store → 补 false
	out = forceCodexStoreFalse([]byte(`{"model":"m"}`))
	if sv := gjson.GetBytes(out, "store"); !sv.Exists() || sv.Type != gjson.False {
		t.Fatalf("缺 store 应补 false: %s", out)
	}
	// store 已 false → 零拷贝短路（真 codex 帧恒带 store:false，不重写）
	falseFrame := []byte(`{"type":"response.create","store":false}`)
	if got := forceCodexStoreFalse(falseFrame); len(got) == 0 || &got[0] != &falseFrame[0] {
		t.Fatal("store 已 false 应零拷贝返回原字节")
	}
	// 非法 JSON 原样
	bad := []byte("not json")
	if got := forceCodexStoreFalse(bad); !bytes.Equal(got, bad) {
		t.Fatalf("非法 JSON 应原样返回: %s", got)
	}
}

// TestPrepareFrameZeroCopyWhenNormalized：已归一帧（顶层键全在白名单内且已含
// store:false）+ 无任何注入 → prepareFrame 零拷贝原样返回（感知不到归一的
// 老路径——真 codex 帧在有身份注入时另经 client_metadata 组装）。
func TestPrepareFrameZeroCopyWhenNormalized(t *testing.T) {
	c := &Client{} // 无 meta/session/trace/turnAuto，注入项为空
	in := []byte(`{"type":"response.create","model":"gpt-5","store":false}`)
	got, err := c.prepareFrame(in)
	if err != nil {
		t.Fatalf("prepareFrame: %v", err)
	}
	if len(got) == 0 || &got[0] != &in[0] {
		t.Fatal("已归一且无注入时 prepareFrame 应零拷贝原样返回")
	}
	if gjson.GetBytes(got, "store").Bool() || gjson.GetBytes(got, "model").String() != "gpt-5" {
		t.Fatalf("归一后帧内容应保持: %s", got)
	}
}

// TestSendNormalizesWsFrame：Send 无条件归一（WS 白名单过滤 + 强制 store:false），
// 白名单内 access_programs 保留，client_metadata 仍按既有机制注入。
func TestSendNormalizesWsFrame(t *testing.T) {
	url, st := startEchoServer(t, "")
	c, err := Dial(context.Background(), PAT("t"), WithTransport(newFixedTransport(t, "https://chatgpt.com/backend-api/codex/responses", url)),
		WithCodexMeta(CodexMeta{InstallationID: "inst-1"}))
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer c.Close(StatusGoingAway, "")

	frame := []byte(`{"type":"response.create","model":"gpt-5","store":true,"max_output_tokens":4096,"access_programs":["org-a"],"evil":1}`)
	if err := c.Send(context.Background(), frame); err != nil {
		t.Fatalf("Send: %v", err)
	}
	waitFor(t, func() bool {
		st.mu.Lock()
		defer st.mu.Unlock()
		return len(st.texts) >= 1
	})
	got := func() string { st.mu.Lock(); defer st.mu.Unlock(); return string(st.texts[0]) }()
	if gjson.Get(got, "max_output_tokens").Exists() || gjson.Get(got, "evil").Exists() {
		t.Fatalf("非 WS 白名单键应被剥: %s", got)
	}
	if gjson.Get(got, "store").Bool() {
		t.Fatalf("store 应被强制 false: %s", got)
	}
	if v := gjson.Get(got, "access_programs"); !v.Exists() || v.Raw != `["org-a"]` {
		t.Fatalf("access_programs 应保留: %s", got)
	}
	if v := gjson.Get(got, "client_metadata.x-codex-installation-id").String(); v != "inst-1" {
		t.Fatalf("client_metadata 注入应仍生效: %s", got)
	}
}

// TestHTTPStreamNormalizesPayload：HTTP Stream 发出的体经 codex 形状归一
// （HTTP 白名单过滤 + 强制 store:false），client_metadata 注入仍生效。
func TestHTTPStreamNormalizesPayload(t *testing.T) {
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
		WithCodexMeta(CodexMeta{InstallationID: "inst-1"}))
	payload := []byte(`{"model":"m","input":"hi","store":true,"max_output_tokens":4096,"type":"response.create","previous_response_id":"resp_1","generate":true,"access_programs":["org-a"]}`)
	if err := hc.Stream(context.Background(), payload, func(raw []byte) error { return nil }); err != nil {
		t.Fatalf("Stream: %v", err)
	}
	for _, k := range []string{"max_output_tokens", "type", "previous_response_id", "generate"} {
		if gjson.GetBytes(gotBody, k).Exists() {
			t.Fatalf("HTTP 白名单外键 %q 应被剥: %s", k, gotBody)
		}
	}
	if gjson.GetBytes(gotBody, "store").Bool() {
		t.Fatalf("store 应被强制 false: %s", gotBody)
	}
	if v := gjson.GetBytes(gotBody, "access_programs"); !v.Exists() || v.Raw != `["org-a"]` {
		t.Fatalf("access_programs 应保留: %s", gotBody)
	}
	if v := gjson.GetBytes(gotBody, "client_metadata.x-codex-installation-id").String(); v != "inst-1" {
		t.Fatalf("client_metadata 注入应生效: %s", gotBody)
	}
}

// TestSendNormalizesUnconditionally：Send 恒做 WS 归一（无开关）——
// 过滤白名单外键 + 强制 store:false；过滤后为空帧不入网。
func TestSendNormalizesUnconditionally(t *testing.T) {
	url, st := startEchoServer(t, "")
	c, err := Dial(context.Background(), PAT("t"), WithTransport(newFixedTransport(t, "https://chatgpt.com/backend-api/codex/responses", url)))
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer c.Close(StatusGoingAway, "")

	frame := []byte(`{"type":"response.create","model":"gpt-5","evil":"drop-me","store":true}`)
	if err := c.Send(context.Background(), frame); err != nil {
		t.Fatalf("Send: %v", err)
	}
	waitFor(t, func() bool {
		st.mu.Lock()
		defer st.mu.Unlock()
		return len(st.texts) >= 1
	})
	got1 := func() string { st.mu.Lock(); defer st.mu.Unlock(); return string(st.texts[0]) }()
	if gjson.Get(got1, "evil").Exists() {
		t.Fatalf("应过滤 evil: %s", got1)
	}
	if gjson.Get(got1, "model").String() != "gpt-5" {
		t.Fatalf("model 应保留: %s", got1)
	}
	if gjson.Get(got1, "store").Bool() {
		t.Fatalf("store 应被强制 false: %s", got1)
	}

	// 过滤后为空：Send 返回 ErrEmptyFrame 且不入网
	if err := c.Send(context.Background(), []byte(`{"evil":1,"foo":2}`)); !errors.Is(err, ErrEmptyFrame) {
		t.Fatalf("Send 应返回 ErrEmptyFrame, got %v", err)
	}
	time.Sleep(50 * time.Millisecond)
	st.mu.Lock()
	defer st.mu.Unlock()
	if len(st.texts) != 1 {
		t.Fatalf("空结果帧不应入网, 服务端收到 %d 帧", len(st.texts))
	}
}

// TestCodexMetaInjection：Send 顶层 client_metadata **整体替换**（网关覆盖语义：
// codex 面客户端自带的 client_metadata **永不透传**——连同非网关键一并丢弃）。
func TestCodexMetaInjection(t *testing.T) {
	url, st := startEchoServer(t, "")
	c, err := Dial(context.Background(), PAT("t"), WithTransport(newFixedTransport(t, "https://chatgpt.com/backend-api/codex/responses", url)),
		WithCodexMeta(CodexMeta{
			InstallationID: "inst-1",
			WindowID:       "win-1",
			Subagent:       "sub-1",
			Traceparent:    "tp-1",
			Tracestate:     "ts-1",
		}))
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer c.Close(StatusGoingAway, "")

	frame := []byte(`{"type":"response.create","model":"gpt-5","client_metadata":{"x-codex-window-id":"existing-win","user":{"a":1}}}`)
	if err := c.Send(context.Background(), frame); err != nil {
		t.Fatalf("Send: %v", err)
	}
	waitFor(t, func() bool {
		st.mu.Lock()
		defer st.mu.Unlock()
		return len(st.texts) >= 1
	})
	got := func() string { st.mu.Lock(); defer st.mu.Unlock(); return string(st.texts[0]) }()

	if v := gjson.Get(got, "client_metadata.x-codex-installation-id").String(); v != "inst-1" {
		t.Fatalf("installation = %q, 期望 inst-1: %s", v, got)
	}
	// 帧内已有的同 key 被网关覆盖（客户端 window 不透传）
	if v := gjson.Get(got, "client_metadata.x-codex-window-id").String(); v != "win-1" {
		t.Fatalf("window 应被网关覆盖为 win-1, got %q: %s", v, got)
	}
	if v := gjson.Get(got, "client_metadata.x-openai-subagent").String(); v != "sub-1" {
		t.Fatalf("subagent = %q, 期望 sub-1", v)
	}
	if v := gjson.Get(got, "client_metadata.ws_request_header_traceparent").String(); v != "tp-1" {
		t.Fatalf("traceparent = %q, 期望 tp-1（静态值优先于自动生成）", v)
	}
	// 客户端非网关键（user）随整体替换一并丢弃（客户端 client_metadata 永不透传）
	if gjson.Get(got, "client_metadata.user").Exists() {
		t.Fatalf("客户端非网关键应被丢弃: %s", got)
	}
	// 顶层其余字段保留
	if gjson.Get(got, "model").String() != "gpt-5" {
		t.Fatalf("model 应保留: %s", got)
	}
}

// TestClientMetadataPassthrough：WithClientMetadata 注入任意 client_metadata 键
// （responses-lite 键 MetaResponsesLiteKey，只注入不解析）；**恒覆盖**帧内已有同 key。
func TestClientMetadataPassthrough(t *testing.T) {
	url, st := startEchoServer(t, "")
	c, err := Dial(context.Background(), PAT("t"), WithTransport(newFixedTransport(t, "https://chatgpt.com/backend-api/codex/responses", url)),
		WithClientMetadata(MetaResponsesLiteKey, "true"))
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer c.Close(StatusGoingAway, "")

	frame := []byte(`{"type":"response.create","model":"gpt-5"}`)
	if err := c.Send(context.Background(), frame); err != nil {
		t.Fatalf("Send: %v", err)
	}
	// 帧内已有同 key：被网关覆盖（客户端值不透传）
	existing := []byte(`{"type":"response.create","model":"gpt-5","client_metadata":{"ws_request_header_x_openai_internal_codex_responses_lite":"false"}}`)
	if err := c.Send(context.Background(), existing); err != nil {
		t.Fatalf("Send #2: %v", err)
	}
	waitFor(t, func() bool {
		st.mu.Lock()
		defer st.mu.Unlock()
		return len(st.texts) >= 2
	})
	st.mu.Lock()
	got0 := st.texts[0]
	got1 := st.texts[1]
	st.mu.Unlock()

	if v := gjson.GetBytes(got0, "client_metadata."+MetaResponsesLiteKey).String(); v != "true" {
		t.Fatalf("透传键 %s 缺失或值不符, got %q: %s", MetaResponsesLiteKey, v, got0)
	}
	if v := gjson.GetBytes(got1, "client_metadata."+MetaResponsesLiteKey).String(); v != "true" {
		t.Fatalf("帧内已有同 key 应被网关覆盖为 true, got %q: %s", v, got1)
	}
	// 该键只进 client_metadata：Dial 只组装默认/会话/WithHeader 握手头，
	// 透传键按构造不可能泄漏为握手头。
}

// TestNewTraceContext：W3C traceparent 格式 + 每次调用新链路 id。
func TestNewTraceContext(t *testing.T) {
	a := NewTraceContext()
	b := NewTraceContext()
	if !traceparentRe.MatchString(a.Traceparent) || !traceparentRe.MatchString(b.Traceparent) {
		t.Fatalf("traceparent 格式不符: %q / %q", a.Traceparent, b.Traceparent)
	}
	if a.Traceparent == b.Traceparent {
		t.Fatal("两次调用应生成不同链路 id")
	}
	if a.Tracestate != "" {
		t.Fatalf("tracestate 默认应为空, got %q", a.Tracestate)
	}
}

// TestTraceInjection：每帧自动生成新 trace 注入帧内 client_metadata
// （握手头与 HTTP 请求不发 trace）；外部 WithTraceContext 静态覆盖；
// WithTraceAuto(false) 关闭。
func TestTraceInjection(t *testing.T) {
	// 自动：每帧新 traceparent，帧间不同；握手头无 trace
	url, st := startEchoServer(t, "")
	c, err := Dial(context.Background(), PAT("t"), WithTransport(newFixedTransport(t, "https://chatgpt.com/backend-api/codex/responses", url)))
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer c.Close(StatusGoingAway, "")
	frame := []byte(`{"type":"response.create","model":"gpt-5"}`)
	for i := 0; i < 2; i++ {
		if err := c.Send(context.Background(), frame); err != nil {
			t.Fatalf("Send #%d: %v", i, err)
		}
	}
	waitFor(t, func() bool {
		st.mu.Lock()
		defer st.mu.Unlock()
		return len(st.texts) >= 2
	})
	st.mu.Lock()
	tp0 := gjson.GetBytes(st.texts[0], "client_metadata.ws_request_header_traceparent").String()
	tp1 := gjson.GetBytes(st.texts[1], "client_metadata.ws_request_header_traceparent").String()
	noHeaderTrace := st.traceparent == ""
	st.mu.Unlock()
	if !traceparentRe.MatchString(tp0) || !traceparentRe.MatchString(tp1) {
		t.Fatalf("帧内 traceparent 格式不符: %q / %q", tp0, tp1)
	}
	if tp0 == tp1 {
		t.Fatal("每帧应生成新的 traceparent（真实客户端同轮多请求 traceparent 不同）")
	}
	if !noHeaderTrace {
		t.Fatal("WS 握手不应带 traceparent 头")
	}

	// 外部注入：静态值每帧一致
	url2, st2 := startEchoServer(t, "")
	external := TraceContext{Traceparent: "00-11111111111111111111111111111111-2222222222222222-01", Tracestate: "vendor=1"}
	c2, err := Dial(context.Background(), PAT("t"), WithTransport(newFixedTransport(t, "https://chatgpt.com/backend-api/codex/responses", url2)), WithTraceContext(external))
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer c2.Close(StatusGoingAway, "")
	for i := 0; i < 2; i++ {
		if err := c2.Send(context.Background(), frame); err != nil {
			t.Fatalf("Send #%d: %v", i, err)
		}
	}
	waitFor(t, func() bool {
		st2.mu.Lock()
		defer st2.mu.Unlock()
		return len(st2.texts) >= 2
	})
	st2.mu.Lock()
	gotTP := gjson.GetBytes(st2.texts[0], "client_metadata.ws_request_header_traceparent").String()
	gotTS := gjson.GetBytes(st2.texts[0], "client_metadata.ws_request_header_tracestate").String()
	st2.mu.Unlock()
	if gotTP != external.Traceparent || gotTS != external.Tracestate {
		t.Fatalf("外部 trace 注入失败: %q / %q", gotTP, gotTS)
	}

	// 关闭自动生成：帧内无 trace key
	url3, st3 := startEchoServer(t, "")
	c3, err := Dial(context.Background(), PAT("t"), WithTransport(newFixedTransport(t, "https://chatgpt.com/backend-api/codex/responses", url3)), WithTraceAuto(false))
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer c3.Close(StatusGoingAway, "")
	if err := c3.Send(context.Background(), frame); err != nil {
		t.Fatalf("Send: %v", err)
	}
	waitFor(t, func() bool {
		st3.mu.Lock()
		defer st3.mu.Unlock()
		return len(st3.texts) >= 1
	})
	got3 := func() string { st3.mu.Lock(); defer st3.mu.Unlock(); return string(st3.texts[0]) }()
	if gjson.Get(got3, "client_metadata.ws_request_header_traceparent").Exists() {
		t.Fatalf("关闭自动生成后不应有 trace key: %s", got3)
	}
}

// uuidv7Re 是 UUIDv7 格式（版本 7 + 变体 10）。
var uuidv7Re = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`)

// TestNewUUIDv7：UUIDv7 格式 + 每次调用新值。
func TestNewUUIDv7(t *testing.T) {
	a := NewUUIDv7()
	b := NewUUIDv7()
	if !uuidv7Re.MatchString(a) || !uuidv7Re.MatchString(b) {
		t.Fatalf("UUIDv7 格式不符: %q / %q", a, b)
	}
	if a == b {
		t.Fatal("两次调用应生成不同 UUID")
	}
}

// TestTurnIDAuto：每帧自动生成新 turn_id（UUIDv7）；CodexMeta.TurnID 静态优先。
func TestTurnIDAuto(t *testing.T) {
	url, st := startEchoServer(t, "")
	c, err := Dial(context.Background(), PAT("t"), WithTransport(newFixedTransport(t, "https://chatgpt.com/backend-api/codex/responses", url)))
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer c.Close(StatusGoingAway, "")
	frame := []byte(`{"type":"response.create","model":"gpt-5"}`)
	for i := 0; i < 2; i++ {
		if err := c.Send(context.Background(), frame); err != nil {
			t.Fatalf("Send #%d: %v", i, err)
		}
	}
	waitFor(t, func() bool {
		st.mu.Lock()
		defer st.mu.Unlock()
		return len(st.texts) >= 2
	})
	st.mu.Lock()
	t0 := gjson.GetBytes(st.texts[0], "client_metadata.turn_id").String()
	t1 := gjson.GetBytes(st.texts[1], "client_metadata.turn_id").String()
	st.mu.Unlock()
	if !uuidv7Re.MatchString(t0) || !uuidv7Re.MatchString(t1) {
		t.Fatalf("turn_id 应为 UUIDv7: %q / %q", t0, t1)
	}
	if t0 == t1 {
		t.Fatal("每帧应生成新的 turn_id")
	}

	// 静态 TurnID 优先
	url2, st2 := startEchoServer(t, "")
	c2, err := Dial(context.Background(), PAT("t"), WithTransport(newFixedTransport(t, "https://chatgpt.com/backend-api/codex/responses", url2)), WithCodexMeta(CodexMeta{TurnID: "static-turn"}))
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer c2.Close(StatusGoingAway, "")
	if err := c2.Send(context.Background(), frame); err != nil {
		t.Fatalf("Send: %v", err)
	}
	waitFor(t, func() bool {
		st2.mu.Lock()
		defer st2.mu.Unlock()
		return len(st2.texts) >= 1
	})
	got := func() string { st2.mu.Lock(); defer st2.mu.Unlock(); return string(st2.texts[0]) }()
	if v := gjson.Get(got, "client_metadata.turn_id").String(); v != "static-turn" {
		t.Fatalf("静态 TurnID 应优先, got %q", v)
	}
}

// TestTurnCounting：连接内 turn 序号自增，turn_metadata 由回调提供并组装。
func TestTurnCounting(t *testing.T) {
	url, st := startEchoServer(t, "")
	c, err := Dial(context.Background(), PAT("t"), WithTransport(newFixedTransport(t, "https://chatgpt.com/backend-api/codex/responses", url)),
		WithTurnMetadata(func(turn uint64) string {
			return fmt.Sprintf(`{"turn":%d}`, turn)
		}))
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer c.Close(StatusGoingAway, "")

	for i := 0; i < 2; i++ {
		if err := c.Send(context.Background(), []byte(`{"type":"response.create","model":"gpt-5"}`)); err != nil {
			t.Fatalf("Send #%d: %v", i, err)
		}
	}
	waitFor(t, func() bool {
		st.mu.Lock()
		defer st.mu.Unlock()
		return len(st.texts) >= 2
	})
	st.mu.Lock()
	defer st.mu.Unlock()
	for i, text := range st.texts {
		want := fmt.Sprintf(`{"turn":%d}`, i+1)
		if got := gjson.GetBytes(text, "client_metadata.x-codex-turn-metadata").String(); got != want {
			t.Fatalf("第 %d 帧 turn_metadata = %q, 期望 %q: %s", i+1, got, want, text)
		}
	}
}

// TestTurnMetadataProviderConcurrent：多 Send 并发下 turn 计数不重复。
func TestTurnMetadataProviderConcurrent(t *testing.T) {
	url, st := startEchoServer(t, "")
	c, err := Dial(context.Background(), PAT("t"), WithTransport(newFixedTransport(t, "https://chatgpt.com/backend-api/codex/responses", url)),
		WithTurnMetadata(func(turn uint64) string {
			return fmt.Sprintf(`{"turn":%d}`, turn)
		}))
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer c.Close(StatusGoingAway, "")

	const n = 20
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = c.Send(context.Background(), []byte(`{"type":"response.create","model":"gpt-5"}`))
		}()
	}
	wg.Wait()
	waitFor(t, func() bool {
		st.mu.Lock()
		defer st.mu.Unlock()
		return len(st.texts) >= n
	})

	seen := make(map[string]bool, n)
	st.mu.Lock()
	defer st.mu.Unlock()
	for _, text := range st.texts {
		v := gjson.GetBytes(text, "client_metadata.x-codex-turn-metadata").String()
		if v == "" || !strings.HasPrefix(v, `{"turn":`) {
			t.Fatalf("turn_metadata 格式不符: %q", v)
		}
		if seen[v] {
			t.Fatalf("turn 序号重复: %s", v)
		}
		seen[v] = true
	}
}

// TestTurnIDOverride：帧内已有 client_metadata.turn_id 恒被网关覆盖为每帧新
// UUIDv7（客户端 turn_id 不透传）；无值帧同样自动生成。
func TestTurnIDOverride(t *testing.T) {
	url, st := startEchoServer(t, "")
	c, err := Dial(context.Background(), PAT("t"), WithTransport(newFixedTransport(t, "https://chatgpt.com/backend-api/codex/responses", url)))
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer c.Close(StatusGoingAway, "")

	// 帧内已有 turn_id：被网关覆盖为新 UUIDv7，其余 metadata key 仍可注入
	withTurn := []byte(`{"type":"response.create","model":"gpt-5","client_metadata":{"turn_id":"existing-turn","user":{"a":1}}}`)
	if err := c.Send(context.Background(), withTurn); err != nil {
		t.Fatalf("Send: %v", err)
	}
	// 无值帧：自动生成 UUIDv7
	withoutTurn := []byte(`{"type":"response.create","model":"gpt-5"}`)
	if err := c.Send(context.Background(), withoutTurn); err != nil {
		t.Fatalf("Send: %v", err)
	}
	waitFor(t, func() bool {
		st.mu.Lock()
		defer st.mu.Unlock()
		return len(st.texts) >= 2
	})
	st.mu.Lock()
	got0 := st.texts[0]
	got1 := st.texts[1]
	st.mu.Unlock()

	if v := gjson.GetBytes(got0, "client_metadata.turn_id").String(); !uuidv7Re.MatchString(v) {
		t.Fatalf("帧内 turn_id 应被网关覆盖为 UUIDv7, got %q", v)
	}
	if v := gjson.GetBytes(got0, "client_metadata.turn_id").String(); v == "existing-turn" {
		t.Fatal("客户端 turn_id 不应透传")
	}
	if gjson.GetBytes(got0, "client_metadata.user").Exists() {
		t.Fatalf("客户端非网关键 metadata 应被丢弃: %s", got0)
	}
	if v := gjson.GetBytes(got1, "client_metadata.turn_id").String(); !uuidv7Re.MatchString(v) {
		t.Fatalf("无值帧应自动生成 UUIDv7 turn_id, got %q", v)
	}
}

// refNeedsFilter 是等价性参考实现（**旧路径**：gjson.ParseBytes(...).ForEach +
// key.String() 查表），与优化前 filterCodexPayload 的判定逐一等价。
func refNeedsFilter(raw []byte, set map[string]struct{}) bool {
	needs := false
	gjson.ParseBytes(raw).ForEach(func(key, _ gjson.Result) bool {
		if _, ok := set[key.String()]; !ok {
			needs = true
			return false
		}
		return true
	})
	return needs
}

// codexTopLevelNeedsFilterCorpus 是与参考实现逐一对照的语料：覆盖干净对象、
// 非白名单顶层键、嵌套（只看顶层）、值内含 {}[]"\\:,\uXXXX/"tools" 字样、
// 空白变体、转义顶层键、空对象、非对象根、重复键等形态。
var codexTopLevelNeedsFilterCorpus = []string{
	// — 干净对象（顶层键全在白名单内）→ false —
	`{"type":"response.create","model":"gpt-5","store":false}`,
	`{"model":"gpt-5.5","instructions":"be helpful","input":[],"tools":[],"tool_choice":"auto","parallel_tool_calls":true,"reasoning":{"effort":"low"},"store":false,"stream":true,"stream_options":{},"include":[],"service_tier":"auto","prompt_cache_key":"k","text":{},"client_metadata":{},"access_programs":[]}`,
	// 空对象 → false
	`{}`,
	// — 非白名单顶层键 → true —
	`{"model":"gpt-5","max_output_tokens":4096}`,
	`{"evil":1}`,
	`{"model":"m","evil":1,"store":false}`,
	`{"max_output_tokens":1}`,
	// — 嵌套：判定只看顶层键（嵌套内的非白名单键不触发）—
	`{"client_metadata":{"evil":1}}`,                            // 顶层键白名单，嵌套 evil 忽略 → false
	`{"reasoning":{"evil":1,"nested":{"max_output_tokens":2}}}`, // 同上 → false
	`{"input":[{"role":"user","evil":1}]}`,                      // 数组元素内的 evil 忽略 → false
	`{"text":{"format":{"type":"text"},"s":"}{[]"}}`,            // 嵌套值含括号字符 → false
	`{"a":{"evil":1}}`,                                          // 顶层键 a 非白名单 → true（决定权在顶层键）
	`{"a":[{"evil":1}]}`,                                        // 同上 → true
	// — 值内含 { } [ ] " \ : \uXXXX , 等字符/字面量（依顶层键判定）—
	`{"model":"a}b,c","instructions":"x\"y"}`,
	`{"model":"a[b]c{d}e","input":[{"content":"p,q"}]}`,
	`{"model":"a:b","store":false}`,
	`{"model":"\\u0041","store":false}`,
	`{"model":"\u0041","store":false}`,
	`{"model":"the tools list","instructions":"use \"tools\" here"}`,
	`{"model":"[{\"nested\":\"fake\"}]","store":false}`,
	`{"model":"x","text":{"v":"a\"b\\c"}}`,
	// — 值类型变体（null / true / false / 数字 / 嵌套数组）—
	`{"model":null,"store":false}`,
	`{"model":"m","parallel_tool_calls":true,"stream":false,"store":false,"include":[1,2,3]}`,
	`{"model":"m","reasoning":{"effort":"low"},"input":[{"a":1},{"b":[true,false,null]}]}`,
	// — 空白变体（键/冒号/值之间、前后、内部换行制表）—
	`{"tools" : []}`,
	`{ "model" : "m" , "input" : [ 1 , 2 ] }`,
	"{\n\t\"model\"\n:\n\"m\"\n}",
	`  {"model":"m"}  `,
	// — 转义顶层键：回退 gjson（精确 unescape 语义）—
	`{"mo\u0064el":1}`,           // 解码 model → false
	`{"ev\u0069l":1}`,            // 解码 evil → true
	"{\"store\\\\\":1}",          // 键解码为 store\ → 非白名单 → true
	`{"model":1,"mo\u0064el":2}`, // 混合（解码后均为 model）→ false
	// — 重复顶层键 —
	`{"model":1,"model":2}`,
	// — 非对象根：回退 gjson（与参考实现完全一致）—
	`[]`,      // → false（空数组无回调）
	`[ ]`,     // → false
	`[1]`,     // → true
	`  [1]  `, // → true
	`123`,     // → true
	`null`,    // → true
	`true`,    // → true
	`"x"`,     // → true
	`"model"`, // → true
}

// TestCodexTopLevelNeedsFilterEquivalence：零拷贝顶层键扫描 codexTopLevelNeedsFilter
// 与旧 gjson ForEach 参考实现 refNeedsFilter 对语料逐一等价（WS / HTTP 两张白名单集）。
func TestCodexTopLevelNeedsFilterEquivalence(t *testing.T) {
	sets := map[string]map[string]struct{}{
		"ws":   codexWsPayloadFieldSet,
		"http": codexHTTPPayloadFieldSet,
	}
	for name, set := range sets {
		for _, raw := range codexTopLevelNeedsFilterCorpus {
			got := codexTopLevelNeedsFilter([]byte(raw), set)
			want := refNeedsFilter([]byte(raw), set)
			if got != want {
				t.Errorf("[%s] codexTopLevelNeedsFilter(%q) = %v, 参考实现 = %v", name, raw, got, want)
			}
		}
	}
}

// TestCodexTopLevelNeedsFilterExplicit 钉住关键形态的**显式期望值**（独立于
// gjson，防上游库语义漂移时静默改写契约），并与参考实现交叉校验（等价 = 无行为变化）。
func TestCodexTopLevelNeedsFilterExplicit(t *testing.T) {
	cases := []struct {
		raw  string
		want bool
	}{
		{`{"type":"response.create","model":"gpt-5","store":false}`, false}, // 顶层键全在白名单
		{`{"model":"m","max_output_tokens":4096}`, true},                    // 白名单外顶层键
		{`{"client_metadata":{"evil":1}}`, false},                           // 嵌套非白名单：只看顶层（外层白名单）
		{`{"input":[{"evil":1}]}`, false},                                   // 嵌套数组同上
		{`{"a":{"evil":1}}`, true},                                          // 顶层键 a 本身非白名单
		{`{"a":[{"evil":1}]}`, true},
		{`{}`, false},
		{`[]`, false}, // 非对象根回退 gjson：空数组根旧行为 = false
		{`[1]`, true},
		{`123`, true},
		{`null`, true},
		{`"x"`, true},
		{`{"mo\u0064el":1}`, false},                        // 转义顶层键解码 = model（白名单）
		{`{"ev\u0069l":1}`, true},                          // 转义顶层键解码 = evil（非白名单）
		{`{"model":"a}b,c","instructions":"x\"y"}`, false}, // 值内含分隔符/转义，顶层键白名单
		{`{"tools" : []}`, false},                          // 键与冒号间空白
	}
	for _, c := range cases {
		got := codexTopLevelNeedsFilter([]byte(c.raw), codexWsPayloadFieldSet)
		if got != c.want {
			t.Errorf("codexTopLevelNeedsFilter(%q) = %v, 期望 %v", c.raw, got, c.want)
		}
		if ref := refNeedsFilter([]byte(c.raw), codexWsPayloadFieldSet); ref != c.want {
			t.Errorf("refNeedsFilter(%q) = %v, 期望 %v（参考实现与显式期望不符）", c.raw, ref, c.want)
		}
	}
}

// 故对 WS / HTTP 两集均 clean），含 store:false，大 input 模拟长会话。
func cleanCodexFrame() []byte {
	var b strings.Builder
	b.WriteString(`{"model":"gpt-5.5","input":[`)
	for i := 0; i < 512; i++ {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString(`{"type":"message","role":"user","content":[{"type":"input_text","text":"`)
		b.WriteString(strings.Repeat("x", 480))
		b.WriteString(`"}]}`)
	}
	b.WriteString(`],"instructions":"be helpful","tools":[],"tool_choice":"auto",`)
	b.WriteString(`"parallel_tool_calls":true,"reasoning":{"effort":"low"},`)
	b.WriteString(`"store":false,"stream":true,"stream_options":{},"include":[],`)
	b.WriteString(`"service_tier":"auto","prompt_cache_key":"k","text":{},"client_metadata":{},"access_programs":[]}`)
	return []byte(b.String())
}

// dirtyCodexFrame 在干净帧首键前插入白名单外键 max_output_tokens（触发重建路径）。
func dirtyCodexFrame() []byte {
	clean := cleanCodexFrame()
	out := make([]byte, 0, len(clean)+24)
	out = append(out, '{')
	out = append(out, `"max_output_tokens":4096,`...)
	out = append(out, clean[1:]...)
	return out
}

// TestCodexPayloadScanZeroAlloc：clean 帧（顶层全白名单键、含 store:false）走
// 零拷贝顶层键扫描 → FilterCodexWsPayload / FilterCodexHTTPPayload 零分配。
func TestCodexPayloadScanZeroAlloc(t *testing.T) {
	clean := cleanCodexFrame()
	if len(clean) < 200*1024 {
		t.Fatalf("clean 帧体量过小（%d 字节），需真实覆盖热路径", len(clean))
	}
	if got, err := FilterCodexWsPayload(clean); err != nil || len(got) == 0 || &got[0] != &clean[0] {
		t.Fatalf("clean WS 帧应零拷贝原样返回: %v", err)
	}
	if got, err := FilterCodexHTTPPayload(clean); err != nil || len(got) == 0 || &got[0] != &clean[0] {
		t.Fatalf("clean HTTP 帧应零拷贝原样返回: %v", err)
	}
	wsAllocs := testing.AllocsPerRun(200, func() {
		_, _ = FilterCodexWsPayload(clean)
	})
	if wsAllocs != 0 {
		t.Fatalf("FilterCodexWsPayload(clean) allocs/op = %v, 期望 0", wsAllocs)
	}
	httpAllocs := testing.AllocsPerRun(200, func() {
		_, _ = FilterCodexHTTPPayload(clean)
	})
	if httpAllocs != 0 {
		t.Fatalf("FilterCodexHTTPPayload(clean) allocs/op = %v, 期望 0", httpAllocs)
	}
}
