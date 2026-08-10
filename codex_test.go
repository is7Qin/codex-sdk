package codexsdk

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/tidwall/gjson"
)

// TestFilterCodexPayload：顶层 key 白名单过滤（纯函数）。
func TestFilterCodexPayload(t *testing.T) {
	in := []byte(`{"type":"response.create","model":"gpt-5","input":"hi","evil":"x","foo":{"bar":1}}`)
	out, err := FilterCodexPayload(in)
	if err != nil {
		t.Fatalf("FilterCodexPayload: %v", err)
	}
	if gjson.GetBytes(out, "evil").Exists() || gjson.GetBytes(out, "foo").Exists() {
		t.Fatalf("非白名单 key 应被删除: %s", out)
	}
	if gjson.GetBytes(out, "type").String() != "response.create" ||
		gjson.GetBytes(out, "model").String() != "gpt-5" ||
		gjson.GetBytes(out, "input").String() != "hi" {
		t.Fatalf("白名单字段应保留: %s", out)
	}

	// 过滤后为空 → ErrEmptyFrame（空结果帧不入网）
	if _, err := FilterCodexPayload([]byte(`{"evil":1,"foo":2}`)); !errors.Is(err, ErrEmptyFrame) {
		t.Fatalf("过滤后为空应返回 ErrEmptyFrame, got %v", err)
	}

	// 无需过滤时零拷贝原样返回
	clean := []byte(`{"type":"response.create"}`)
	got, err := FilterCodexPayload(clean)
	if err != nil {
		t.Fatalf("FilterCodexPayload: %v", err)
	}
	if len(got) == 0 || &got[0] != &clean[0] {
		t.Fatal("干净输入应零拷贝原样返回")
	}

	// 非法 JSON / 空输入原样返回
	bad := []byte("not json")
	got, err = FilterCodexPayload(bad)
	if err != nil || !bytes.Equal(got, bad) {
		t.Fatalf("非法 JSON 应原样返回: %v %s", err, got)
	}
	got, err = FilterCodexPayload(nil)
	if err != nil || got != nil {
		t.Fatalf("空输入应原样返回: %v", err)
	}
}

// TestSendDefaultFiltering：Send 默认应用白名单过滤，Options 可关。
func TestSendDefaultFiltering(t *testing.T) {
	url, st := startEchoServer(t, "")
	c, err := Dial(context.Background(), url, PAT("t"))
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer c.Close(StatusGoingAway, "")

	frame := []byte(`{"type":"response.create","model":"gpt-5","evil":"drop-me"}`)
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
		t.Fatalf("默认应过滤 evil: %s", got1)
	}
	if gjson.Get(got1, "model").String() != "gpt-5" {
		t.Fatalf("model 应保留: %s", got1)
	}

	// 关闭过滤
	url2, st2 := startEchoServer(t, "")
	c2, err := Dial(context.Background(), url2, PAT("t"), WithPayloadFiltering(false))
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
	got2 := func() string { st2.mu.Lock(); defer st2.mu.Unlock(); return string(st2.texts[0]) }()
	if !gjson.Get(got2, "evil").Exists() {
		t.Fatalf("关闭过滤后 evil 应保留: %s", got2)
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

// TestCodexMetaInjection：Send 顶层 client_metadata 组装（浅合并，不覆盖已存在）。
func TestCodexMetaInjection(t *testing.T) {
	url, st := startEchoServer(t, "")
	c, err := Dial(context.Background(), url, PAT("t"),
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
	// 已存在的 key 不被覆盖
	if v := gjson.Get(got, "client_metadata.x-codex-window-id").String(); v != "existing-win" {
		t.Fatalf("window 应保留帧内原值 existing-win, got %q: %s", v, got)
	}
	if v := gjson.Get(got, "client_metadata.x-openai-subagent").String(); v != "sub-1" {
		t.Fatalf("subagent = %q, 期望 sub-1", v)
	}
	if v := gjson.Get(got, "client_metadata.ws_request_header_traceparent").String(); v != "tp-1" {
		t.Fatalf("traceparent = %q, 期望 tp-1（静态值优先于自动生成）", v)
	}
	// 原 client_metadata 其余内容保留
	if v := gjson.Get(got, "client_metadata.user.a").Int(); v != 1 {
		t.Fatalf("原 client_metadata 内容应保留: %s", got)
	}
	// 顶层其余字段保留
	if gjson.Get(got, "model").String() != "gpt-5" {
		t.Fatalf("model 应保留: %s", got)
	}
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

// TestTraceInjection：trace 默认自动生成并注入（升级头 + client_metadata 对齐），
// 可注入外部值或关闭。
func TestTraceInjection(t *testing.T) {
	// 自动：升级头 traceparent 与帧内 client_metadata 的 trace key 值一致
	url, st := startEchoServer(t, "")
	c, err := Dial(context.Background(), url, PAT("t"))
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer c.Close(StatusGoingAway, "")
	waitFor(t, func() bool {
		st.mu.Lock()
		defer st.mu.Unlock()
		return st.traceparent != ""
	})
	if err := c.Send(context.Background(), []byte(`{"type":"response.create","model":"gpt-5"}`)); err != nil {
		t.Fatalf("Send: %v", err)
	}
	waitFor(t, func() bool {
		st.mu.Lock()
		defer st.mu.Unlock()
		return len(st.texts) >= 1
	})
	headerTP := func() string { st.mu.Lock(); defer st.mu.Unlock(); return st.traceparent }()
	metaTP := func() string {
		st.mu.Lock()
		defer st.mu.Unlock()
		return gjson.GetBytes(st.texts[0], "client_metadata.ws_request_header_traceparent").String()
	}()
	if metaTP != headerTP {
		t.Fatalf("metadata trace 应等于升级头 trace: header=%s metadata=%s", headerTP, metaTP)
	}

	// 外部注入
	url2, st2 := startEchoServer(t, "")
	external := TraceContext{Traceparent: "00-11111111111111111111111111111111-2222222222222222-01", Tracestate: "vendor=1"}
	c2, err := Dial(context.Background(), url2, PAT("t"), WithTraceContext(external))
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer c2.Close(StatusGoingAway, "")
	waitFor(t, func() bool {
		st2.mu.Lock()
		defer st2.mu.Unlock()
		return st2.traceparent != ""
	})
	if got := func() string { st2.mu.Lock(); defer st2.mu.Unlock(); return st2.traceparent }(); got != external.Traceparent {
		t.Fatalf("外部 traceparent 注入失败: %s", got)
	}
	if got := func() string { st2.mu.Lock(); defer st2.mu.Unlock(); return st2.tracestate }(); got != external.Tracestate {
		t.Fatalf("外部 tracestate 注入失败: %s", got)
	}

	// 关闭自动生成：无 trace 头，帧内无 trace key
	url3, st3 := startEchoServer(t, "")
	c3, err := Dial(context.Background(), url3, PAT("t"), WithTraceAuto(false))
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer c3.Close(StatusGoingAway, "")
	if err := c3.Send(context.Background(), []byte(`{"type":"response.create","model":"gpt-5"}`)); err != nil {
		t.Fatalf("Send: %v", err)
	}
	waitFor(t, func() bool {
		st3.mu.Lock()
		defer st3.mu.Unlock()
		return len(st3.texts) >= 1
	})
	if got := func() string { st3.mu.Lock(); defer st3.mu.Unlock(); return st3.traceparent }(); got != "" {
		t.Fatalf("关闭自动生成后不应有 traceparent 头: %q", got)
	}
	got3 := func() string { st3.mu.Lock(); defer st3.mu.Unlock(); return string(st3.texts[0]) }()
	if gjson.Get(got3, "client_metadata.ws_request_header_traceparent").Exists() {
		t.Fatalf("关闭自动生成后不应有 trace key: %s", got3)
	}
}

// TestTurnCounting：连接内 turn 序号自增，turn_metadata 由回调提供并组装。
func TestTurnCounting(t *testing.T) {
	url, st := startEchoServer(t, "")
	c, err := Dial(context.Background(), url, PAT("t"),
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
	c, err := Dial(context.Background(), url, PAT("t"),
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
