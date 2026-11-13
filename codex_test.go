package codexsdk

import (
	"regexp"
	"strings"
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

// TestRewriteEnvironmentContextCursorGuard 覆盖游标越界护栏：gjson 正常输入恒给
// 单调且正确的 Index，但嵌套 / 构造结果可能给出 0 或回退的 Index（start <
// cursor），此时 raw[cursor:start] 会越界 panic——护栏必须拦下并保守跳过。
func TestRewriteEnvironmentContextCursorGuard(t *testing.T) {
	cases := []struct {
		name                  string
		cursor, start, end, n int
		ok                    bool
	}{
		{"正常区间", 0, 5, 9, 100, true},
		{"start==cursor", 5, 5, 9, 100, true},
		{"Index 回退（非单调）", 10, 5, 9, 100, false},
		{"Index==0 且游标已前进", 10, 0, 4, 100, false},
		{"end 越过原文末尾", 0, 5, 101, 100, false},
	}
	for _, c := range cases {
		if got := envContextSpliceOK(c.cursor, c.start, c.end, c.n); got != c.ok {
			t.Fatalf("%s: envContextSpliceOK(%d,%d,%d,%d)=%v, 期望 %v",
				c.name, c.cursor, c.start, c.end, c.n, got, c.ok)
		}
	}
}

// TestFilterCodexWsPayload：WS 顶层 key 白名单过滤（纯函数）。
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
