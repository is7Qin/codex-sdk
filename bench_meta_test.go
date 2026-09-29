package codexsdk

import (
	"strings"
	"testing"
)

// benchPayload 构造代表性 codex /responses 请求体：大 input（~256KB，模拟长会话）
// + 客户端自带 client_metadata（含非网关键 user）。
func benchPayload() []byte {
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
	b.WriteString(`],"client_metadata":{"turn_id":"client-turn","user":{"a":1}}}`)
	return []byte(b.String())
}

func benchClient() *HTTPClient {
	return NewHTTPClient(PAT("t"),
		WithCodexMeta(CodexMeta{
			InstallationID: "inst-1", SessionID: "sess-1", ThreadID: "thread-1",
			WindowID: "thread-1:0",
		}),
		WithSession(Session{SessionID: "sess-1", ThreadID: "thread-1", WindowID: "thread-1:0"}))
}

// BenchmarkInjectClientMetadata：现路径——ValidBytes + 单次 sjson.SetRawBytes
// 整体替换 client_metadata。
func BenchmarkInjectClientMetadata(b *testing.B) {
	hc := benchClient()
	p := benchPayload()
	b.ReportAllocs()
	b.SetBytes(int64(len(p)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = hc.injectResponsesClientMetadata(p)
	}
}

// BenchmarkInjectBaseline：无注入基线（下界参照）。
func BenchmarkInjectBaseline(b *testing.B) {
	p := benchPayload()
	b.ReportAllocs()
	b.SetBytes(int64(len(p)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = len(p)
	}
}
