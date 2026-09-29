package codexsdk

import (
	"strings"
	"testing"

	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
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

func benchEntries() []metadataEntry {
	return []metadataEntry{
		{codexMetaInstallationKey, "inst-1"},
		{codexMetaSessionKey, "sess-1"},
		{codexMetaThreadKey, "thread-1"},
		{codexMetaWindowKey, "thread-1:0"},
		{codexMetaTurnKey, "01a0ec62-daad-74a2-be2c-95ba7e7eb5ca"},
	}
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

// BenchmarkInjectKeyLoopLegacy：历史实现——ValidBytes + N× sjson.SetBytes 逐键
// 覆盖（N 次重序列化整份 body）。保留以固化"整体替换更快"的实测结论。
func BenchmarkInjectKeyLoopLegacy(b *testing.B) {
	p := benchPayload()
	entries := benchEntries()
	b.ReportAllocs()
	b.SetBytes(int64(len(p)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if !gjson.ValidBytes(p) {
			continue
		}
		out := p
		for _, e := range entries {
			next, err := sjson.SetBytes(out, "client_metadata."+e.key, e.value)
			if err != nil {
				break
			}
			out = next
		}
		_ = out
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
