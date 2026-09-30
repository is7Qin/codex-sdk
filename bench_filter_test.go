package codexsdk

import "testing"

// BenchmarkCodexPayloadFilterClean：285KB 干净 WS 帧（顶层键全在白名单内）——
// 走零拷贝顶层键扫描，B/op 与 allocs/op 均为 0（消除每帧 payload 级拷贝的
// GC 压力）。注：ns/op 不承诺下降（纯扫描可能略高于 gjson ForEach）。
func BenchmarkCodexPayloadFilterClean(b *testing.B) {
	raw := cleanCodexFrame()
	b.ReportAllocs()
	b.SetBytes(int64(len(raw)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = FilterCodexWsPayload(raw)
	}
}

// BenchmarkCodexPayloadFilterDirty：含 max_output_tokens（白名单外键）的 285KB
// 帧——扫描命中后走按需重建路径（值原样搬移，非零拷贝；仅作对照）。
func BenchmarkCodexPayloadFilterDirty(b *testing.B) {
	raw := dirtyCodexFrame()
	b.ReportAllocs()
	b.SetBytes(int64(len(raw)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = FilterCodexWsPayload(raw)
	}
}
