package codexsdk

import (
	"testing"
	"time"
)

// BenchmarkHTTPShapeZstd84KB：HTTP→HTTP 每请求的整形 + zstd 成本，输入是官方
// 0.169.0 的原始请求体（testdata/official-0.169.0/http-body-turn.zst 解压后
// 83595 字节），非匿名身份。
func BenchmarkHTTPShapeZstd84KB(b *testing.B) {
	raw, err := decodeZstd(loadOfficial(b, "http-body-turn.zst"))
	if err != nil {
		b.Fatalf("解压夹具: %v", err)
	}
	id := testID()
	cfg := shapeConfig{}
	b.ReportAllocs()
	b.SetBytes(int64(len(raw)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sh, err := shapeBody(raw, surfaceHTTP, id, time.Now(), cfg)
		if err != nil {
			b.Fatalf("shapeBody: %v", err)
		}
		benchSink = len(zstdCompress(assembleHTTP(&sh)))
	}
}
