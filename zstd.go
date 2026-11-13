package codexsdk

import (
	"bytes"
	"sync"

	"github.com/klauspost/compress/zstd"
)

// zstdEncoderPool 复用流式 zstd 编码器（不得每请求新建：单个编码器带窗口与
// 哈希表，新建代价远高于压缩本身）。
//
// 参数对齐官方 zstd::stream::encode_all(…, 3)：级别 3（SpeedDefault）、无校验和、
// 不写内容大小、windowLog 21；并发 1（不为每块派生 goroutine）。
var zstdEncoderPool = sync.Pool{New: func() any {
	enc, err := zstd.NewWriter(nil,
		zstd.WithEncoderLevel(zstd.SpeedDefault),
		zstd.WithEncoderCRC(false),
		zstd.WithWindowSize(1<<21),
		zstd.WithEncoderConcurrency(1),
	)
	if err != nil { // 选项是常量：只有实现缺陷才会到这里
		panic("codexsdk: 构造 zstd 编码器失败: " + err.Error())
	}
	return enc
}}

// zstdCompress 把 /responses 请求体压成一个 zstd 帧。
//
// 必须 Reset → Write → Flush → Close：没有 Flush 时，首块尚未写出就 Close 会走
// 单段编码，写入内容大小并按输入缩小窗口，帧头就不再是官方的
// 28b52ffd 00 58（无内容大小、非单段、无校验和、windowLog 21）。
func zstdCompress(body []byte) []byte {
	enc := zstdEncoderPool.Get().(*zstd.Encoder)
	var buf bytes.Buffer
	buf.Grow(len(body)/3 + 64)
	enc.Reset(&buf)
	// 写入 bytes.Buffer 不会失败；编码器自身错误只可能来自实现缺陷。
	_, _ = enc.Write(body)
	_ = enc.Flush()
	_ = enc.Close()
	// 不再 Reset(nil)：Reset 会清编码器状态，下一次取用时还要再做一次；池里的
	// 编码器暂时引用本次输出缓冲（只读、无后续写入）可以接受。
	zstdEncoderPool.Put(enc)
	return buf.Bytes()
}
