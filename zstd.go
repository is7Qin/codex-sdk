package codexsdk

import (
	"bytes"
	"runtime"

	"github.com/klauspost/compress/zstd"
)

// zstdEncoderOptions 对齐官方 zstd::stream::encode_all(…, 3)：级别 3
// （SpeedDefault）、无校验和、不写内容大小、windowLog 21；并发 1（不为每块派生
// goroutine）。WithLowerEncoderMem 只改变编码器内部缓冲的分配方式（单个编码器约
// 6MB → 约 3.7MB），输出字节不变（TestZstdLowerEncoderMemSameBytes 钉住）。
func zstdEncoderOptions(lowerMem bool) []zstd.EOption {
	return []zstd.EOption{
		zstd.WithEncoderLevel(zstd.SpeedDefault),
		zstd.WithEncoderCRC(false),
		zstd.WithWindowSize(1 << 21),
		zstd.WithEncoderConcurrency(1),
		zstd.WithLowerEncoderMem(lowerMem),
	}
}

func newZstdEncoder() *zstd.Encoder {
	enc, err := zstd.NewWriter(nil, zstdEncoderOptions(true)...)
	if err != nil { // 选项是常量：只有实现缺陷才会到这里
		panic("codexsdk: 构造 zstd 编码器失败: " + err.Error())
	}
	return enc
}

// zstdIdle 是空闲编码器的有界列表（容量 = 进程启动时的 GOMAXPROCS）。
//
// 不用 sync.Pool：单个编码器带窗口与哈希表（约 3.7MB），sync.Pool 不设上限，
// 突发并发会让堆按"并发数 × 编码器"上涨，GC 后又整体清空重建。这里空闲列表
// 有上限：列表空时临时新建，用完放回；列表满时多余的直接丢弃（交给 GC）。
// 常驻内存因此不超过 GOMAXPROCS 个编码器。
var zstdIdle = make(chan *zstd.Encoder, max(1, runtime.GOMAXPROCS(0)))

func getZstdEncoder() *zstd.Encoder {
	select {
	case enc := <-zstdIdle:
		return enc
	default:
		return newZstdEncoder()
	}
}

func putZstdEncoder(enc *zstd.Encoder) {
	// 放回前断开对本次输出缓冲的引用（Reset 不重新分配窗口与哈希表）。
	enc.Reset(nil)
	select {
	case zstdIdle <- enc:
	default: // 空闲列表已满：丢弃
	}
}

// zstdCompress 把 /responses 请求体压成一个 zstd 帧。
//
// 必须 Reset → Write → Flush → Close：没有 Flush 时，首块尚未写出就 Close 会走
// 单段编码，写入内容大小并按输入缩小窗口，帧头就不再是官方的
// 28b52ffd 00 58（无内容大小、非单段、无校验和、windowLog 21）。
func zstdCompress(body []byte) []byte {
	enc := getZstdEncoder()
	var buf bytes.Buffer
	buf.Grow(len(body)/3 + 64)
	enc.Reset(&buf)
	// 写入 bytes.Buffer 不会失败；编码器自身错误只可能来自实现缺陷。
	_, _ = enc.Write(body)
	_ = enc.Flush()
	_ = enc.Close()
	putZstdEncoder(enc)
	return buf.Bytes()
}
