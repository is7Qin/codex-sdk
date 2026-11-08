package codexsdk

import (
	"context"
	"io"

	"github.com/tidwall/gjson"
)

// sseBlockOverhead 是一帧渲染成 SSE 块时附加的字节余量：`event:` 行（含事件名）
// + `data:` 前缀 + 帧尾空行。聚合扫描上限按它放大，使"恰好顶到帧上限的合法帧"
// 不会因为多出的包装字节在扫描阶段撞上同一档上限。
const sseBlockOverhead = 1024

// sseView 把入站帧流（frameStream）表达成 SSE 字节流（io.ReadCloser）。
//
// 渲染规则（与 HTTP 裸 body 逐字节等价）：
//   - 每帧产出 `event:<data.type>\n` + `data:<frame>\n\n`，帧字节 **verbatim**
//     （不再序列化：字段名集合与顺序与上游逐字节一致）；
//   - `response.completed` → 本帧后补 `data: [DONE]\n\n` 并结束；
//   - `response.failed` / `response.incomplete` → 作为**帧**透传后结束（不补
//     [DONE]：失败终态不该被读成"正常收尾"）；
//   - 其余帧（含 `type:"error"`——**不分** created 前后）原样透传继续读：
//     HTTP 臂的 200 流里 in-band error 也是普通帧，由下游按终态事件判定；
//   - 无 type 的帧只出 data 行（不写空的 `event:` 名）。
//
// 终态即提交：终态帧一旦从流里取出，本视图就不再取帧（返回 EOF / 终止原因），
// 此后的取消与超时不会改写已交付的结果；帧自身的事件语义（failed /
// incomplete）仍由下游聚合器解释。
//
// 并发契约：Read 与 Close 由**同一个** goroutine 串行调用（单消费者）。
type sseView struct {
	ctx    context.Context
	stream frameStream
	limit  int64

	block    []byte // 可复用的块缓冲（单消费者，渲染下一帧时复用）
	pending  []byte // 当前块的未读余量
	terminal bool   // 已渲染终态帧：不再取帧
	done     bool   // 流已结束（err 为结束原因）
	err      error
}

var _ io.ReadCloser = (*sseView)(nil)

// newSSEView 构造视图（可用版：正确优先，零拷贝分段读取属后续优化）。
func newSSEView(ctx context.Context, stream frameStream, limit int64) *sseView {
	return &sseView{ctx: ctx, stream: stream, limit: limit}
}

// Read 实现 io.Reader：一次最多消费当前块剩余字节，块耗尽才取下一帧。
func (v *sseView) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	if len(v.pending) == 0 {
		if v.done {
			return 0, v.err
		}
		frame, err := v.stream.Next(v.ctx)
		if err != nil {
			v.done = true
			v.err = err
			return 0, err
		}
		block, terminal, berr := appendFrameBlock(v.block[:0], frame, v.limit)
		if berr != nil {
			v.done = true
			v.err = berr
			return 0, berr
		}
		v.terminal = terminal
		v.block = block
		v.pending = block
	}
	n := copy(p, v.pending)
	v.pending = v.pending[n:]
	if len(v.pending) == 0 && v.terminal {
		v.done = true
		v.err = io.EOF
	}
	return n, nil
}

// Close 关闭上游（幂等）。
func (v *sseView) Close() error {
	v.done = true
	if v.err == nil {
		v.err = io.EOF
	}
	return v.stream.Close()
}

// renderFrameBlock 把一帧渲染成独立的 SSE 块（P2 bench 的测量对象）。
func renderFrameBlock(raw []byte, limit int64) ([]byte, bool, error) {
	return appendFrameBlock(nil, raw, limit)
}

// appendFrameBlock 把一帧渲染进 dst（追加语义），并报告它是否终结了本流。
// 容量按实际需要预留（event: + type + \n + data: + frame + \n\n = 14 字节包装）。
func appendFrameBlock(dst, raw []byte, limit int64) ([]byte, bool, error) {
	if limit > 0 && int64(len(raw)) > limit {
		return nil, false, ErrMessageTooBig
	}
	typ := gjson.GetBytes(raw, "type").String()
	block := dst
	if cap(block) < len(typ)+len(raw)+14 {
		block = make([]byte, 0, len(typ)+len(raw)+14)
	}
	if typ != "" {
		block = append(block, "event:"...)
		block = append(block, typ...)
		block = append(block, '\n')
	}
	block = append(block, "data:"...)
	block = append(block, raw...)
	block = append(block, '\n', '\n')
	switch typ {
	case responseEventCompleted:
		return append(block, "data: [DONE]\n\n"...), true, nil
	case responseEventFailed, responseEventIncomplete:
		return block, true, nil
	}
	return block, false, nil
}
