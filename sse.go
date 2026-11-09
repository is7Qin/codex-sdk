package codexsdk

import (
	"bytes"
	"context"
	"io"

	"github.com/tidwall/gjson"
)

// SSE 块的三段式落地。一帧渲染成 **三段**，按序吐出：
//
//	prefix  `event:<data.type>\n` + `data:`（无 type 时只有 `data:`）
//	raw     帧字节 **原样**（只被引用，不复制）
//	suffix  `\n\n`；`response.completed` 追加 `data: [DONE]\n\n`
//
// 为什么要分段：T2 的可用版把一帧 append 成一个新块（帧体被复制一遍，且每帧
// 一次分配）。分段后帧体只从段里 copy 进调用方的 p 一次——那是 io.Reader 的
// 必然代价，中间缓冲一次都没有（P2：0 B/op, 0 allocs/op）。
const (
	sseSegPrefix = iota // `event:<type>\n` + `data:`
	sseSegRaw           // 帧体（verbatim）
	sseSegSuffix        // 帧尾空行（completed 时带 [DONE]）
	sseSegCount
)

// 段字节。completed 的帧尾与 [DONE] 拼成**一条**常量：原块是
// 帧体 + "\n\n" + "data: [DONE]\n\n"，合成一条既保持逐字节等价，又不用为
// "两段相邻静态串"付出额外分配。
const (
	sseEventPrefix  = "event:"
	sseFrameTypeKey = "type"
)

// suffix 段的两种形态（包级切片：Read 只读，不会就地改写）。
var (
	sseSuffixFrame = []byte("\n\n")
	sseSuffixDone  = []byte("\n\ndata: [DONE]\n\n")
)

// sseViewPrefixCap 是 prefix 段的固定落地窗：包装字节 12B（`event:`+`\n`+
// `data:`）之外还给 type 留 ≥116B。真实事件名最长约 30B（
// `response.output_text.annotation.added` 级），装不下只可能是上游的自定义
// 事件，那时才走一次分配兜底（帧体仍不复制）。
const sseViewPrefixCap = 128

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
// 终态即提交：终态帧一旦从流里取出，本视图就不再取帧（吐完三段后 EOF），此后
// 的取消与超时不会改写已交付的结果；帧自身的事件语义（failed / incomplete）
// 仍由下游聚合器解释。
//
// 消费者只剩 StreamBody 的 ws 臂（需要"原始 SSE 字节"的外部调用方）；
// HTTPClient.Responses 自 T3 起直接从 wsExchange 取帧聚合，不再经过本视图。
//
// 并发契约：Read 与 Close 由**同一个** goroutine 串行调用（单消费者）。
type sseView struct {
	ctx    context.Context
	stream frameStream
	limit  int64

	pfxBuf [sseViewPrefixCap]byte // prefix 段的固定落地窗（零分配路径）
	segs   [sseSegCount][]byte    // 当前帧的三段
	seg    int                    // 正在吐的段；== sseSegCount ⇒ 需要取下一帧

	terminal bool  // 已取出终态帧：三段吐完即 EOF
	done     bool  // 流已结束（err 为结束原因）
	err      error // 结束原因
}

var _ io.ReadCloser = (*sseView)(nil)

// newSSEView 构造视图（零拷贝分段版）。seg 预置为 sseSegCount：第一次 Read 才
// 取帧，构造本身不碰上游。
func newSSEView(ctx context.Context, stream frameStream, limit int64) *sseView {
	return &sseView{ctx: ctx, stream: stream, limit: limit, seg: sseSegCount}
}

// Read 实现 io.Reader：先把 p 从当前段填满，再换下一段；当前段耗尽且无下一帧
// 时才 Next。不拼接、不复制帧体（帧体只从 raw 段 copy 进 p）。
//
// 终态：completed 的 suffix（含 [DONE]）吐完后返回 io.EOF；failed / incomplete
// 同样在三段后 EOF（只是没有 [DONE]）。
func (v *sseView) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	if v.done {
		return 0, v.err
	}
	n := 0
	for n < len(p) {
		if v.seg == sseSegCount { // 当前帧三段吐完：取下一帧
			if v.terminal { // 终态帧已交付完 ⇒ 流结束（[DONE] 已随 suffix 出去）
				v.done, v.err = true, io.EOF
				break
			}
			if n > 0 {
				// 已经产出字节：先交付，不要为了填满 p 而阻塞等下一帧。SSE 的
				// 逐事件时延靠这条——否则上一帧会卡到下一条帧到达才被看到
				// （模型思考期间就是秒级空白）。
				break
			}
			if err := v.load(); err != nil {
				v.done, v.err = true, err
				break
			}
		}
		seg := v.segs[v.seg]
		copied := copy(p[n:], seg)
		n += copied
		if copied == len(seg) {
			v.seg++
			continue
		}
		v.segs[v.seg] = seg[copied:] // 段内偏移：下次从断点接着吐
	}
	// 循环内刚置 done（终态帧吐尽 / load 失败）且本轮未产出字节 ⇒ 立刻交付终止
	// 原因。入口的 `if v.done` 只覆盖"上次已结束"，这里覆盖"本轮才结束"；已产出
	// 字节时错误留到下次调用（io.Reader 约定：先交付 n>0）。
	if n == 0 && v.done {
		return 0, v.err
	}
	return n, nil
}

// Close 收尾上游（幂等）。
//
//   - 已取出终态帧（`v.terminal`）⇒ **Release**：帧已整帧读出 socket，连接停在
//     干净边界，可以交回缓存（这正是复用能生效的唯一出口）；
//   - 其余情况 ⇒ Close：本次请求被放弃，连接不能留给下一个请求。
//
// 判据只看 `v.terminal`——本方法自己会把 `v.err` 改成 io.EOF，用 err 判断会把
// "正常读完"与"中途放弃"混为一谈。
func (v *sseView) Close() error {
	v.done = true
	if v.err == nil {
		v.err = io.EOF
	}
	if v.terminal {
		v.stream.Release()
		return nil
	}
	return v.stream.Close()
}

// load 取下一帧并把它切成三段：prefix / raw / suffix。
// 帧体只被引用（segs[sseSegRaw] = raw），不复制、不拼接。
func (v *sseView) load() error {
	raw, err := v.stream.Next(v.ctx)
	if err != nil {
		return err
	}
	if v.limit > 0 && int64(len(raw)) > v.limit {
		return ErrMessageTooBig
	}
	typ := frameEventType(raw)
	if prefixNeed(typ) > sseViewPrefixCap {
		v.segs[sseSegPrefix] = buildLongPrefix(typ) // 冷路径：事件名装不进固定窗
	} else {
		v.segs[sseSegPrefix] = v.pfxBuf[:v.buildPrefix(typ)]
	}
	v.segs[sseSegRaw] = raw
	v.segs[sseSegSuffix] = sseSuffixFrame
	switch {
	case bytesAre(typ, responseEventCompleted):
		v.segs[sseSegSuffix] = sseSuffixDone // 正常终态：本帧后补 [DONE]
		v.terminal = true
	case bytesAre(typ, responseEventFailed), bytesAre(typ, responseEventIncomplete):
		v.terminal = true // 失败终态：帧照原样透传，不补 [DONE]
	}
	v.seg = sseSegPrefix
	return nil
}

// prefixNeed 返回一帧 prefix 段的字节数（无 type 时只有 `data:`）。
func prefixNeed(typ []byte) int {
	need := len(sseDataPrefix) + len(typ)
	if len(typ) > 0 {
		need += len(sseEventPrefix) + 1
	}
	return need
}

// buildPrefix 把 prefix 段组装进固定窗口（`event:<type>\n` + `data:`；无 type 时
// 只有 `data:`），返回写入长度。写进窗口的只有包装字节，帧体不进窗口。
// 调用方已按 prefixNeed 确认装得下（超限走 buildLongPrefix）。
func (v *sseView) buildPrefix(typ []byte) int {
	dst := v.pfxBuf[:0]
	if len(typ) > 0 {
		dst = append(dst, sseEventPrefix...)
		dst = append(dst, typ...)
		dst = append(dst, '\n')
	}
	dst = append(dst, sseDataPrefix...)
	return len(dst)
}

// buildLongPrefix 是 prefix 固定窗装不下事件名时的兜底（冷路径）：独立分配一条
// 前缀，不碰视图的窗口——热路径的逃逸分析因此不受这条分配影响（视图可留在栈上）。
func buildLongPrefix(typ []byte) []byte {
	out := make([]byte, 0, prefixNeed(typ))
	out = append(out, sseEventPrefix...)
	out = append(out, typ...)
	out = append(out, '\n')
	return append(out, sseDataPrefix...)
}

// frameEventType 返回帧**顶层** "type" 的字符串值（零分配：返回值是 raw 内部的
// 子切片，不转 string）。
//
// 为什么自己扫：P2 要求"帧 → SSE 渲染"零分配，而 gjson.GetBytes(raw,
// "type").String() 会把值转成 string（至少一次分配），故此处按 JSON 词法做一次
// 浅扫描——外层对象的键值对逐个跳过，嵌套对象/数组里的同名字段不算（与 gjson
// 的 "type" 路径语义一致：只看顶层键）。
//
// 非对象根、非法 JSON、缺失 type、type 非字符串 ⇒ nil。
//
// 与 gjson.GetBytes(raw, "type").String() 的两处**理论**差异（都不影响 SSE 形状：
// 事件名只出现在 `event:` 行，下游按 `data:` 解析）：
//   - type 非字符串：gjson 会序列化成 `123` / `true`，本函数返回 nil ⇒ 不出
//     `event:` 行（空事件名比错事件名安全）。
//   - type 值内含转义：本函数返回**原始字节**（不 unescape）⇒ 事件名保留 `\"` 之类
//     的转义序列；真实上游的 type 恒为 `[a-z._]`，走不到这条。
func frameEventType(raw []byte) []byte {
	i := skipJSONSpace(raw, 0)
	if i >= len(raw) || raw[i] != '{' {
		return nil
	}
	depth := 0
	for ; i < len(raw); i++ {
		switch raw[i] {
		case '"':
			key, next, ok := scanJSONStringAt(raw, i)
			if !ok {
				return nil
			}
			i = next - 1 // 循环自增后落到闭引号之后
			if depth != 1 {
				continue // 嵌套层里的字符串（对象值 / 数组元素）
			}
			j := skipJSONSpace(raw, next)
			if j >= len(raw) || raw[j] != ':' {
				continue // 值位置的字符串：不是键
			}
			if !bytesAre(key, sseFrameTypeKey) {
				continue
			}
			j = skipJSONSpace(raw, j+1)
			if j < len(raw) && raw[j] == '"' {
				if val, _, vok := scanJSONStringAt(raw, j); vok {
					if bytes.IndexByte(val, '\\') < 0 {
						return val // 快路径：真实上游的事件名恒无转义（零分配）
					}
					// 冷路径：事件名含转义（合法 JSON，例如 "response\\u002ecompleted"）。
					// 交给 gjson 展开——"合法的 completed 必须被识别为终态"不能因为一个
					// `\uXXXX` 就变成"事件流截断"。只在这条路径分配。
					return []byte(gjson.GetBytes(raw, "type").String())
				}
			}
			return nil // type 不是字符串 ⇒ 不出 event 行
		case '{', '[':
			depth++
		case '}', ']':
			depth--
			if depth <= 0 {
				return nil // 外层对象已闭合，没有顶层 type
			}
		}
	}
	return nil
}

// scanJSONStringAt 扫描 raw[i] 处的 JSON 字符串（raw[i] 必须是 '"'），返回字符串
// 内容（不含引号，指向 raw 内部）与闭引号之后的下标。转义按"跳过下一字节"处理：
// 键名与事件名都不含转义，值里的转义也不需要展开（原样透传帧字节）。
func scanJSONStringAt(raw []byte, i int) (val []byte, next int, ok bool) {
	start := i
	for i++; i < len(raw); i++ {
		switch raw[i] {
		case '\\':
			i++ // 跳过被转义的字符（\" \\ \n \uXXXX 的首字节）
		case '"':
			return raw[start+1 : i], i + 1, true
		}
	}
	return nil, 0, false
}

// skipJSONSpace 返回 raw[i] 之后第一个非空白字节的下标。
func skipJSONSpace(raw []byte, i int) int {
	for i < len(raw) {
		switch raw[i] {
		case ' ', '\t', '\n', '\r':
			i++
		default:
			return i
		}
	}
	return i
}

// bytesAre 逐字节比较 b 与 s（**不**做 []byte(s) 转换——那次转换会分配）。
func bytesAre(b []byte, s string) bool {
	if len(b) != len(s) {
		return false
	}
	for i := 0; i < len(b); i++ {
		if b[i] != s[i] {
			return false
		}
	}
	return true
}
