package codexsdk

import (
	"bytes"
	"errors"
	"time"

	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

// responsesCreateType 是 WS response.create 帧的 type 值（WS 面恒带，HTTP
// 面没有该字段）。
const responsesCreateType = "response.create"

// errResponsesFrameInvalid 是请求体无法构造成 WS response.create 帧（本地输入
// 错误，非上游错误）：非 JSON / 非对象根。
//
// 为什么本地拒绝而不是原样发出：HTTP 面对坏 body 是把字节原样发给上游、由
// 上游 400 兜底；WS 面没有这条等价路径——sjson/gjson 对 `null`/标量/数组根
// 会**静默**把请求体改写成 `{}`，发出去就成了"一个只剩 type/stream 的空帧"，
// 与 HTTP 臂语义立刻不同。白名单过滤后为空（ErrEmptyFrame）也归在此列。
var errResponsesFrameInvalid = errors.New("codexsdk: 无法构造 ws response.create 帧：请求体必须是 JSON 对象")

// encodeMode 选择 wire 编码形态。
type encodeMode uint8

const (
	// encodeNativeWS 只做 WS 归一（白名单 + store:false + client_metadata），
	// 用于公开 Client.Send 的原生帧。
	encodeNativeWS encodeMode = iota
	// encodeWSResponses 额外做 HTTP→WS 的协议形状差（type 覆写 / 删续接键 /
	// 强制 stream），用于 Responses 传输臂。
	encodeWSResponses
)

// frameInput 是一次编码的请求级输入（快照，不随后续状态变化）。
type frameInput struct {
	Identity Identity  // 本次请求的全部身份（Session/Meta/TurnState）
	Turn     uint64    // 0 = 不生成 turn_metadata
	Now      time.Time // 环境时间改写基准
}

// frameCodec 负责把请求体 / 原生帧编码成 wire 字节。**无可变状态**（配置在
// 构造期固定），可并发使用；请求级差异全部经 frameInput 传入（身份不驻留
// codec）。
type frameCodec struct {
	trace        *TraceContext
	traceAuto    bool
	turnAuto     bool
	turnProvider func(turn uint64) string
}

// newFrameCodec 从 options 构造 codec（值拷贝语义：之后改 options 不影响它）。
func newFrameCodec(o *options) *frameCodec {
	return &frameCodec{
		trace:        o.trace,
		traceAuto:    o.traceAuto,
		turnAuto:     o.turnAuto,
		turnProvider: o.turnProvider,
	}
}

// encode 产出 wire 字节：先按形态做协议形状归一，再套伪装层。
//
// 主路径是**单遍**编码（encodeOnePass：一次遍历、一份输出缓冲，把协议形状差与
// 伪装层合并落地）；旧链（buildResponsesCreateFrame + normalize，每步整帧重写）
// 保留为回退路径，只服务单遍不保证字节一致的形态（见 encodeOnePass 的条件表）。
//
// 环境时间改写与 metadata 组装在两条路径上同位同义：改写先做（只替换值内文本，
// 不动键序、不动其余字节），metadata 只组装一次（obj 由两条路径共用——
// turnProvider 恒只调一次）。
func (fc *frameCodec) encode(raw []byte, mode encodeMode, in frameInput) ([]byte, error) {
	now := in.Now
	if now.IsZero() {
		now = time.Now()
	}
	frame := RewriteEnvironmentContextTime(raw, now)
	// 惰性组装 entries：先在栈上预留 8 项（典型身份键数），超出才转堆——
	// shared helper 下须显式保留栈缓冲，否则返回切片的 helper 会使 entries
	// 逃逸到堆、平白多一次分配（HTTP 面同理）。
	//
	// 组装发生在单遍/回退判定**之前**：注定报错的帧（非法 JSON / 非对象根 /
	// ErrEmptyFrame）也会调一次 turnProvider、耗一次 crypto/rand。代价换来的是
	// "两条路径共用同一份 obj、turnProvider 恒只调一次"——比给错误帧另写一套
	// 预检更省事，也让 T3 的回退链语义一处不变。
	var entryBuf [8]metadataEntry
	obj := clientMetadataObject(fc.metaEntries(entryBuf[:0], in))
	if out, ok := encodeOnePass(frame, mode, obj); ok {
		return out, nil
	}
	return fc.normalize(frame, mode, obj)
}

// metaEntries 组装本次编码的 client_metadata 注入项。优先级：Identity.Meta >
// Identity.Session > turn-state 回传 > 自动机制（turn_id / trace）>
// turn_metadata 回调；同名键取首次出现（clientMetadataObject 去重）。
//
// buf 由调用方在栈上预留并传入：helper 返回切片，缓冲若留在本函数内就会逃逸到堆。
func (fc *frameCodec) metaEntries(buf []metadataEntry, in frameInput) metaEntries {
	entries := metaEntries(buf[:0])
	entries = entries.withCodexMetaIdentity(in.Identity.Meta)
	entries = entries.add(codexMetaTurnMetadataKey, in.Identity.Meta.TurnMetadata)
	entries = entries.add(codexMetaTraceparentKey, in.Identity.Meta.Traceparent)
	entries = entries.add(codexMetaTracestateKey, in.Identity.Meta.Tracestate)
	entries = entries.withSessionIdentity(in.Identity.Session)
	// turn-state 回传（TurnState 非空时帧 metadata 恒带）：真实 codex WS
	// 发送路径显式追加注入（client.rs:1626-1631，帧体携带 :1702-1711），与
	// HTTP 头面（client.rs:1202）构成双面机制——此注入与真实逐点一致，勿删
	// （完整实证见 doc.go"上游协议与参考"）。
	if in.Identity.TurnState != "" {
		entries = entries.add(codexMetaTurnStateKey, in.Identity.TurnState)
	}
	// turn_id 恒带且覆盖：帧内已有 client_metadata.turn_id 不参与透传——
	// 无静态值时每帧自动生成 UUIDv7（Identity.Meta.TurnID 静态值优先），网关值恒为准。
	if fc.turnAuto && in.Identity.Meta.TurnID == "" {
		entries = entries.add(codexMetaTurnKey, NewUUIDv7())
	}
	// 每帧 trace：外部 WithTraceContext 静态值优先，否则每帧自动生成
	//（真实客户端每请求新 span，同轮多请求 traceparent 不同；帧内已有
	// traceparent 不参与透传，网关值恒为准）。
	if fc.trace != nil {
		entries = entries.add(codexMetaTraceparentKey, fc.trace.Traceparent)
		entries = entries.add(codexMetaTracestateKey, fc.trace.Tracestate)
	} else if fc.traceAuto {
		tc := NewTraceContext()
		entries = entries.add(codexMetaTraceparentKey, tc.Traceparent)
		entries = entries.add(codexMetaTracestateKey, tc.Tracestate)
	}
	if fc.turnProvider != nil && in.Turn > 0 {
		entries = entries.add(codexMetaTurnMetadataKey, fc.turnProvider(in.Turn))
	}
	return entries
}

// normalize 是回退路径的伪装层（与 T3 旧链逐字节同义）：白名单过滤 + 强制
// store:false + client_metadata 整体替换。注入对象由 encode 组装后传入
// （obj 为空 = 无有效项 ⇒ 删除帧内该键，不是"不动帧"）。
//
// 只有 encodeOnePass 明确不覆盖的形态走到这里：白名单外顶层键（旧链按白名单
// 顺序重建）、转义键（gjson unescape 语义）、重复键（sjson 首现语义）、
// 非紧凑输入（sjson 原位替换会保留原有空白）、非法 JSON / 非对象根。
func (fc *frameCodec) normalize(frame []byte, mode encodeMode, obj []byte) ([]byte, error) {
	if mode == encodeWSResponses {
		var err error
		if frame, err = buildResponsesCreateFrame(frame); err != nil {
			return nil, err
		}
	}
	frame, err := FilterCodexWsPayload(frame) // 始终过滤（不再有开关）
	if err != nil {
		return nil, err
	}
	frame = forceCodexStoreFalse(frame)
	// 非法 JSON 放弃注入、帧原样返回（对齐 HTTP 面 injectResponsesClientMetadata
	// 前置守卫——实测 sjson.SetRawBytes 对非法 JSON 静默产出损坏字节且 err=nil）。
	if !gjson.ValidBytes(frame) {
		return frame, nil
	}
	return injectClientMetadataObject(frame, obj), nil
}

// buildResponsesCreateFrame 把 HTTP Responses 请求体转成 WS response.create 帧。
// 规则（缺一不可——写错会让两个传输不可比）：
//  1. 自校验 JSON 合法性**与对象根**（见 errResponsesFrameInvalid）；
//  2. 覆写 type:"response.create"（不是"缺了才补"——HTTP 面没有 type，客户端
//     也可以塞任意值）；
//  3. 删 previous_response_id / generate（HTTP 面没有这两项，透传等于让 WS 走
//     服务端续接，两臂语义立刻不同）；
//  4. 强制 stream:true（stream:false 的 WS 语义未知；非流式由聚合器合成）；
//  5. 过 FilterCodexWsPayload（19 键白名单）；白名单后为空 → ErrEmptyFrame。
//
// 帧的其余归一**不在这里**（store:false 强制、client_metadata 组装、turn-state
// 回传）：它们在 frameCodec.encode 内无条件执行，是 WS 面共用的唯一归一入口。
//
// 生产路径的协议形状差由 encodeOnePass 单遍承担（同样这五条规则）；本函数是
// **回退实现**：单遍只覆盖紧凑 / 无转义键 / 无重复键 / 顶层键全在白名单内的
// 对象，其余形态（旧链的白名单序重建、gjson unescape、sjson 首现语义）仍走
// 这里，字节语义与 T3 完全一致（单测亦直接钉住本函数）。
func buildResponsesCreateFrame(payload []byte) ([]byte, error) {
	if !gjson.ValidBytes(payload) || !isJSONObjectRoot(payload) {
		return nil, errResponsesFrameInvalid
	}
	frame, err := sjson.SetBytes(payload, "type", responsesCreateType)
	if err != nil {
		return nil, errResponsesFrameInvalid
	}
	// 删除吞错：键为静态字面量、输入已过 gjson.ValidBytes，删除不会失败
	//（与 filterCodexPayload 吞 sjson 错误同款理由）。
	frame, _ = sjson.DeleteBytes(frame, "previous_response_id")
	frame, _ = sjson.DeleteBytes(frame, "generate")
	if frame, err = sjson.SetBytes(frame, "stream", true); err != nil {
		return nil, errResponsesFrameInvalid
	}
	return FilterCodexWsPayload(frame)
}

// isJSONObjectRoot 报告 raw 是否为对象根（`{` 开头，允许前导 JSON 空白）。
// 调用方须先过 gjson.ValidBytes——本函数只做根形状判定，不校验结构。
func isJSONObjectRoot(raw []byte) bool {
	for i := 0; i < len(raw); i++ {
		switch raw[i] {
		case ' ', '\t', '\n', '\r':
			continue
		default:
			return raw[i] == '{'
		}
	}
	return false
}

// ---- 单遍编码器 ----
//
// T3 的链是"每归一一步整帧重写一遍"：Set(type) → Delete×2 → Set(stream) →
// 白名单重建 → 强制 store → 整体替换 client_metadata。3.7KB 体上每一步都是
// 一次全帧复制 + 一次全帧校验（P1 基线 33.4µs / 22KB / 26 allocs），而其中
// **只有值在变、键序不变**——所以这里把它们折成"一次遍历、一份输出缓冲"。
//
// 字节一致性的落点：旧链在"顶层键全在白名单内"时不做白名单重建，只原位替换
// 值（键序 = 输入序），单遍按输入顺序搬移原始值即天然一致；其余形态一律回退
// 旧链（见 encodeOnePass 的条件表），不赌语义。

// wsKeySemantics 标记单遍编码器要对某个顶层键做的语义（其余白名单键原样搬移）。
type wsKeySemantics uint8

const (
	wsKeyPlain              wsKeySemantics = iota
	wsKeyType                              // encodeWSResponses：覆写为 response.create
	wsKeyPreviousResponseID                // encodeWSResponses：删除（HTTP 面无续接键）
	wsKeyGenerate                          // encodeWSResponses：删除
	wsKeyStream                            // encodeWSResponses：强制 true
	wsKeyStore                             // 两种模式都强制 false
	wsKeyClientMetadata                    // 有注入项时整体替换
)

// wsTopKey 是白名单键的单遍编码查表项：idx 为它在 codexWsPayloadFields 中的
// 位置（作重复键检测位图的下标，见 buildWSTopKeys 的 64 键上限），sem 为编码语义。
type wsTopKey struct {
	idx uint8
	sem wsKeySemantics
}

// wsTopKeys 是 codexWsPayloadFields 的构建期派生表。单一事实来源仍是白名单
// 本身（加键/删键只改白名单，语义按 key 名另给）；只读，勿改。
var wsTopKeys = buildWSTopKeys()

// wsTopKeyLimit 是重复键位图的宽度上限（seen 是 uint64）。超限时 `1 << idx`
// 在 Go 里合法且等于 0 ⇒ 重复键检测**静默关闭** ⇒ 单遍会把 `{"k":1,"k":2}` 原样
// 输出两个键，而回退链（sjson 首现语义）只留一个，wire 字节立刻分叉。故在构建期
// 直接 panic：这是源码级不变量，不能靠随机语料撞出来。
const wsTopKeyLimit = 64

func buildWSTopKeys() map[string]wsTopKey {
	if len(codexWsPayloadFields) > wsTopKeyLimit {
		panic("codexsdk: codexWsPayloadFields 超过 64 键，重复键位图会静默失效")
	}
	m := make(map[string]wsTopKey, len(codexWsPayloadFields))
	for i, field := range codexWsPayloadFields {
		m[field] = wsTopKey{idx: uint8(i), sem: wsKeySemanticsOf(field)}
	}
	return m
}

func wsKeySemanticsOf(field string) wsKeySemantics {
	switch field {
	case "type":
		return wsKeyType
	case "previous_response_id":
		return wsKeyPreviousResponseID
	case "generate":
		return wsKeyGenerate
	case "stream":
		return wsKeyStream
	case "store":
		return wsKeyStore
	case clientMetadataKey:
		return wsKeyClientMetadata
	default:
		return wsKeyPlain
	}
}

// 单遍写出的字面量：与 sjson 对同一键的产出**逐字节一致**（紧凑、无空格），
// 故"值本来就对也照写"等于"原样搬移"。只读，勿改。
var (
	wsTypeField   = []byte(`"type":"` + responsesCreateType + `"`)
	wsStreamField = []byte(`"stream":true`)
	wsStoreField  = []byte(`"store":false`)
	wsMetaField   = []byte(`"client_metadata":`)
	// wsTypeValue / wsTrueValue / wsFalseValue 是上面三者的"值部分"，供
	// "值本来就对 → 不算改写"的判定复用（判定为未改写时 encode 原切片返回，
	// 保住 Send 的零拷贝快速路径）。
	wsTypeValue  = []byte(`"` + responsesCreateType + `"`)
	wsTrueValue  = []byte("true")
	wsFalseValue = []byte("false")
)

// encodeOnePass 是 encode 的单遍实现：一次顶层扫描把请求体直接写成 wire 字节。
// 返回 ok=false 表示"本输入不在单遍的字节保证内"，调用方回退旧链。
//
// 单遍只接**紧凑、无转义键、无重复键、顶层键全在白名单内**的对象：此时旧链
// 的逐键改写只换值不动键序，按输入顺序搬移原始值即逐字节等价。回退条件与对应
// 原因（一条都不能放宽——放宽即字节走样）：
//
//	非法 JSON / 非对象根      → 旧链的错误语义（errResponsesFrameInvalid / ErrEmptyFrame）
//	首字节非 '{'、末字节非 '}' → 前后空白：sjson 保留原字节
//	顶层键含 '\'              → gjson 的 unescape 键匹配语义（本扫描只比原始字节）
//	顶层键不在白名单          → 旧链按**白名单顺序**重建（键序会变）
//	重复顶层键                → sjson "首现"语义（本扫描只记位图）
//	键/冒号/值/逗号之间有空白 → sjson 原位替换会保留原空白，本扫描不重建空白
//
// 输出缓冲**懒物化**：out == nil 表示"到目前为止一个字节都不用改"，出现第一处真实
// 差异才分配（并把 frame[:字段起点] 原样搬进来）。因此"已归一帧"连输出缓冲都不
// 付：原切片返回，分配次数与字节数都是 0（doc.go 的"零额外分配"由
// TestEncodeZeroCopyAllocFree 钉住）。
func encodeOnePass(frame []byte, mode encodeMode, obj []byte) ([]byte, bool) {
	n := len(frame)
	if n < 2 || frame[0] != '{' || frame[n-1] != '}' || !gjson.ValidBytes(frame) {
		return nil, false
	}
	var out []byte // 懒物化：仅在出现第一处真实差异时分配
	mark := 0      // frame[mark:] 尚未搬入 out（仅 out != nil 时有意义）
	wroteType, wroteStream, wroteStore, wroteMeta := false, false, false, false
	seen := uint64(0) // 白名单位置位图（宽度上限见 wsTopKeyLimit）：重复键 → 回退
	i := 1
	for i < n-1 {
		if frame[i] != '"' {
			return nil, false
		}
		keyStart := i + 1 // 开引号之后
		colon := skipJSONString(frame, i)
		if colon < 0 || colon >= n-1 || frame[colon] != ':' {
			return nil, false
		}
		key := frame[keyStart : colon-1]
		if bytes.IndexByte(key, '\\') >= 0 {
			return nil, false
		}
		slot, ok := wsTopKeys[string(key)] // 编译器特化的无分配 map 查找
		if !ok {
			return nil, false
		}
		bit := uint64(1) << slot.idx
		if seen&bit != 0 {
			return nil, false
		}
		seen |= bit
		valStart := colon + 1
		valEnd := skipJSONValue(frame, valStart)
		if valEnd < 0 || valEnd > n-1 {
			return nil, false
		}
		if valEnd < n-1 && frame[valEnd] != ',' {
			return nil, false // 值后有空白：sjson 会保留它，本扫描不重建空白
		}
		value := frame[valStart:valEnd]
		fieldStart := keyStart - 1 // 开引号
		i = valEnd + 1
		// 每个键三选一：原样搬移 / 删除 / 改写。判定用**值字节是否已经正确**——
		// 本来就对就按搬移处理，输出才可能保持"零分配 + 原切片返回"。
		act, field, withObj := wsActCopy, []byte(nil), false
		switch slot.sem {
		case wsKeyPreviousResponseID, wsKeyGenerate:
			if mode == encodeWSResponses { // 续接键只在 HTTP 面存在，WS 面删除
				act = wsActDelete
			}
		case wsKeyType:
			if mode == encodeWSResponses {
				wroteType = true
				if !bytes.Equal(value, wsTypeValue) {
					act, field = wsActWrite, wsTypeField
				}
			}
		case wsKeyStream:
			if mode == encodeWSResponses {
				wroteStream = true
				if !bytes.Equal(value, wsTrueValue) {
					act, field = wsActWrite, wsStreamField
				}
			}
		case wsKeyStore:
			wroteStore = true
			if !bytes.Equal(value, wsFalseValue) {
				act, field = wsActWrite, wsStoreField
			}
		case wsKeyClientMetadata:
			wroteMeta = true
			switch {
			case len(obj) == 0:
				// 身份只来自 Identity；本次为空 ⇒ 删掉 payload 自带的身份，
				// 否则"匿名"会把调用方的旧 session/thread/ts 透传给上游。
				act, field = wsActDelete, wsMetaField
			case !bytes.Equal(value, obj): // 注入对象已一致 ⇒ 不必重写
				act, field, withObj = wsActWrite, wsMetaField, true
			}
		}
		if act == wsActCopy {
			if out != nil { // 已物化：原字节照搬（键、冒号、值、前导逗号）
				out = appendWSChunk(out, frame[mark:valEnd])
				mark = valEnd
			}
			continue
		}
		// 改写 / 删除：把 out 物化到字段起点（首次分配），并去掉尾部逗号——
		// 分隔符统一由 appendWSField 补，避免"删掉首个字段后留下前导逗号"。
		if out == nil {
			out = make([]byte, 0, n+len(obj)+32)
			out = append(out, frame[0:fieldStart]...)
		} else {
			out = appendWSChunk(out, frame[mark:fieldStart])
		}
		if l := len(out); l > 0 && out[l-1] == ',' {
			out = out[:l-1]
		}
		mark = valEnd
		if act == wsActWrite {
			out = appendWSField(out, field)
			if withObj {
				out = append(out, obj...)
			}
		}
	}
	// 尾部补齐：缺失字段按旧链 sjson.Set 的追加顺序排在末尾。
	if mode == encodeWSResponses && !wroteType {
		out, mark = wsMaterializeTail(out, mark, frame, n, len(obj))
		out = appendWSField(out, wsTypeField)
	}
	if mode == encodeWSResponses && !wroteStream {
		out, mark = wsMaterializeTail(out, mark, frame, n, len(obj))
		out = appendWSField(out, wsStreamField)
	}
	if !wroteStore {
		out, mark = wsMaterializeTail(out, mark, frame, n, len(obj))
		out = appendWSField(out, wsStoreField)
	}
	if !wroteMeta && len(obj) > 0 {
		out, mark = wsMaterializeTail(out, mark, frame, n, len(obj))
		out = appendWSField(out, wsMetaField)
		out = append(out, obj...)
	}
	if out == nil {
		return frame, true // 一个字节都不用改：原切片、零分配返回
	}
	out = appendWSChunk(out, frame[mark:])
	return out, true
}

// wsAct 是单遍编码对单个顶层键的动作。
type wsAct uint8

const (
	wsActCopy   wsAct = iota // 原样搬移（值已正确，或该键与本次模式无关）
	wsActDelete              // 删除（WS 面的续接键）
	wsActWrite               // 改写为固定字面量 / 注入对象
)

// wsMaterializeTail 保证 out 已覆盖 frame[:n-1]（除尾部 '}' 外的全部字节），供
// "缺失字段追加在末尾"使用；mark 随之推到 n-1。
func wsMaterializeTail(out []byte, mark int, frame []byte, n, extra int) ([]byte, int) {
	if out == nil {
		out = make([]byte, 0, n+extra+32)
	}
	return appendWSChunk(out, frame[mark:n-1]), n - 1
}

// appendWSChunk 追加一段原样字节。唯一例外：out 刚停在 '{'（首个字段被删）时丢掉
// 该段的前导 ','——它与已删字段的分隔符是同一个，留着就是 `{,"a":1`。
func appendWSChunk(out, chunk []byte) []byte {
	if len(chunk) > 0 && chunk[0] == ',' && len(out) > 0 && out[len(out)-1] == '{' {
		chunk = chunk[1:]
	}
	return append(out, chunk...)
}

// appendWSField 追加一个顶层字段（需要时先补分隔符 ','）。调用方须保证 out 以
// '{' 或某个值结尾（待替换字段前的逗号已去掉）。
func appendWSField(out, field []byte) []byte {
	if l := len(out); l > 0 && out[l-1] != '{' {
		out = append(out, ',')
	}
	return append(out, field...)
}
