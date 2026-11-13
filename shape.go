package codexsdk

import (
	"bytes"
	"errors"
	"math/rand/v2"
	"strconv"
	"strings"
	"time"

	"github.com/tidwall/gjson"
)

// ErrInvalidRequestBody 表示请求体不能整形成 Codex responses 请求：不是 JSON
// 对象、缺少 model 或 model 不是非空且不含控制字节（<0x20、0x7f）的字符串、
// service_tier 含控制字节，或 input 既不是数组、字符串也不是 null。
// HTTP 与 WS 都在本地拒绝（不发上游、不烧握手）。
var ErrInvalidRequestBody = errors.New("codexsdk: 请求体无效：必须是带字符串 model 的 JSON 对象")

// ---- 两段式整形 ----
//
// 第一段 parseRequest 只做一次顶层扫描（白名单、同名键首现、值的原始片段）；
// 第二段 shapeRequest 只做与身份、模型有关的变换，产出可组装的部件；
// assembleHTTP / assembleWS 按官方键序一次写出。组装后的字节不再做任何改写
// （唯一例外是 x-codex-ws-stream-request-start-ms 的 13 位数字，在取得写闸后由
// conn.writeStamped 原地覆写）。

// surface 是请求的出站面。
type surface uint8

const (
	surfaceHTTP       surface = iota // HTTP→HTTP
	surfaceWSFromHTTP                // HTTP→WS（HTTPClient 的 WS 臂）
	surfaceWSNative                  // WS→WS（公开 Dial / Client.Send）
)

// shapeKey 是规范顶层键。
type shapeKey uint8

const (
	keyModel shapeKey = iota
	keyStream
	keyServiceTier
	keyPreviousResponseID
	keyInput
	keyTools
	keyToolChoice
	keyParallelToolCalls
	keyReasoning
	keyStore
	keyStreamOptions
	keyInclude
	keyPromptCacheKey
	keyText
	keyGenerate
	keyAccessPrograms
	keyInstructions
	nShapeKeys
)

// shapeKeyOf 把解码后的顶层键名映射为规范键（白名单）。previous_response_id 与
// generate 只在 WS→WS 面认；type、client_metadata 与白名单外键一律丢弃。
func shapeKeyOf(name string, s surface) (shapeKey, bool) {
	switch name {
	case "model":
		return keyModel, true
	case "instructions":
		return keyInstructions, true
	case "input":
		return keyInput, true
	case "tools":
		return keyTools, true
	case "tool_choice":
		return keyToolChoice, true
	case "parallel_tool_calls":
		return keyParallelToolCalls, true
	case "reasoning":
		return keyReasoning, true
	case "store":
		return keyStore, true
	case "stream":
		return keyStream, true
	case "stream_options":
		return keyStreamOptions, true
	case "include":
		return keyInclude, true
	case "service_tier":
		return keyServiceTier, true
	case "prompt_cache_key":
		return keyPromptCacheKey, true
	case "text":
		return keyText, true
	case "access_programs":
		return keyAccessPrograms, true
	case "previous_response_id":
		return keyPreviousResponseID, s == surfaceWSNative
	case "generate":
		return keyGenerate, s == surfaceWSNative
	}
	return 0, false
}

// parsedRequest 是第一段的结果：各规范键的原始值片段（不复制）。
type parsedRequest struct {
	surface surface
	raw     []byte // 环境时间改写之后的请求体（零拷贝候选）
	vals    [nShapeKeys][]byte
	// compact 表示顶层 token 之间没有空白：值与 input 元素之间的分隔可以原样
	// 搬移；否则组装时 input 逐元素重写分隔符。
	compact bool
	// inputCompact 表示 input 数组元素之间没有空白（可整段搬移）。
	inputCompact bool
	// inputItems 是 input 顶层元素个数（字符串 input 视为 1）。
	inputItems int
	// hasAdditionalTools：input 顶层有 type=="additional_tools" 的元素。
	hasAdditionalTools bool
	duplicates         int
}

// validHeaderToken 报告解码后的 model / service_tier 能安全进入请求头与
// turn-metadata：不含 <0x20 与 0x7f 字节。
func validHeaderToken(s []byte) bool {
	for _, c := range s {
		if c < 0x20 || c == 0x7f {
			return false
		}
	}
	return true
}

// parseRequest 是第一段：校验 JSON 对象根 → 环境时间改写 → 一次扫描顶层成员
// （键按解码后的名字匹配，同名键只认第一次出现，重复的值不参与任何判断）→
// 白名单 → model / service_tier 校验。
//
// 前置条件：调用方须已用有深度上限的校验器（如 encoding/json 的 json.Valid，
// 迭代实现、上限 10000 层）校验过请求体。这里的 gjson 校验是递归实现，不设深度
// 上限；也不做 UTF-8 全量检查（不为此再扫一遍整包）。
func parseRequest(raw []byte, s surface, now time.Time) (parsedRequest, error) {
	p := parsedRequest{surface: s, compact: true, inputCompact: true}
	if len(raw) == 0 || !gjson.ValidBytes(raw) {
		return p, ErrInvalidRequestBody
	}
	if bytes.Contains(raw, envContextTag) {
		raw = rewriteEnvironmentContextTime(raw, now) // 已校验；改写结果仍是合法 JSON
	}
	p.raw = raw
	i := skipSpace(raw, 0)
	if i != 0 {
		p.compact = false
	}
	if i >= len(raw) || raw[i] != '{' {
		return p, ErrInvalidRequestBody
	}
	i++
	var seen uint32 // 规范键与 type/client_metadata 的首现位图
	for {
		j := skipSpace(raw, i)
		if j != i {
			p.compact = false
		}
		i = j
		if raw[i] == '}' {
			if skipSpace(raw, i+1) != len(raw) {
				p.compact = false
			}
			break
		}
		if raw[i] == ',' {
			i++
			continue
		}
		keyStart := i
		keyEnd := skipJSONString(raw, i)
		key := raw[i+1 : keyEnd-1]
		i = skipSpace(raw, keyEnd)
		if i != keyEnd {
			p.compact = false
		}
		i++ // ':'
		j = skipSpace(raw, i)
		if j != i {
			p.compact = false
		}
		valStart := j
		var name string
		if bytes.IndexByte(key, '\\') >= 0 {
			name = gjson.ParseBytes(raw[keyStart:keyEnd]).String()
		}
		var k shapeKey
		var ok bool
		if name != "" {
			k, ok = shapeKeyOf(name, s)
		} else {
			k, ok = shapeKeyOf(string(key), s)
		}
		bit := uint32(0)
		switch {
		case ok:
			bit = 1 << k
		case name == "type" || string(key) == "type":
			bit = 1 << 30
		case name == clientMetadataKey || string(key) == clientMetadataKey:
			bit = 1 << 31
		}
		dup := bit != 0 && seen&bit != 0
		seen |= bit
		// 先判重再扫值：只有首现的 input 参与元素计数与 lite 判定。
		var valEnd int
		if ok && !dup && k == keyInput && raw[valStart] == '[' {
			valEnd = p.scanInput(raw, valStart)
		} else {
			valEnd = skipJSONValue(raw, valStart)
		}
		i = valEnd
		if dup {
			p.duplicates++
			continue
		}
		if ok {
			p.vals[k] = raw[valStart:valEnd]
		}
	}
	m, ok := jsonStringValue(p.vals[keyModel])
	if !ok || len(m) == 0 || !validHeaderToken(m) {
		return p, ErrInvalidRequestBody
	}
	if v := p.vals[keyServiceTier]; v != nil {
		st, ok := jsonStringValue(v)
		switch {
		case !ok: // 非字符串：丢弃（官方字段是 Option<String>）
			p.vals[keyServiceTier] = nil
		case !validHeaderToken(st):
			return p, ErrInvalidRequestBody
		}
	}
	if in := p.vals[keyInput]; in != nil {
		switch in[0] {
		case '[':
		case '"':
			p.inputItems = 1
		case 'n': // null：按缺省处理
			p.vals[keyInput] = nil
		default:
			return p, ErrInvalidRequestBody
		}
	}
	return p, nil
}

// scanInput 扫描 input 数组（raw[i] == '['），记录元素数、分隔是否紧凑、是否含
// additional_tools；返回数组末字节之后的位置。
func (p *parsedRequest) scanInput(raw []byte, i int) int {
	i++
	for {
		j := skipSpace(raw, i)
		if j != i {
			p.inputCompact = false
		}
		i = j
		switch raw[i] {
		case ']':
			return i + 1
		case ',':
			i++
			continue
		}
		end := skipJSONValue(raw, i)
		if raw[i] == '{' && !p.hasAdditionalTools {
			if t, ok := objectMember(raw[i:end], "type"); ok && bytes.Equal(t, litAdditionalTools) {
				p.hasAdditionalTools = true
			}
		}
		p.inputItems++
		i = end
	}
}

func skipSpace(raw []byte, i int) int {
	for i < len(raw) && isJSONSpace(raw[i]) {
		i++
	}
	return i
}

// objectMember 在 JSON 对象 obj（合法、对象根）的顶层查找成员 name，返回值的
// 原始片段。键含转义时按解码后的名字比较。只看第一次出现。零分配（无转义键时）。
func objectMember(obj []byte, name string) ([]byte, bool) {
	var found []byte
	ok := false
	forEachMember(obj, func(key, val []byte, escaped bool) bool {
		if keyIs(key, escaped, name) {
			found, ok = val, true
			return false
		}
		return true
	})
	return found, ok
}

// keyIs 报告原始键（不含引号）解码后是否等于 name。
func keyIs(key []byte, escaped bool, name string) bool {
	if !escaped {
		return string(key) == name
	}
	q := make([]byte, 0, len(key)+2)
	q = append(append(append(q, '"'), key...), '"')
	return gjson.ParseBytes(q).String() == name
}

// forEachMember 依次交付对象顶层成员（键原始字节不含引号、值原始片段）。
func forEachMember(obj []byte, fn func(key, val []byte, escaped bool) bool) {
	i := skipSpace(obj, 0)
	if i >= len(obj) || obj[i] != '{' {
		return
	}
	i++
	for i < len(obj) {
		i = skipSpace(obj, i)
		if i >= len(obj) || obj[i] == '}' {
			return
		}
		if obj[i] == ',' {
			i++
			continue
		}
		keyEnd := skipJSONString(obj, i)
		if keyEnd < 0 {
			return
		}
		key := obj[i+1 : keyEnd-1]
		i = skipSpace(obj, keyEnd) + 1
		i = skipSpace(obj, i)
		end := skipJSONValue(obj, i)
		if end < 0 {
			return
		}
		if !fn(key, obj[i:end], bytes.IndexByte(key, '\\') >= 0) {
			return
		}
		i = end
	}
}

// forEachElement 依次交付数组顶层元素的原始片段。
func forEachElement(arr []byte, fn func(el []byte) bool) {
	i := skipSpace(arr, 0)
	if i >= len(arr) || arr[i] != '[' {
		return
	}
	i++
	for i < len(arr) {
		i = skipSpace(arr, i)
		if i >= len(arr) || arr[i] == ']' {
			return
		}
		if arr[i] == ',' {
			i++
			continue
		}
		end := skipJSONValue(arr, i)
		if end < 0 {
			return
		}
		if !fn(arr[i:end]) {
			return
		}
		i = end
	}
}

// jsonStringValue 返回 JSON 字符串 token 解码后的字节（无转义时直接切片，零分配）。
func jsonStringValue(tok []byte) ([]byte, bool) {
	if len(tok) < 2 || tok[0] != '"' {
		return nil, false
	}
	inner := tok[1 : len(tok)-1]
	if bytes.IndexByte(inner, '\\') < 0 {
		return inner, true
	}
	return []byte(gjson.ParseBytes(tok).Str), true
}

// 常用字面量（只读）。
var (
	litTrue            = []byte("true")
	litFalse           = []byte("false")
	litAuto            = []byte(`"auto"`)
	litAllTurns        = []byte(`"all_turns"`)
	litEmptyArray      = []byte("[]")
	litDefaultInclude  = []byte(`["reasoning.encrypted_content"]`)
	litAdditionalTools = []byte(`"additional_tools"`)
	litFunctionsNSName = "functions"
	litMCPAttribution  = `{"status":"none"}`
	litResponseCreate  = []byte(`"response.create"`)
)

// shapeConfig 是整形用到的客户端级配置。
type shapeConfig struct {
	trace     *TraceContext // WithTraceContext（WS 帧 client_metadata）
	traceAuto bool          // WithTraceAuto(true)：每帧生成 trace
	stats     *shapeCounters
}

// shapeConfigOf 取客户端级整形配置（计数为进程级）。
func shapeConfigOf(o *options) shapeConfig {
	return shapeConfig{trace: o.trace, traceAuto: o.traceAuto, stats: &shapeStats}
}

// shapeBody 是两段的组合：parseRequest + shapeRequest。
func shapeBody(raw []byte, s surface, id Identity, now time.Time, cfg shapeConfig) (shapedRequest, error) {
	p, err := parseRequest(raw, s, now)
	if err != nil {
		return shapedRequest{}, err
	}
	return shapeRequest(&p, id, now, cfg), nil
}

// shapedRequest 是第二段的结果：可组装的部件。
type shapedRequest struct {
	surface      surface
	raw          []byte // 零拷贝候选（parse 的输入）
	compact      bool
	model        []byte // 解码后的 model
	serviceTier  []byte // 解码后的 service_tier（非字符串 = nil）
	lite         bool
	info         modelInfo
	values       [nShapeKeys][]byte // 每个规范键的最终值字节（nil = 不写）；input 除外
	inputPrefix  [][]byte           // SDK 插入的输入项（工具项、指令消息），按官方顺序
	inputRaw     []byte             // 客户端 input 数组的原始字节（nil = 空数组）
	inputCompact bool
	inputItems   int
	textPrewarm  []byte // 预热帧用的 text（去掉 format；空对象 = nil）
	turnMeta     []byte // turn 形态 turn-metadata（匿名 = nil）
	prewarmMeta  []byte // prewarm 形态 turn-metadata（匿名 = nil）
	anonymous    bool

	// client_metadata 的身份值（匿名不写 client_metadata）。
	installationID, sessionID, threadID, windowID, turnID string
	clientRequestID                                       string
	trace                                                 TraceContext

	// WS→WS：客户端原值（经 wsAssembly 交给组装）。
	clientPrevID   string
	clientGenerate []byte
	// nativePrewarm：WS→WS 客户端自己的预热帧（generate:false）。client_metadata
	// 与握手 turn-metadata 用 prewarm 形态，不带 turn-state；输入按客户端原样。
	nativePrewarm bool
	// hint 是 routingHint 的缓存（"" = 未生成）。
	hint string
}

// injectHash 是本次插入项（工具项 + 指令消息）内容的 FNV-64a；无插入项为 0。
// 只有 WS→WS 增量帧去重用到，按需计算（HTTP 与 HTTP→WS 不算）。
func (sh *shapedRequest) injectHash() uint64 {
	if len(sh.inputPrefix) == 0 {
		return 0
	}
	h := uint64(fnvOffset64)
	for _, it := range sh.inputPrefix {
		h = fnv64aAppend(h, it)
	}
	return h
}

// turnState 返回要带给上游的 turn-state（匿名不带）。
func (sh *shapedRequest) turnState(id Identity) string {
	if sh.anonymous {
		return ""
	}
	return id.Turn.State
}

// handshakeTurnMeta 返回这条连接第一帧（本请求）对应的握手 turn-metadata。
func (sh *shapedRequest) handshakeTurnMeta(prewarm bool) []byte {
	if prewarm || sh.nativePrewarm {
		return sh.prewarmMeta
	}
	return sh.turnMeta
}

// routingHint 返回 x-codex-routing-hint 的值。首次调用时生成并记在 sh 上：
// HTTP→WS 的连接缓存键与握手头共用同一个字符串（一次分配）。
func (sh *shapedRequest) routingHint() string {
	if sh.hint == "" {
		if len(sh.serviceTier) > 0 {
			sh.hint = "model=" + string(sh.model) + ";tier=" + string(sh.serviceTier)
		} else {
			sh.hint = "model=" + string(sh.model)
		}
	}
	return sh.hint
}

// prewarmInputItems 是预热帧的输入项数：lite 为 0；非 lite 为 SDK 由顶层
// instructions 生成的那条消息（inputPrefix 在非 lite 下只有它；没有则 0）。
// 预热后的真实帧以它作 inputSkip。
func (sh *shapedRequest) prewarmInputItems() int {
	if sh.lite {
		return 0
	}
	return len(sh.inputPrefix)
}

// prewarmChains 报告预热帧与真实帧的非输入字段是否逐项相等（官方
// core/src/client.rs 的判定）。两帧由同一份整形结果组装，唯一可能的差别是
// text：真实 text 带 format 时预热帧去掉了它。相等时真实帧带预热 response id
// 并跳过预热输入；否则不带、发全量。
func (sh *shapedRequest) prewarmChains() bool {
	return bytes.Equal(sh.values[keyText], sh.textPrewarm)
}

// shapeRequest 是第二段：只做与身份、模型有关的变换（spec §5.1 变换 1–11）。
func shapeRequest(p *parsedRequest, id Identity, now time.Time, cfg shapeConfig) shapedRequest {
	st := cfg.stats
	sh := shapedRequest{
		surface:      p.surface,
		raw:          p.raw,
		compact:      p.compact,
		inputRaw:     p.vals[keyInput],
		inputCompact: p.inputCompact,
		inputItems:   p.inputItems,
		anonymous:    id.anonymous(),
	}
	if st != nil {
		if p.duplicates > 0 {
			st.duplicateKeys.Add(int64(p.duplicates))
		}
		if sh.anonymous {
			st.anonymous.Add(1)
			if id.partial() {
				st.partialIdentity.Add(1)
			}
		}
	}
	sh.model, _ = jsonStringValue(p.vals[keyModel])
	info, known := lookupModel(string(sh.model))
	if !known && st != nil {
		st.unknownModels.Add(1)
	}
	sh.info = info
	sh.lite = info.Lite || p.hasAdditionalTools
	if v := p.vals[keyServiceTier]; v != nil {
		if s, ok := jsonStringValue(v); ok && len(s) > 0 {
			sh.serviceTier = s
		}
	}
	if sh.surface == surfaceWSNative {
		if v := p.vals[keyPreviousResponseID]; v != nil {
			if s, ok := jsonStringValue(v); ok {
				sh.clientPrevID = string(s)
			}
		}
		sh.clientGenerate = p.vals[keyGenerate]
		sh.nativePrewarm = bytes.Equal(sh.clientGenerate, litFalse)
	}

	// 字符串 input：官方恒发数组，转成一条 user 消息。
	if in := sh.inputRaw; len(in) > 0 && in[0] == '"' {
		text, _ := jsonStringValue(in)
		b := make([]byte, 0, len(in)+80)
		b = append(b, `[{"type":"message","role":"user","content":[{"type":"input_text","text":`...)
		b = appendJSONString(b, string(text))
		b = append(b, "}]}]"...)
		sh.inputRaw, sh.inputCompact = b, true
	}

	v := &sh.values
	v[keyModel] = p.vals[keyModel]
	v[keyStream] = litTrue
	v[keyServiceTier] = p.vals[keyServiceTier]
	v[keyStore] = litFalse
	v[keyStreamOptions] = p.vals[keyStreamOptions]
	v[keyAccessPrograms] = p.vals[keyAccessPrograms]
	v[keyInclude] = p.vals[keyInclude]
	if v[keyInclude] == nil {
		v[keyInclude] = litDefaultInclude
	}

	// 身份（非匿名）。
	turnID, contextID, startedMs := "", "", int64(0)
	if !sh.anonymous {
		sh.installationID = id.InstallationID
		sh.sessionID = id.Session.SessionID
		sh.threadID = id.Session.ThreadID
		sh.clientRequestID = id.Session.ClientRequestID
		if sh.clientRequestID == "" {
			sh.clientRequestID = sh.threadID
		}
		sh.windowID = id.Session.WindowID
		if sh.windowID == "" {
			sh.windowID = sh.threadID + ":" + strconv.FormatUint(id.Window.Number, 10)
		}
		turnID = id.Turn.ID
		if turnID == "" {
			turnID = NewUUIDv7()
		}
		contextID = id.Window.ContextID
		if contextID == "" {
			contextID = NewUUIDv7()
		}
		startedMs = id.Turn.StartedAtMs
		if startedMs == 0 {
			startedMs = now.UnixMilli()
		}
		sh.turnID = turnID
		v[keyPromptCacheKey] = appendJSONString(nil, sh.sessionID)
		switch {
		case cfg.trace != nil:
			sh.trace = *cfg.trace
		case cfg.traceAuto:
			sh.trace = NewTraceContext()
		}
	}

	// 工具与 lite 规则。
	tools := p.vals[keyTools]
	var atItem []byte
	if sh.lite {
		switch {
		case p.hasAdditionalTools:
			if tools != nil && st != nil {
				st.dualToolSources.Add(1)
			}
		case tools != nil && len(tools) > 0 && tools[0] == '[':
			if arr := liteToolsJSON(tools); arr != nil {
				atItem = make([]byte, 0, len(arr)+96)
				atItem = append(atItem, `{"type":"additional_tools",`...)
				if !sh.anonymous {
					tsSrc := id.Window.ContextID
					if tsSrc == "" {
						tsSrc = sh.threadID
					}
					atItem = append(atItem, `"id":"at_`...)
					atItem = append(atItem, liteItemID(tsSrc, sh.threadID, "at", arr)...)
					atItem = append(atItem, `",`...)
				}
				atItem = append(atItem, `"role":"developer","tools":`...)
				atItem = append(atItem, arr...)
				atItem = append(atItem, '}')
			}
		}
		v[keyTools] = nil
		v[keyParallelToolCalls] = litFalse
		v[keyToolChoice] = litAuto
	} else {
		if tools != nil {
			v[keyTools] = flattenToolsArray(tools)
		} else {
			v[keyTools] = litEmptyArray
		}
		v[keyToolChoice] = p.vals[keyToolChoice]
		if v[keyToolChoice] == nil {
			v[keyToolChoice] = litAuto
		}
		v[keyParallelToolCalls] = p.vals[keyParallelToolCalls]
		if v[keyParallelToolCalls] == nil {
			v[keyParallelToolCalls] = litTrue
		}
	}

	// 指令 → developer 消息（工具项之后）。
	var instrItem []byte
	if ins := p.vals[keyInstructions]; ins != nil {
		text, ok := jsonStringValue(ins)
		switch {
		case !ok:
			if st != nil {
				st.nonStringInstructions.Add(1)
			}
		case len(text) > 0:
			instrItem = make([]byte, 0, len(text)+len(text)/16+320)
			instrItem = append(instrItem, `{"type":"message",`...)
			if !sh.anonymous {
				instrItem = append(instrItem, `"id":"msg_`...)
				if sh.lite {
					tsSrc := id.Window.ContextID
					if tsSrc == "" {
						tsSrc = sh.threadID
					}
					instrItem = append(instrItem, liteItemID(tsSrc, sh.threadID, "msg", text)...)
				} else {
					instrItem = append(instrItem, instructionsIDV5(sh.threadID, text)...)
				}
				instrItem = append(instrItem, `",`...)
			}
			instrItem = append(instrItem, `"role":"developer","content":[{"type":"input_text","text":`...)
			instrItem = appendJSONStringBytes(instrItem, text)
			if sh.lite {
				// lite 官方的这条消息来自历史（带轮次相关的 passthrough），网关不合成。
				instrItem = append(instrItem, `}]}`...)
			} else {
				// 非 lite：官方每请求重建指令消息，带这段只取决于项类型的常量元数据
				// （gpt-5.5 抓包；core/src/client.rs）。
				instrItem = append(instrItem, `}],"internal_chat_message_metadata_passthrough":{"content_item_kinds":["model.base_instructions"]}}`...)
			}
		}
	}
	if atItem != nil || instrItem != nil {
		sh.inputPrefix = make([][]byte, 0, 2)
		if atItem != nil {
			sh.inputPrefix = append(sh.inputPrefix, atItem)
		}
		if instrItem != nil {
			sh.inputPrefix = append(sh.inputPrefix, instrItem)
		}
	}

	// reasoning：恒写；键序 effort, summary, context, 其余。
	var effortTok []byte
	var defEffort string
	v[keyReasoning], effortTok, defEffort = shapeReasoning(p.vals[keyReasoning], info, sh.lite)
	// text：键序 verbosity, format, 其余。
	v[keyText], sh.textPrewarm = shapeText(p.vals[keyText], info)

	if !sh.anonymous {
		var effort []byte
		if effortTok != nil {
			effort, _ = jsonStringValue(effortTok) // 非字符串 effort：不写 reasoning_effort
		} else if defEffort != "" {
			effort = []byte(defEffort)
		}
		tm := turnMetaInput{
			installationID: sh.installationID,
			sessionID:      sh.sessionID,
			threadID:       sh.threadID,
			turnID:         turnID,
			windowID:       sh.windowID,
			windowNumber:   id.Window.Number,
			contextID:      contextID,
			startedMs:      startedMs,
			info:           info,
			model:          sh.model,
			effort:         effort,
		}
		// 两种形态写进同一块缓冲（一次分配）；三下标切片防止后续 append 互相覆盖。
		buf := appendTurnMetadata(make([]byte, 0, 1600), &tm, false)
		n := len(buf)
		buf = appendTurnMetadata(buf, &tm, true)
		sh.turnMeta = buf[:n:n]
		sh.prewarmMeta = buf[n:len(buf):len(buf)]
	}
	return sh
}

// reasoningParts 是 reasoning 的规范化输入（键序 effort, summary, context, 其余）。
type reasoningParts struct {
	raw, effort, summary []byte
	defEffort            string
	lite                 bool
}

func (r *reasoningParts) write(w *jsonWriter) {
	w.byte('{')
	o := objFields{w: w}
	switch {
	case r.effort != nil:
		o.rawField("effort", r.effort)
	case r.defEffort != "":
		o.key("effort")
		w.byte('"')
		w.str(r.defEffort)
		w.byte('"')
	}
	o.rawField("summary", r.summary)
	if r.lite {
		o.rawField("context", litAllTurns)
	}
	forEachMember(r.raw, func(key, val []byte, escaped bool) bool {
		if keyIs(key, escaped, "effort") || keyIs(key, escaped, "summary") || keyIs(key, escaped, "context") {
			return true
		}
		o.rawKeyField(key, val)
		return true
	})
	w.byte('}')
}

// shapeReasoning 产出 reasoning 的最终值（恒写，至少 {}）与最终 effort（客户端
// 原值 token，或表默认值字符串；用于 turn-metadata）。已是规范形态时返回原切片。
func shapeReasoning(raw []byte, info modelInfo, lite bool) (out, effortTok []byte, defEffort string) {
	if len(raw) == 0 || raw[0] != '{' {
		raw = nil
	}
	r := reasoningParts{raw: raw, lite: lite}
	if raw != nil {
		r.effort, _ = objectMember(raw, "effort")
		r.summary, _ = objectMember(raw, "summary")
	}
	if r.effort == nil {
		r.defEffort = info.DefaultReasoningLevel
	}
	if raw != nil {
		cw := jsonWriter{ref: raw, cmp: true}
		r.write(&cw)
		if cw.matched() {
			return raw, r.effort, r.defEffort
		}
	}
	w := jsonWriter{buf: make([]byte, 0, len(raw)+48)}
	r.write(&w)
	return w.buf, r.effort, r.defEffort
}

// textParts 是 text 的规范化输入（键序 verbosity, format, 其余）。
type textParts struct {
	raw, verbosity, format []byte
	defVerbosity           string
}

func (x *textParts) write(w *jsonWriter, withFormat bool) {
	w.byte('{')
	o := objFields{w: w}
	switch {
	case x.verbosity != nil:
		o.rawField("verbosity", x.verbosity)
	case x.defVerbosity != "":
		o.key("verbosity")
		w.byte('"')
		w.str(x.defVerbosity)
		w.byte('"')
	}
	if withFormat {
		o.rawField("format", x.format)
	}
	forEachMember(x.raw, func(key, val []byte, escaped bool) bool {
		if keyIs(key, escaped, "verbosity") || keyIs(key, escaped, "format") {
			return true
		}
		o.rawKeyField(key, val)
		return true
	})
	w.byte('}')
}

// shapeText 产出 text 的最终值（无则 nil）与预热用的 text（去掉 format；空对象 = nil）。
func shapeText(raw []byte, info modelInfo) ([]byte, []byte) {
	if len(raw) == 0 || raw[0] != '{' {
		raw = nil
	}
	x := textParts{raw: raw}
	if raw != nil {
		x.verbosity, _ = objectMember(raw, "verbosity")
		x.format, _ = objectMember(raw, "format")
	}
	if x.verbosity == nil && info.SupportVerbosity {
		x.defVerbosity = info.DefaultVerbosity
	}
	if raw == nil && x.defVerbosity == "" {
		return nil, nil
	}
	var text []byte
	if raw != nil {
		cw := jsonWriter{ref: raw, cmp: true}
		x.write(&cw, true)
		if cw.matched() {
			text = raw
		}
	}
	if text == nil {
		w := jsonWriter{buf: make([]byte, 0, len(raw)+32)}
		x.write(&w, true)
		text = w.buf
	}
	prewarm := text
	if x.format != nil {
		w := jsonWriter{buf: make([]byte, 0, len(raw))}
		x.write(&w, false)
		prewarm = w.buf
	}
	if len(prewarm) == 2 { // {}
		prewarm = nil
	}
	return text, prewarm
}

// ---- 工具转换 ----

// flattenTool 把 Chat Completions 风格的 {"type":"function","function":{...}}
// 拍平成 Responses 风格，键序 type, name, description, strict, parameters（官方
// 实际键序）；缺的键不写。其余工具原样返回。
func flattenTool(el []byte) []byte {
	if len(el) == 0 || el[0] != '{' {
		return el
	}
	typ, _ := objectMember(el, "type")
	if !bytes.Equal(typ, []byte(`"function"`)) {
		return el
	}
	fn, ok := objectMember(el, "function")
	if !ok || len(fn) == 0 || fn[0] != '{' {
		return el
	}
	if _, has := objectMember(el, "name"); has {
		return el
	}
	out := make([]byte, 0, len(fn)+24)
	out = append(out, `{"type":"function"`...)
	for _, k := range [...]string{"name", "description", "strict", "parameters"} {
		if val, ok := objectMember(fn, k); ok {
			out = append(out, ',', '"')
			out = append(out, k...)
			out = append(out, '"', ':')
			out = append(out, val...)
		}
	}
	return append(out, '}')
}

// flattenToolsArray 拍平数组里的 Chat Completions 风格工具；无变化返回原切片。
func flattenToolsArray(arr []byte) []byte {
	if len(arr) == 0 || arr[0] != '[' {
		return arr
	}
	changed := false
	forEachElement(arr, func(el []byte) bool {
		if f := flattenTool(el); len(f) != len(el) || (len(f) > 0 && &f[0] != &el[0]) {
			changed = true
			return false
		}
		return true
	})
	if !changed {
		return arr
	}
	out := make([]byte, 0, len(arr)+64)
	out = append(out, '[')
	first := true
	forEachElement(arr, func(el []byte) bool {
		if !first {
			out = append(out, ',')
		}
		first = false
		out = append(out, flattenTool(el)...)
		return true
	})
	return append(out, ']')
}

// liteToolsJSON 按官方 create_tools_json_for_responses_lite（tools/src/
// tool_spec.rs）把顶层 tools 转成 additional_tools.tools 数组：function / custom
// 按原顺序并进名为 functions 的命名空间（放在第一个函数工具原位置；客户端自带的
// functions 命名空间并入、描述非空则用之），其余工具原样保留。结果为空返回 nil。
func liteToolsJSON(tools []byte) []byte {
	var others [][]byte
	var fns [][]byte
	desc := ""
	fnIndex := -1
	forEachElement(tools, func(el []byte) bool {
		var typ, name []byte
		if len(el) > 0 && el[0] == '{' {
			t, _ := objectMember(el, "type")
			typ, _ = jsonStringValue(t)
			n, _ := objectMember(el, "name")
			name, _ = jsonStringValue(n)
		}
		switch {
		case string(typ) == "function" || string(typ) == "custom":
			fns = append(fns, flattenTool(el))
		case string(typ) == "namespace" && string(name) == litFunctionsNSName:
			if d, ok := objectMember(el, "description"); ok {
				if s, ok := jsonStringValue(d); ok && strings.TrimSpace(string(s)) != "" {
					desc = string(s)
				}
			}
			if inner, ok := objectMember(el, "tools"); ok {
				forEachElement(inner, func(t []byte) bool {
					fns = append(fns, flattenTool(t))
					return true
				})
			}
		default:
			others = append(others, el)
			return true
		}
		if fnIndex < 0 {
			fnIndex = len(others)
		}
		return true
	})
	var ns []byte
	if fnIndex >= 0 && len(fns) > 0 {
		ns = append(ns, `{"type":"namespace","name":"functions","description":`...)
		ns = appendJSONString(ns, desc)
		ns = append(ns, `,"tools":[`...)
		for i, f := range fns {
			if i > 0 {
				ns = append(ns, ',')
			}
			ns = append(ns, f...)
		}
		ns = append(ns, "]}"...)
	} else {
		fnIndex = -1
	}
	if len(others) == 0 && ns == nil {
		return nil
	}
	out := make([]byte, 0, len(tools)+64)
	out = append(out, '[')
	n := 0
	add := func(b []byte) {
		if n > 0 {
			out = append(out, ',')
		}
		n++
		out = append(out, b...)
	}
	for i, o := range others {
		if i == fnIndex {
			add(ns)
		}
		add(o)
	}
	if fnIndex == len(others) {
		add(ns)
	}
	return append(out, ']')
}

// ---- turn-metadata ----

type turnMetaInput struct {
	installationID, sessionID, threadID, turnID, windowID, contextID string
	windowNumber                                                     uint64
	startedMs                                                        int64
	info                                                             modelInfo
	model, effort                                                    []byte
}

// appendTurnMetadata 按官方固定键序写 turn-metadata（ASCII JSON，等价
// to_ascii_json_string）。InstallationID 为空时省略 installation_id。
// prewarm 形态：turn_id 为空串，不写 root_turn_id、turn_trigger、
// turn_started_at_unix_ms，request_kind 为 prewarm。
// sandbox / sandbox_mode 固定为 none / danger-full-access（官方
// PermissionProfile::Disabled 的组合）；workspaces 恒不写。
func appendTurnMetadata(b []byte, m *turnMetaInput, prewarm bool) []byte {
	str := func(key, val string) {
		b = append(b, ',', '"')
		b = append(b, key...)
		b = append(b, '"', ':')
		b = appendASCIIJSONString(b, val)
	}
	boolean := func(key string, v bool) {
		b = append(b, ',', '"')
		b = append(b, key...)
		b = append(b, '"', ':')
		b = strconv.AppendBool(b, v)
	}
	// 空 InstallationID：省略 installation_id（与 client_metadata 的
	// x-codex-installation-id 一致）。
	if m.installationID != "" {
		b = append(b, `{"installation_id":`...)
		b = appendASCIIJSONString(b, m.installationID)
		str("session_id", m.sessionID)
	} else {
		b = append(b, `{"session_id":`...)
		b = appendASCIIJSONString(b, m.sessionID)
	}
	str("thread_id", m.threadID)
	str("agent_name", "/root")
	if prewarm {
		str("turn_id", "")
	} else {
		str("turn_id", m.turnID)
	}
	str("window_id", m.windowID)
	b = append(b, `,"window_number":`...)
	b = strconv.AppendUint(b, m.windowNumber, 10)
	str("context_window_id", m.contextID)
	if prewarm {
		str("request_kind", "prewarm")
	} else {
		str("request_kind", "turn")
	}
	str("history_initialization", "new")
	if !prewarm {
		str("root_turn_id", m.turnID)
	}
	str("thread_source", "user")
	if !prewarm {
		str("turn_trigger", "user")
	}
	str("sandbox", "none")
	str("sandbox_mode", "danger-full-access")
	boolean("auto_review_enabled", false)
	boolean("node_repl_auto_review_required", m.info.NodeReplAutoReviewRequired)
	boolean("node_repl_disabled", m.info.NodeReplDisabled)
	if !prewarm {
		b = append(b, `,"turn_started_at_unix_ms":`...)
		b = strconv.AppendInt(b, m.startedMs, 10)
	}
	boolean("analytics_enabled", true)
	str("model", string(m.model))
	if m.effort != nil {
		str("reasoning_effort", string(m.effort))
	}
	return append(b, '}')
}

// ---- 组装 ----

// jsonWriter 是组装输出：写入模式追加到 buf；比较模式（ref != nil）逐段与 ref
// 比对，不分配——全部相等且长度一致即可直接返回 ref（零拷贝）。
type jsonWriter struct {
	buf []byte
	ref []byte
	pos int
	cmp bool
	bad bool
}

func (w *jsonWriter) raw(b []byte) {
	if !w.cmp {
		w.buf = append(w.buf, b...)
		return
	}
	if w.bad {
		return
	}
	end := w.pos + len(b)
	if end > len(w.ref) || !bytes.Equal(w.ref[w.pos:end], b) {
		w.bad = true
		return
	}
	w.pos = end
}

func (w *jsonWriter) str(s string) {
	if !w.cmp {
		w.buf = append(w.buf, s...)
		return
	}
	if w.bad {
		return
	}
	end := w.pos + len(s)
	if end > len(w.ref) || string(w.ref[w.pos:end]) != s {
		w.bad = true
		return
	}
	w.pos = end
}

func (w *jsonWriter) byte(c byte) {
	if !w.cmp {
		w.buf = append(w.buf, c)
		return
	}
	if w.bad {
		return
	}
	if w.pos >= len(w.ref) || w.ref[w.pos] != c {
		w.bad = true
		return
	}
	w.pos++
}

// matched 报告比较模式下输出与 ref 逐字节相等。
func (w *jsonWriter) matched() bool { return w.cmp && !w.bad && w.pos == len(w.ref) }

// wsAssembly 是一次 WS 帧组装的帧级参数。
type wsAssembly struct {
	prevID    string // "" = 不写 previous_response_id；surfaceWSNative 用客户端原值
	inputSkip int    // 跳过 inputPrefix+客户端 input 合并序列的前 n 项（预热后的真实帧）
	turnState string // client_metadata x-codex-turn-state（"" = 不写）
	prewarm   bool   // 预热帧：输入=预热输入、generate:false、prewarm 形态元数据
	generate  []byte // surfaceWSNative 的客户端 generate 原值
}

// metaEntry 是一条 client_metadata（未序列化）。
type metaEntry struct {
	key, val string
	raw      []byte // 非 nil 时以它为值（避免 []byte→string 的复制）
	startMs  bool   // 值为发送时刻毫秒占位（返回其偏移）
}

// assembleHTTP 按官方 HTTP 键序写出请求体。
func assembleHTTP(sh *shapedRequest) []byte {
	if sh.anonymous {
		cw := jsonWriter{ref: sh.raw, cmp: true}
		sh.writeHTTP(&cw)
		if cw.matched() {
			return sh.raw
		}
	}
	w := jsonWriter{buf: make([]byte, 0, sh.sizeHint())}
	sh.writeHTTP(&w)
	return w.buf
}

// assembleWS 按官方 WS 键序写出 response.create 帧；startMsOff 是
// x-codex-ws-stream-request-start-ms 13 位数字的偏移（无该键为 -1）。
func assembleWS(sh *shapedRequest, a wsAssembly) (frame []byte, startMsOff int) {
	if sh.anonymous && !a.prewarm && a.inputSkip == 0 {
		cw := jsonWriter{ref: sh.raw, cmp: true}
		sh.writeWS(&cw, &a)
		if cw.matched() {
			return sh.raw, -1
		}
	}
	w := jsonWriter{buf: make([]byte, 0, sh.sizeHint())}
	off := sh.writeWS(&w, &a)
	return w.buf, off
}

func (sh *shapedRequest) sizeHint() int {
	n := len(sh.raw) + 2048 + len(sh.turnMeta)
	for _, it := range sh.inputPrefix {
		n += len(it)
	}
	return n
}

// objFields 是顶层对象的字段写入器（负责逗号）。
type objFields struct {
	w   *jsonWriter
	sep bool
}

func (o *objFields) key(name string) {
	if o.sep {
		o.w.byte(',')
	}
	o.sep = true
	o.w.byte('"')
	o.w.str(name)
	o.w.str(`":`)
}

// rawKeyField 写一个原始键（不含引号的原始字节）与原始值。
func (o *objFields) rawKeyField(key, v []byte) {
	if o.sep {
		o.w.byte(',')
	}
	o.sep = true
	o.w.byte('"')
	o.w.raw(key)
	o.w.str(`":`)
	o.w.raw(v)
}

func (o *objFields) rawField(name string, v []byte) {
	if v == nil {
		return
	}
	o.key(name)
	o.w.raw(v)
}

func (sh *shapedRequest) writeHTTP(w *jsonWriter) {
	w.byte('{')
	o := objFields{w: w}
	o.rawField("model", sh.values[keyModel])
	o.rawField("stream", sh.values[keyStream])
	o.rawField("service_tier", sh.values[keyServiceTier])
	o.key("input")
	sh.writeInput(w, 0, false)
	sh.writeCommonTail(&o, sh.values[keyText])
	if !sh.anonymous && !w.cmp {
		o.key(clientMetadataKey)
		sh.writeMeta(w, metaHTTP, nil)
	}
	o.rawField("access_programs", sh.values[keyAccessPrograms])
	w.byte('}')
}

// writeCommonTail 写 tools … text（HTTP 与 WS 共用的中段键序）。
func (sh *shapedRequest) writeCommonTail(o *objFields, text []byte) {
	o.rawField("tools", sh.values[keyTools])
	o.rawField("tool_choice", sh.values[keyToolChoice])
	o.rawField("parallel_tool_calls", sh.values[keyParallelToolCalls])
	o.rawField("reasoning", sh.values[keyReasoning])
	o.rawField("store", sh.values[keyStore])
	o.rawField("stream_options", sh.values[keyStreamOptions])
	o.rawField("include", sh.values[keyInclude])
	o.rawField("prompt_cache_key", sh.values[keyPromptCacheKey])
	o.rawField("text", text)
}

func (sh *shapedRequest) writeWS(w *jsonWriter, a *wsAssembly) int {
	w.byte('{')
	o := objFields{w: w}
	o.rawField("type", litResponseCreate)
	o.rawField("model", sh.values[keyModel])
	o.rawField("stream", sh.values[keyStream])
	o.rawField("service_tier", sh.values[keyServiceTier])
	if a.prevID != "" && !a.prewarm {
		o.key("previous_response_id")
		if w.cmp {
			w.byte('"')
			w.str(a.prevID)
			w.byte('"')
			if strings.ContainsAny(a.prevID, "\"\\") {
				w.bad = true
			}
		} else {
			w.buf = appendJSONString(w.buf, a.prevID)
		}
	}
	o.key("input")
	sh.writeInput(w, a.inputSkip, a.prewarm)
	text := sh.values[keyText]
	if a.prewarm {
		text = sh.textPrewarm
	}
	sh.writeCommonTail(&o, text)
	if a.prewarm {
		o.rawField("generate", litFalse)
	} else {
		o.rawField("generate", a.generate)
	}
	off := -1
	if !sh.anonymous && !w.cmp {
		o.key(clientMetadataKey)
		kind := metaWS
		if a.prewarm || sh.nativePrewarm {
			kind = metaWSPrewarm
		}
		off = sh.writeMeta(w, kind, a)
	}
	o.rawField("access_programs", sh.values[keyAccessPrograms])
	w.byte('}')
	return off
}

// writeInput 写 input 数组：inputPrefix 与客户端元素的合并序列跳过前 skip 项。
// 预热：lite 为空；非 lite 为 SDK 由顶层 instructions 生成的那条消息（没有则空）。
func (sh *shapedRequest) writeInput(w *jsonWriter, skip int, prewarm bool) {
	w.byte('[')
	n := 0
	put := func(b []byte) {
		if n > 0 {
			w.byte(',')
		}
		n++
		w.raw(b)
	}
	if prewarm {
		for _, it := range sh.inputPrefix[:sh.prewarmInputItems()] {
			put(it)
		}
		w.byte(']')
		return
	}
	idx := 0
	for _, it := range sh.inputPrefix {
		if idx >= skip {
			put(it)
		}
		idx++
	}
	in := sh.inputRaw
	if len(in) > 0 {
		if idx >= skip && sh.inputCompact && in[0] == '[' {
			// 紧凑数组：元素与分隔整段搬移。
			inner := in[1 : len(in)-1]
			if len(inner) > 0 {
				put(inner)
			}
		} else {
			forEachElement(in, func(el []byte) bool {
				if idx >= skip {
					put(el)
				}
				idx++
				return true
			})
		}
	}
	w.byte(']')
}

type metaKind uint8

const (
	metaHTTP metaKind = iota
	metaWS
	metaWSPrewarm
)

// writeMeta 写 client_metadata 对象：条目顺序每次随机（官方是 Rust HashMap），
// 值按 serde_json 规则序列化。返回发送时刻毫秒数字的偏移（无为 -1）。
func (sh *shapedRequest) writeMeta(w *jsonWriter, kind metaKind, a *wsAssembly) int {
	var buf [16]metaEntry
	e := buf[:0]
	add := func(k, v string) { e = append(e, metaEntry{key: k, val: v}) }
	add("session_id", sh.sessionID)
	add("thread_id", sh.threadID)
	add("x-codex-window-id", sh.windowID)
	if sh.installationID != "" { // 空 = 省略（turn-metadata 同样省略 installation_id）
		add("x-codex-installation-id", sh.installationID)
	}
	if kind == metaWSPrewarm {
		add("turn_id", "")
		e = append(e, metaEntry{key: "x-codex-turn-metadata", raw: sh.prewarmMeta})
	} else {
		add("turn_id", sh.turnID)
		add("root_turn_id", sh.turnID)
		e = append(e, metaEntry{key: "x-codex-turn-metadata", raw: sh.turnMeta})
	}
	add("mcp_attribution", litMCPAttribution)
	add("guardian_credits_requested", "true")
	if kind != metaHTTP {
		if kind == metaWS && a != nil && a.turnState != "" {
			add("x-codex-turn-state", a.turnState)
		}
		e = append(e, metaEntry{key: "x-codex-ws-stream-request-start-ms", startMs: true})
		if sh.lite {
			add(MetaResponsesLiteKey, "true")
		}
		if sh.trace.Traceparent != "" {
			add("ws_request_header_traceparent", sh.trace.Traceparent)
		}
		if sh.trace.Tracestate != "" {
			add("ws_request_header_tracestate", sh.trace.Tracestate)
		}
	}
	rand.Shuffle(len(e), func(i, j int) { e[i], e[j] = e[j], e[i] })
	off := -1
	w.byte('{')
	for i, m := range e {
		if i > 0 {
			w.byte(',')
		}
		w.buf = appendJSONString(w.buf, m.key)
		w.byte(':')
		if m.startMs {
			w.byte('"')
			off = len(w.buf)
			w.buf = appendStartMs(w.buf, time.Now())
			w.byte('"')
			continue
		}
		if m.raw != nil {
			w.buf = appendJSONStringBytes(w.buf, m.raw)
			continue
		}
		w.buf = appendJSONString(w.buf, m.val)
	}
	w.byte('}')
	return off
}

// appendStartMs 写 13 位毫秒数字（不足 13 位左补零；只在 2286 年后溢出）。
func appendStartMs(b []byte, t time.Time) []byte {
	ms := t.UnixMilli()
	var d [13]byte
	for i := 12; i >= 0; i-- {
		d[i] = byte('0' + ms%10)
		ms /= 10
	}
	return append(b, d[:]...)
}

// stampStartMs 用当前毫秒原地覆写 wire[off:off+13]（off<0 不覆写；零分配）。
func stampStartMs(wire []byte, off int, t time.Time) {
	if off < 0 || off+13 > len(wire) {
		return
	}
	ms := t.UnixMilli()
	for i := 12; i >= 0; i-- {
		wire[off+i] = byte('0' + ms%10)
		ms /= 10
	}
}
