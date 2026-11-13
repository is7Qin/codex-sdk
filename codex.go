package codexsdk

import (
	"bytes"
	"crypto/rand"
	"fmt"
	"strings"
	"time"

	"github.com/tidwall/gjson"
)

var easternTime = func() *time.Location {
	loc, err := time.LoadLocation("America/New_York")
	if err != nil {
		loc = time.FixedZone("EST", -5*60*60)
	}
	return loc
}()

// 伪装层：Codex 客户端形态对齐（逐点对齐官方 codex-tui 0.169.0 源码与抓包）。
// 头常量、UA / originator / version / beta 头；请求整形见 shape.go。

// CodexVersion 是伪装的官方客户端版本（UA 与 version 头共用）。
const CodexVersion = "0.169.0"

// DefaultOriginator 默认 originator 头值（codex-tui）。WithHeader("originator", ...) 可覆盖。
const DefaultOriginator = "codex-tui"

// DefaultCodexUserAgent 默认 codex UA（照抓包原样：codex-tui 0.169.0 + Ubuntu +
// gnome-terminal）。WithHeader("User-Agent", ...) 可覆盖。
const DefaultCodexUserAgent = "codex-tui/0.169.0 (Ubuntu 24.4.0; x86_64) gnome-terminal (codex-tui; 0.169.0)"

// DefaultBetaFeatures 是 x-codex-beta-features 的值（默认安装下只有这一项）。
const DefaultBetaFeatures = "remote_compaction_v2"

// DefaultBetaWS 是现役唯一的 Responses WS beta 值（真实源码全仓库唯一常量），
// 仅 WS 握手注入。
const DefaultBetaWS = "2026-02-06"

// Codex 请求头名常量（对齐真实 codex 客户端头名，供调用方组装请求头）。
const (
	HeaderSessionID       = "session-id"
	HeaderThreadID        = "thread-id"
	HeaderClientRequestID = "x-client-request-id"
	HeaderInstallationID  = "x-codex-installation-id"
	HeaderWindowID        = "x-codex-window-id"
	HeaderBetaFeatures    = "x-codex-beta-features"
	HeaderTurnState       = "x-codex-turn-state"
	HeaderTurnMetadata    = "x-codex-turn-metadata"
	HeaderMemgenRequest   = "x-openai-memgen-request"
	HeaderOAIAAttestation = "x-oai-attestation"
	HeaderTraceparent     = "traceparent"
	HeaderTracestate      = "tracestate"
	HeaderVersion         = "version"
	HeaderRoutingHint     = "x-codex-routing-hint"
	// HeaderResponsesLite 是 responses-lite internal 标记头（值 "true"）：SDK 对
	// lite 模型的 HTTP 请求自动发送（WS 走 client_metadata 的 MetaResponsesLiteKey）。
	HeaderResponsesLite = "x-openai-internal-codex-responses-lite"
)

// MetaResponsesLiteKey 是 responses-lite 的 client_metadata 键（值 "true"；
// 服务端约定把 ws_request_header_ 前缀键还原为请求头，与 HeaderResponsesLite
// 同一标记）。SDK 在 lite 模型的 WS 帧里自动写入。
const MetaResponsesLiteKey = "ws_request_header_x_openai_internal_codex_responses_lite"

// clientMetadataKey 是顶层 client_metadata 的规范拼写（解码后的名字）。
const clientMetadataKey = "client_metadata"

// skipJSONString 跳过 JSON 字符串（raw[i] 为开引号），返回闭引号之后的下标；
// 未闭合返回 -1。'\' 转义分支跳 1 字节（+循环自增 = 跳过转义目标；\"、\\、
// \uXXXX 的 4 hex 无引号/反斜杠）。语义对齐网关 strip_scan.go 的 skipJSONString。
func skipJSONString(raw []byte, i int) int {
	for i++; i < len(raw); i++ {
		switch raw[i] {
		case '\\':
			i++ // 跳过转义目标
		case '"':
			return i + 1
		}
	}
	return -1
}

// skipJSONValue 从 raw[i] 起跳过完整 JSON 值，返回末字节后一位置；结构非法
// 返回 -1。字符串走 skipJSONString；{}[] 深度计数（容器内字符串同样按
// skipJSONString 跳过，值内含 "[{" 不误计）；true/false/null 校验字面量内容；
// 数字跳过至空白 / ',' / '}' / ']'。语义对齐网关 strip_scan.go 的 skipValue / skipElement。
func skipJSONValue(raw []byte, i int) int {
	if i >= len(raw) {
		return -1
	}
	switch raw[i] {
	case '"':
		return skipJSONString(raw, i)
	case '{', '[':
		depth := 1
		for i++; i < len(raw); i++ {
			switch raw[i] {
			case '"':
				end := skipJSONString(raw, i)
				if end < 0 {
					return -1
				}
				i = end - 1 // 循环 i++ 抵消
			case '{', '[':
				depth++
			case '}', ']':
				depth--
				if depth == 0 {
					return i + 1
				}
			}
		}
		return -1
	case 't':
		if i+4 <= len(raw) && bytes.Equal(raw[i:i+4], jsonTrueBytes) {
			return i + 4
		}
		return -1
	case 'f':
		if i+5 <= len(raw) && bytes.Equal(raw[i:i+5], jsonFalseBytes) {
			return i + 5
		}
		return -1
	case 'n':
		if i+4 <= len(raw) && bytes.Equal(raw[i:i+4], jsonNullBytes) {
			return i + 4
		}
		return -1
	default:
		// 数字：跳过至空白 / ',' / '}' / ']'（gjson.ValidBytes 前置保证合法终止）。
		for i < len(raw) && !isJSONSpace(raw[i]) && raw[i] != ',' && raw[i] != '}' && raw[i] != ']' {
			i++
		}
		return i
	}
}

// isJSONSpace 判定 JSON 空白字符。
func isJSONSpace(c byte) bool {
	return c == ' ' || c == '\t' || c == '\n' || c == '\r'
}

// jsonTrueBytes/jsonFalseBytes/jsonNullBytes 是不可变字节常量（编译期静态引用，
// 零分配），供 skipJSONValue 校验字面量内容。
var (
	jsonTrueBytes  = []byte("true")
	jsonFalseBytes = []byte("false")
	jsonNullBytes  = []byte("null")
)

// envContextTag 是环境上下文的开标签（原样出现在 JSON 字符串里；转义写法不改写）。
var envContextTag = []byte("<environment_context>")

// RewriteEnvironmentContextTime 把 Codex 环境上下文中的本地时间改成美东时间。
// 只替换 <current_date> 与 <timezone> 的文本内容，其余字节保持不变；被改写的
// 字符串按 serde_json 规则重新编码（不做 HTML 转义，与官方请求体一致）。
// 非法 JSON 原样返回。
func RewriteEnvironmentContextTime(raw []byte, now time.Time) []byte {
	if len(raw) == 0 || !bytes.Contains(raw, envContextTag) || !gjson.ValidBytes(raw) {
		return raw
	}
	return rewriteEnvironmentContextTime(raw, now)
}

// rewriteEnvironmentContextTime 是改写本体，调用方保证 raw 是合法 JSON。
//
// 只解析含开标签的字符串 token：用 bytes.Index 找标签，再从上次的位置向前跳过
// 字符串 token（合法 JSON 里字符串之外不会出现 '"'），找到包含标签的那个
// token。整体是一次线性扫描，不展开整棵树、不解码其它字符串。
func rewriteEnvironmentContextTime(raw []byte, now time.Time) []byte {
	var out []byte
	cursor := 0 // raw[cursor:] 尚未写入 out
	scan := 0   // 字符串 token 扫描位置
	date := ""
	for {
		k := bytes.Index(raw[scan:], envContextTag)
		if k < 0 {
			break
		}
		target := scan + k
		start, end := -1, -1
		for scan < len(raw) {
			q := bytes.IndexByte(raw[scan:], '"')
			if q < 0 {
				scan = len(raw)
				break
			}
			s := scan + q
			e := skipJSONString(raw, s)
			if e < 0 {
				scan = len(raw)
				break
			}
			scan = e
			if e > target {
				start, end = s, e
				break
			}
		}
		if start < 0 {
			break
		}
		if start < target {
			tok := raw[start:end]
			str := gjson.ParseBytes(tok).Str
			if date == "" {
				date = now.In(easternTime).Format("2006-01-02")
			}
			next := replaceEnvironmentTime(str, date)
			if next != str && envContextSpliceOK(cursor, start, end, len(raw)) {
				if out == nil {
					out = make([]byte, 0, len(raw)+64)
				}
				out = append(out, raw[cursor:start]...)
				out = appendJSONString(out, next)
				cursor = end
			}
		}
	}
	if out == nil {
		return raw
	}
	return append(out, raw[cursor:]...)
}

// envContextSpliceOK 报告在已消费游标 cursor 与待改写字符串原文区间
// [start, end) 上做字节拼接是否安全。gjson 正常输入恒给出单调且正确的 Index，
// 但嵌套 / 构造结果可能给出 0 或回退的 Index（start < cursor）——此时
// raw[cursor:start] 会越界 panic；end 越过原文字节数同理。任一不满足即返回
// false，调用方保守跳过该次改写（输出保持原样，不 panic）。
func envContextSpliceOK(cursor, start, end, rawLen int) bool {
	return start >= cursor && end <= rawLen
}

func replaceEnvironmentTime(value, date string) string {
	value = replaceXMLElement(value, "current_date", date)
	return replaceXMLElement(value, "timezone", "America/New_York")
}

func replaceXMLElement(value, name, replacement string) string {
	open := "<" + name + ">"
	close := "</" + name + ">"
	var b strings.Builder
	rest := value
	for {
		start := strings.Index(rest, open)
		if start < 0 {
			b.WriteString(rest)
			return b.String()
		}
		from := start + len(open)
		rel := strings.Index(rest[from:], close)
		if rel < 0 {
			b.WriteString(rest)
			return b.String()
		}
		b.WriteString(rest[:from])
		b.WriteString(replacement)
		rest = rest[from+rel:]
	}
}

// Session 是会话级标识（真实 codex 客户端 WS 握手恒带
// x-client-request-id / session-id / thread-id / x-codex-window-id）。
// 会话内稳定，新会话换新值（UUIDv7，见 NewUUIDv7）。经 Identity.Session
// 注入握手头，并补齐帧内 client_metadata 的 session_id / thread_id /
// x-codex-window-id（Identity.Meta 中同 key 优先）。
type Session struct {
	SessionID       string // 头 session-id / metadata session_id
	ThreadID        string // 头 thread-id / metadata thread_id
	WindowID        string // 头 x-codex-window-id / metadata x-codex-window-id（{thread_id}:{n}，n 自 0 起）
	ClientRequestID string // 头 x-client-request-id（空则用 ThreadID）
}

// TraceContext 是 W3C trace context（仅注入 WS 帧内 client_metadata——
// 真实客户端 WS 握手与 HTTP 请求均不发 trace 头）。
type TraceContext struct {
	Traceparent string // 如 "00-<32位hex trace id>-<16位hex parent id>-01"
	Tracestate  string // 可为空（调用方可补充 vendor 数据）
}

// NewTraceContext 生成新的 W3C trace context（每次调用新链路 id，crypto/rand）。
//
// 逐字节等价于 "00-"+hex(16B)+"-"+hex(8B)+"-01"，但直接在栈上拼定长缓冲
// （55B）后一次转 string：每次编码只付 1 次分配（原实现 hex.EncodeToString ×2
// + 三段拼接 = 5 次，占 P1 每帧分配预算的大头）。
func NewTraceContext() TraceContext {
	var buf [55]byte
	copy(buf[:3], "00-")
	var traceID [16]byte
	randFill(traceID[:])
	hexEncode(buf[3:35], traceID[:])
	buf[35] = '-'
	var spanID [8]byte
	randFill(spanID[:])
	hexEncode(buf[36:52], spanID[:])
	copy(buf[52:], "-01") // 尾部 3 字节：'-','0','1'
	return TraceContext{Traceparent: string(buf[:])}
}

// NewUUIDv7 生成 UUIDv7（RFC 9562：48bit 毫秒时间戳 + 版本 7 + 变体 10 +
// 随机数），对齐真实 codex 客户端的 session_id / thread_id / turn_id 取值。
//
// 逐字节等价于 fmt.Sprintf("%x-%x-%x-%x-%x", ...)（36B 小写 hex），但栈上拼装
// 后一次转 string：每次 1 次分配（原实现经 fmt 反射路径付 8 次，占 P1 每帧分配
// 预算的大头）。
func NewUUIDv7() string {
	var b [16]byte
	now := time.Now().UnixMilli()
	b[0] = byte(now >> 40)
	b[1] = byte(now >> 32)
	b[2] = byte(now >> 24)
	b[3] = byte(now >> 16)
	b[4] = byte(now >> 8)
	b[5] = byte(now)
	randFill(b[6:])
	b[6] = b[6]&0x0f | 0x70 // 版本 7
	b[8] = b[8]&0x3f | 0x80 // 变体 10
	var s [36]byte
	hexEncode(s[0:8], b[0:4])
	s[8] = '-'
	hexEncode(s[9:13], b[4:6])
	s[13] = '-'
	hexEncode(s[14:18], b[6:8])
	s[18] = '-'
	hexEncode(s[19:23], b[8:10])
	s[23] = '-'
	hexEncode(s[24:36], b[10:16])
	return string(s[:])
}

// hexEncode 把小写 hex 写进 dst（dst 长度须为 len(src)*2）。手写而非
// hex.EncodeToString：后者每次调用分配一份中间 []byte。
func hexEncode(dst, src []byte) {
	const digits = "0123456789abcdef"
	for i, c := range src {
		dst[i*2] = digits[c>>4]
		dst[i*2+1] = digits[c&0x0f]
	}
}

// randFill 用 crypto/rand 填满 b；失败 panic（理由见包级文档「crypto/rand 失败」）。
func randFill(b []byte) {
	if _, err := rand.Read(b); err != nil {
		panic(fmt.Sprintf("codexsdk: crypto/rand 失败: %v", err))
	}
}
