package codexsdk

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

// 伪装层：Codex 客户端形态对齐（真实源码对照见 IMPERSONATION.md）。
// UA / originator / beta 头、头常量、字段白名单、client_metadata 组装、
// trace 与 turn_id 自动生成、会话标识与 turn-state 回传。
// 机制进 SDK，业务值由调用方（网关）提供。

// DefaultOriginator 默认 originator 头值（用户拍板：codex-tui；
// 首方值集合：codex_cli_rs / codex-tui / codex_vscode / codex_exec）。
// WithHeader("Originator", ...) 可覆盖。
const DefaultOriginator = "codex-tui"

// DefaultCodexUserAgent 默认 codex UA（用户拍板：codex-tui/0.147.0 +
// Ubuntu 指纹；真实形态 "{originator}/{version} ({os} {os_version}; {arch})
// {terminal} ({originator}; {version})"——UA 前缀与 originator 保持一致）。
// WithHeader("User-Agent", ...) 可覆盖。
const DefaultCodexUserAgent = "codex-tui/0.147.0 (Ubuntu 24.4.0; x86_64) xterm-256color (codex-tui; 0.147.0)"

// DefaultBetaWS 是现役唯一的 Responses WS beta 值（真实源码全仓库唯一常量；
// 2026-02-04 为旧值、无真实来源），仅 WS 握手注入。
const DefaultBetaWS = "2026-02-06"

// HTTPBetaResponsesV1 是 Responses HTTP 的 OpenAI-Beta 参考值。
// 真实客户端 HTTP /responses 路径不发 OpenAI-Beta——SDK 默认同样不发，
// 需要时调用方以 WithHeader("OpenAI-Beta", ...) 显式注入。
const HTTPBetaResponsesV1 = "responses=v1"

// Codex 请求头名常量（对齐真实 codex 客户端头名，供调用方组装请求头）。
const (
	HeaderSessionID       = "session-id"
	HeaderThreadID        = "thread-id"
	HeaderClientRequestID = "x-client-request-id"
	HeaderInstallationID  = "x-codex-installation-id"
	HeaderWindowID        = "x-codex-window-id"
	HeaderParentThreadID  = "x-codex-parent-thread-id"
	HeaderBetaFeatures    = "x-codex-beta-features"
	HeaderTurnState       = "x-codex-turn-state"
	HeaderTurnMetadata    = "x-codex-turn-metadata"
	HeaderSubagent        = "x-openai-subagent"
	HeaderMemgenRequest   = "x-openai-memgen-request"
	HeaderOAIAAttestation = "x-oai-attestation"
	HeaderTraceparent     = "traceparent"
	HeaderTracestate      = "tracestate"
	// HeaderResponsesLite 是 responses-lite internal 标记头（值 "true"；
	// 仅 gpt-5.6-sol/terra/luna 等 lite 模型触发）：HTTP 请求以
	// WithHeader 透传，SDK 只透传不解析（lite 触发与请求体形态由网关决定）。
	HeaderResponsesLite = "x-openai-internal-codex-responses-lite"
)

// client_metadata 内的 key 名（对齐真实 client_metadata()：
// session_id/thread_id/turn_id 为 snake_case 且与头名不同；
// x-codex-turn-state 的 metadata key 名即头名）。
const (
	codexMetaInstallationKey = "x-codex-installation-id"
	codexMetaSessionKey      = "session_id"
	codexMetaThreadKey       = "thread_id"
	codexMetaTurnKey         = "turn_id"
	codexMetaWindowKey       = "x-codex-window-id"
	codexMetaSubagentKey     = "x-openai-subagent"
	codexMetaParentThreadKey = "x-codex-parent-thread-id" // 条件键（真实 client_metadata()：responses_metadata.rs:274-279）
	codexMetaParentTurnKey   = "parent_turn_id"           // 条件键（真实 client_metadata()：responses_metadata.rs:280-282）
	codexMetaTurnMetadataKey = "x-codex-turn-metadata"
	codexMetaTurnStateKey    = "x-codex-turn-state"
	codexMetaTraceparentKey  = "ws_request_header_traceparent"
	codexMetaTracestateKey   = "ws_request_header_tracestate"
	// MetaResponsesLiteKey 是 responses-lite 的 client_metadata 键（值 "true"；
	// 服务端约定把 ws_request_header_ 前缀键还原为请求头，与 HeaderResponsesLite
	// 同一标记）。以 WithClientMetadata 透传，SDK 只透传不解析。
	MetaResponsesLiteKey = "ws_request_header_x_openai_internal_codex_responses_lite"
)

// CodexPayloadFields 是 response.create 顶层 key 白名单（18 字段 + type，
// 对齐真实 ResponseCreateWsRequest——真实存在 stream_options）。
// 只读，勿修改。
var CodexPayloadFields = []string{
	"type", "model", "instructions", "previous_response_id", "input",
	"tools", "tool_choice", "parallel_tool_calls", "reasoning",
	"store", "stream", "stream_options", "include", "service_tier",
	"prompt_cache_key", "text", "generate", "client_metadata",
}

var codexPayloadFieldSet = func() map[string]struct{} {
	m := make(map[string]struct{}, len(CodexPayloadFields))
	for _, f := range CodexPayloadFields {
		m[f] = struct{}{}
	}
	return m
}()

// ErrEmptyFrame 是帧经白名单过滤后无任何白名单字段时返回的错误
// （空结果帧不入网）。
var ErrEmptyFrame = errors.New("codexsdk: 帧经白名单过滤后为空")

// FilterCodexPayload 顶层 key 白名单过滤（纯函数）：删除不在
// CodexPayloadFields 中的顶层 key，白名单字段的值原样搬移（gjson raw，
// 值内容零解析，只动顶层不深入嵌套）。
//
// 空输入/非法 JSON 原样返回；无需删除时零拷贝返回原字节；
// 过滤后无任何白名单字段时返回 ErrEmptyFrame。
func FilterCodexPayload(raw []byte) ([]byte, error) {
	if len(raw) == 0 || !gjson.ValidBytes(raw) {
		return raw, nil
	}
	needsFilter := false
	gjson.ParseBytes(raw).ForEach(func(key, _ gjson.Result) bool {
		if _, ok := codexPayloadFieldSet[key.String()]; !ok {
			needsFilter = true
			return false
		}
		return true
	})
	if !needsFilter {
		return raw, nil
	}
	filtered := []byte(`{}`)
	for _, field := range CodexPayloadFields {
		v := gjson.GetBytes(raw, field)
		if !v.Exists() {
			continue
		}
		filtered, _ = sjson.SetRawBytes(filtered, field, []byte(v.Raw))
	}
	if len(filtered) == 2 { // "{}"
		return nil, ErrEmptyFrame
	}
	return filtered, nil
}

// Session 是会话级标识（真实 codex 客户端 WS 握手恒带
// x-client-request-id / session-id / thread-id / x-codex-window-id）。
// 会话内稳定，新会话换新值（UUIDv7，见 NewUUIDv7）。WithSession 注入
// 握手头，并补齐帧内 client_metadata 的 session_id / thread_id /
// x-codex-window-id（CodexMeta 中同 key 优先）。
type Session struct {
	SessionID       string // 头 session-id / metadata session_id
	ThreadID        string // 头 thread-id / metadata thread_id
	WindowID        string // 头 x-codex-window-id / metadata x-codex-window-id（{thread_id}:{n}，n 自 0 起）
	ClientRequestID string // 头 x-client-request-id（空则用 ThreadID）
}

// CodexMeta 是 client_metadata 静态载体（值由调用方提供，SDK 只组装不生成；
// trace 与 turn_id 另有自动机制，见 WithTraceContext / WithTurnAuto，
// 也可用本结构静态注入。优先级：帧内已存在 > CodexMeta > WithSession >
// 自动机制）。
type CodexMeta struct {
	InstallationID string // metadata "x-codex-installation-id"（UUIDv4，账号级持久）
	SessionID      string // metadata "session_id"（UUIDv7，会话级）
	ThreadID       string // metadata "thread_id"（UUIDv7，线程级）
	TurnID         string // metadata "turn_id"（UUIDv7；为空时 SDK 每轮自动生成）
	WindowID       string // metadata "x-codex-window-id"（{thread_id}:{n}）
	Subagent       string // metadata "x-openai-subagent"（条件）
	ParentThreadID string // metadata "x-codex-parent-thread-id"（条件；续接/子代理）
	ParentTurnID   string // metadata "parent_turn_id"（条件；续接/子代理）
	TurnMetadata   string // metadata "x-codex-turn-metadata"
	Traceparent    string // metadata "ws_request_header_traceparent"（为空时自动生成）
	Tracestate     string // metadata "ws_request_header_tracestate"
}

func (m *CodexMeta) empty() bool {
	return m == nil || (m.InstallationID == "" && m.SessionID == "" && m.ThreadID == "" &&
		m.TurnID == "" && m.WindowID == "" && m.Subagent == "" && m.ParentThreadID == "" &&
		m.ParentTurnID == "" && m.TurnMetadata == "" && m.Traceparent == "" && m.Tracestate == "")
}

// TraceContext 是 W3C trace context（仅注入 WS 帧内 client_metadata——
// 真实客户端 WS 握手与 HTTP 请求均不发 trace 头）。
type TraceContext struct {
	Traceparent string // 如 "00-<32位hex trace id>-<16位hex parent id>-01"
	Tracestate  string // 可为空（调用方可补充 vendor 数据）
}

// NewTraceContext 生成新的 W3C trace context（每次调用新链路 id，crypto/rand）。
func NewTraceContext() TraceContext {
	return TraceContext{
		Traceparent: "00-" + randomHex(16) + "-" + randomHex(8) + "-01",
	}
}

// NewUUIDv7 生成 UUIDv7（RFC 9562：48bit 毫秒时间戳 + 版本 7 + 变体 10 +
// 随机数），对齐真实 codex 客户端的 session_id / thread_id / turn_id 取值。
func NewUUIDv7() string {
	var b [16]byte
	now := time.Now().UnixMilli()
	b[0] = byte(now >> 40)
	b[1] = byte(now >> 32)
	b[2] = byte(now >> 24)
	b[3] = byte(now >> 16)
	b[4] = byte(now >> 8)
	b[5] = byte(now)
	copy(b[6:], randomBytes(10))
	b[6] = b[6]&0x0f | 0x70 // 版本 7
	b[8] = b[8]&0x3f | 0x80 // 变体 10
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}

func randomHex(n int) string {
	return hex.EncodeToString(randomBytes(n))
}

func randomBytes(n int) []byte {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		// crypto/rand 失败属系统级异常：panic 比静默复用链路 id 安全。
		panic(fmt.Sprintf("codexsdk: crypto/rand 失败: %v", err))
	}
	return b
}

// hasClientMetadataKey 判断帧内 client_metadata 是否已含 key
// （透传优先预检：帧内已有值时零生成直接透传）。
func hasClientMetadataKey(frame []byte, key string) bool {
	return gjson.GetBytes(frame, "client_metadata."+key).Exists()
}

// metadataEntry 是 client_metadata 注入项（key 值均由调用方/机制提供）。
type metadataEntry struct {
	key   string
	value string
}

// injectClientMetadataKeys 把 entries 写入帧顶层 client_metadata（浅合并：
// 帧内已存在的 key 不覆盖，只补充缺失；client_metadata 不存在时创建）。
// 只动顶层 client_metadata key，其余字节零改动。返回帧（可能为新分配）。
func injectClientMetadataKeys(frame []byte, entries []metadataEntry) []byte {
	if len(entries) == 0 {
		return frame
	}
	out := frame
	for _, e := range entries {
		if e.value == "" {
			continue
		}
		if gjson.GetBytes(out, "client_metadata."+e.key).Exists() {
			continue
		}
		next, err := sjson.SetBytes(out, "client_metadata."+e.key, e.value)
		if err != nil {
			return out // 非法 JSON：放弃本次注入，保持帧原样
		}
		out = next
	}
	return out
}
