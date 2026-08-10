package codexsdk

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"

	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

// 伪装层：Codex 客户端形态对齐（UA / originator / beta 头、头常量、
// 字段白名单、client_metadata 组装、trace 生成、turn 计数）。
// 机制进 SDK，业务值由调用方（网关）提供。

// DefaultCodexUserAgent 默认 codex-tui UA（WS 升级与 HTTP 请求默认注入，
// WithHeader("User-Agent", ...) 可覆盖）。
const DefaultCodexUserAgent = "codex-tui/0.144.1 (Mac OS 26.5.0; arm64) Apple_Terminal/470.2 (codex-tui; 0.144.1)"

// DefaultOriginator 默认 originator 头值（WS 升级与 HTTP 请求默认注入）。
const DefaultOriginator = "codex-tui"

// Responses WS beta 版本（对齐 sub2api openAIWSBetaV1Value/V2Value）。
const (
	DefaultBetaWSV1 = "2026-02-04"
	DefaultBetaWSV2 = "2026-02-06"
)

// HTTPBetaResponsesV1 是 Responses HTTP 的 OpenAI-Beta 头值（与 WS 的
// responses_websockets= 不同——HTTP 路径默认注入该值，WithHeader 可覆盖）。
const HTTPBetaResponsesV1 = "responses=v1"

// Codex 请求头名常量（对齐 sub2api openAICodex*Header 头名集合，
// 供调用方组装请求头）。
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
)

// client_metadata 内的 key 名（对齐 sub2api build_ws_client_metadata：
// trace 的 metadata key 名与 header 名不同）。
const (
	codexMetaInstallationKey = "x-codex-installation-id"
	codexMetaWindowKey       = "x-codex-window-id"
	codexMetaSubagentKey     = "x-openai-subagent"
	codexMetaTurnMetadataKey = "x-codex-turn-metadata"
	codexMetaTraceparentKey  = "ws_request_header_traceparent"
	codexMetaTracestateKey   = "ws_request_header_tracestate"
)

// CodexPayloadFields 是 response.create 顶层 key 白名单（17 字段，
// 对齐 sub2api wsallowlistFields）。只读，勿修改。
var CodexPayloadFields = []string{
	"type", "model", "instructions", "previous_response_id", "input",
	"tools", "tool_choice", "parallel_tool_calls", "reasoning",
	"store", "stream", "include", "service_tier", "prompt_cache_key",
	"text", "generate", "client_metadata",
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

// CodexMeta 是 client_metadata 静态载体（值由调用方提供，SDK 只组装不生成；
// trace 与 turn_metadata 另有自动机制，见 WithTraceContext / WithTurnMetadata，
// 也可用本结构静态注入。优先级：帧内已存在 > CodexMeta > 自动机制）。
type CodexMeta struct {
	InstallationID string // client_metadata."x-codex-installation-id"
	WindowID       string // client_metadata."x-codex-window-id"
	Subagent       string // client_metadata."x-openai-subagent"
	TurnMetadata   string // client_metadata."x-codex-turn-metadata"
	Traceparent    string // client_metadata."ws_request_header_traceparent"
	Tracestate     string // client_metadata."ws_request_header_tracestate"
}

func (m *CodexMeta) empty() bool {
	return m == nil || (m.InstallationID == "" && m.WindowID == "" && m.Subagent == "" &&
		m.TurnMetadata == "" && m.Traceparent == "" && m.Tracestate == "")
}

// TraceContext 是 W3C trace context。
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

func randomHex(n int) string {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		// crypto/rand 失败属系统级异常：panic 比静默复用链路 id 安全。
		panic(fmt.Sprintf("codexsdk: crypto/rand 失败: %v", err))
	}
	return hex.EncodeToString(b)
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
