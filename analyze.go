package codexsdk

import (
	"bytes"
	"encoding/binary"

	"github.com/tidwall/gjson"
)

// ConversationInput 是对一次请求 input 的内容分析（只看顶层元素的 type / role）。
//
// 用途：判断本请求是不是"同一轮的后续请求"（FollowUp），算轮次键（TurnKey）与
// 压缩标记（Compaction）。槽位路径用 FollowUp 的反值作为 Turn.First；会话簿用
// 其余字段。
type ConversationInput struct {
	// Items 是 input 顶层元素个数（字符串 input 计 1）。
	Items int
	// Anchor 是锚点下标：最后一个既不是模型输出也不是工具输出的元素；-1 = 无。
	Anchor int
	// TurnKey = FNV-64a(锚点下标 uint32 LE ‖ 锚点原始字节)；无锚点为 0。
	TurnKey uint64
	// Compaction 是最后一个压缩项（compaction / compaction_summary /
	// context_compaction）原始字节的 FNV-64a；没有为 0。
	Compaction uint64

	followUp bool
}

// FollowUp 报告最后一个元素是工具输出（type 以 _call_output 结尾）：同一轮的
// 后续请求。其余情况（含空 input）都按"新的一轮"处理。
func (c ConversationInput) FollowUp() bool { return c.followUp }

// AnalyzeConversationInput 分析请求体的 input（WS 帧同样适用）。input 为字符串时
// 视为 1 个 user 消息；省略 type 的 {"role":...} 视为 message。不解析
// generate:false（预热由调用方另判）。非法 JSON、根不是对象、无 input 返回零值
// （Anchor=-1）。
//
// 顶层同名键只认第一次出现（与整形一致，键按解码后的名字匹配）；扫描零拷贝，
// 不复制 input。
//
// 前置条件：调用方须已用有深度上限的校验器（如 encoding/json 的 json.Valid）
// 校验过 payload；这里的 gjson 校验是递归实现，不设深度上限。
func AnalyzeConversationInput(payload []byte) ConversationInput {
	out := ConversationInput{Anchor: -1}
	if !gjson.ValidBytes(payload) {
		return out
	}
	input, ok := objectMember(payload, "input")
	if !ok || len(input) == 0 {
		return out
	}
	switch input[0] {
	case '"':
		out.Items = 1
		out.Anchor = 0
		out.TurnKey = turnKeyOf(0, input)
		return out
	case '[':
	default:
		return out
	}
	lastKind := itemOther
	idx := 0
	forEachElement(input, func(el []byte) bool {
		kind := classifyInputItem(el)
		switch kind {
		case itemOther:
			out.Anchor = idx
			out.TurnKey = turnKeyOf(idx, el)
		case itemCompaction:
			out.Compaction = fnv64a(el)
		}
		lastKind = kind
		idx++
		return true
	})
	out.Items = idx
	out.followUp = idx > 0 && lastKind == itemToolOutput
	return out
}

type inputItemKind uint8

const (
	itemOther       inputItemKind = iota // 锚点候选（用户消息、developer 消息、agent_message……）
	itemModelOutput                      // 模型输出
	itemCompaction                       // 压缩项（也是模型输出）
	itemToolOutput                       // 工具输出
)

// classifyInputItem 只读元素顶层的 type / role（首现）。非对象元素与非字符串
// type 按锚点候选处理。
func classifyInputItem(el []byte) inputItemKind {
	if len(el) == 0 || el[0] != '{' {
		return itemOther
	}
	typTok, hasType := objectMember(el, "type")
	roleTok, hasRole := objectMember(el, "role")
	var t []byte
	if hasType {
		t, _ = jsonStringValue(typTok)
	} else if hasRole {
		t = []byte("message")
	}
	switch string(t) {
	case "compaction", "compaction_summary", "context_compaction":
		return itemCompaction
	case "reasoning", "additional_tools":
		return itemModelOutput
	case "message":
		if role, _ := jsonStringValue(roleTok); string(role) == "assistant" {
			return itemModelOutput
		}
		return itemOther
	}
	if bytes.HasSuffix(t, []byte("_call_output")) {
		return itemToolOutput
	}
	if bytes.HasSuffix(t, []byte("_call")) {
		return itemModelOutput
	}
	return itemOther
}

// FNV-64a（内联实现，零分配）。
const (
	fnvOffset64 = 14695981039346656037
	fnvPrime64  = 1099511628211
)

func fnv64aAppend(h uint64, b []byte) uint64 {
	for _, c := range b {
		h ^= uint64(c)
		h *= fnvPrime64
	}
	return h
}

func turnKeyOf(idx int, raw []byte) uint64 {
	var b [4]byte
	binary.LittleEndian.PutUint32(b[:], uint32(idx))
	return fnv64aAppend(fnv64aAppend(fnvOffset64, b[:]), raw)
}

func fnv64a(raw []byte) uint64 { return fnv64aAppend(fnvOffset64, raw) }
