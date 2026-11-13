package codexsdk

import (
	"encoding/binary"
	"hash/fnv"
	"strings"

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
// generate:false（预热由调用方另判）。非法 JSON / 无 input 返回零值（Anchor=-1）。
func AnalyzeConversationInput(payload []byte) ConversationInput {
	out := ConversationInput{Anchor: -1}
	if !gjson.ValidBytes(payload) {
		return out
	}
	input := gjson.GetBytes(payload, "input")
	switch {
	case input.Type == gjson.String:
		out.Items = 1
		out.Anchor = 0
		out.TurnKey = turnKeyOf(0, []byte(input.Raw))
		return out
	case !input.IsArray():
		return out
	}
	lastKind := itemOther
	idx := 0
	input.ForEach(func(_, el gjson.Result) bool {
		raw := []byte(el.Raw)
		kind := classifyInputItem(el)
		switch kind {
		case itemOther:
			out.Anchor = idx
			out.TurnKey = turnKeyOf(idx, raw)
		case itemCompaction:
			out.Compaction = fnv64a(raw)
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

func classifyInputItem(el gjson.Result) inputItemKind {
	typ := el.Get("type")
	t := typ.String()
	if !typ.Exists() && el.Get("role").Exists() {
		t = "message"
	}
	switch t {
	case "compaction", "compaction_summary", "context_compaction":
		return itemCompaction
	case "reasoning", "additional_tools":
		return itemModelOutput
	case "message":
		if el.Get("role").String() == "assistant" {
			return itemModelOutput
		}
		return itemOther
	}
	if strings.HasSuffix(t, "_call_output") {
		return itemToolOutput
	}
	if strings.HasSuffix(t, "_call") {
		return itemModelOutput
	}
	return itemOther
}

func turnKeyOf(idx int, raw []byte) uint64 {
	h := fnv.New64a()
	var b [4]byte
	binary.LittleEndian.PutUint32(b[:], uint32(idx))
	_, _ = h.Write(b[:])
	_, _ = h.Write(raw)
	return h.Sum64()
}

func fnv64a(raw []byte) uint64 {
	h := fnv.New64a()
	_, _ = h.Write(raw)
	return h.Sum64()
}
