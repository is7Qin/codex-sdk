package codexsdk

import (
	"bytes"
	"strings"
	"testing"

	"github.com/tidwall/gjson"
)

// 以下是单遍惰性分类之前的实现（每个元素独立查 type 与 role、每个锚点候选都
// 算一次哈希），作为差分测试的语义基准。
func classifyInputItemEager(el []byte) inputItemKind {
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

func analyzeEager(payload []byte) ConversationInput {
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
		kind := classifyInputItemEager(el)
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

var classifyDiffCases = []string{
	`{}`, `[]`, `1`, `"s"`, `null`,
	`{"role":"user","content":"a"}`,
	`{"role":"assistant","content":"a"}`,
	`{"content":"a","role":"assistant"}`,
	`{"type":"message","role":"assistant"}`,
	`{"role":"assistant","type":"message"}`,
	`{"type":"message","content":"x","role":"assistant"}`,
	`{"type":"message","role":"user"}`,
	`{"type":"message"}`,
	`{"type":"m\u0065ssage","r\u006fle":"assistant"}`,
	`{"typ\u0065":"function_call_output","role":"assistant"}`,
	`{"type":"message","role":"assistant","role":"user"}`,
	`{"type":"message","role":"user","type":"reasoning","role":"assistant"}`,
	`{"type":"reasoning","type":"message","role":"assistant"}`,
	`{"role":"user","type":"function_call_output"}`,
	`{"role":"assistant","type":"function_call"}`,
	`{"type":"function_call_output","output":"x"}`,
	`{"type":"custom_tool_call_output"}`,
	`{"type":"local_shell_call"}`,
	`{"type":"compaction","x":1}`,
	`{"type":"compaction_summary"}`,
	`{"type":"context_compaction"}`,
	`{"type":"additional_tools"}`,
	`{"type":1,"role":"assistant"}`,
	`{"type":null,"role":"assistant"}`,
	`{"type":"message","role":1}`,
	`{"role":null}`,
	` { "type" : "message" , "role" : "assistant" } `,
	`{"type":"agent_message"}`,
}

// escapedInputKeyPayloads：顶层 input 键用转义拼写（按解码名比较、只认首现）。
// 第二条里首个解码为 input 的成员是字符串，其后的字面 "input" 数组须被忽略。
var escapedInputKeyPayloads = []string{
	`{"inp\u0075t":[{"role":"user","content":"a"},{"type":"function_call_output"}]}`,
	`{"x":{"input":[1]},"inp\u0075t":"hi","input":[{"role":"user"}]}`,
}

// TestClassifyInputItemMatchesEager：单遍惰性分类与旧的逐成员查找语义一致
// （含转义键、重复键首现、非字符串 type/role、role 在 type 之前）。
func TestClassifyInputItemMatchesEager(t *testing.T) {
	for _, c := range classifyDiffCases {
		el := []byte(strings.TrimSpace(c))
		if got, want := classifyInputItem(el), classifyInputItemEager(el); got != want {
			t.Errorf("%s: got %d want %d", c, got, want)
		}
	}
	payload := `{"model":"m","input":[` + strings.Join(classifyDiffCases[5:], ",") + `]}`
	for _, p := range append([]string{payload, payload[:len(payload)-3] + `,{"type":"function_call_output"}]}`}, escapedInputKeyPayloads...) {
		if got, want := AnalyzeConversationInput([]byte(p)), analyzeEager([]byte(p)); got != want {
			t.Errorf("Analyze 不一致\n got=%+v\nwant=%+v", got, want)
		}
	}
}

func FuzzClassifyInputItemMatchesEager(f *testing.F) {
	for _, c := range classifyDiffCases {
		f.Add([]byte(`{"input":[` + c + `]}`))
	}
	f.Add([]byte(`{"input":"hi"}`))
	for _, p := range escapedInputKeyPayloads {
		f.Add([]byte(p))
	}
	f.Add([]byte(`{"input":[{"role":"user"},{"type":"compaction"},{"type":"x_call_output"}]}`))
	f.Fuzz(func(t *testing.T, b []byte) {
		if !gjson.ValidBytes(b) {
			return
		}
		if got, want := AnalyzeConversationInput(b), analyzeEager(b); got != want {
			t.Fatalf("Analyze 不一致 %q\n got=%+v\nwant=%+v", b, got, want)
		}
		if input, ok := objectMember(b, "input"); ok {
			forEachElement(input, func(el []byte) bool {
				if got, want := classifyInputItem(el), classifyInputItemEager(el); got != want {
					t.Fatalf("分类不一致 %q: got %d want %d", el, got, want)
				}
				return true
			})
		}
	})
}

// largeToolOutputPayload：2000 个约 4KB 的 function_call_output（共约 8MB），
// 末尾一个 user 消息作锚点。
func largeToolOutputPayload() []byte {
	var b bytes.Buffer
	b.WriteString(`{"model":"m","input":[{"role":"user","content":"go"}`)
	out := strings.Repeat("x", 4096)
	for i := 0; i < 2000; i++ {
		b.WriteString(`,{"type":"function_call_output","call_id":"c","output":"`)
		b.WriteString(out)
		b.WriteString(`"}`)
	}
	b.WriteString(`]}`)
	return b.Bytes()
}

func BenchmarkAnalyzeLargeToolOutput(b *testing.B) {
	p := largeToolOutputPayload()
	b.SetBytes(int64(len(p)))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = AnalyzeConversationInput(p)
	}
}
