package codexsdk

import (
	"bytes"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/tidwall/gjson"
)

// testID 是一个完整的非匿名身份（线程号是 UUIDv7 形态，lite id 取其时间戳）。
func testID() Identity {
	return Identity{
		InstallationID: "inst-t",
		Session:        Session{SessionID: "01a1d4d7-0000-7000-8000-000000000001", ThreadID: "01a1d4d7-0000-7000-8000-000000000001", WindowID: "01a1d4d7-0000-7000-8000-000000000001:0"},
		Window:         Window{ContextID: "01a1d4d8-0000-7000-8000-000000000002"},
		Turn:           Turn{ID: "turn-1", StartedAtMs: 1794572237107, State: "ts-1"},
	}
}

func topKeys(t *testing.T, b []byte) []string {
	t.Helper()
	var keys []string
	gjson.ParseBytes(b).ForEach(func(k, _ gjson.Result) bool { keys = append(keys, k.String()); return true })
	return keys
}

// TestShapeThirdPartyRequests：第三方客户端的各种形态。
func TestShapeThirdPartyRequests(t *testing.T) {
	before := ShapeStatsSnapshot()
	in := []byte(" {\n  \"tools\": [{\"type\":\"function\",\"function\":{\"parameters\":{\"type\":\"object\"},\"strict\":true,\"name\":\"sh\",\"description\":\"run\"}}],\n" +
		`  "instructions": "be brief", "st\u006fre": true, "store": true, "input": [{"role":"user","content":"hi"}] ,` +
		`"prompt_cache_options":{"ttl":"1h"}, "unknown": 1, "type":"x", "client_metadata":{"session_id":"evil"},` +
		`"prompt_cache_key":"client-key", "m\u006fdel": "gpt-5.5", "previous_response_id":"resp_a", "previous_response_id":"resp_b"}`)
	sh := mustShape(t, in, surfaceHTTP, testID())
	out := assembleHTTP(&sh)
	if !gjson.ValidBytes(out) {
		t.Fatalf("输出不是合法 JSON: %s", out)
	}
	wantKeys := "model,stream,input,tools,tool_choice,parallel_tool_calls,reasoning,store,include,prompt_cache_key,text,client_metadata"
	if got := strings.Join(topKeys(t, out), ","); got != wantKeys {
		t.Fatalf("顶层键序 = %s\n期望 %s", got, wantKeys)
	}
	if bytes.ContainsAny(out[:1], " \n") || bytes.Contains(out, []byte(`" :`)) {
		t.Fatalf("顶层应紧凑: %.80s", out)
	}
	for _, bad := range []string{`"instructions"`, "prompt_cache_options", "unknown", "client-key", "evil", "resp_a", "previous_response_id", `"type":"x"`} {
		if bytes.Contains(out, []byte(bad)) {
			t.Fatalf("不应出现 %q: %s", bad, out)
		}
	}
	if gjson.GetBytes(out, "store").Bool() || strings.Count(string(out), `"store"`) != 1 {
		t.Fatalf("store 应只有一个且为 false: %s", out)
	}
	if got := gjson.GetBytes(out, "prompt_cache_key").String(); got != testID().Session.SessionID {
		t.Fatalf("prompt_cache_key = %q，期望会话号", got)
	}
	// 指令 → developer 消息（非 lite：UUIDv5 id），排在客户端输入之前。
	msg := gjson.GetBytes(out, "input.0")
	if msg.Get("role").String() != "developer" || msg.Get("content.0.text").String() != "be brief" ||
		msg.Get("id").String() != "msg_"+instructionsIDV5(testID().Session.ThreadID, []byte("be brief")) {
		t.Fatalf("指令消息 = %s", msg.Raw)
	}
	if gjson.GetBytes(out, "input.1.content").String() != "hi" {
		t.Fatalf("客户端输入应原样跟在后面: %s", gjson.GetBytes(out, "input").Raw)
	}
	// Chat Completions 风格工具拍平，键序 type,name,description,strict,parameters。
	if got := gjson.GetBytes(out, "tools.0").Raw; got != `{"type":"function","name":"sh","description":"run","strict":true,"parameters":{"type":"object"}}` {
		t.Fatalf("拍平工具 = %s", got)
	}
	after := ShapeStatsSnapshot()
	if after.DuplicateKeys-before.DuplicateKeys != 1 { // 两个 store（转义拼写也算同名）；HTTP 面不认 previous_response_id
		t.Fatalf("重复键计数增量 = %d，期望 1", after.DuplicateKeys-before.DuplicateKeys)
	}

	// WS→WS：重复的 previous_response_id 只认第一个。
	ws := mustShape(t, []byte(`{"model":"m","previous_response_id":"resp_a","previous_response_id":"resp_b","input":[]}`), surfaceWSNative, testID())
	frame, _ := assembleWS(&ws, wsAssembly{prevID: ws.clientPrevID})
	if strings.Count(string(frame), "previous_response_id") != 1 || gjson.GetBytes(frame, "previous_response_id").String() != "resp_a" {
		t.Fatalf("重复 previous_response_id 应只留第一个: %s", frame)
	}

	// 非字符串 instructions 丢弃并计数；空字符串不插入。
	b0 := ShapeStatsSnapshot().NonStringInstructions
	sh = mustShape(t, []byte(`{"model":"gpt-5.5","instructions":{"x":1}}`), surfaceHTTP, testID())
	if len(sh.inputPrefix) != 0 || ShapeStatsSnapshot().NonStringInstructions != b0+1 {
		t.Fatalf("非字符串 instructions 应丢弃并计数")
	}
	if sh = mustShape(t, []byte(`{"model":"gpt-5.5","instructions":""}`), surfaceHTTP, testID()); len(sh.inputPrefix) != 0 {
		t.Fatal("空 instructions 不应插入消息")
	}

	// 无效请求体。
	for _, bad := range []string{`{}`, `{"input":[]}`, `{"model":null}`, `{"model":1}`, `[]`, `null`, `{"model":"m","input":5}`, `{"model":"m"`} {
		if _, err := shapeBody([]byte(bad), surfaceHTTP, testID(), time.Now(), shapeConfig{}); !errors.Is(err, ErrInvalidRequestBody) {
			t.Fatalf("%s: 期望 ErrInvalidRequestBody, got %v", bad, err)
		}
	}

	// 重复 input：只认第一次出现；被丢弃的那份不参与 lite 判定与元素计数。
	d0 := ShapeStatsSnapshot().DuplicateKeys
	sh = mustShape(t, []byte(`{"model":"gpt-5.5","input":[{"role":"user","content":"a"}],"input":[{"type":"additional_tools"}]}`), surfaceHTTP, testID())
	out = assembleHTTP(&sh)
	if sh.lite || sh.inputItems != 1 || ShapeStatsSnapshot().DuplicateKeys != d0+1 ||
		bytes.Contains(out, []byte("additional_tools")) || gjson.GetBytes(out, "parallel_tool_calls").Raw != "true" {
		t.Fatalf("重复 input 不应影响 lite 判定与计数: lite=%v items=%d out=%s", sh.lite, sh.inputItems, out)
	}
	sh = mustShape(t, []byte(`{"model":"gpt-5.5","input":[{"role":"user","content":"a"}],"input":[ 1 , 2 ]}`), surfaceHTTP, testID())
	if sh.inputItems != 1 || !sh.inputCompact {
		t.Fatalf("重复 input 不应影响元素数与紧凑标记: items=%d compact=%v", sh.inputItems, sh.inputCompact)
	}

	// model 必须是非空、不含控制字节的字符串；service_tier 非字符串丢弃、含控制
	// 字节拒绝。三个面一致。
	for _, s := range []surface{surfaceHTTP, surfaceWSFromHTTP, surfaceWSNative} {
		for _, bad := range []string{
			`{"model":"gpt\r\nX-Evil: 1","input":"hi"}`,
			`{"model":"a\u0000","input":"hi"}`,
			`{"model":"a\u007f"}`,
			"{\"model\":\"a\x7f\"}",
			`{"model":"a\tb"}`,
			`{"model":""}`,
			`{"model":"gpt-5.5","service_tier":"pri\nority"}`,
			`{"model":"gpt-5.5","service_tier":"a\u001f"}`,
		} {
			if _, err := shapeBody([]byte(bad), s, testID(), time.Now(), shapeConfig{}); !errors.Is(err, ErrInvalidRequestBody) {
				t.Fatalf("surface %d %s: 期望 ErrInvalidRequestBody, got %v", s, bad, err)
			}
		}
	}
	sh = mustShape(t, []byte(`{"model":"gpt-5.5","service_tier":5}`), surfaceHTTP, testID())
	if out = assembleHTTP(&sh); gjson.GetBytes(out, "service_tier").Exists() || sh.routingHint() != "model=gpt-5.5" {
		t.Fatalf("非字符串 service_tier 应丢弃: hint=%q out=%.120s", sh.routingHint(), out)
	}

	// 匿名：不写 client_metadata、删除 prompt_cache_key、指令消息不带 id。
	sh = mustShape(t, []byte(`{"model":"gpt-5.5","prompt_cache_key":"k","instructions":"x","client_metadata":{"a":"b"}}`), surfaceHTTP, Identity{})
	out = assembleHTTP(&sh)
	if gjson.GetBytes(out, "client_metadata").Exists() || gjson.GetBytes(out, "prompt_cache_key").Exists() || gjson.GetBytes(out, "input.0.id").Exists() {
		t.Fatalf("匿名形态不符: %s", out)
	}
	// 身份残缺（SessionID 空）按匿名并计数。
	p0 := ShapeStatsSnapshot().PartialIdentity
	sh = mustShape(t, []byte(`{"model":"m"}`), surfaceHTTP, Identity{Session: Session{ThreadID: "t"}})
	if !sh.anonymous || ShapeStatsSnapshot().PartialIdentity != p0+1 {
		t.Fatal("SessionID 为空的残缺身份应按匿名处理并计数")
	}

	// 环境时间改写在整形里生效（迁移自旧注入测试）。
	sh = mustShape(t, []byte(`{"model":"m","input":[{"role":"user","content":"<environment_context>\n  <current_date>2026-09-28</current_date>\n  <timezone>Asia/Shanghai</timezone>\n</environment_context>"}]}`), surfaceHTTP, testID())
	if out = assembleHTTP(&sh); !bytes.Contains(out, []byte("<timezone>America/New_York</timezone>")) {
		t.Fatalf("环境时间未改写: %s", out)
	}
	// 字符串 input 转成一条 user 消息。
	sh = mustShape(t, []byte(`{"model":"m","input":"hi"}`), surfaceHTTP, Identity{})
	if got := gjson.GetBytes(assembleHTTP(&sh), "input").Raw; got != `[{"type":"message","role":"user","content":[{"type":"input_text","text":"hi"}]}]` {
		t.Fatalf("字符串 input = %s", got)
	}
}

// TestShapeLiteRules：lite / 非 lite 的工具分组、reasoning、parallel_tool_calls、
// tool_choice、text 键序与默认值。
func TestShapeLiteRules(t *testing.T) {
	tools := `[{"type":"web_search"},` +
		`{"type":"function","function":{"name":"a","parameters":{}}},` +
		`{"type":"namespace","name":"other","description":"o","tools":[]},` +
		`{"type":"custom","name":"c","description":"cd"},` +
		`{"type":"namespace","name":"functions","description":"fn desc","tools":[{"type":"function","name":"b"}]}]`
	req := `{"model":"gpt-6.1-sol","tools":` + tools + `,"tool_choice":"required","parallel_tool_calls":true,` +
		`"reasoning":{"x":1,"context":"current_turn","summary":"auto"},"text":{"format":{"type":"json"},"z":1},"instructions":"sys","input":[{"role":"user","content":"hi"}]}`
	sh := mustShape(t, []byte(req), surfaceWSFromHTTP, testID())
	frame, _ := assembleWS(&sh, wsAssembly{})
	if !sh.lite || gjson.GetBytes(frame, "tools").Exists() {
		t.Fatalf("lite：顶层 tools 应删除: %s", frame)
	}
	at := gjson.GetBytes(frame, "input.0")
	wantTools := `[{"type":"web_search"},{"type":"namespace","name":"functions","description":"fn desc","tools":[{"type":"function","name":"a","parameters":{}},{"type":"custom","name":"c","description":"cd"},{"type":"function","name":"b"}]},{"type":"namespace","name":"other","description":"o","tools":[]}]`
	if at.Get("type").String() != "additional_tools" || at.Get("role").String() != "developer" || at.Get("tools").Raw != wantTools {
		t.Fatalf("additional_tools = %s\n期望 tools %s", at.Raw, wantTools)
	}
	if keys := strings.Join(topKeys(t, []byte(at.Raw)), ","); keys != "type,id,role,tools" {
		t.Fatalf("additional_tools 键序 = %s", keys)
	}
	// lite id：UUIDv7，时间戳取窗口 UUID；同内容同 id。
	atID := at.Get("id").String()
	if !strings.HasPrefix(atID, "at_01a1d4d8-0000-7") {
		t.Fatalf("lite 工具项 id = %s，期望时间戳取 Window.ContextID", atID)
	}
	if again := mustShape(t, []byte(req), surfaceWSFromHTTP, testID()); !bytes.Equal(again.inputPrefix[0], sh.inputPrefix[0]) || again.injectHash() != sh.injectHash() {
		t.Fatal("同一内容的插入项应逐字节相同")
	}
	instr := gjson.GetBytes(frame, "input.1")
	if instr.Get("role").String() != "developer" || !strings.HasPrefix(instr.Get("id").String(), "msg_01a1d4d8-0000-7") {
		t.Fatalf("lite 指令消息 = %s", instr.Raw)
	}
	// lite 指令消息不带 passthrough（官方 lite 的这条消息来自历史，网关不合成）。
	if instr.Get("internal_chat_message_metadata_passthrough").Exists() ||
		strings.Join(topKeys(t, []byte(instr.Raw)), ",") != "type,id,role,content" {
		t.Fatalf("lite 指令消息不应带 passthrough: %s", instr.Raw)
	}
	for path, want := range map[string]string{
		"parallel_tool_calls": "false",
		"tool_choice":         `"auto"`,
		"reasoning":           `{"effort":"low","summary":"auto","context":"all_turns","x":1}`,
		"text":                `{"verbosity":"low","format":{"type":"json"},"z":1}`,
	} {
		if got := gjson.GetBytes(frame, path).Raw; got != want {
			t.Fatalf("%s = %s，期望 %s", path, got, want)
		}
	}
	if got := string(sh.textPrewarm); got != `{"verbosity":"low","z":1}` {
		t.Fatalf("预热 text = %s", got)
	}
	if !bytes.Contains(frame, []byte(`"`+MetaResponsesLiteKey+`":"true"`)) {
		t.Fatalf("lite 帧 client_metadata 应带 lite 键: %s", frame)
	}

	// 非 lite：tools 保留，删 reasoning.context，缺省 parallel=true / tool_choice=auto。
	sh = mustShape(t, []byte(`{"model":"gpt-5.5","tools":[{"type":"function","name":"a"}],"reasoning":{"context":"all_turns","effort":"high"}}`), surfaceHTTP, testID())
	out := assembleHTTP(&sh)
	for path, want := range map[string]string{
		"tools":               `[{"type":"function","name":"a"}]`,
		"parallel_tool_calls": "true",
		"tool_choice":         `"auto"`,
		"reasoning":           `{"effort":"high"}`,
		"text":                `{"verbosity":"low"}`,
		"include":             `["reasoning.encrypted_content"]`,
	} {
		if got := gjson.GetBytes(out, path).Raw; got != want {
			t.Fatalf("非 lite %s = %s，期望 %s", path, got, want)
		}
	}
	// 非 lite 指令消息带常量 passthrough（官方每请求重建，gpt-5.5 抓包）。
	sh = mustShape(t, []byte(`{"model":"gpt-5.5","instructions":"sys"}`), surfaceHTTP, testID())
	if got := gjson.GetBytes(assembleHTTP(&sh), "input.0.internal_chat_message_metadata_passthrough").Raw; got != `{"content_item_kinds":["model.base_instructions"]}` {
		t.Fatalf("非 lite 指令消息 passthrough = %s", got)
	}
	// 非 lite 没有 tools：官方恒写 tools（空数组）。
	sh = mustShape(t, []byte(`{"model":"gpt-5.5"}`), surfaceHTTP, testID())
	if got := gjson.GetBytes(assembleHTTP(&sh), "tools").Raw; got != `[]` {
		t.Fatalf("非 lite 缺省 tools = %s，期望 []", got)
	}
	// 表外模型：reasoning 恒写 {}、不补 text、计数。
	u0 := ShapeStatsSnapshot().UnknownModels
	sh = mustShape(t, []byte(`{"model":"not-in-table"}`), surfaceHTTP, testID())
	out = assembleHTTP(&sh)
	if gjson.GetBytes(out, "reasoning").Raw != `{}` || gjson.GetBytes(out, "text").Exists() || ShapeStatsSnapshot().UnknownModels != u0+1 {
		t.Fatalf("表外模型形态不符: %s", out)
	}
	// input 里已有 additional_tools（官方客户端判成 lite）：顶层 tools 丢弃并计数，
	// 表外模型也按 lite。
	d0 := ShapeStatsSnapshot().DualToolSources
	sh = mustShape(t, []byte(`{"model":"not-in-table","tools":[{"type":"function","name":"a"}],"input":[{"type":"additional_tools","tools":[]}]}`), surfaceHTTP, testID())
	out = assembleHTTP(&sh)
	if !sh.lite || gjson.GetBytes(out, "tools").Exists() || len(sh.inputPrefix) != 0 || ShapeStatsSnapshot().DualToolSources != d0+1 {
		t.Fatalf("双工具来源处理不符: %s", out)
	}
}

// TestClientMetadataKeySets：HTTP / WS 真实帧 / WS 预热帧的 client_metadata 键集合。
func TestClientMetadataKeySets(t *testing.T) {
	base := []string{"session_id", "thread_id", "x-codex-window-id", "x-codex-installation-id", "x-codex-turn-metadata", "mcp_attribution", "guardian_credits_requested"}
	keysOf := func(b []byte) map[string]string {
		m := map[string]string{}
		gjson.GetBytes(b, "client_metadata").ForEach(func(k, v gjson.Result) bool { m[k.String()] = v.String(); return true })
		return m
	}
	check := func(label string, got map[string]string, want []string) {
		t.Helper()
		if len(got) != len(want) {
			t.Fatalf("%s: 键 %v，期望 %v", label, got, want)
		}
		for _, k := range want {
			if _, ok := got[k]; !ok {
				t.Fatalf("%s: 缺 %s（%v）", label, k, got)
			}
		}
	}
	id := testID()
	sh := mustShape(t, []byte(`{"model":"gpt-6.1-sol"}`), surfaceHTTP, id)
	httpM := keysOf(assembleHTTP(&sh))
	check("HTTP", httpM, append([]string{"turn_id", "root_turn_id"}, base...))
	if httpM["mcp_attribution"] != `{"status":"none"}` || httpM["guardian_credits_requested"] != "true" || httpM["turn_id"] != "turn-1" || httpM["root_turn_id"] != "turn-1" {
		t.Fatalf("HTTP 值不符: %v", httpM)
	}
	sh = mustShape(t, []byte(`{"model":"gpt-6.1-sol"}`), surfaceWSFromHTTP, id)
	f, _ := assembleWS(&sh, wsAssembly{turnState: "ts-1"})
	check("WS 真实帧", keysOf(f), append([]string{"turn_id", "root_turn_id", "x-codex-turn-state", "x-codex-ws-stream-request-start-ms", MetaResponsesLiteKey}, base...))
	p, _ := assembleWS(&sh, wsAssembly{prewarm: true, turnState: "ts-1"})
	pm := keysOf(p)
	check("WS 预热帧", pm, append([]string{"turn_id", "x-codex-ws-stream-request-start-ms", MetaResponsesLiteKey}, base...))
	if pm["turn_id"] != "" || !strings.Contains(pm["x-codex-turn-metadata"], `"request_kind":"prewarm"`) {
		t.Fatalf("预热帧值不符: %v", pm)
	}
	// 非 lite 不带 lite 键；trace 只在 WithTraceContext / WithTraceAuto(true) 时带。
	sh = mustShape(t, []byte(`{"model":"gpt-5.5"}`), surfaceWSFromHTTP, id)
	f, _ = assembleWS(&sh, wsAssembly{})
	check("WS 非 lite 无 ts", keysOf(f), append([]string{"turn_id", "root_turn_id", "x-codex-ws-stream-request-start-ms"}, base...))
	sh, _ = shapeBody([]byte(`{"model":"gpt-5.5"}`), surfaceWSFromHTTP, id, time.Now(), shapeConfig{trace: &TraceContext{Traceparent: "00-x-y-01", Tracestate: "s"}})
	f, _ = assembleWS(&sh, wsAssembly{})
	if m := keysOf(f); m["ws_request_header_traceparent"] != "00-x-y-01" || m["ws_request_header_tracestate"] != "s" {
		t.Fatalf("WithTraceContext 应带 trace 键: %v", m)
	}
	sh, _ = shapeBody([]byte(`{"model":"gpt-5.5"}`), surfaceWSFromHTTP, id, time.Now(), shapeConfig{traceAuto: true})
	f, _ = assembleWS(&sh, wsAssembly{})
	if m := keysOf(f); !traceparentRe.MatchString(m["ws_request_header_traceparent"]) {
		t.Fatalf("WithTraceAuto(true) 应生成 traceparent: %v", m)
	}
	if o := defaultOptions(); o.traceAuto {
		t.Fatal("traceAuto 默认应关闭")
	}
}

// TestClientMetadataOrderRandomized：100 次组装至少出现 2 种键序。
func TestClientMetadataOrderRandomized(t *testing.T) {
	sh := mustShape(t, []byte(`{"model":"gpt-6.1-sol"}`), surfaceWSFromHTTP, testID())
	orders := map[string]bool{}
	for i := 0; i < 100; i++ {
		f, _ := assembleWS(&sh, wsAssembly{})
		var ks []string
		gjson.GetBytes(f, "client_metadata").ForEach(func(k, _ gjson.Result) bool { ks = append(ks, k.String()); return true })
		orders[strings.Join(ks, ",")] = true
	}
	if len(orders) < 2 {
		t.Fatalf("100 次组装只有 %d 种键序", len(orders))
	}
}

// TestEncodeZeroCopyAllocFree：匿名 + 规范键序与规范值的输入 → 返回原切片、
// 整条整形链（parse + shape + assemble）零分配。
func TestEncodeZeroCopyAllocFree(t *testing.T) {
	cases := []struct {
		name string
		s    surface
		raw  string
	}{
		{"http 表外模型", surfaceHTTP, `{"model":"m","stream":true,"input":[{"type":"message","role":"user","content":[{"type":"input_text","text":"hi"}]}],"tools":[],"tool_choice":"auto","parallel_tool_calls":true,"reasoning":{},"store":false,"include":["reasoning.encrypted_content"]}`},
		{"http lite", surfaceHTTP, `{"model":"gpt-6.1-sol","stream":true,"input":[],"tool_choice":"auto","parallel_tool_calls":false,"reasoning":{"effort":"low","context":"all_turns"},"store":false,"include":["reasoning.encrypted_content"],"text":{"verbosity":"low"}}`},
		{"ws native", surfaceWSNative, `{"type":"response.create","model":"gpt-5.5","stream":true,"input":[],"tools":[{"type":"function","name":"a"}],"tool_choice":"auto","parallel_tool_calls":true,"reasoning":{"effort":"medium"},"store":false,"include":["reasoning.encrypted_content"],"text":{"verbosity":"low"}}`},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			raw := []byte(c.raw)
			now := time.Now()
			run := func() []byte {
				sh, err := shapeBody(raw, c.s, Identity{}, now, shapeConfig{})
				if err != nil {
					t.Fatal(err)
				}
				if c.s == surfaceHTTP {
					return assembleHTTP(&sh)
				}
				out, off := assembleWS(&sh, wsAssembly{prevID: sh.clientPrevID, generate: sh.clientGenerate})
				if off != -1 {
					t.Fatalf("匿名帧不应有发送时刻偏移: %d", off)
				}
				return out
			}
			out := run()
			if len(out) != len(raw) || &out[0] != &raw[0] {
				t.Fatalf("应零拷贝返回原切片\n实际 %s\n输入 %s", out, raw)
			}
			if n := testing.AllocsPerRun(100, func() { _ = run() }); n != 0 {
				t.Fatalf("零拷贝路径分配 %.1f 次，期望 0", n)
			}
		})
	}
}

// TestModelTableMatchesOfficial0169：12 行，逐项取自 rust-v0.169.0 models.json。
func TestModelTableMatchesOfficial0169(t *testing.T) {
	want := map[string]modelInfo{
		"gpt-6-astra":              {true, true, false, true, "low", "low"},
		"gpt-6.1-sol":              {true, true, false, true, "low", "low"},
		"gpt-6.1-terra":            {true, true, false, true, "low", "low"},
		"gpt-6-sol":                {true, true, false, true, "low", "medium"},
		"gpt-6-luna":               {true, false, false, true, "low", "medium"},
		"gpt-5.6-sol":              {true, false, false, true, "low", "low"},
		"gpt-5.6-terra":            {true, false, false, true, "low", "medium"},
		"gpt-5.6-luna":             {true, false, false, true, "low", "medium"},
		"gpt-daybreak-blue-latest": {true, false, false, true, "low", "low"},
		"gpt-daybreak-red-latest":  {true, false, false, true, "high", "medium"},
		"gpt-5.5":                  {false, false, false, true, "low", "medium"},
		"codex-auto-review":        {true, false, false, true, "low", "medium"},
	}
	if len(modelTable) != 12 {
		t.Fatalf("模型表 %d 行，期望 12", len(modelTable))
	}
	for k, w := range want {
		if got, ok := modelTable[k]; !ok || got != w {
			t.Fatalf("%s = %+v，期望 %+v", k, got, w)
		}
	}
}

// TestAnalyzeConversationInput。
func TestAnalyzeConversationInput(t *testing.T) {
	user := `{"type":"message","role":"user","content":"hi"}`
	cases := []struct {
		name     string
		payload  string
		followUp bool
		anchor   int
		anchorEl string
	}{
		{"用户消息结尾", `{"input":[` + user + `]}`, false, 0, user},
		{"工具输出结尾是后续", `{"input":[` + user + `,{"type":"function_call","call_id":"c"},{"type":"function_call_output","call_id":"c"}]}`, true, 0, user},
		{"custom 工具输出", `{"input":[` + user + `,{"type":"custom_tool_call"},{"type":"custom_tool_call_output"}]}`, true, 0, user},
		{"多代理 agent_message 结尾不是后续", `{"input":[` + user + `,{"type":"function_call_output"},{"type":"agent_message","content":"x"}]}`, false, 2, `{"type":"agent_message","content":"x"}`},
		{"assistant 与 reasoning 不是锚点", `{"input":[` + user + `,{"type":"reasoning"},{"role":"assistant","content":"a"}]}`, false, 0, user},
		{"省略 type 的 role 项按 message", `{"input":[{"role":"user","content":"x"},{"role":"assistant","content":"y"}]}`, false, 0, `{"role":"user","content":"x"}`},
		{"additional_tools 不是锚点", `{"input":[{"type":"additional_tools","tools":[]}]}`, false, -1, ""},
		{"空 input", `{"input":[]}`, false, -1, ""},
	}
	for _, c := range cases {
		got := AnalyzeConversationInput([]byte(c.payload))
		if got.FollowUp() != c.followUp || got.Anchor != c.anchor {
			t.Fatalf("%s: FollowUp=%v Anchor=%d，期望 %v %d", c.name, got.FollowUp(), got.Anchor, c.followUp, c.anchor)
		}
		want := uint64(0)
		if c.anchor >= 0 {
			want = turnKeyOf(c.anchor, []byte(c.anchorEl))
		}
		if got.TurnKey != want {
			t.Fatalf("%s: TurnKey=%x，期望 %x", c.name, got.TurnKey, want)
		}
	}
	// 字符串 input：1 个 user 消息。
	if got := AnalyzeConversationInput([]byte(`{"input":"hello"}`)); got.Items != 1 || got.Anchor != 0 || got.FollowUp() || got.TurnKey != turnKeyOf(0, []byte(`"hello"`)) {
		t.Fatalf("字符串 input: %+v", got)
	}
	// 同一句话在不同下标 → 不同轮次键。
	a := AnalyzeConversationInput([]byte(`{"input":[` + user + `]}`))
	b := AnalyzeConversationInput([]byte(`{"input":[` + user + `,{"role":"assistant","content":"y"},` + user + `]}`))
	if a.TurnKey == b.TurnKey {
		t.Fatal("下标不同的同一句话应得到不同轮次键")
	}
	// 压缩标记：最后一个压缩项的 FNV-64a。
	c1 := `{"type":"compaction","encrypted_content":"A"}`
	c2 := `{"type":"context_compaction","encrypted_content":"B"}`
	got := AnalyzeConversationInput([]byte(`{"input":[` + c1 + `,` + user + `,` + c2 + `]}`))
	if got.Compaction != fnv64a([]byte(c2)) || got.Anchor != 1 {
		t.Fatalf("压缩标记 = %x，期望最后一个压缩项的哈希; %+v", got.Compaction, got)
	}
	if AnalyzeConversationInput([]byte(`{"input":[`+user+`]}`)).Compaction != 0 {
		t.Fatal("没有压缩项时压缩标记应为 0")
	}
}

// TestSlotIdentityShape：槽位路径的 IdentityState.Identity 整形结果：身份来自
// 槽位，形态为新形态；窗口推进时 ContextWindowID 更换。
func TestSlotIdentityShape(t *testing.T) {
	st := NewIdentityState("inst-slot", RotatePolicy{})
	if !uuidv7Re.MatchString(st.ContextWindowID) {
		t.Fatalf("NewIdentityState 应生成窗口 UUIDv7: %q", st.ContextWindowID)
	}
	payload := []byte(`{"model":"gpt-6.1-sol","input":[{"role":"user","content":"hi"}],"client_metadata":{"session_id":"evil"}}`)
	first := !AnalyzeConversationInput(payload).FollowUp()
	var sink turnStateSink
	id := st.Identity(&sink, first)
	if !id.Turn.First || id.Turn.ID != "" || id.Window.ContextID != st.ContextWindowID || id.Sink != &sink {
		t.Fatalf("Identity = %+v", id)
	}
	sh := mustShape(t, payload, surfaceHTTP, id)
	out := assembleHTTP(&sh)
	cm := gjson.GetBytes(out, "client_metadata")
	if cm.Get("session_id").String() != st.ThreadID || cm.Get("x-codex-window-id").String() != st.WindowID() ||
		cm.Get("x-codex-installation-id").String() != "inst-slot" || gjson.GetBytes(out, "prompt_cache_key").String() != st.ThreadID {
		t.Fatalf("身份应来自槽位: %s", cm.Raw)
	}
	tm := gjson.Parse(cm.Get("x-codex-turn-metadata").String())
	if tm.Get("context_window_id").String() != st.ContextWindowID || tm.Get("window_number").Uint() != st.WindowN || tm.Get("request_kind").String() != "turn" {
		t.Fatalf("turn-metadata = %s", tm.Raw)
	}
	// 后续请求（工具输出结尾）→ First=false。
	if !AnalyzeConversationInput([]byte(`{"input":[{"type":"function_call_output"}]}`)).FollowUp() {
		t.Fatal("工具输出结尾应判为后续")
	}
	// 窗口推进 → ContextWindowID 更换。
	cur := st
	for cur.WindowN == st.WindowN {
		cur = Step(cur, RotatePolicy{})
	}
	if cur.ContextWindowID == st.ContextWindowID || !uuidv7Re.MatchString(cur.ContextWindowID) {
		t.Fatalf("窗口推进后 ContextWindowID 应更换: %q → %q", st.ContextWindowID, cur.ContextWindowID)
	}
}

// TestTurnMetadataIgnoresClientValues：客户端 client_metadata 里的
// turn-metadata（任意 sandbox / sandbox_mode / workspaces，含带凭据的远端 URL）
// 不影响出网：恒为 none / danger-full-access、无 workspaces，哨兵不出网。
func TestTurnMetadataIgnoresClientValues(t *testing.T) {
	const cred = "https://alice:SENTINEL-TOKEN@github.com/org/repo.git"
	clientTM := `{"sandbox":"seatbelt","sandbox_mode":"workspace-write","workspaces":{"/Users/alice/SENTINEL-PATH":{"associated_remote_urls":{"origin":"` + cred + `"}}},"session_id":"SENTINEL-SESSION"}`
	for _, cm := range []string{
		`{"x-codex-turn-metadata":` + string(appendJSONString(nil, clientTM)) + `}`,
		`{"x-codex-turn-metadata":{"sandbox":"seatbelt"}}`,
		`"not an object"`,
		`{"x-codex-turn-metadata":"{broken"}`,
	} {
		req := []byte(`{"model":"gpt-6.1-sol","client_metadata":` + cm + `}`)
		for _, s := range []surface{surfaceHTTP, surfaceWSFromHTTP, surfaceWSNative} {
			sh := mustShape(t, req, s, testID())
			var outs [][]byte
			if s == surfaceHTTP {
				outs = append(outs, assembleHTTP(&sh))
			} else {
				f, _ := assembleWS(&sh, wsAssembly{})
				p, _ := assembleWS(&sh, wsAssembly{prewarm: true})
				outs = append(outs, f, p)
			}
			outs = append(outs, sh.turnMeta, sh.prewarmMeta)
			for _, o := range outs {
				for _, bad := range []string{"SENTINEL", "seatbelt", "workspace-write", "workspaces", "alice"} {
					if bytes.Contains(o, []byte(bad)) {
						t.Fatalf("出网字节含 %q: %s", bad, o)
					}
				}
			}
			for _, tmb := range [][]byte{sh.turnMeta, sh.prewarmMeta} {
				tm := gjson.ParseBytes(tmb)
				if tm.Get("sandbox").String() != "none" || tm.Get("sandbox_mode").String() != "danger-full-access" || tm.Get("workspaces").Exists() {
					t.Fatalf("turn-metadata = %s", tmb)
				}
			}
		}
	}
}
