package codexsdk

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/tidwall/gjson"
)

const fuzzSentinel = "SENTINELzz"

// fuzzTopKeysUnique 断言输出顶层是对象且键唯一，返回键集合。
func fuzzTopKeysUnique(t *testing.T, b []byte) map[string]bool {
	t.Helper()
	dec := json.NewDecoder(bytes.NewReader(b))
	if tok, err := dec.Token(); err != nil || tok != json.Delim('{') {
		t.Fatalf("根不是对象: %v %q", err, b)
	}
	keys := map[string]bool{}
	for dec.More() {
		k, err := dec.Token()
		if err != nil {
			t.Fatalf("键: %v", err)
		}
		ks := k.(string)
		if keys[ks] {
			t.Fatalf("重复键 %q: %q", ks, b)
		}
		keys[ks] = true
		var v json.RawMessage
		if err := dec.Decode(&v); err != nil {
			t.Fatalf("值: %v", err)
		}
	}
	return keys
}

func fuzzCheckOutput(t *testing.T, in, out []byte, s surface, prewarm bool) {
	t.Helper()
	if !json.Valid(out) {
		t.Fatalf("输出不是合法 JSON surface=%d\nin=%q\nout=%q", s, in, out)
	}
	keys := fuzzTopKeysUnique(t, out)
	if s == surfaceHTTP && keys["previous_response_id"] {
		t.Fatalf("HTTP 面出现 previous_response_id: %q", in)
	}
	if s != surfaceWSNative && keys["generate"] && !prewarm {
		t.Fatalf("非 WS→WS 面出现客户端 generate: %q", in)
	}
	gjson.GetBytes(out, "client_metadata").ForEach(func(k, v gjson.Result) bool {
		if k.String() != "x-codex-turn-metadata" && strings.Contains(v.String(), fuzzSentinel) {
			t.Fatalf("客户端值进入 client_metadata[%s]: in=%q out=%q", k.String(), in, out)
		}
		return true
	})
	if strings.Contains(gjson.GetBytes(out, "prompt_cache_key").Raw, fuzzSentinel) {
		t.Fatalf("客户端 prompt_cache_key 出网: in=%q", in)
	}
	if gjson.GetBytes(out, "store").Raw != "false" || gjson.GetBytes(out, "stream").Raw != "true" {
		t.Fatalf("store/stream: %q", out)
	}
}

// FuzzShapeOutputInvariants：任意输入经三种面 × 匿名/非匿名 × 多种 WS 组装参数
// 整形后：要么本地拒绝（ErrInvalidRequestBody），要么输出是顶层键唯一的合法
// JSON、HTTP 不出 previous_response_id、非 WS→WS 面不出客户端 generate、客户端
// client_metadata / prompt_cache_key 不出网、store=false / stream=true、
// turn-metadata 是纯 ASCII 合法 JSON、routing-hint 不含控制字节、输入缓冲不被修改。
func FuzzShapeOutputInvariants(f *testing.F) {
	seeds := []string{
		`{"model":"gpt-5.5","input":[{"type":"message","role":"user","content":"hi"}],"instructions":"x","tools":[{"type":"function","function":{"name":"a","parameters":{}}}]}`,
		`{"model":"gpt-5.4","input":"hello","tools":[{"type":"function","name":"f"},{"type":"web_search"},{"type":"namespace","name":"functions","description":"d","tools":[{"type":"custom","name":"c"}]}],"reasoning":{"summary":"auto","effort":"low","x":1},"text":{"format":{"type":"text"},"z":1}}`,
		`{"m\u006fdel":"gpt-5.4","model":1,"client_metadata":{"a":"SENTINELzz"},"client_\u006detadata":{"b":"SENTINELzz"},"prompt_cache_key":"SENTINELzz","previous_response_id":"r","prev\u0069ous_response_id":"r2","generate":false}`,
		` { "model" : "x" , "input" : [ {"type":"additional_tools","tools":[]} , 1 ] , "tools":[] } `,
		`{"model":"\ud83d\ude00\u0000\r\n","input":null,"service_tier":"pri\"ority","text":{"verbosity":"low","verbosity":"high"}}`,
		`{"model":"gpt-5.5","input":[{"type":"message","content":"<environment_context><current_date>2020</current_date><timezone>X</timezone></environment_context>"}]}`,
		`{"model":"a","instructions":"\ud800","input":[[[[[]]]]]}`,
		`{"model":"a","reasoning":{"eff\u006frt":"high","context":"x"},"text":{"form\u0061t":1}}`,
		`{"model":"gpt-5.5","input":[{"role":"user","content":"a"}],"input":[{"type":"additional_tools"}]}`,
		`{"model":"gpt\r\nX-Evil: 1","service_tier":5,"input":"hi"}`,
		`{"model":"gpt-6.1-sol","service_tier":"fl\u0007ex","instructions":"s","tools":[{"type":"custom","name":"c"}]}`,
		`{"model":"gpt-6.1-sol","previous_response_id":"resp_1","generate":true,"instructions":"s","input":[{"type":"function_call_output","call_id":"c","output":"o"}]}`,
	}
	for _, s := range seeds {
		f.Add([]byte(s))
	}
	fid := Identity{
		InstallationID: "inst-1",
		Session:        Session{SessionID: "019a0000-0000-7000-8000-000000000001", ThreadID: "019a0000-0000-7000-8000-000000000001", WindowID: "019a0000-0000-7000-8000-000000000001:0"},
		Turn:           Turn{ID: "t1", State: "ts1"},
	}
	f.Fuzz(func(t *testing.T, raw []byte) {
		orig := bytes.Clone(raw)
		now := time.Unix(1700000000, 0)
		_ = AnalyzeConversationInput(raw)
		_ = RewriteEnvironmentContextTime(raw, now)
		for _, s := range []surface{surfaceHTTP, surfaceWSFromHTTP, surfaceWSNative} {
			for _, id := range []Identity{{}, fid} {
				sh, err := shapeBody(raw, s, id, now, shapeConfig{stats: &shapeStats, traceAuto: true})
				if err != nil {
					continue
				}
				if !validHeaderToken([]byte(sh.routingHint())) {
					t.Fatalf("routing-hint 含控制字节: %q", sh.routingHint())
				}
				if s == surfaceHTTP {
					fuzzCheckOutput(t, raw, assembleHTTP(&sh), s, false)
				} else {
					for _, a := range []wsAssembly{
						{prevID: sh.clientPrevID, generate: sh.clientGenerate, turnState: sh.turnState(id)},
						{prewarm: true},
						{inputSkip: 1, prevID: "resp_\"x"},
						{inputSkip: 3},
					} {
						out, off := assembleWS(&sh, a)
						stampStartMs(out, off, time.Now())
						fuzzCheckOutput(t, raw, out, s, a.prewarm)
					}
				}
				for _, tm := range [][]byte{sh.turnMeta, sh.prewarmMeta} {
					if len(tm) == 0 {
						continue
					}
					if !json.Valid(tm) {
						t.Fatalf("turn-metadata 不是合法 JSON: %q", tm)
					}
					for _, c := range tm {
						if c >= 0x80 || c < 0x20 {
							t.Fatalf("turn-metadata 不是纯 ASCII: %q", tm)
						}
					}
				}
			}
		}
		if !bytes.Equal(orig, raw) {
			t.Fatal("输入缓冲被修改")
		}
	})
}
