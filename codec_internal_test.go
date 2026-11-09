package codexsdk

// 单遍编码器的字节一致性护栏（T4）。
//
// 为什么需要它：`encode` 有两条路径——单遍快路径与回退旧链。快路径之所以敢
// "按输入顺序搬移原始值"，靠的是一串条件（紧凑 / 无转义键 / 无重复键 / 顶层键
// 全在白名单内）。这些条件只要放宽一格，输出就会在键序或转义上与旧链分叉，而
// 既有断言（codex_test.go / transport_ws_contract_test.go 的 W14）覆盖不到全部
// 形态。所以这里用回退链当**参考实现**（它就是 T3 的字节语义），对同一批输入
// 逐字节比对：快路径恒等于回退链。另有两例钉住"快路径真的被走到"与"该回退的
// 形态确实回退"，防止护栏因快路径被悄悄关掉而变成空转。

import (
	"bytes"
	"math/rand"
	"strings"
	"testing"
	"time"

	"github.com/tidwall/gjson"
)

// encodeViaLegacyChain 显式只走回退链（跳过单遍），作为字节参考实现。
// 与 encode 的唯一差别是不尝试 encodeOnePass，故两者的字节必须一致。
func encodeViaLegacyChain(fc *frameCodec, raw []byte, mode encodeMode, in frameInput) ([]byte, error) {
	now := in.Now
	if now.IsZero() {
		now = time.Now()
	}
	frame := RewriteEnvironmentContextTime(raw, now)
	var entryBuf [8]metadataEntry
	obj := clientMetadataObject(fc.metaEntries(entryBuf[:0], in))
	return fc.normalize(frame, mode, obj)
}

// codecDiffCorpus 是逐字节比对的语料：覆盖运行期真实形状（3.7KB bench 体）、
// 协议形状差（type/续接键/stream/store 组合）、白名单外键、嵌套值里的分隔符与
// 转义、环境上下文改写、空白变体、转义/重复顶层键、非法 JSON 与非对象根。
var codecDiffCorpus = []string{
	string(wsBenchPayload),
	`{}`,
	`{"model":"gpt-5"}`,
	`{"type":"response.create","model":"gpt-5","store":false}`,
	`{"type":"wrong.type","stream":false,"previous_response_id":"resp_old","generate":true,"model":"m"}`,
	`{"model":"m","previous_response_id":"p","generate":false,"store":true,"stream":false}`,
	`{"model":"m","previous_response_id":"p"}`,
	`{"model":"m","generate":true}`,
	`{"model":"m","evil":1}`,
	`{"evil":1,"foo":2}`,
	`{"foo":1}`,
	`{"model":"m","max_output_tokens":4096,"store":true}`,
	`{"model":1,"model":2}`,
	`{"mo\u0064el":1}`,
	`{"model":"\u0041","store":false}`,
	`{"model":"a}b,c","instructions":"x\"y"}`,
	`{"tools" : []}`,
	`{ "model" : "m" , "input" : [ 1 , 2 ] }`,
	"{\n\t\"model\"\n:\n\"m\"\n}",
	`  {"model":"m"}  `,
	`{"model":null,"store":false}`,
	`{"model":"m","parallel_tool_calls":true,"stream":false,"store":false,"include":[1,2,3]}`,
	`{"model":"m","reasoning":{"effort":"low"},"input":[{"a":1},{"b":[true,false,null]}]}`,
	`{"text":{"format":{"type":"text"},"s":"}{[]"}}`,
	`{"client_metadata":{"evil":1}}`,
	`{"client_metadata":{},"model":"m"}`,
	`{"client_metadata":{"x":"y"},"type":"response.create","model":"m","store":false,"stream":true}`,
	`{"type":"response.create","model":"gpt-5","client_metadata":{"x-codex-window-id":"existing-win","user":{"a":1}}}`,
	`{"store":false}`,
	`{"store": true}`,
	`{"stream":true}`,
	`{"stream": false}`,
	`{"previous_response_id":"p"}`,
	`{"access_programs":["org-a"]}`,
	`{"model":"m","access_programs":["org-a"],"stream_options":{"reasoning_summary_delivery":"sequential_cutoff"}}`,
	`{"model":"gpt-5","input":[{"role":"user","content":"<environment_context>\n  <current_date>2026-09-28</current_date>\n  <timezone>Asia/Shanghai</timezone>\n</environment_context>"}],"store":false}`,
	`{"instructions":"<environment_context><current_date>2020-01-01</current_date><timezone>UTC</timezone></environment_context>"}`,
	`{"input":[{"content":"<environment_context><current_date>2020-01-01</current_date><timezone>UTC</timezone></environment_context>"}],"evil":1}`,
	`{"model":"\u00e9\u4e2d","input":"😀"}`,
	`{"model":"m","text":{"v":"a\"b\\c"}}`,
	`null`,
	`123`,
	`"str"`,
	`[1]`,
	`[]`,
	`true`,
	`  null  `,
	``,
	`{`,
	`{"a":}`,
	`{"a":1,}`,
	`not json`,
	// 位图上界：19 键全带（触达位图最高位 idx=18，并验证全键集合不误判重复）
	`{"type":"response.create","model":"m","instructions":"i","previous_response_id":"p","input":[],"tools":[],"tool_choice":"auto","parallel_tool_calls":true,"reasoning":{},"store":false,"stream":true,"stream_options":{},"include":[],"service_tier":"default","prompt_cache_key":"k","text":{},"generate":false,"client_metadata":{},"access_programs":[]}`,
	// 值里出现顶层键的名字面串：不得被当成顶层键
	`{"model":"m","instructions":"{\"type\":\"response.create\"}","input":"{\"store\":true}"}`,
	// 非法 UTF-8 / 控制字符 / lone surrogate：appendJSONString 与 gjson 不同源
	"{\"model\":\"\xff\x00\"}",
	`{"model":"\ud800"}`,
	// 字符串内不配对括号：skipJSONValue 的 depth 计数不得被假闭合符带偏
	`{"model":"a{b]c[d","input":{"x":[1,{"y":"}"}]}}`,
}

// codecDiffCodecs 是确定性的 codec 组合（不含 turnAuto/traceAuto：那两个每帧
// 生成随机值，逐字节比对无从下手；它们的组装键序由静态组合同样覆盖）。
func codecDiffCodecs() map[string]*frameCodec {
	return map[string]*frameCodec{
		"无注入": {},
		"静态 trace": newFrameCodec(&options{
			trace: &TraceContext{Traceparent: "00-aaa-bbb-01", Tracestate: "st"},
		}),
		"turn 回调": newFrameCodec(&options{
			turnProvider: func(uint64) string { return "turn-meta" },
		}),
	}
}

// TestEncodeByteIdenticalToLegacyChain：对语料逐字节比对"单遍"与"回退链"
// （两种模式 × 三种 codec 组合 × 三种 frameInput）。
func TestEncodeByteIdenticalToLegacyChain(t *testing.T) {
	now := time.Date(2026, time.November, 9, 3, 30, 0, 0, time.UTC)
	ident := Identity{
		Session: Session{SessionID: "sess", ThreadID: "thr", WindowID: "thr:0"},
		Meta: CodexMeta{
			InstallationID: "inst", SessionID: "sess", ThreadID: "thr",
			WindowID: "thr:0", TurnID: "static-turn", TurnMetadata: "tm",
			Traceparent: "tp", Tracestate: "ts",
		},
		TurnState: "gw-state",
	}
	inputs := map[string]frameInput{
		"零值输入":   {Now: now},
		"带身份+ts": {Now: now, Identity: ident, Turn: 3},
		"Now 未设": {Identity: Identity{Meta: CodexMeta{InstallationID: "inst"}}},
	}
	modes := map[string]encodeMode{"native": encodeNativeWS, "responses": encodeWSResponses}
	for cname, fc := range codecDiffCodecs() {
		for mname, mode := range modes {
			for iname, in := range inputs {
				for _, raw := range codecDiffCorpus {
					got, gotErr := fc.encode([]byte(raw), mode, in)
					want, wantErr := encodeViaLegacyChain(fc, []byte(raw), mode, in)
					if errText(gotErr) != errText(wantErr) {
						t.Fatalf("[%s/%s/%s] %q 错误不一致：单遍=%v 回退=%v", cname, mname, iname, raw, gotErr, wantErr)
					}
					if gotErr != nil {
						continue
					}
					if !bytes.Equal(got, want) {
						t.Fatalf("[%s/%s/%s] 字节不一致\n输入 = %s\n单遍 = %s\n回退 = %s",
							cname, mname, iname, raw, got, want)
					}
				}
			}
		}
	}
}

// TestEncodeByteIdenticalRandomFrames：随机帧（键集合 / 值类型 / 空白 / 转义键 /
// 重复键 / 环境上下文随机组合）上的同一比对——固定种子，可复现。
func TestEncodeByteIdenticalRandomFrames(t *testing.T) {
	now := time.Date(2026, time.November, 9, 3, 30, 0, 0, time.UTC)
	keys := append([]string{}, codexWsPayloadFields...)
	keys = append(keys, "evil", "max_output_tokens", "foo", "a")
	values := []string{
		`"x"`, `42`, `true`, `false`, `null`, `[]`, `{}`, `[1,2]`, `{"k":"v"}`,
		`"a}b,c"`,
		`"<environment_context><current_date>2020-01-01</current_date><timezone>UTC</timezone></environment_context>"`,
	}
	// 紧凑占 2/3：单成员"全程紧凑"概率从 12.5% 提到 ~30%，随机比对才真压到快路径
	// （否则多数帧在测回退链自己，护栏分辨率不够）。
	spaces := []string{"", "", "", "", "", "", " ", "\t", "\n"}
	rng := rand.New(rand.NewSource(20261109))
	for _, fc := range codecDiffCodecs() {
		for iter := 0; iter < 2000; iter++ {
			var b strings.Builder
			b.WriteString("{")
			used := map[string]bool{}
			for k, nk := 0, rng.Intn(6); k < nk; k++ {
				key := keys[rng.Intn(len(keys))]
				if used[key] && rng.Intn(4) != 0 {
					continue
				}
				used[key] = true
				if b.Len() > 1 {
					b.WriteString(",")
				}
				name := key
				if rng.Intn(20) == 0 {
					name = `mo\u0064el` // 转义顶层键：必须回退旧链
				}
				b.WriteString(`"` + name + `"`)
				b.WriteString(spaces[rng.Intn(len(spaces))])
				b.WriteString(":")
				b.WriteString(spaces[rng.Intn(len(spaces))])
				b.WriteString(values[rng.Intn(len(values))])
				b.WriteString(spaces[rng.Intn(len(spaces))])
			}
			b.WriteString("}")
			raw := []byte(b.String())
			for _, mode := range []encodeMode{encodeNativeWS, encodeWSResponses} {
				in := frameInput{Now: now, Identity: Identity{Meta: CodexMeta{InstallationID: "inst"}, TurnState: "ts"}, Turn: 1}
				got, gotErr := fc.encode(raw, mode, in)
				want, wantErr := encodeViaLegacyChain(fc, raw, mode, in)
				if errText(gotErr) != errText(wantErr) {
					t.Fatalf("iter %d %q 错误不一致：单遍=%v 回退=%v", iter, raw, gotErr, wantErr)
				}
				if gotErr != nil {
					continue
				}
				if !bytes.Equal(got, want) {
					t.Fatalf("iter %d 字节不一致\n输入 = %s\n单遍 = %s\n回退 = %s", iter, raw, got, want)
				}
			}
		}
	}
}

// TestEncodeOnePassEngagesOnRuntimeShape：P1 的测量输入（3.7KB 运行期形状）必须
// 真的走单遍路径——否则本文件的比对会退化成"回退链 vs 回退链"，护栏空转。
func TestEncodeOnePassEngagesOnRuntimeShape(t *testing.T) {
	fc := newFrameCodec(&options{traceAuto: true, turnAuto: true})
	frame := RewriteEnvironmentContextTime(wsBenchPayload, time.Now())
	var entryBuf [8]metadataEntry
	in := frameInput{Identity: Identity{Meta: CodexMeta{InstallationID: "inst"}}}
	obj := clientMetadataObject(fc.metaEntries(entryBuf[:0], in))
	out, ok := encodeOnePass(frame, encodeWSResponses, obj)
	if !ok {
		t.Fatal("运行期 3.7KB 请求体应走单遍路径（回退会破坏 P1 门槛）")
	}
	if typ := gjson.GetBytes(out, "type").String(); typ != responsesCreateType {
		t.Fatalf("单遍应补 type=%q，实际 %q", responsesCreateType, typ)
	}
	if !gjson.GetBytes(out, "stream").Bool() {
		t.Fatal("单遍应强制 stream:true")
	}
	if gjson.GetBytes(out, "store").Bool() {
		t.Fatal("单遍应强制 store:false")
	}
}

// TestEncodeOnePassFallsBackOnUncoveredShapes：单遍只保证"紧凑 / 无转义键 /
// 无重复键 / 顶层键全在白名单内"的对象；其余形态必须回退（放宽即字节走样），
// 且回退后仍由既有语义给出结果（含错误）。
func TestEncodeOnePassFallsBackOnUncoveredShapes(t *testing.T) {
	cases := []struct {
		name string
		raw  string
	}{
		{"白名单外顶层键（旧链按白名单序重建）", `{"model":"m","evil":1}`},
		{"转义顶层键（gjson unescape 语义）", `{"mo\u0064el":1}`},
		{"重复顶层键（sjson 首现语义）", `{"model":1,"model":2}`},
		{"键与冒号间空白", `{"model" :"m"}`},
		{"值后有空白", `{"model":"m" }`},
		{"前导空白", ` {"model":"m"}`},
		{"非法 JSON", `{"model":`},
		{"非对象根", `[1]`},
	}
	for _, c := range cases {
		if _, ok := encodeOnePass([]byte(c.raw), encodeWSResponses, nil); ok {
			t.Fatalf("%s：应回退旧链，实际走了单遍（%s）", c.name, c.raw)
		}
	}
	// 反向：白名单内的紧凑对象必须走单遍（含空对象——type/stream/store 由尾部补齐）
	for _, raw := range []string{`{}`, `{"model":"m"}`, `{"model":"m","store":false}`} {
		if _, ok := encodeOnePass([]byte(raw), encodeWSResponses, nil); !ok {
			t.Fatalf("紧凑白名单对象应走单遍：%s", raw)
		}
	}
}

// TestEncodeZeroCopyPreservedAcrossModes：帧已归一且无注入项时 encode 返回原
// 切片（Send 的零拷贝快速路径不因单遍改造而丢）。
func TestEncodeZeroCopyPreservedAcrossModes(t *testing.T) {
	fc := &frameCodec{}
	in := []byte(`{"type":"response.create","model":"gpt-5","store":false}`)
	got, err := fc.encode(in, encodeNativeWS, frameInput{Now: time.Now()})
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	if len(got) == 0 || &got[0] != &in[0] {
		t.Fatal("已归一且无注入时 encode 应零拷贝原样返回")
	}
}

// TestEncodeEmptyIdentityDropsPayloadMetadata：身份只来自 Identity。Identity 为空
// （且没有自动字段）时，payload 自带的 client_metadata 必须被**删除**——否则"匿名"
// 会把调用方的旧 session/thread/ts 透传给上游（oracle 评审 blocking）。
//
// 两条编码路径都要覆盖：单遍（白名单紧凑对象）与回退链（sjson）。
func TestEncodeEmptyIdentityDropsPayloadMetadata(t *testing.T) {
	fc := newFrameCodec(&options{}) // turn/trace 自动关：本次身份对象为空
	now := time.Date(2026, time.November, 9, 3, 30, 0, 0, time.UTC)
	for _, c := range []struct {
		name  string
		frame string
	}{
		{"单遍", `{"type":"response.create","model":"m","store":false,"client_metadata":{"thread_id":"old","x-codex-turn-state":"old-ts"}}`},
		{"回退", `{ "type":"response.create", "model":"m", "store":false, "client_metadata":{"thread_id":"old"} }`},
	} {
		t.Run(c.name, func(t *testing.T) {
			got, err := fc.encode([]byte(c.frame), encodeNativeWS, frameInput{Now: now})
			if err != nil {
				t.Fatalf("encode: %v", err)
			}
			if gjson.GetBytes(got, "client_metadata").Exists() {
				t.Fatalf("空身份时不得保留 payload 身份：%s", got)
			}
			if !gjson.GetBytes(got, "model").Exists() {
				t.Fatalf("删除身份不得伤及其它字段：%s", got)
			}
		})
	}

	// 反向：payload 本来没有身份字段 ⇒ 不新增、不复制（零拷贝）。
	in := []byte(`{"type":"response.create","model":"m","store":false}`)
	got, err := fc.encode(in, encodeNativeWS, frameInput{Now: now})
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	if len(got) == 0 || &got[0] != &in[0] {
		t.Fatal("无身份字段的已归一帧应零拷贝返回")
	}
}

// TestEncodeZeroCopyAllocFree：已归一帧必须"原切片返回 + **零分配**"。
//
// 指针相等只看得见"有没有复制"，看不见"白分配一份再丢弃"——后者会让已归一帧
// 每次 Send 产生一份等于帧长的垃圾（doc.go 承诺的"零额外分配"当场作废）。
// 单遍用懒物化（out 为 nil 就不建缓冲），这条断言把它钉死。
func TestEncodeZeroCopyAllocFree(t *testing.T) {
	fc := newFrameCodec(&options{})
	now := time.Date(2026, time.November, 9, 3, 30, 0, 0, time.UTC)
	in := []byte(`{"type":"response.create","model":"m","stream":true,"store":false,"input":[{"role":"user","content":"hello"}]}`)
	for _, mode := range []encodeMode{encodeNativeWS, encodeWSResponses} {
		got, err := fc.encode(in, mode, frameInput{Now: now})
		if err != nil {
			t.Fatalf("mode %d: encode: %v", mode, err)
		}
		if len(got) == 0 || &got[0] != &in[0] {
			t.Fatalf("mode %d：已归一帧应返回原切片", mode)
		}
		if a := testing.AllocsPerRun(200, func() {
			if _, err := fc.encode(in, mode, frameInput{Now: now}); err != nil {
				panic(err)
			}
		}); a != 0 {
			t.Fatalf("mode %d：已归一帧不应分配，实测 %.0f allocs/op", mode, a)
		}
	}
}

func errText(err error) string {
	if err == nil {
		return "<nil>"
	}
	return err.Error()
}
