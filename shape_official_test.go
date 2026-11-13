package codexsdk

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

// 官方 codex-tui 0.169.0 抓包夹具（来源、脱敏与比对替换规则见
// testdata/official-0.169.0/README.md）。

const officialDir = "testdata/official-0.169.0"

func loadOfficial(t testing.TB, name string) []byte {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(officialDir, name))
	if err != nil {
		t.Fatalf("读取夹具 %s: %v", name, err)
	}
	return b
}

func loadOfficialHeaders(t testing.TB, name string) map[string]string {
	t.Helper()
	var h map[string]string
	if err := json.Unmarshal(loadOfficial(t, name), &h); err != nil {
		t.Fatalf("解析 %s: %v", name, err)
	}
	return h
}

// officialNormalize 把抓包形态替换成 SDK 恒定的取值（README「比对替换规则」）：
// sandbox / sandbox_mode 固定为 none / danger-full-access；exec 抓包的
// turn_trigger、history_initialization 换成 tui 取值。作用在 JSON 字节或头值上
// （turn-metadata 在请求体里是转义过的字符串，两种写法都替换）。
func officialNormalize(b []byte) []byte {
	s := string(b)
	for _, q := range []string{`"`, `\"`} {
		r := strings.NewReplacer(
			q+"sandbox"+q+":"+q+"seccomp"+q, q+"sandbox"+q+":"+q+"none"+q,
			q+"sandbox_mode"+q+":"+q+"workspace-write"+q, q+"sandbox_mode"+q+":"+q+"danger-full-access"+q,
			q+"sandbox_mode"+q+":"+q+"read-only"+q, q+"sandbox_mode"+q+":"+q+"danger-full-access"+q,
			q+"turn_trigger"+q+":"+q+"exec"+q, q+"turn_trigger"+q+":"+q+"user"+q,
			q+"history_initialization"+q+":"+q+"cold_resume"+q, q+"history_initialization"+q+":"+q+"new"+q,
		)
		s = r.Replace(s)
	}
	return []byte(s)
}

// officialIdentity 从抓包帧的 client_metadata 与 turn-metadata 还原官方使用的身份。
func officialIdentity(t testing.TB, frame []byte) Identity {
	t.Helper()
	cm := gjson.GetBytes(frame, "client_metadata")
	tm := gjson.Parse(cm.Get(`x-codex-turn-metadata`).String())
	if !tm.IsObject() {
		t.Fatalf("帧没有 turn-metadata: %.200s", frame)
	}
	return Identity{
		InstallationID: cm.Get("x-codex-installation-id").String(),
		Session: Session{
			SessionID: cm.Get("session_id").String(),
			ThreadID:  cm.Get("thread_id").String(),
			WindowID:  cm.Get("x-codex-window-id").String(),
		},
		Window: Window{Number: tm.Get("window_number").Uint(), ContextID: tm.Get("context_window_id").String()},
		Turn: Turn{
			ID:          cm.Get("turn_id").String(),
			StartedAtMs: tm.Get("turn_started_at_unix_ms").Int(),
			State:       cm.Get("x-codex-turn-state").String(),
		},
	}
}

var startMsRe = regexp.MustCompile(`^[0-9]{13}$`)

// assertSameAsOfficial：除 client_metadata 外逐字节相等；client_metadata 按集合
// 比较（键序官方是随机的），发送时刻只校验形态。
func assertSameAsOfficial(t *testing.T, label string, got, want []byte) {
	t.Helper()
	strip := func(b []byte) []byte {
		out, err := sjson.DeleteBytes(b, "client_metadata")
		if err != nil {
			t.Fatalf("%s: 删 client_metadata: %v", label, err)
		}
		return out
	}
	if g, w := strip(got), strip(want); !bytes.Equal(g, w) {
		i := 0
		for i < len(g) && i < len(w) && g[i] == w[i] {
			i++
		}
		lo := max(0, i-120)
		t.Fatalf("%s: 字节不同（偏移 %d）\n实际 …%s\n期望 …%s", label, i, g[lo:min(len(g), i+120)], w[lo:min(len(w), i+120)])
	}
	gm := map[string]string{}
	wm := map[string]string{}
	gjson.GetBytes(got, "client_metadata").ForEach(func(k, v gjson.Result) bool { gm[k.String()] = v.String(); return true })
	gjson.GetBytes(want, "client_metadata").ForEach(func(k, v gjson.Result) bool { wm[k.String()] = v.String(); return true })
	const startKey = "x-codex-ws-stream-request-start-ms"
	if _, ok := wm[startKey]; ok {
		if !startMsRe.MatchString(gm[startKey]) {
			t.Fatalf("%s: %s = %q，期望 13 位毫秒", label, startKey, gm[startKey])
		}
		delete(gm, startKey)
		delete(wm, startKey)
	}
	if len(gm) != len(wm) {
		t.Fatalf("%s: client_metadata 键数 %d，期望 %d\n实际 %v\n期望 %v", label, len(gm), len(wm), gm, wm)
	}
	for k, v := range wm {
		if gm[k] != v {
			t.Fatalf("%s: client_metadata[%s]\n实际 %q\n期望 %q", label, k, gm[k], v)
		}
	}
}

func mustShape(t testing.TB, raw []byte, s surface, id Identity) shapedRequest {
	t.Helper()
	sh, err := shapeBody(raw, s, id, time.Now(), shapeConfig{stats: &shapeStats})
	if err != nil {
		t.Fatalf("shapeBody: %v", err)
	}
	return sh
}

// TestShapeMatchesOfficialCapture（A-1）：抓包请求体作输入、给定同一身份，三种面
// 的组装结果与抓包逐字节相等（client_metadata 按集合比较，发送时刻除外）。
func TestShapeMatchesOfficialCapture(t *testing.T) {
	now := time.Now()
	expect := func(name string) []byte {
		return RewriteEnvironmentContextTime(officialNormalize(loadOfficial(t, name)), now)
	}

	// WS→WS：抓包帧原样作为客户端帧（全量帧、带 previous_response_id 的增量帧、
	// 客户端自己的预热帧）。
	for _, name := range []string{
		"tui-turn.json", "tui-followup.json", "tui-prewarm.json",
		"exec-ws-turn.json", "exec-ws-followup.json", "exec-ws-prewarm.json",
		"gpt55-turn.json", "gpt55-followup.json",
	} {
		t.Run("ws-native/"+name, func(t *testing.T) {
			want := expect(name)
			id := officialIdentity(t, want)
			sh := mustShape(t, loadOfficial(t, name), surfaceWSNative, id)
			a := wsAssembly{prevID: sh.clientPrevID, generate: sh.clientGenerate}
			if !sh.nativePrewarm {
				a.turnState = id.Turn.State
			}
			got, off := assembleWS(&sh, a)
			if off < 0 || !startMsRe.Match(got[off:off+13]) {
				t.Fatalf("发送时刻偏移 %d 不指向 13 位数字", off)
			}
			assertSameAsOfficial(t, name, got, want)
		})
	}

	// HTTP→WS：全量帧作为 HTTP 请求体（type / generate / previous_response_id
	// 被丢弃），组装出真实帧；同一份输入组装预热帧（lite：空输入）。
	for _, c := range []struct{ turn, prewarm string }{
		{"tui-turn.json", "tui-prewarm.json"},
		{"exec-ws-turn.json", "exec-ws-prewarm.json"},
	} {
		t.Run("ws-from-http/"+c.turn, func(t *testing.T) {
			want := expect(c.turn)
			id := officialIdentity(t, want)
			sh := mustShape(t, loadOfficial(t, c.turn), surfaceWSFromHTTP, id)
			got, _ := assembleWS(&sh, wsAssembly{turnState: id.Turn.State})
			assertSameAsOfficial(t, c.turn, got, want)

			pre, _ := assembleWS(&sh, wsAssembly{prewarm: true})
			assertSameAsOfficial(t, c.prewarm, pre, expect(c.prewarm))
		})
	}

	// gpt-5.5（非 lite）：第三方形态的请求（顶层 instructions + 全量输入 + tools）
	// 组装出的预热帧 = 抓包预热帧（输入只有 SDK 生成的指令消息，id 是官方 UUIDv5）。
	t.Run("ws-from-http/gpt55-prewarm", func(t *testing.T) {
		want := expect("gpt55-prewarm.json")
		id := officialIdentity(t, want)
		req, _ := sjson.DeleteBytes(loadOfficial(t, "gpt55-turn.json"), "previous_response_id")
		req, _ = sjson.SetBytes(req, "instructions", string(loadOfficial(t, "gpt55-instructions.txt")))
		sh := mustShape(t, req, surfaceWSFromHTTP, id)
		got, _ := assembleWS(&sh, wsAssembly{prewarm: true})
		assertSameAsOfficial(t, "gpt55-prewarm", got, want)
		turnWant := expect("gpt55-turn.json")
		// 预热后的真实帧：跳过指令前缀、带预热 response id = 抓包的 turn 帧（身份取
		// turn 帧的，预热帧里 turn_id 为空）。
		shTurn := mustShape(t, req, surfaceWSFromHTTP, officialIdentity(t, turnWant))
		turnFrame, _ := assembleWS(&shTurn, wsAssembly{
			prevID: gjson.GetBytes(turnWant, "previous_response_id").String(), inputSkip: 1,
			turnState: officialIdentity(t, turnWant).Turn.State,
		})
		assertSameAsOfficial(t, "gpt55-turn", turnFrame, turnWant)
	})

	// HTTP→HTTP（exec 抓包，替换规则见 README）。
	for _, name := range []string{"exec-http-turn.json", "exec-http-followup.json"} {
		t.Run("http/"+name, func(t *testing.T) {
			want := expect(name)
			id := officialIdentity(t, want)
			sh := mustShape(t, loadOfficial(t, name), surfaceHTTP, id)
			assertSameAsOfficial(t, name, assembleHTTP(&sh), want)
		})
	}

	// HTTP→HTTP 原始 zstd 体（未脱敏的线上字节）。
	for _, name := range []string{"http-body-turn.zst", "http-body-followup.zst"} {
		t.Run("http-raw/"+name, func(t *testing.T) {
			raw, err := decodeZstd(loadOfficial(t, name))
			if err != nil {
				t.Fatalf("解压 %s: %v", name, err)
			}
			want := RewriteEnvironmentContextTime(officialNormalize(raw), now)
			sh := mustShape(t, raw, surfaceHTTP, officialIdentity(t, want))
			assertSameAsOfficial(t, name, assembleHTTP(&sh), want)
		})
	}
}

// TestInstructionsIDMatchesOfficialV5（A-4）：非 lite 指令消息 id 用官方算法
// UUIDv5(UUIDv5(NAMESPACE_OID, thread_id), 指令原文)，与 gpt-5.5 抓包逐字节一致。
func TestInstructionsIDMatchesOfficialV5(t *testing.T) {
	text := loadOfficial(t, "gpt55-instructions.txt")
	const thread = "01a1d4f4-55cd-7901-b518-4f7e16785633"
	if got := "msg_" + instructionsIDV5(thread, text); got != "msg_15cdc362-ddfe-52ec-820f-773da376cd06" {
		t.Fatalf("指令消息 id = %s，期望抓包 msg_15cdc362-ddfe-52ec-820f-773da376cd06", got)
	}
	pre := loadOfficial(t, "gpt55-prewarm.json")
	if got := gjson.GetBytes(pre, "input.0.id").String(); got != "msg_15cdc362-ddfe-52ec-820f-773da376cd06" {
		t.Fatalf("夹具 id = %s", got)
	}
	// 经整形产出的消息同样是这个 id（身份来自抓包）。
	id := officialIdentity(t, pre)
	req := []byte(`{"model":"gpt-5.5","input":[]}`)
	req, _ = sjson.SetBytes(req, "instructions", string(text))
	sh := mustShape(t, req, surfaceHTTP, id)
	if len(sh.inputPrefix) != 1 || gjson.GetBytes(sh.inputPrefix[0], "id").String() != "msg_15cdc362-ddfe-52ec-820f-773da376cd06" {
		t.Fatalf("整形插入的指令消息 id 不符: %.120s", sh.inputPrefix)
	}
}

// TestTurnMetadataMatchesOfficial（A-5）：turn 与 prewarm 两种形态逐字节等于 tui
// 抓包（sandbox / sandbox_mode 替换为固定值）；ASCII 编码器的官方测试向量。
func TestTurnMetadataMatchesOfficial(t *testing.T) {
	turn := loadOfficial(t, "tui-turn.json")
	pre := loadOfficial(t, "tui-prewarm.json")
	wantTurn := officialNormalize([]byte(gjson.GetBytes(turn, "client_metadata.x-codex-turn-metadata").String()))
	wantPre := officialNormalize([]byte(gjson.GetBytes(pre, "client_metadata.x-codex-turn-metadata").String()))
	sh := mustShape(t, turn, surfaceWSNative, officialIdentity(t, turn))
	if !bytes.Equal(sh.turnMeta, wantTurn) {
		t.Fatalf("turn 形态\n实际 %s\n期望 %s", sh.turnMeta, wantTurn)
	}
	if !bytes.Equal(sh.prewarmMeta, wantPre) {
		t.Fatalf("prewarm 形态\n实际 %s\n期望 %s", sh.prewarmMeta, wantPre)
	}
	// 握手头里的 turn-metadata 与预热帧 client_metadata 里的是同一份字节。
	hs := loadOfficialHeaders(t, "tui-handshake.headers.json")
	if got := string(officialNormalize([]byte(hs["x-codex-turn-metadata"]))); got != string(wantPre) {
		t.Fatalf("握手头 turn-metadata 与预热帧不同:\n%s\n%s", got, wantPre)
	}

	// to_ascii_json_string 官方测试向量（非 ASCII → 小写 \uxxxx；BMP 外 → 代理对）。
	for in, want := range map[string]string{
		"/tmp/東京":       `"/tmp/\u6771\u4eac"`,
		"😀":             `"\ud83d\ude00"`,
		"🚀":             `"\ud83d\ude80"`,
		"Agentlarım":    `"Agentlar\u0131m"`,
		"a\"b\\c\n\x01": `"a\"b\\c\n\u0001"`,
		"<>&\u2028":     `"<>&\u2028"`,
		"\x7f":          "\"\x7f\"",
	} {
		if got := string(appendASCIIJSONString(nil, in)); got != want {
			t.Fatalf("appendASCIIJSONString(%q) = %s，期望 %s", in, got, want)
		}
	}
	// 非 ASCII 的模型名在 turn-metadata 里被转义，整串仍是合法 JSON 且纯 ASCII。
	sh = mustShape(t, []byte(`{"model":"模型😀"}`), surfaceHTTP, slotLikeIdentity())
	for _, c := range sh.turnMeta {
		if c >= 0x80 {
			t.Fatalf("turn-metadata 含非 ASCII 字节: %s", sh.turnMeta)
		}
	}
	if got := gjson.GetBytes(sh.turnMeta, "model").String(); got != "模型😀" {
		t.Fatalf("turn-metadata model 解码 = %q", got)
	}
}

// slotLikeIdentity 是一个完整的非匿名测试身份。
func slotLikeIdentity() Identity {
	return Identity{
		InstallationID: "inst-1",
		Session:        Session{SessionID: "01a1d4d7-390e-7192-b769-109c0a9eb6ba", ThreadID: "01a1d4d7-390e-7192-b769-109c0a9eb6ba", WindowID: "01a1d4d7-390e-7192-b769-109c0a9eb6ba:0"},
		Window:         Window{Number: 0, ContextID: "01a1d4d7-3918-7bb3-93ec-11a42067a74d"},
	}
}
