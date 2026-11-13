package codexsdk

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	coderws "github.com/coder/websocket"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

// 本文件是 HTTP→WS 预热（新建上游连接且一轮的第一个请求：先发 generate:false 的
// 预热帧、读完其响应再发真实帧）的验收用例。

// prewarmReply 是假上游对**一帧**的回应。
type prewarmReply struct {
	events []string
	// closeAfter 为真时写完 events 就断 TCP（不发 close frame）：客户端读到 EOF。
	closeAfter bool
	// wait 非 nil 时写完 events 后阻塞到它关闭（把"预热已回完、真实帧还没到"变成
	// 确定性屏障）。
	wait <-chan struct{}
	// gate 非 nil 时先阻塞到它关闭再写 events。
	gate <-chan struct{}
}

// prewarmUpstream 是按帧脚本化的假 Responses WS 上游：
//
//   - 每次升级记录握手头；每读到一帧记录并交给 respond(连接序号, 全局帧序号, 帧)；
//   - 依次写出 reply.events，然后按 closeAfter / wait 收尾；
//   - respond 为 nil 时按"预热回 created/metadata/completed、真实帧回 created/completed"
//     的默认脚本回应（预热响应 id 与 ts 由 respID/ts 决定）。
type prewarmUpstream struct {
	srv     *httptest.Server
	mu      sync.Mutex
	respond func(conn, idx int, frame []byte) prewarmReply
	frames  [][]byte
	headers []http.Header
	// respID/ts 是默认脚本的预热响应 id 与它签发的 turn-state。
	respID string
	ts     string

	upgrades atomic.Int64
	frameSig chan struct{}
	// live 是仍在运行的 handler 数；exited[i] 在第 i 条连接的 handler 返回时关闭
	// （客户端终止连接 ⇒ handler 的 Read 出错返回：断言"连接已关"的屏障）。
	live   atomic.Int64
	exited []chan struct{}
}

func newPrewarmUpstream(t *testing.T) *prewarmUpstream {
	t.Helper()
	u := &prewarmUpstream{frameSig: make(chan struct{}, 1), respID: "resp_pw", ts: "T-prewarm"}
	u.srv = httptest.NewServer(http.HandlerFunc(u.handle))
	t.Cleanup(func() {
		u.srv.CloseClientConnections()
		u.srv.Close()
	})
	return u
}

func (u *prewarmUpstream) script(respond func(conn, idx int, frame []byte) prewarmReply) {
	u.mu.Lock()
	u.respond = respond
	u.mu.Unlock()
}

func (u *prewarmUpstream) defaultReply(conn int, frame []byte) prewarmReply {
	u.mu.Lock()
	respID, ts := u.respID, u.ts
	u.mu.Unlock()
	if gjson.GetBytes(frame, "generate").Type == gjson.False {
		return prewarmReply{events: []string{
			tsCreatedFrame(respID),
			tsMetadataFrame(respID, ts),
			tsCompletedFrame(respID),
		}}
	}
	id := "resp_real_" + strconv.Itoa(conn)
	return prewarmReply{events: []string{tsCreatedFrame(id), tsCompletedFrame(id)}}
}

// exitedCh 返回第 conn 条连接的 handler 退出屏障（惰性创建）。
func (u *prewarmUpstream) exitedCh(conn int) chan struct{} {
	u.mu.Lock()
	defer u.mu.Unlock()
	for len(u.exited) <= conn {
		u.exited = append(u.exited, make(chan struct{}))
	}
	return u.exited[conn]
}

// waitConnClosed 等第 conn 条连接的 handler 退出（屏障 + deadline 兜底）。
func (u *prewarmUpstream) waitConnClosed(t *testing.T, conn int, why string) {
	t.Helper()
	select {
	case <-u.exitedCh(conn):
	case <-time.After(3 * time.Second):
		t.Fatalf("第 %d 条上游连接未关闭（%s）", conn, why)
	}
}

func (u *prewarmUpstream) handle(w http.ResponseWriter, r *http.Request) {
	conn := int(u.upgrades.Add(1)) - 1
	u.live.Add(1)
	done := u.exitedCh(conn)
	defer func() {
		u.live.Add(-1)
		close(done)
	}()
	c, err := coderws.Accept(w, r, nil)
	if err != nil {
		return
	}
	defer c.CloseNow()
	c.SetReadLimit(64 << 20)
	u.mu.Lock()
	u.headers = append(u.headers, r.Header.Clone())
	u.mu.Unlock()
	for idx := 0; ; idx++ {
		_, data, rerr := c.Read(r.Context())
		if rerr != nil {
			return
		}
		u.mu.Lock()
		u.frames = append(u.frames, append([]byte(nil), data...))
		respond := u.respond
		global := len(u.frames) - 1
		u.mu.Unlock()
		select {
		case u.frameSig <- struct{}{}:
		default:
		}
		reply := u.defaultReply(conn, data)
		if respond != nil {
			reply = respond(conn, global, data)
		}
		if reply.gate != nil {
			select {
			case <-reply.gate:
			case <-r.Context().Done():
				return
			}
		}
		for _, f := range reply.events {
			if werr := c.Write(r.Context(), coderws.MessageText, []byte(f)); werr != nil {
				return
			}
		}
		if reply.closeAfter {
			return
		}
		if reply.wait != nil {
			select {
			case <-reply.wait:
			case <-r.Context().Done():
				return
			}
		}
	}
}

func (u *prewarmUpstream) client(t *testing.T, opts ...Option) *HTTPClient {
	t.Helper()
	base := []Option{
		WithResponsesTransport(TransportWS),
		WithTransport(newFixedTransport(t, "https://chatgpt.com/backend-api/codex/responses", u.srv.URL)),
		WithTraceAuto(false),
	}
	return NewHTTPClient(PAT("pat-pw"), append(base, opts...)...)
}

func (u *prewarmUpstream) sentFrames(t *testing.T) [][]byte {
	t.Helper()
	u.mu.Lock()
	defer u.mu.Unlock()
	out := make([][]byte, len(u.frames))
	copy(out, u.frames)
	return out
}

func (u *prewarmUpstream) handshakeHeaders(t *testing.T, i int) http.Header {
	t.Helper()
	u.mu.Lock()
	defer u.mu.Unlock()
	if i >= len(u.headers) {
		t.Fatalf("第 %d 次握手不存在（共 %d 次）", i, len(u.headers))
	}
	return u.headers[i].Clone()
}

// waitFrames 等到上游记录到第 n 帧（通道通知 + deadline 兜底，不轮询）。
func (u *prewarmUpstream) waitFrames(t *testing.T, n int) {
	t.Helper()
	deadline := time.After(5 * time.Second)
	for {
		u.mu.Lock()
		got := len(u.frames)
		u.mu.Unlock()
		if got >= n {
			return
		}
		select {
		case <-u.frameSig:
		case <-deadline:
			t.Fatalf("等待第 %d 帧超时（已记录 %d）", n, got)
		}
	}
}

// prewarmIdentity 是一轮第一个请求的身份（Turn.First=true ⇒ 新建连接时预热）。
func prewarmIdentity(thread string, sink TurnStateSink) Identity {
	return Identity{
		InstallationID: "inst-pw",
		Session:        Session{SessionID: "sess-" + thread, ThreadID: thread, WindowID: thread + ":0"},
		Window:         Window{Number: 0, ContextID: "01a1d4d7-3918-7bb3-93ec-11a42067a74d"},
		Turn:           Turn{First: true},
		Sink:           sink,
	}
}

// followUpIdentity 是同一轮的后续请求身份（不预热）。
func followUpIdentity(thread, ts string, sink TurnStateSink) Identity {
	id := prewarmIdentity(thread, sink)
	id.Turn.First = false
	id.Turn.State = ts
	return id
}

// prewarmExpect 读抓包夹具并做比对前的替换（README：sandbox/sandbox_mode 固定；
// 标题线程的 thread_source 在 SDK 恒为 user——网关不产生标题线程请求）。
func prewarmExpect(t *testing.T, name string) []byte {
	t.Helper()
	b := officialNormalize(loadOfficial(t, name))
	b = bytes.ReplaceAll(b, []byte(`"thread_source":"thread_title"`), []byte(`"thread_source":"user"`))
	b = bytes.ReplaceAll(b, []byte(`\"thread_source\":\"thread_title\"`), []byte(`\"thread_source\":\"user\"`))
	return RewriteEnvironmentContextTime(b, time.Now())
}

// officialFirstIdentity 是抓包 turn 帧的身份，作为一轮第一个请求（First=true）；
// Turn.State 清空：真实帧的 ts 必须来自预热响应。
func officialFirstIdentity(t *testing.T, turnFixture string, sink TurnStateSink) Identity {
	t.Helper()
	id := officialIdentity(t, prewarmExpect(t, turnFixture))
	id.Turn.State = ""
	id.Turn.First = true
	id.Sink = sink
	return id
}

// sinkFunc 把函数适配成 TurnStateSink（用来在预热读循环内插入屏障）。
type sinkFunc func(string)

func (f sinkFunc) SetTurnState(v string) { f(v) }

func isPrewarmFrame(frame []byte) bool {
	return gjson.GetBytes(frame, "generate").Type == gjson.False
}

// TestPrewarmLite：lite 模型、新建连接、一轮首个请求。
// 预热帧 = tui 抓包的预热帧（空输入、无 previous_response_id、generate:false）；
// 真实帧 = 抓包的 turn 帧（预热响应 id + 全量输入 + 预热签发的 ts）。
func TestPrewarmLite(t *testing.T) {
	up := newPrewarmUpstream(t)
	up.script(func(_, _ int, frame []byte) prewarmReply {
		if isPrewarmFrame(frame) {
			return prewarmReply{events: []string{
				tsCreatedFrame("resp_mock_1"),
				tsMetadataFrame("resp_mock_1", "ts-event-1"),
				tsCompletedFrame("resp_mock_1"),
			}}
		}
		return prewarmReply{events: []string{
			tsCreatedFrame("resp_real"),
			tsCompletedFrame("resp_real"),
		}}
	})
	hc := up.client(t, WithWSConnCache(2))

	before := ShapeStatsSnapshot()
	body := loadOfficial(t, "tui-chained-turn.json") // 走 HTTP 面：type / prev 由 SDK 丢弃
	id := officialFirstIdentity(t, "tui-chained-turn.json", nil)
	resp, got := doStreamBodyPayload(t, hc, id, body)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("状态码 = %d", resp.StatusCode)
	}
	if !strings.Contains(got, "response.completed") {
		t.Fatalf("真实帧的完成事件未交付: %q", got)
	}

	if n := up.upgrades.Load(); n != 1 {
		t.Fatalf("upgrades = %d, 期望 1（预热与真实帧同一条连接）", n)
	}
	frames := up.sentFrames(t)
	if len(frames) != 2 {
		t.Fatalf("上游收到 %d 帧, 期望 2（预热 + 真实）", len(frames))
	}
	assertSameAsOfficial(t, "预热帧", frames[0], prewarmExpect(t, "tui-chained-prewarm.json"))
	if gjson.GetBytes(frames[0], "input").Get("#").Int() != 0 {
		t.Fatalf("lite 预热输入必须为空: %.160s", frames[0])
	}
	assertSameAsOfficial(t, "真实帧", frames[1], prewarmExpect(t, "tui-chained-turn.json"))
	if got := gjson.GetBytes(frames[1], "previous_response_id").String(); got != "resp_mock_1" {
		t.Fatalf("真实帧 previous_response_id = %q, 期望预热响应 id", got)
	}
	if got := frameTurnState(t, frames[1]); got != "ts-event-1" {
		t.Fatalf("真实帧 ts = %q, 期望预热签发的 ts-event-1", got)
	}
	after := ShapeStatsSnapshot()
	if after.Prewarms-before.Prewarms != 1 || after.PrewarmChained-before.PrewarmChained != 1 {
		t.Fatalf("计数增量 Prewarms=%d PrewarmChained=%d, 期望 1/1",
			after.Prewarms-before.Prewarms, after.PrewarmChained-before.PrewarmChained)
	}
	// 握手头是预热帧的形态（prewarm turn-metadata），不是真实帧的。
	hs := up.handshakeHeaders(t, 0)
	wantHandshake := loadOfficialHeaders(t, "tui-chained-handshake.headers.json")
	if got := string(officialNormalize([]byte(hs.Get(HeaderTurnMetadata)))); got != string(officialNormalize([]byte(wantHandshake[HeaderTurnMetadata]))) {
		t.Fatalf("握手 turn-metadata 不是 prewarm 形态:\n实际 %s\n期望 %s", got, wantHandshake[HeaderTurnMetadata])
	}
}

// TestPrewarmNonLite：gpt-5.5（非 lite）。
// 预热帧带顶层 tools 与指令消息（输入 1 项）；真实帧带预热 id、输入去掉该前缀。
func TestPrewarmNonLite(t *testing.T) {
	up := newPrewarmUpstream(t)
	up.script(func(_, _ int, frame []byte) prewarmReply {
		if isPrewarmFrame(frame) {
			return prewarmReply{events: []string{
				tsCreatedFrame("resp_mock_1"),
				tsMetadataFrame("resp_mock_1", "ts-event-1"),
				tsCompletedFrame("resp_mock_1"),
			}}
		}
		return prewarmReply{events: []string{tsCreatedFrame("resp_real"), tsCompletedFrame("resp_real")}}
	})
	hc := up.client(t, WithWSConnCache(2))

	body, err := sjson.DeleteBytes(loadOfficial(t, "gpt55-turn.json"), "previous_response_id")
	if err != nil {
		t.Fatal(err)
	}
	body, err = sjson.SetBytes(body, "instructions", string(loadOfficial(t, "gpt55-instructions.txt")))
	if err != nil {
		t.Fatal(err)
	}
	doStreamBodyPayload(t, hc, officialFirstIdentity(t, "gpt55-turn.json", nil), body)

	frames := up.sentFrames(t)
	if len(frames) != 2 {
		t.Fatalf("上游收到 %d 帧, 期望 2", len(frames))
	}
	assertSameAsOfficial(t, "预热帧", frames[0], prewarmExpect(t, "gpt55-prewarm.json"))
	if got := gjson.GetBytes(frames[0], "input.#").Int(); got != 1 {
		t.Fatalf("非 lite 预热输入项数 = %d, 期望 1（指令消息）", got)
	}
	if !gjson.GetBytes(frames[0], "tools").IsArray() {
		t.Fatalf("非 lite 预热帧必须带顶层 tools: %.160s", frames[0])
	}
	assertSameAsOfficial(t, "真实帧", frames[1], prewarmExpect(t, "gpt55-turn.json"))
	if got := gjson.GetBytes(frames[1], "input.#").Int(); got != 3 {
		t.Fatalf("真实帧输入项数 = %d, 期望 3（去掉指令前缀）", got)
	}
}

// TestPrewarmMismatchSendsFull：真实帧的 text 带 format（标题线程）：预热帧的 text
// 去掉了 format，与真实帧不相等 ⇒ 真实帧不带预热 id、发全量。
func TestPrewarmMismatchSendsFull(t *testing.T) {
	up := newPrewarmUpstream(t)
	up.ts = "ts-event-4" // 抓包里预热签发的 ts
	before := ShapeStatsSnapshot()
	hc := up.client(t, WithWSConnCache(2))

	body, err := sjson.DeleteBytes(loadOfficial(t, "title-turn.json"), "previous_response_id")
	if err != nil {
		t.Fatal(err)
	}
	doStreamBodyPayload(t, hc, officialFirstIdentity(t, "title-turn.json", nil), body)

	frames := up.sentFrames(t)
	if len(frames) != 2 {
		t.Fatalf("上游收到 %d 帧, 期望 2", len(frames))
	}
	// 预热帧与抓包的唯一差别是 effort（reasoning.effort 与 turn-metadata 的
	// reasoning_effort）：官方标题线程的预热用模型默认 effort（medium），SDK 的
	// 预热帧与真实帧同出一份整形（low；网关不产生标题线程请求）。把抓包的这两处
	// 换成真实帧的值后逐字节比。
	effort := gjson.GetBytes(frames[1], "reasoning.effort").String()
	wantPrewarm, err := sjson.SetBytes(prewarmExpect(t, "title-prewarm.json"), "reasoning.effort", effort)
	if err != nil {
		t.Fatal(err)
	}
	official := gjson.GetBytes(loadOfficial(t, "title-prewarm.json"), "reasoning.effort").String()
	wantPrewarm = bytes.Replace(wantPrewarm,
		[]byte(`\"reasoning_effort\":\"`+official+`\"`), []byte(`\"reasoning_effort\":\"`+effort+`\"`), 1)
	assertSameAsOfficial(t, "预热帧", frames[0], wantPrewarm)
	if gjson.GetBytes(frames[0], "text.format").Exists() {
		t.Fatalf("预热帧的 text 不得带 format: %.160s", frames[0])
	}
	if gjson.GetBytes(frames[1], "previous_response_id").Exists() {
		t.Fatalf("text 不相等时不得带 previous_response_id: %.160s", frames[1])
	}
	if got := gjson.GetBytes(frames[1], "input.#").Int(); got != 5 {
		t.Fatalf("真实帧输入项数 = %d, 期望 5（全量）", got)
	}
	// 除 client_metadata 外与抓包逐字节相等（标题线程的 turn-metadata 没有
	// turn_trigger，SDK 恒按用户线程写，不比）。
	strip := func(b []byte) []byte {
		out, err := sjson.DeleteBytes(b, "client_metadata")
		if err != nil {
			t.Fatal(err)
		}
		return out
	}
	if g, w := strip(frames[1]), strip(prewarmExpect(t, "title-turn.json")); !bytes.Equal(g, w) {
		t.Fatalf("真实帧与抓包不同:\n实际 %.300s\n期望 %.300s", g, w)
	}
	if got := frameTurnState(t, frames[1]); got != "ts-event-4" {
		t.Fatalf("真实帧 ts = %q, 期望预热签发的 ts-event-4", got)
	}
	if got := ShapeStatsSnapshot().PrewarmUnchained - before.PrewarmUnchained; got != 1 {
		t.Fatalf("PrewarmUnchained 增量 = %d, 期望 1", got)
	}
}

// TestNoPrewarmOnReuseOrFollowup：复用连接、一轮的后续请求、槽位路径 First=false
// 都不预热，发全量、不带 previous_response_id。
func TestNoPrewarmOnReuseOrFollowup(t *testing.T) {
	payload := []byte(`{"model":"gpt-6.1-sol","input":[{"type":"message","role":"user","content":[{"type":"input_text","text":"hi"}]}]}`)

	t.Run("复用连接", func(t *testing.T) {
		up := newPrewarmUpstream(t)
		hc := up.client(t, WithWSConnCache(2))
		id := prewarmIdentity("thread-pw-reuse", nil)
		doStreamBodyPayload(t, hc, id, payload) // 新建 ⇒ 预热 + 真实
		doStreamBodyPayload(t, hc, followUpIdentity("thread-pw-reuse", "T-first", nil), payload)

		if n := up.upgrades.Load(); n != 1 {
			t.Fatalf("upgrades = %d, 期望 1（后续请求复用连接）", n)
		}
		frames := up.sentFrames(t)
		if len(frames) != 3 {
			t.Fatalf("上游收到 %d 帧, 期望 3（预热 + 真实 + 复用的真实帧）", len(frames))
		}
		if !isPrewarmFrame(frames[0]) {
			t.Fatalf("首个请求的前置帧必须是预热帧: %.120s", frames[0])
		}
		if isPrewarmFrame(frames[2]) {
			t.Fatalf("复用连接不得预热: %.120s", frames[2])
		}
		if gjson.GetBytes(frames[2], "previous_response_id").Exists() {
			t.Fatalf("复用连接不带 previous_response_id: %.160s", frames[2])
		}
		if got := gjson.GetBytes(frames[2], "input.#").Int(); got != 1 {
			t.Fatalf("复用连接的输入 = %d 项, 期望全量 1 项", got)
		}
	})

	t.Run("一轮的后续请求", func(t *testing.T) {
		up := newPrewarmUpstream(t)
		hc := up.client(t)
		doStreamBodyPayload(t, hc, followUpIdentity("thread-pw-follow", "T-first", nil), payload)
		frames := up.sentFrames(t)
		if len(frames) != 1 {
			t.Fatalf("上游收到 %d 帧, 期望 1（后续请求不预热）", len(frames))
		}
		if isPrewarmFrame(frames[0]) {
			t.Fatalf("后续请求不得预热: %.120s", frames[0])
		}
		if gjson.GetBytes(frames[0], "previous_response_id").Exists() {
			t.Fatalf("后续请求不带 previous_response_id: %.160s", frames[0])
		}
	})

	t.Run("槽位路径首请求之外的请求", func(t *testing.T) {
		up := newPrewarmUpstream(t)
		hc := up.client(t, WithWSConnCache(2))
		state := NewIdentityState("inst-slot", RotatePolicy{})
		id := state.Identity(nil, false) // 非首请求
		doStreamBodyPayload(t, hc, id, payload)
		frames := up.sentFrames(t)
		if len(frames) != 1 {
			t.Fatalf("上游收到 %d 帧, 期望 1（First=false 不预热）", len(frames))
		}
		if gjson.GetBytes(frames[0], "previous_response_id").Exists() {
			t.Fatalf("不带 previous_response_id: %.160s", frames[0])
		}
		// 槽位身份 First=true 但连接从缓存复用：不预热。
		doStreamBodyPayload(t, hc, state.Identity(nil, true), payload)
		if n := up.upgrades.Load(); n != 1 {
			t.Fatalf("upgrades = %d, 期望 1", n)
		}
		if frames := up.sentFrames(t); len(frames) != 2 || isPrewarmFrame(frames[1]) {
			t.Fatalf("复用连接不得预热: %d 帧", len(frames))
		}
		// 新建连接（不开缓存）+ First=true：预热。
		doStreamBodyPayload(t, up.client(t), state.Identity(nil, true), payload)
		if frames := up.sentFrames(t); len(frames) != 4 || !isPrewarmFrame(frames[2]) {
			t.Fatalf("新建连接且 First=true 时必须预热: %d 帧", len(frames))
		}
	})

	t.Run("匿名身份", func(t *testing.T) {
		before := ShapeStatsSnapshot()
		up := newPrewarmUpstream(t)
		hc := up.client(t)
		doStreamBodyPayload(t, hc, Identity{Turn: Turn{First: true}}, payload) // 没有会话 ⇒ 匿名
		frames := up.sentFrames(t)
		if len(frames) != 1 || isPrewarmFrame(frames[0]) {
			t.Fatalf("匿名身份不得预热: %d 帧", len(frames))
		}
		if got := ShapeStatsSnapshot().Prewarms - before.Prewarms; got != 0 {
			t.Fatalf("Prewarms 增量 = %d, 期望 0", got)
		}
	})

	t.Run("缓存命中但连接已死", func(t *testing.T) {
		// 命中的连接在取用复查时已不可用 ⇒ 重拨，算新建连接 ⇒ First=true 时预热。
		up := newPrewarmUpstream(t)
		hc := up.client(t, WithWSConnCache(2))
		doStreamBodyPayload(t, hc, followUpIdentity("thread-pw-dead", "", nil), payload) // 不预热、归还
		hc.wsCache.mu.Lock()
		if len(hc.wsCache.idles) != 1 {
			hc.wsCache.mu.Unlock()
			t.Fatalf("idle = %d, 期望 1", len(hc.wsCache.idles))
		}
		cn := hc.wsCache.idles[0].cn
		hc.wsCache.mu.Unlock()
		cn.terminate(io.EOF) // 连接在表里死掉（不摘表：留给取用复查发现）
		<-cn.closeDone
		hits, misses := hc.wsCache.hits.Load(), hc.wsCache.misses.Load()

		doStreamBodyPayload(t, hc, prewarmIdentity("thread-pw-dead", nil), payload)
		if d := hc.wsCache.hits.Load() - hits; d != 0 {
			t.Fatalf("hits 增量 = %d, 期望 0（死连接不算命中）", d)
		}
		if d := hc.wsCache.misses.Load() - misses; d != 1 {
			t.Fatalf("misses 增量 = %d, 期望 1（取用复查失败）", d)
		}
		if n := up.upgrades.Load(); n != 2 {
			t.Fatalf("upgrades = %d, 期望 2（重拨）", n)
		}
		frames := up.sentFrames(t)
		if len(frames) != 3 || !isPrewarmFrame(frames[1]) || isPrewarmFrame(frames[2]) {
			t.Fatalf("死连接命中后的新连接必须先预热: %d 帧", len(frames))
		}
		if got := gjson.GetBytes(frames[2], "previous_response_id").String(); got != "resp_pw" {
			t.Fatalf("真实帧 previous_response_id = %q, 期望预热响应 id", got)
		}
	})
}

// TestHTTPWSCacheHitTurnState：HTTP→WS 缓存命中路径的真实帧必须带上
// Identity.Turn.State（同一轮的后续请求命中缓存连接）。
func TestHTTPWSCacheHitTurnState(t *testing.T) {
	payload := []byte(`{"model":"gpt-6.1-sol","input":[{"type":"message","role":"user","content":[{"type":"input_text","text":"hi"}]}]}`)
	up := newPrewarmUpstream(t)
	hc := up.client(t, WithWSConnCache(2))
	doStreamBodyPayload(t, hc, prewarmIdentity("thread-pw-hit-ts", nil), payload) // 新建 ⇒ 预热 + 真实，归还
	hits := hc.wsCache.hits.Load()
	doStreamBodyPayload(t, hc, followUpIdentity("thread-pw-hit-ts", "T-hit", nil), payload)

	if d := hc.wsCache.hits.Load() - hits; d != 1 {
		t.Fatalf("hits 增量 = %d, 期望 1（后续请求命中缓存连接）", d)
	}
	if n := up.upgrades.Load(); n != 1 {
		t.Fatalf("upgrades = %d, 期望 1", n)
	}
	frames := up.sentFrames(t)
	if len(frames) != 3 {
		t.Fatalf("上游收到 %d 帧, 期望 3（预热 + 真实 + 命中的真实帧）", len(frames))
	}
	if got := frameTurnState(t, frames[2]); got != "T-hit" {
		t.Fatalf("命中缓存的真实帧 ts = %q, 期望 %q（Identity.Turn.State）", got, "T-hit")
	}
}

// TestPrewarmFailure：预热失败的三类处理。
func TestPrewarmFailure(t *testing.T) {
	payload := []byte(`{"model":"gpt-6.1-sol","input":[{"type":"message","role":"user","content":[{"type":"input_text","text":"hi"}]}]}`)

	t.Run("连接类-EOF", func(t *testing.T) {
		before := ShapeStatsSnapshot()
		up := newPrewarmUpstream(t)
		up.script(func(_, _ int, frame []byte) prewarmReply {
			// 预热帧的回应只给一半就断 TCP：真实请求不可能发出。
			return prewarmReply{events: []string{tsCreatedFrame("resp_pw")}, closeAfter: true}
		})
		hc := up.client(t)
		_, err := hc.StreamBody(context.Background(), payload, prewarmIdentity("thread-pw-eof", nil))
		if err == nil {
			t.Fatal("预热阶段断连必须返回错误")
		}
		var he *HTTPError
		if errors.As(err, &he) {
			t.Fatalf("连接类预热失败不得是 *HTTPError: %v", err)
		}
		if !errors.Is(err, io.EOF) {
			t.Fatalf("错误链必须保留底层原因（EOF）: %v", err)
		}
		if frames := up.sentFrames(t); len(frames) != 1 {
			t.Fatalf("上游收到 %d 帧, 期望 1（真实帧未发出）", len(frames))
		}
		after := ShapeStatsSnapshot()
		if got := after.PrewarmConnFailures - before.PrewarmConnFailures; got != 1 {
			t.Fatalf("PrewarmConnFailures 增量 = %d, 期望 1", got)
		}
	})

	t.Run("连接类-ctx 超时", func(t *testing.T) {
		up := newPrewarmUpstream(t)
		hang := make(chan struct{})
		t.Cleanup(func() { close(hang) })
		up.script(func(_, _ int, _ []byte) prewarmReply {
			return prewarmReply{wait: hang} // 预热帧不回应
		})
		hc := up.client(t, WithTimeout(120*time.Millisecond))
		_, err := hc.StreamBody(context.Background(), payload, prewarmIdentity("thread-pw-ctx", nil))
		if err == nil {
			t.Fatal("预热阶段 ctx 结束必须返回错误")
		}
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("errors.Is(err, DeadlineExceeded) 必须成立: %v", err)
		}
		var he *HTTPError
		if errors.As(err, &he) {
			t.Fatalf("ctx 结束不得是 *HTTPError: %v", err)
		}
	})

	t.Run("连接类-调用方取消", func(t *testing.T) {
		up := newPrewarmUpstream(t)
		hang := make(chan struct{})
		t.Cleanup(func() { close(hang) })
		ctx, cancel := context.WithCancel(context.Background())
		up.script(func(_, _ int, _ []byte) prewarmReply {
			cancel() // 预热帧已到上游、响应未回：调用方放弃
			return prewarmReply{wait: hang}
		})
		hc := up.client(t)
		_, err := hc.StreamBody(ctx, payload, prewarmIdentity("thread-pw-cancel", nil))
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("errors.Is(err, Canceled) 必须成立: %v", err)
		}
		var he *HTTPError
		if errors.As(err, &he) {
			t.Fatalf("取消不得是 *HTTPError: %v", err)
		}
		if frames := up.sentFrames(t); len(frames) != 1 {
			t.Fatalf("上游收到 %d 帧, 期望 1（真实帧未发出）", len(frames))
		}
	})

	t.Run("连接类-空闲超时", func(t *testing.T) {
		up := newPrewarmUpstream(t)
		hang := make(chan struct{})
		t.Cleanup(func() { close(hang) })
		up.script(func(_, _ int, _ []byte) prewarmReply { return prewarmReply{wait: hang} })
		hc := up.client(t, WithWSIdleTimeout(80*time.Millisecond))
		_, err := hc.StreamBody(context.Background(), payload, prewarmIdentity("thread-pw-idle", nil))
		if !errors.Is(err, ErrWSIdleTimeout) {
			t.Fatalf("errors.Is(err, ErrWSIdleTimeout) 必须成立: %v", err)
		}
	})

	t.Run("4xx error 帧", func(t *testing.T) {
		before := ShapeStatsSnapshot()
		up := newPrewarmUpstream(t)
		up.script(func(_, _ int, _ []byte) prewarmReply {
			return prewarmReply{events: []string{
				`{"type":"error","status":429,"error":{"type":"rate_limit_error","message":"slow down"}}`,
			}}
		})
		hc := up.client(t)
		_, err := hc.StreamBody(context.Background(), payload, prewarmIdentity("thread-pw-429", nil))
		var he *HTTPError
		if !errors.As(err, &he) || he.StatusCode != http.StatusTooManyRequests {
			t.Fatalf("4xx error 帧必须以 *HTTPError 返回: %v", err)
		}
		if !bytes.Contains(he.Raw, []byte("rate_limit_error")) {
			t.Fatalf("HTTPError 必须带上游错误体: %s", he.Raw)
		}
		if n := up.upgrades.Load(); n != 1 {
			t.Fatalf("upgrades = %d, 期望 1（4xx 不重拨）", n)
		}
		if frames := up.sentFrames(t); len(frames) != 1 {
			t.Fatalf("上游收到 %d 帧, 期望 1（只发预热帧）", len(frames))
		}
		after := ShapeStatsSnapshot()
		if got := after.PrewarmRejections - before.PrewarmRejections; got != 1 {
			t.Fatalf("PrewarmRejections 增量 = %d, 期望 1", got)
		}
		// 直接返回前 exchange 已收尾：连接关闭（父 ctx 是 Background、没有预算，
		// 不收尾的话连接与 exchange ctx 永远挂着）。
		up.waitConnClosed(t, 0, "4xx 直接返回必须收尾连接")
		if n := up.live.Load(); n != 0 {
			t.Fatalf("上游仍有 %d 个 handler 存活", n)
		}
	})

	t.Run("调用方已取消不重拨", func(t *testing.T) {
		// 失败终态已入队、调用方随后取消：不得再拨一次（白烧握手与鉴权），
		// 按连接类失败返回，取消错误链保留。
		before := ShapeStatsSnapshot()
		queued := make(chan struct{})
		var upgrades atomic.Int64
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			upgrades.Add(1)
			c, err := coderws.Accept(w, r, nil)
			if err != nil {
				return
			}
			defer c.CloseNow()
			if _, _, rerr := c.Read(r.Context()); rerr != nil {
				return
			}
			for _, f := range []string{
				tsCreatedFrame("resp_pw"),
				tsMetadataFrame("resp_pw", "T-held"),
				`{"type":"response.failed","response":{"id":"resp_pw","object":"response","status":"failed"}}`,
			} {
				if werr := c.Write(r.Context(), coderws.MessageText, []byte(f)); werr != nil {
					return
				}
			}
			// 客户端 pump 顺序读：它回 pong 时一定已把 failed 帧入队（屏障）。
			ctx := c.CloseRead(r.Context())
			if c.Ping(ctx) == nil {
				close(queued)
			}
			<-ctx.Done()
		}))
		t.Cleanup(srv.Close)
		hc := NewHTTPClient(PAT("pat-pw"),
			WithResponsesTransport(TransportWS),
			WithTransport(newFixedTransport(t, "https://chatgpt.com/backend-api/codex/responses", srv.URL)),
			WithTraceAuto(false))
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		// 预热 metadata 交给 Sink 时（预热读循环内、failed 帧之前）：等 failed 入队后取消。
		sink := sinkFunc(func(string) {
			select {
			case <-queued:
			case <-time.After(3 * time.Second):
				t.Error("failed 帧入队屏障超时")
			}
			cancel()
		})
		_, err := hc.StreamBody(ctx, payload, prewarmIdentity("thread-pw-cancel-redial", sink))
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("errors.Is(err, Canceled) 必须成立: %v", err)
		}
		var he *HTTPError
		if errors.As(err, &he) {
			t.Fatalf("取消不得是 *HTTPError: %v", err)
		}
		after := ShapeStatsSnapshot()
		if got := after.PrewarmRedials - before.PrewarmRedials; got != 0 {
			t.Fatalf("PrewarmRedials 增量 = %d, 期望 0（已取消不重拨）", got)
		}
		if got := after.PrewarmConnFailures - before.PrewarmConnFailures; got != 1 {
			t.Fatalf("PrewarmConnFailures 增量 = %d, 期望 1", got)
		}
		if n := upgrades.Load(); n != 1 {
			t.Fatalf("upgrades = %d, 期望 1", n)
		}
	})

	for _, c := range []struct {
		name   string
		events []string
		ts     string // 预热已签发、重拨后的真实帧必须照样带上的 ts（"" = 预热没给）
	}{
		{"response.failed", []string{
			tsCreatedFrame("resp_pw"),
			tsMetadataFrame("resp_pw", "T-held"),
			`{"type":"response.failed","response":{"id":"resp_pw","object":"response","status":"failed","error":{"code":"server_error","message":"boom"}}}`,
		}, "T-held"},
		{"response.incomplete", []string{
			tsCreatedFrame("resp_pw"),
			tsMetadataFrame("resp_pw", "T-held-inc"),
			`{"type":"response.incomplete","response":{"id":"resp_pw","object":"response","status":"incomplete","incomplete_details":{"reason":"max_output_tokens"}}}`,
		}, "T-held-inc"},
		{"5xx error 帧", []string{`{"type":"error","status":503,"error":{"type":"server_error","message":"unavailable"}}`}, ""},
		{"无状态 error 帧", []string{`{"type":"error","error":{"type":"server_error","message":"no status"}}`}, ""},
	} {
		t.Run("重拨-"+c.name, func(t *testing.T) {
			before := ShapeStatsSnapshot()
			up := newPrewarmUpstream(t)
			gate := make(chan struct{})
			up.script(func(conn, _ int, frame []byte) prewarmReply {
				if isPrewarmFrame(frame) {
					return prewarmReply{events: c.events}
				}
				// 重拨后的真实帧先挂住：此刻旧连接必须已经关掉（重拨前 abandon），
				// 而不是等 exchange 结束才被取消守卫收尾。
				return prewarmReply{
					events: []string{tsCreatedFrame("resp_real_" + strconv.Itoa(conn)), tsCompletedFrame("resp_real_" + strconv.Itoa(conn))},
					gate:   gate,
				}
			})
			hc := up.client(t)
			var sink turnStateSink
			var body string
			var rerr error
			finished := make(chan struct{})
			var release sync.Once
			t.Cleanup(func() { // 断言失败提前退出时也放行并等请求 goroutine 结束
				release.Do(func() { close(gate) })
				<-finished
			})
			go func() {
				defer close(finished)
				// 带超时：回归（重拨挂住/不放行）时请求 goroutine 快速结束，
				// 而不是让 Cleanup 一直等到 300s 空闲超时。
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()
				resp, err := hc.StreamBody(ctx, payload, prewarmIdentity("thread-pw-redial", &sink))
				if err != nil {
					rerr = err
					return
				}
				defer resp.Body.Close()
				b, err := io.ReadAll(resp.Body)
				body, rerr = string(b), err
			}()
			up.waitFrames(t, 2) // 重拨后的真实帧已到上游
			up.waitConnClosed(t, 0, "重拨前必须放弃预热失败的连接")
			release.Do(func() { close(gate) })
			<-finished
			if rerr != nil || !strings.Contains(body, "response.completed") {
				t.Fatalf("重拨后必须成功: %v %q", rerr, body)
			}
			if n := up.upgrades.Load(); n != 2 {
				t.Fatalf("upgrades = %d, 期望 2（重拨一次）", n)
			}
			frames := up.sentFrames(t)
			if len(frames) != 2 {
				t.Fatalf("上游收到 %d 帧, 期望 2（预热 + 重拨后的真实帧）", len(frames))
			}
			if !isPrewarmFrame(frames[0]) {
				t.Fatalf("第 1 帧应是预热帧: %.120s", frames[0])
			}
			if isPrewarmFrame(frames[1]) {
				t.Fatalf("重拨后不得再预热: %.120s", frames[1])
			}
			if gjson.GetBytes(frames[1], "previous_response_id").Exists() {
				t.Fatalf("重拨后的真实帧不得带 previous_response_id: %.160s", frames[1])
			}
			if got := gjson.GetBytes(frames[1], "input.#").Int(); got != 1 {
				t.Fatalf("重拨后的输入 = %d 项, 期望全量 1 项", got)
			}
			// 预热已拿到的 ts：交给了 Sink，重拨后的真实帧也必须带（两者一致）。
			if got := frameTurnState(t, frames[1]); got != c.ts {
				t.Fatalf("重拨后真实帧 ts = %q, 期望 %q（预热已签发的值）", got, c.ts)
			}
			if c.ts != "" {
				if got := sink.values(); len(got) != 1 || got[0] != c.ts {
					t.Fatalf("Sink = %v, 期望 [%s]", got, c.ts)
				}
			}
			// 握手：第一次是 prewarm 形态，重拨的是 turn 形态。
			for i, want := range []string{"prewarm", "turn"} {
				md := up.handshakeHeaders(t, i).Get(HeaderTurnMetadata)
				if got := gjson.Get(md, "request_kind").String(); got != want {
					t.Fatalf("第 %d 次握手 request_kind = %q, 期望 %q: %s", i+1, got, want, md)
				}
			}
			after := ShapeStatsSnapshot()
			if got := after.PrewarmRedials - before.PrewarmRedials; got != 1 {
				t.Fatalf("PrewarmRedials 增量 = %d, 期望 1", got)
			}
		})
	}
}

// TestPrewarmResponseIDAndTurnState：预热响应 id 与 ts 的取法。
func TestPrewarmResponseIDAndTurnState(t *testing.T) {
	payload := []byte(`{"model":"gpt-6.1-sol","input":[{"type":"message","role":"user","content":[{"type":"input_text","text":"hi"}]}]}`)

	t.Run("completed 的 id 兜底", func(t *testing.T) {
		// created 没给 id：真实帧链到 completed.response.id。
		up := newPrewarmUpstream(t)
		up.script(func(_, _ int, frame []byte) prewarmReply {
			if isPrewarmFrame(frame) {
				return prewarmReply{events: []string{
					`{"type":"response.created","response":{"object":"response","status":"in_progress"}}`,
					tsCompletedFrame("resp_late"),
				}}
			}
			return prewarmReply{events: []string{tsCreatedFrame("resp_real"), tsCompletedFrame("resp_real")}}
		})
		doStreamBodyPayload(t, up.client(t), prewarmIdentity("thread-pw-lateid", nil), payload)
		frames := up.sentFrames(t)
		if len(frames) != 2 {
			t.Fatalf("上游收到 %d 帧, 期望 2", len(frames))
		}
		if got := gjson.GetBytes(frames[1], "previous_response_id").String(); got != "resp_late" {
			t.Fatalf("真实帧 previous_response_id = %q, 期望 completed 给的 resp_late", got)
		}
	})

	t.Run("ts 取首值", func(t *testing.T) {
		// 同一预热 response 先后给两个 ts：真实帧带第一个（官方 OnceLock）。
		up := newPrewarmUpstream(t)
		up.script(func(_, _ int, frame []byte) prewarmReply {
			if isPrewarmFrame(frame) {
				return prewarmReply{events: []string{
					tsCreatedFrame("resp_pw"),
					tsMetadataFrame("resp_pw", "T-first"),
					tsMetadataFrame("resp_pw", "T-second"),
					tsCompletedFrame("resp_pw"),
				}}
			}
			return prewarmReply{events: []string{tsCreatedFrame("resp_real"), tsCompletedFrame("resp_real")}}
		})
		doStreamBodyPayload(t, up.client(t), prewarmIdentity("thread-pw-ts-first", nil), payload)
		frames := up.sentFrames(t)
		if len(frames) != 2 {
			t.Fatalf("上游收到 %d 帧, 期望 2", len(frames))
		}
		if got := frameTurnState(t, frames[1]); got != "T-first" {
			t.Fatalf("真实帧 ts = %q, 期望首值 T-first", got)
		}
	})
}

// TestPrewarmEventsHidden：预热事件不进返回流、不计用量；Release 只在真实帧终态后
// 发生（预热期间连接不归还）。
func TestPrewarmEventsHidden(t *testing.T) {
	const pwUsage = `{"input_tokens":1000,"output_tokens":1000,"total_tokens":2000}`
	up := newPrewarmUpstream(t)
	gate := make(chan struct{})
	up.script(func(_, _ int, frame []byte) prewarmReply {
		if isPrewarmFrame(frame) {
			// 预热带只有预热才有的内容与用量：一旦漏进返回流就能被看见。
			return prewarmReply{events: []string{
				`{"type":"response.created","response":{"id":"resp_pw","object":"response","status":"in_progress"}}`,
				`{"type":"response.metadata","response_id":"resp_pw","headers":{"x-codex-turn-state":"T-prewarm"}}`,
				`{"type":"response.output_item.done","item":{"type":"message","role":"assistant","id":"msg_pw","content":[{"type":"output_text","text":"PREWARM-ONLY-TEXT"}]}}`,
				`{"type":"response.completed","response":{"id":"resp_pw","object":"response","status":"completed","usage":` + pwUsage + `}}`,
			}}
		}
		return prewarmReply{
			events: []string{
				`{"type":"response.created","response":{"id":"resp_real","object":"response","status":"in_progress"}}`,
				`{"type":"response.output_item.done","item":{"type":"message","role":"assistant","id":"msg_real","content":[{"type":"output_text","text":"real"}]}}`,
				`{"type":"response.completed","response":{"id":"resp_real","object":"response","status":"completed","usage":` + respUsage + `}}`,
			},
			gate: gate, // 真实帧的事件先不发：此刻预热已完成、真实帧尚未终态
		}
	})
	hc := up.client(t, WithWSConnCache(2))

	type outcome struct {
		body string
		err  error
		agg  *HTTPResponse
	}
	done := make(chan outcome, 2)
	go func() {
		_, body := doStreamBodyPayload(t, hc, prewarmIdentity("thread-pw-hidden-a", nil), []byte(`{"model":"gpt-6.1-sol","input":[]}`))
		done <- outcome{body: body}
	}()
	// 屏障：真实帧已到上游 ⇒ 预热已读完；真实帧的事件被 gate 挂住、未终态。
	// 此刻连接不得在缓存里（预热若调了 Release，连接会进表且 exchange 置
	// closed，真实帧读到 EOF——下面的 "real" 断言同样会失败）。
	up.waitFrames(t, 2)
	if n := cacheIdleCount(hc); n != 0 {
		t.Fatalf("真实帧终态前连接被归还（idle=%d）", n)
	}
	close(gate)
	out := <-done
	if !strings.Contains(out.body, "real") {
		t.Fatalf("真实帧内容缺失: %q", out.body)
	}
	for _, leak := range []string{"PREWARM-ONLY-TEXT", "msg_pw", "resp_pw", "2000"} {
		if strings.Contains(out.body, leak) {
			t.Fatalf("预热事件漏进返回流（%s）: %q", leak, out.body)
		}
	}
	// 预热不触发 Release，真实帧终态后恰好归还一次（归还在 body.Close 内同步
	// 完成，done 收到时已在表里）。
	if n := cacheIdleCount(hc); n != 1 {
		t.Fatalf("真实帧终态后 idle = %d, 期望 1", n)
	}

	// 聚合臂：用量只来自真实帧。
	var sink turnStateSink
	resp, err := hc.Responses(context.Background(), []byte(`{"model":"gpt-6.1-sol","input":[]}`), prewarmIdentity("thread-pw-hidden-b", &sink))
	if err != nil {
		t.Fatalf("Responses: %v", err)
	}
	if got := gjson.GetBytes(resp.Raw, "usage").Raw; got != respUsage {
		t.Fatalf("聚合用量 = %s, 期望只有真实帧的用量 %s", got, respUsage)
	}
	if bytes.Contains(resp.Raw, []byte("PREWARM-ONLY-TEXT")) {
		t.Fatalf("预热内容进了聚合结果: %s", resp.Raw)
	}
	if got := sink.values(); len(got) != 1 || got[0] != "T-prewarm" {
		t.Fatalf("Sink = %v, 期望 [T-prewarm]（预热签发的 ts 只交一次给 Sink）", got)
	}
}

// TestConnCacheKeyRoutingHint：连接缓存键含 routing-hint（model/service_tier）：
// 同线程换模型不命中；同线程同模型命中；RetireThread 仍按 threadKey 精确摘除。
func TestConnCacheKeyRoutingHint(t *testing.T) {
	up := newPrewarmUpstream(t)
	hc := up.client(t, WithWSConnCache(4))
	body := func(model string) []byte {
		b, err := sjson.SetBytes([]byte(`{"model":"x","input":[{"type":"message","role":"user","content":[{"type":"input_text","text":"hi"}]}]}`), "model", model)
		if err != nil {
			t.Fatal(err)
		}
		return b
	}
	id := func(first bool) Identity {
		if first {
			return prewarmIdentity("thread-pw-hint", nil)
		}
		return followUpIdentity("thread-pw-hint", "", nil)
	}

	doStreamBodyPayload(t, hc, id(true), body("gpt-6.1-sol")) // 新建 ⇒ 预热 + 真实
	doStreamBodyPayload(t, hc, id(false), body("gpt-5.5"))    // 换模型 ⇒ 不命中、不预热
	doStreamBodyPayload(t, hc, id(false), body("gpt-5.5"))    // 同模型 ⇒ 命中
	if n := up.upgrades.Load(); n != 2 {
		t.Fatalf("upgrades = %d, 期望 2（换模型不得复用）", n)
	}
	for i, want := range []string{"model=gpt-6.1-sol", "model=gpt-5.5"} {
		if got := up.handshakeHeaders(t, i).Get(HeaderRoutingHint); got != want {
			t.Fatalf("第 %d 次握手 routing-hint = %q, 期望 %q", i+1, got, want)
		}
	}
	if n := cacheIdleCount(hc); n != 2 {
		t.Fatalf("idle = %d, 期望 2（两个 routing-hint 各一条）", n)
	}
	hc.RetireThread(id(true).threadKey())
	if n := cacheIdleCount(hc); n != 0 {
		t.Fatalf("RetireThread 后 idle = %d, 期望 0（按 threadKey 摘掉全部路由）", n)
	}
}

// TestPrewarmClientValuesNeverEgress：预热帧、真实帧、握手头都不带客户端身份值。
func TestPrewarmClientValuesNeverEgress(t *testing.T) {
	up := newPrewarmUpstream(t)
	cap := &wireCapture{inner: newFixedTransport(t, "https://chatgpt.com/backend-api/codex/responses", up.srv.URL)}
	hc := NewHTTPClient(PAT("pat-pw"), WithResponsesTransport(TransportWS), WithTransport(cap), WithWSConnCache(2))
	id := prewarmIdentity("thread-pw-sentinel", nil)
	doStreamBodyPayload(t, hc, id, []byte(sentinelRequest))
	up.waitFrames(t, 2)

	frames := up.sentFrames(t)
	if len(frames) != 2 || !isPrewarmFrame(frames[0]) {
		t.Fatalf("帧序不符（期望预热 + 真实）: %d 帧", len(frames))
	}
	for i, f := range frames {
		assertNoSentinel(t, fmt.Sprintf("第 %d 帧", i+1), f)
	}
	for i := range cap.all() {
		assertNoSentinel(t, "握手头", headerBytes(up.handshakeHeaders(t, i)))
	}
}

// TestPrewarmCallerTurnStateWins：调用方已给 Turn.State（同 owner 的已记录 ts）时，
// 预热签发的新 ts 只交给 Sink，不盖掉它——真实帧带 Turn.State（官方 OnceLock：已有
// 值优先；与 WS→WS 会话路径 takePrewarmTS 一致）。Turn.State 为空时才用预热 ts。
func TestPrewarmCallerTurnStateWins(t *testing.T) {
	payload := []byte(`{"model":"gpt-6.1-sol","input":[{"type":"message","role":"user","content":[{"type":"input_text","text":"hi"}]}]}`)

	t.Run("会话簿重试落到新连接", func(t *testing.T) {
		// 一轮第一个请求的客户端重试（同一 TurnKey）：会话簿给 First=true 且带已记录的
		// 同 owner ts S1；重试落到新连接（新 client）⇒ 预热，上游签发 S2。
		book := NewConversationBook(NewMemoryConversationStore())
		req := ConversationRequest{
			Key: NewConversationKey("d", 1, "conv-retry-ts"), InstallationID: "inst",
			Owner: [16]byte{1}, Input: AnalyzeConversationInput(payload),
		}
		up := newPrewarmUpstream(t)
		up.mu.Lock()
		up.ts = "S1"
		up.mu.Unlock()
		id1 := book.Prepare(context.Background(), req)
		doStreamBodyPayload(t, up.client(t), id1, payload)
		if err := book.Flush(context.Background()); err != nil {
			t.Fatal(err)
		}
		up.mu.Lock()
		up.ts = "S2"
		up.mu.Unlock()
		id2 := book.Prepare(context.Background(), req)
		if !id2.Turn.First || id2.Turn.State != "S1" {
			t.Fatalf("重试身份 First=%v State=%q, 期望 First=true State=S1", id2.Turn.First, id2.Turn.State)
		}
		doStreamBodyPayload(t, up.client(t), id2, payload)
		frames := up.sentFrames(t)
		if len(frames) != 4 || !isPrewarmFrame(frames[2]) || isPrewarmFrame(frames[3]) {
			t.Fatalf("期望 4 帧（两次 预热+真实），实际 %d", len(frames))
		}
		if got := frameTurnState(t, frames[3]); got != "S1" {
			t.Fatalf("重试的真实帧 ts = %q, 期望 S1（Turn.State 优先，不得被预热 ts S2 覆盖）", got)
		}
		// 会话簿仍保留 S1：本轮后续请求与真实帧一致。
		if err := book.Flush(context.Background()); err != nil {
			t.Fatal(err)
		}
		next := req
		next.Input = AnalyzeConversationInput([]byte(`{"model":"gpt-6.1-sol","input":[{"type":"message","role":"user","content":[{"type":"input_text","text":"hi"}]},{"type":"function_call","call_id":"c","name":"x","arguments":"{}"},{"type":"function_call_output","call_id":"c","output":"o"}]}`))
		id3 := book.Prepare(context.Background(), next)
		if id3.Turn.First || id3.Turn.State != "S1" {
			t.Fatalf("后续请求 First=%v State=%q, 期望 First=false State=S1", id3.Turn.First, id3.Turn.State)
		}
	})

	t.Run("预热成功", func(t *testing.T) {
		up := newPrewarmUpstream(t) // 默认脚本：预热签发 T-prewarm
		var sink turnStateSink
		id := prewarmIdentity("thread-pw-caller-ts", &sink)
		id.Turn.State = "S1"
		doStreamBodyPayload(t, up.client(t), id, payload)
		frames := up.sentFrames(t)
		if len(frames) != 2 || !isPrewarmFrame(frames[0]) {
			t.Fatalf("期望 预热+真实 两帧，实际 %d", len(frames))
		}
		if got := frameTurnState(t, frames[1]); got != "S1" {
			t.Fatalf("真实帧 ts = %q, 期望调用方的 S1", got)
		}
		if got := sink.values(); len(got) != 1 || got[0] != "T-prewarm" {
			t.Fatalf("Sink = %v, 期望 [T-prewarm]（预热 ts 照样交给 Sink）", got)
		}
	})

	t.Run("预热失败重拨", func(t *testing.T) {
		up := newPrewarmUpstream(t)
		up.script(func(conn, _ int, frame []byte) prewarmReply {
			if isPrewarmFrame(frame) {
				return prewarmReply{events: []string{
					tsCreatedFrame("resp_pw"),
					tsMetadataFrame("resp_pw", "T-held"),
					`{"type":"response.failed","response":{"id":"resp_pw","object":"response","status":"failed"}}`,
				}}
			}
			id := "resp_real_" + strconv.Itoa(conn)
			return prewarmReply{events: []string{tsCreatedFrame(id), tsCompletedFrame(id)}}
		})
		id := prewarmIdentity("thread-pw-caller-ts-redial", nil)
		id.Turn.State = "S1"
		doStreamBodyPayload(t, up.client(t), id, payload)
		frames := up.sentFrames(t)
		if len(frames) != 2 || up.upgrades.Load() != 2 {
			t.Fatalf("期望重拨一次共 2 帧，实际 %d 帧 / %d 次握手", len(frames), up.upgrades.Load())
		}
		if got := frameTurnState(t, frames[1]); got != "S1" {
			t.Fatalf("重拨后真实帧 ts = %q, 期望调用方的 S1", got)
		}
	})
}
