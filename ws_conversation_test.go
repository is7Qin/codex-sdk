package codexsdk

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/tidwall/gjson"
)

// ---- WS→WS 会话测试工具 ----

const wsConvURL = "https://chatgpt.com/backend-api/codex/responses"

// wsConvKey 是这些用例共用的对话键。
var wsConvKey = NewConversationKey("dom", 9, "ws-conv")

func dialConversationClient(t *testing.T, up *tsUpstream, book *ConversationBook, first string) *Client {
	t.Helper()
	c, err := Dial(context.Background(), PAT("pat-conv"), Identity{},
		WithTransport(newFixedTransport(t, wsConvURL, up.srv.URL)),
		WithConversation(book, ConversationRequest{Key: wsConvKey, InstallationID: "inst-ws", Owner: ownerA}, []byte(first)))
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	t.Cleanup(func() { _ = c.CloseNow() })
	return c
}

// respondTurn 回放一次完整响应：created、metadata(ts)、completed。
func respondTurn(id, ts string) []string {
	out := []string{tsCreatedFrame(id)}
	if ts != "" {
		out = append(out, tsMetadataFrame(id, ts))
	}
	return append(out, tsCompletedFrame(id))
}

// recvUntil 读帧直到收到 n 个终态事件（completed / failed / incomplete / error）。
func recvUntil(t *testing.T, c *Client, n int) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for n > 0 {
		data, err := c.Recv(ctx)
		if err != nil {
			t.Fatalf("Recv: %v", err)
		}
		switch gjson.GetBytes(data, "type").String() {
		case responseEventCompleted, responseEventFailed, responseEventIncomplete, responseEventError:
			n--
		}
	}
}

func mustSend(t *testing.T, c *Client, frame string) {
	t.Helper()
	if err := c.Send(context.Background(), []byte(frame)); err != nil {
		t.Fatalf("Send: %v", err)
	}
}

func frameTurnID(t *testing.T, frame []byte) string {
	t.Helper()
	return frameMeta(t, frame, codexMetaTurnKey)
}

func frameInputTypes(frame []byte) []string {
	var out []string
	for _, it := range gjson.GetBytes(frame, "input").Array() {
		typ := it.Get("type").String()
		if typ == "" {
			typ = "message"
		}
		if r := it.Get("role").String(); r != "" {
			typ += ":" + r
		}
		out = append(out, typ)
	}
	return out
}

func handshakeTurnMetaField(t *testing.T, up *tsUpstream, field string) gjson.Result {
	t.Helper()
	return gjson.Get(up.handshakeHeaders(t, 0).Get(HeaderTurnMetadata), field)
}

// 帧样本。
const (
	wsPrewarmFrame   = `{"type":"response.create","model":"gpt-5.5","input":[],"generate":false}`
	wsFullFrame      = `{"type":"response.create","model":"gpt-5.5","input":[` + userHi + `]}`
	wsFollowIncr     = `{"type":"response.create","model":"gpt-5.5","previous_response_id":"r-prev","input":[` + toolCall + `,` + toolOutput + `]}`
	wsNewUserIncr    = `{"type":"response.create","model":"gpt-5.5","previous_response_id":"r-prev","input":[` + assistantMsg + `,{"role":"user","content":[{"type":"input_text","text":"next"}]}]}`
	wsFullInstrFrame = `{"type":"response.create","model":"gpt-5.5","instructions":"be brief","input":[` + userHi + `]}`
)

func wsIncrWithInstr(instr string) string {
	return `{"type":"response.create","model":"gpt-5.5","instructions":"` + instr + `","previous_response_id":"r-prev","input":[` + toolOutput + `]}`
}

// WS→WS 逐帧会话合成。
func TestWSConversationFrames(t *testing.T) {
	t.Run("ConstructionPanics", func(t *testing.T) {
		book := newTestBook(NewMemoryConversationStore(), nil)
		req := ConversationRequest{Key: wsConvKey}
		for name, call := range map[string]func(){
			"WithFirstFrame": func() {
				_, _ = Dial(context.Background(), PAT("p"), Identity{}, WithFirstFrame([]byte(wsFullFrame)), WithConversation(book, req, []byte(wsFullFrame)))
			},
			"NonZeroIdentity": func() {
				_, _ = Dial(context.Background(), PAT("p"), Identity{InstallationID: "x"}, WithConversation(book, req, []byte(wsFullFrame)))
			},
			"NilBook": func() {
				_, _ = Dial(context.Background(), PAT("p"), Identity{}, WithConversation(nil, req, []byte(wsFullFrame)))
			},
		} {
			func() {
				defer func() {
					if recover() == nil {
						t.Fatalf("%s: 期望构造期 panic", name)
					}
				}()
				call()
			}()
		}
	})

	t.Run("PrewarmFirstFrameWritesNoTurn", func(t *testing.T) {
		s := newCountingStore(NewMemoryConversationStore())
		book := newTestBook(s, nil)
		up := newTSUpstream(t, nil)
		c := dialConversationClient(t, up, book, wsPrewarmFrame)
		if got := handshakeTurnMetaField(t, up, "request_kind").String(); got != "prewarm" {
			t.Fatalf("握手 turn-metadata request_kind = %q", got)
		}
		mustSend(t, c, wsPrewarmFrame)
		up.waitFrames(t, 1)
		f := up.sentFrames(t)[0]
		if gjson.GetBytes(f, "generate").Raw != "false" || frameTurnID(t, f) != "" || frameTurnState(t, f) != "" {
			t.Fatalf("预热帧形态不对: %s", f)
		}
		if s.opens.Load() != 1 || s.loads.Load() != 0 || len(book.queue) != 0 || book.Stats().TurnEntries != 0 {
			t.Fatalf("预热首帧只应取对话记录: opens=%d loads=%d queue=%d %+v", s.opens.Load(), s.loads.Load(), len(book.queue), book.Stats())
		}
		th := gjson.GetBytes(f, "client_metadata.thread_id").String()
		if th == "" || up.handshakeHeaders(t, 0).Get(HeaderThreadID) != th {
			t.Fatalf("握手线程号与帧不一致: %q", th)
		}
	})

	t.Run("FirstFrameNotPreparedTwice", func(t *testing.T) {
		s := newCountingStore(NewMemoryConversationStore())
		book := newTestBook(s, nil)
		up := newTSUpstream(t, nil)
		c := dialConversationClient(t, up, book, wsFullFrame)
		mustSend(t, c, wsFullFrame)
		up.waitFrames(t, 1)
		f := up.sentFrames(t)[0]
		if s.calls() != 1 || len(book.queue) != 1 {
			t.Fatalf("首帧应只 Prepare 一次: calls=%d queue=%d", s.calls(), len(book.queue))
		}
		// Dial 那次 Prepare 不经缓存命中；首帧若又 Prepare 一次，对话与轮次缓存必然各命中一次。
		if st := book.Stats(); st.ConversationCacheHits != 0 || st.TurnCacheHits != 0 {
			t.Fatalf("首帧不应再 Prepare（缓存命中说明重算了）: %+v", st)
		}
		if got := handshakeTurnMetaField(t, up, "turn_id").String(); got == "" || got != frameTurnID(t, f) {
			t.Fatalf("握手 turn_id %q 与首帧 %q 不一致", got, frameTurnID(t, f))
		}
		if handshakeTurnMetaField(t, up, "request_kind").String() != "turn" {
			t.Fatal("全量首帧握手应为 turn 形态")
		}
	})

	t.Run("FullFrameOpensTurnIncrementalCarriesFirstTS", func(t *testing.T) {
		book := newTestBook(NewMemoryConversationStore(), nil)
		up := newTSUpstream(t, func(i int, _ []byte) []string {
			if i == 0 {
				return respondTurn("r0", "") // 预热不回 ts（预热 ts 的带入见 PrewarmTSCarriedIntoNewTurn）
			}
			return respondTurn("r"+strconv.Itoa(i), "ts-"+strconv.Itoa(i))
		})
		c := dialConversationClient(t, up, book, wsPrewarmFrame)
		mustSend(t, c, wsPrewarmFrame)
		recvUntil(t, c, 1)
		mustSend(t, c, wsFullFrame) // 全量帧：开轮
		recvUntil(t, c, 1)
		mustSend(t, c, wsFollowIncr) // 增量后续：沿用并带本轮首个 ts
		recvUntil(t, c, 1)
		mustSend(t, c, wsFollowIncr) // ts-2 被忽略（同 owner 首值生效）
		recvUntil(t, c, 1)
		mustSend(t, c, wsNewUserIncr) // 增量里的新用户消息：连接内新轮
		recvUntil(t, c, 1)
		mustSend(t, c, wsFollowIncr)
		recvUntil(t, c, 1)
		fs := up.sentFrames(t)
		full, inc1, inc2, newTurn, after := fs[1], fs[2], fs[3], fs[4], fs[5]
		turn := frameTurnID(t, full)
		if turn == "" || frameTurnID(t, inc1) != turn || frameTurnID(t, inc2) != turn {
			t.Fatalf("增量帧应沿用全量帧的轮次: %q %q %q", turn, frameTurnID(t, inc1), frameTurnID(t, inc2))
		}
		if frameTurnState(t, full) != "" || frameTurnState(t, inc1) != "ts-1" || frameTurnState(t, inc2) != "ts-1" {
			t.Fatalf("ts: full=%q inc1=%q inc2=%q", frameTurnState(t, full), frameTurnState(t, inc1), frameTurnState(t, inc2))
		}
		if nt := frameTurnID(t, newTurn); nt == "" || nt == turn || frameTurnState(t, newTurn) != "" {
			t.Fatalf("新用户消息应开新轮且不带旧 ts: %q ts=%q", nt, frameTurnState(t, newTurn))
		}
		if st := c.conv.state.Load(); st.turn.ID != frameTurnID(t, newTurn) || !st.turn.First || st.source != turnSourceLocal {
			t.Fatalf("连接内新轮应 First=true、来源 local: %+v source=%d", st.turn, st.source)
		}
		if frameTurnID(t, after) != frameTurnID(t, newTurn) || frameTurnState(t, after) != "ts-4" {
			t.Fatalf("连接内新轮的 ts 只进连接快照: turn=%q ts=%q", frameTurnID(t, after), frameTurnState(t, after))
		}
		if prev := gjson.GetBytes(inc1, "previous_response_id").String(); prev != "r-prev" {
			t.Fatalf("增量帧 previous_response_id = %q", prev)
		}
		// 会话簿开的轮次：ts 也交给会话簿 Sink。
		id := book.Prepare(bgctx, ConversationRequest{Key: wsConvKey, Owner: ownerA, Input: AnalyzeConversationInput([]byte(followPayload))})
		if id.Turn.ID != turn || id.Turn.State != "ts-1" {
			t.Fatalf("会话簿 = %+v, 期望轮次 %q 带 ts-1", id.Turn, turn)
		}
		if len(book.queue) != 2 { // 开轮写入 + ts 写入；连接内新轮不写
			t.Fatalf("写入队列 = %d", len(book.queue))
		}
	})

	// 首帧就是增量帧（带 previous_response_id）：Dial 先补对话记录，再按增量规则。
	t.Run("IncrementalFirstFrame", func(t *testing.T) {
		for _, tc := range []struct {
			name, frame string
			first       bool
			temporary   int64
		}{
			{"FollowUp", wsFollowIncr, false, 1},
			{"NewUserMessage", wsNewUserIncr, true, 0},
		} {
			t.Run(tc.name, func(t *testing.T) {
				s := newCountingStore(NewMemoryConversationStore())
				book := newTestBook(s, nil)
				up := newTSUpstream(t, nil)
				c := dialConversationClient(t, up, book, tc.frame)
				st := c.conv.state.Load()
				if st.conv == nil || st.turn.ID == "" || st.source != turnSourceLocal || st.turn.First != tc.first {
					t.Fatalf("增量首帧快照 = %+v source=%d", st.turn, st.source)
				}
				if s.opens.Load() != 1 || s.loads.Load() != 0 || len(book.queue) != 0 ||
					book.Stats().WSLocalTemporaryTurns != tc.temporary {
					t.Fatalf("增量首帧只取对话记录、不读写轮次: opens=%d loads=%d queue=%d %+v",
						s.opens.Load(), s.loads.Load(), len(book.queue), book.Stats())
				}
				mustSend(t, c, tc.frame)
				up.waitFrames(t, 1)
				f := up.sentFrames(t)[0]
				if got := handshakeTurnMetaField(t, up, "turn_id").String(); got != st.turn.ID || frameTurnID(t, f) != got {
					t.Fatalf("握手 turn_id %q / 帧 %q / 快照 %q", got, frameTurnID(t, f), st.turn.ID)
				}
				if th := gjson.GetBytes(f, "client_metadata.thread_id").String(); th != st.conv.rec.Thread || th == "" {
					t.Fatalf("帧线程号 %q ≠ 对话记录 %q", th, st.conv.rec.Thread)
				}
			})
		}
	})

	// 客户端预热帧拿到的 ts 存进连接快照；随后本连接新开的一轮没有同 owner 的
	// ts 时带上它（官方抓包 log169b-tui seq35：预热后的第一帧带预热响应给的 ts）。
	t.Run("PrewarmTSCarriedIntoNewTurn", func(t *testing.T) {
		prewarmFirst := func(t *testing.T) (*ConversationBook, *tsUpstream, *Client) {
			book := newTestBook(NewMemoryConversationStore(), nil)
			up := newTSUpstream(t, func(i int, _ []byte) []string {
				if i == 0 {
					return respondTurn("r0", "ts-pw")
				}
				return respondTurn("r"+strconv.Itoa(i), "ts-"+strconv.Itoa(i))
			})
			c := dialConversationClient(t, up, book, wsPrewarmFrame)
			mustSend(t, c, wsPrewarmFrame)
			recvUntil(t, c, 1)
			if got := c.conv.state.Load().prewarmTS; got != "ts-pw" {
				t.Fatalf("预热 ts 未进连接快照: %q", got)
			}
			return book, up, c
		}
		t.Run("IncrementalNewTurn", func(t *testing.T) {
			book, up, c := prewarmFirst(t)
			mustSend(t, c, wsNewUserIncr) // 官方形态：previous_response_id = 预热 id，新用户消息
			recvUntil(t, c, 1)
			mustSend(t, c, wsFollowIncr)
			recvUntil(t, c, 1)
			mustSend(t, c, wsNewUserIncr) // 再开一轮：预热 ts 只用一次
			up.waitFrames(t, 4)
			fs := up.sentFrames(t)
			if frameTurnState(t, fs[1]) != "ts-pw" || frameTurnState(t, fs[2]) != "ts-pw" {
				t.Fatalf("预热后新轮应带预热 ts（同轮首值）: %q %q", frameTurnState(t, fs[1]), frameTurnState(t, fs[2]))
			}
			if frameTurnID(t, fs[2]) != frameTurnID(t, fs[1]) || frameTurnState(t, fs[3]) != "" {
				t.Fatalf("预热 ts 不应带入下一轮: turn=%q/%q ts=%q", frameTurnID(t, fs[2]), frameTurnID(t, fs[1]), frameTurnState(t, fs[3]))
			}
			if len(book.queue) != 0 {
				t.Fatalf("连接内新轮不写存储: queue=%d", len(book.queue))
			}
		})
		t.Run("FullFrameNewTurn", func(t *testing.T) {
			book, up, c := prewarmFirst(t)
			mustSend(t, c, wsFullFrame) // 经 Prepare 开轮，会话簿记录里没有 ts
			recvUntil(t, c, 1)
			mustSend(t, c, wsFollowIncr)
			up.waitFrames(t, 3)
			fs := up.sentFrames(t)
			if frameTurnState(t, fs[1]) != "ts-pw" || frameTurnState(t, fs[2]) != "ts-pw" {
				t.Fatalf("全量新轮应带预热 ts: %q %q", frameTurnState(t, fs[1]), frameTurnState(t, fs[2]))
			}
			// 会话簿开的轮次：预热 ts 也记进会话簿（跨副本后续请求带同一个 ts）。
			id := book.Prepare(bgctx, ConversationRequest{Key: wsConvKey, Owner: ownerA, Input: AnalyzeConversationInput([]byte(followPayload))})
			if id.Turn.ID != frameTurnID(t, fs[1]) || id.Turn.State != "ts-pw" {
				t.Fatalf("会话簿 = %+v, 期望轮次 %q 带 ts-pw", id.Turn, frameTurnID(t, fs[1]))
			}
		})
		t.Run("BookTSWins", func(t *testing.T) {
			// 会话簿记录里已有同 owner 的 ts（另一连接拿到的）：用记录里的，不用预热 ts。
			book, up, c := prewarmFirst(t)
			id := book.Prepare(bgctx, ConversationRequest{Key: wsConvKey, Owner: ownerA, Input: AnalyzeConversationInput([]byte(turnPayload))})
			id.Sink.SetTurnState("ts-book")
			mustSend(t, c, wsFullFrame)
			up.waitFrames(t, 2)
			if got := frameTurnState(t, up.sentFrames(t)[1]); got != "ts-book" {
				t.Fatalf("会话簿已有 ts 时应带它: %q", got)
			}
			if c.conv.state.Load().prewarmTS != "ts-pw" {
				t.Fatal("未用掉的预热 ts 应保留")
			}
		})
	})

	t.Run("PrewarmThenFollowupIncrementalIsTemporary", func(t *testing.T) {
		book := newTestBook(NewMemoryConversationStore(), nil)
		up := newTSUpstream(t, nil)
		c := dialConversationClient(t, up, book, wsPrewarmFrame)
		mustSend(t, c, wsPrewarmFrame)
		mustSend(t, c, wsFollowIncr)
		up.waitFrames(t, 2)
		f := up.sentFrames(t)[1]
		if frameTurnID(t, f) == "" || book.Stats().WSLocalTemporaryTurns != 1 || len(book.queue) != 0 {
			t.Fatalf("临时轮次: turn=%q %+v queue=%d", frameTurnID(t, f), book.Stats(), len(book.queue))
		}
	})

	t.Run("IncrementalDoesNotReinject", func(t *testing.T) {
		book := newTestBook(NewMemoryConversationStore(), nil)
		up := newTSUpstream(t, nil)
		c := dialConversationClient(t, up, book, wsFullInstrFrame)
		mustSend(t, c, wsFullInstrFrame)
		mustSend(t, c, wsIncrWithInstr("be brief"))
		mustSend(t, c, wsIncrWithInstr("be verbose"))
		mustSend(t, c, wsIncrWithInstr("be verbose"))
		up.waitFrames(t, 4)
		fs := up.sentFrames(t)
		want := [][]string{
			{"message:developer", "message:user"},
			{"function_call_output"},
			{"message:developer", "function_call_output"},
			{"function_call_output"},
		}
		for i, f := range fs {
			if got := frameInputTypes(f); strings.Join(got, ",") != strings.Join(want[i], ",") {
				t.Fatalf("帧 %d 输入 = %v, 期望 %v", i, got, want[i])
			}
			if gjson.GetBytes(f, "instructions").Exists() {
				t.Fatalf("帧 %d 带顶层 instructions", i)
			}
		}
		if book.Stats().WSIncrementalInjects != 1 {
			t.Fatalf("WSIncrementalInjects = %d", book.Stats().WSIncrementalInjects)
		}
	})

	t.Run("ErrorWithoutCreatedKeepsAlignment", func(t *testing.T) {
		book := newTestBook(NewMemoryConversationStore(), nil)
		up := newTSUpstream(t, func(i int, _ []byte) []string {
			if i == 0 {
				return []string{`{"type":"error","status":400,"error":{"message":"bad previous_response_id"}}`}
			}
			return respondTurn("r"+strconv.Itoa(i), "ts-"+strconv.Itoa(i))
		})
		c := dialConversationClient(t, up, book, wsFullFrame)
		mustSend(t, c, wsFullFrame)
		recvUntil(t, c, 1)
		mustSend(t, c, wsNewUserIncr) // 连接内新轮，回 ts-1
		recvUntil(t, c, 1)
		mustSend(t, c, wsFollowIncr)
		up.waitFrames(t, 3)
		fs := up.sentFrames(t)
		if frameTurnID(t, fs[2]) != frameTurnID(t, fs[1]) || frameTurnState(t, fs[2]) != "ts-1" {
			t.Fatalf("error 之后 ts 归属错位: turn=%q/%q ts=%q", frameTurnID(t, fs[2]), frameTurnID(t, fs[1]), frameTurnState(t, fs[2]))
		}
		if book.Stats().WSLateTurnStates != 0 {
			t.Fatalf("不应有迟到 ts: %+v", book.Stats())
		}
	})

	t.Run("MidStreamErrorDoesNotPop", func(t *testing.T) {
		// 帧 0 已绑定（created 到达）尚未结束时帧 1 写出，FIFO 里有帧 1 的身份；
		// 这时帧 0 的流中途 error 不得弹走它，帧 1 的 ts 仍归帧 1 的轮次。
		book := newTestBook(NewMemoryConversationStore(), nil)
		up := newTSUpstream(t, func(i int, _ []byte) []string {
			switch i {
			case 0:
				return []string{tsCreatedFrame("r0")}
			case 1:
				return append([]string{`{"type":"error","status":500,"error":{"message":"boom"}}`}, respondTurn("r1", "ts-1")...)
			}
			return nil
		})
		c := dialConversationClient(t, up, book, wsFullFrame)
		mustSend(t, c, wsFullFrame)
		mustSend(t, c, wsNewUserIncr) // 连接内新轮；身份入 FIFO 后上游才回 error
		recvUntil(t, c, 2)            // error + 帧 1 的 completed
		if n := len(c.conv.fifo); n != 0 {
			t.Fatalf("FIFO 残留 %d", n)
		}
		mustSend(t, c, wsFollowIncr)
		up.waitFrames(t, 3)
		fs := up.sentFrames(t)
		if frameTurnID(t, fs[2]) != frameTurnID(t, fs[1]) || frameTurnState(t, fs[2]) != "ts-1" {
			t.Fatalf("流中途 error 不得弹 FIFO: turn=%q/%q ts=%q", frameTurnID(t, fs[2]), frameTurnID(t, fs[1]), frameTurnState(t, fs[2]))
		}
	})

	// 上游在帧写出的瞬间就回完整响应（created、metadata、completed），写方还没
	// 从 ws.Write 返回：测试连接在帧 1 的字节交给内核之后、Write 返回之前停住，
	// 直到 Recv 处理完整个响应。本帧身份与快照都在写闸内、写帧之前登记，ts 归属
	// 正确（任何"写后登记"的实现都会把 created 绑到空身份上）。
	t.Run("CreatedBeforeWriteReturns", func(t *testing.T) {
		book := newTestBook(NewMemoryConversationStore(), nil)
		up := newTSUpstream(t, func(i int, _ []byte) []string {
			return respondTurn("r"+strconv.Itoa(i), "ts-"+strconv.Itoa(i))
		})
		tr := newStallTransport(t, up.srv.URL)
		c, err := Dial(context.Background(), PAT("pat-conv"), Identity{}, WithTransport(tr),
			WithConversation(book, ConversationRequest{Key: wsConvKey, InstallationID: "inst-ws", Owner: ownerA}, []byte(wsFullFrame)))
		if err != nil {
			t.Fatalf("Dial: %v", err)
		}
		t.Cleanup(func() { _ = c.CloseNow() })
		mustSend(t, c, wsFullFrame)
		recvUntil(t, c, 1)

		seen := make(chan struct{}) // Recv 已处理完帧 1 的整个响应
		done := make(chan error, 1)
		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			for {
				data, err := c.Recv(ctx)
				if err != nil {
					done <- err
					return
				}
				if gjson.GetBytes(data, "type").String() == responseEventCompleted {
					close(seen)
					done <- nil
					return
				}
			}
		}()
		tr.stallNext(seen)
		mustSend(t, c, wsNewUserIncr) // 连接内新轮：ts 只进连接快照
		if err := <-done; err != nil {
			t.Fatalf("Recv: %v", err)
		}
		if !tr.stalled.Load() {
			t.Fatal("写帧未在 Write 返回前停住（测试无效）")
		}
		mustSend(t, c, wsFollowIncr)
		up.waitFrames(t, 3)
		fs := up.sentFrames(t)
		if frameTurnID(t, fs[2]) != frameTurnID(t, fs[1]) || frameTurnState(t, fs[2]) != "ts-1" {
			t.Fatalf("极快 created 下 ts 归属错位: turn=%q/%q ts=%q", frameTurnID(t, fs[2]), frameTurnID(t, fs[1]), frameTurnState(t, fs[2]))
		}
		if st := book.Stats(); st.WSLateTurnStates != 0 {
			t.Fatalf("不应有迟到 ts: %+v", st)
		}
	})

	// 写闸内写帧失败：本帧身份撤销（pop 跳过）、快照换回。
	t.Run("WriteFailureInsideGateRollsBack", func(t *testing.T) {
		book := newTestBook(NewMemoryConversationStore(), nil)
		up := newTSUpstream(t, nil)
		tr := newStallTransport(t, up.srv.URL)
		c, err := Dial(context.Background(), PAT("pat-conv"), Identity{}, WithTransport(tr),
			WithConversation(book, ConversationRequest{Key: wsConvKey, InstallationID: "inst-ws", Owner: ownerA}, []byte(wsFullFrame)))
		if err != nil {
			t.Fatalf("Dial: %v", err)
		}
		t.Cleanup(func() { _ = c.CloseNow() })
		mustSend(t, c, wsFullFrame)
		up.waitFrames(t, 1)
		before := c.conv.state.Load()
		tr.failNext.Store(true) // 已登记、网络写失败
		if err := c.Send(context.Background(), []byte(wsNewUserIncr)); err == nil {
			t.Fatal("写帧应失败")
		}
		if c.conv.state.Load() != before {
			t.Fatal("写失败应换回原快照")
		}
		if f := c.conv.pop(); f == nil || f.turnID != before.turn.ID {
			t.Fatalf("FIFO 头应是帧 0 的身份: %+v", f)
		}
		if f := c.conv.pop(); f != nil {
			t.Fatalf("写失败的身份应已撤销: %+v", f)
		}
	})

	t.Run("WriteFailureNotQueued", func(t *testing.T) {
		book := newTestBook(NewMemoryConversationStore(), nil)
		up := newTSUpstream(t, nil)
		c := dialConversationClient(t, up, book, wsFullFrame)
		mustSend(t, c, wsFullFrame)
		before := c.conv.state.Load()
		n := len(c.conv.fifo)
		_ = c.CloseNow()
		if err := c.Send(context.Background(), []byte(wsNewUserIncr)); err == nil {
			t.Fatal("关闭后 Send 应失败")
		}
		if len(c.conv.fifo) != n || c.conv.state.Load() != before {
			t.Fatalf("写失败不得入队/改快照: fifo %d→%d", n, len(c.conv.fifo))
		}
	})

	t.Run("FIFOFullStopsCapture", func(t *testing.T) {
		book := newTestBook(NewMemoryConversationStore(), nil)
		up := newTSUpstream(t, func(i int, _ []byte) []string {
			if i < wsConversationFIFO+1 {
				return nil // 不回任何事件：身份堆积
			}
			return respondTurn("rx", "ts-x")
		})
		c := dialConversationClient(t, up, book, wsFullFrame)
		mustSend(t, c, wsFullFrame)
		for range wsConversationFIFO {
			mustSend(t, c, wsFollowIncr)
		}
		if !c.conv.untracked.Load() || book.Stats().WSUntrackedConnections != 1 {
			t.Fatalf("FIFO 满应停止捕获: %+v", book.Stats())
		}
		mustSend(t, c, wsFollowIncr)
		recvUntil(t, c, 1)
		mustSend(t, c, wsFollowIncr)
		up.waitFrames(t, wsConversationFIFO+3)
		fs := up.sentFrames(t)
		if ts := frameTurnState(t, fs[len(fs)-1]); ts != "" {
			t.Fatalf("停止捕获后仍捕获了 ts: %q", ts)
		}
	})

	t.Run("LateTurnStateNotWrittenToNewTurn", func(t *testing.T) {
		book := newTestBook(NewMemoryConversationStore(), nil)
		up := newTSUpstream(t, func(i int, _ []byte) []string {
			switch i {
			case 0:
				return []string{tsCreatedFrame("r0")}
			case 1:
				return append([]string{tsMetadataFrame("r0", "late-A"), tsCompletedFrame("r0")}, respondTurn("r1", "ts-B")...)
			}
			return nil
		})
		c := dialConversationClient(t, up, book, wsFullFrame)
		mustSend(t, c, wsFullFrame)
		mustSend(t, c, wsNewUserIncr) // 连接内新轮 B（Recv 之前已发布快照）
		recvUntil(t, c, 2)
		mustSend(t, c, wsFollowIncr)
		up.waitFrames(t, 3)
		fs := up.sentFrames(t)
		if frameTurnID(t, fs[2]) != frameTurnID(t, fs[1]) || frameTurnState(t, fs[2]) != "ts-B" {
			t.Fatalf("旧轮 ts 写进了新轮: turn=%q ts=%q", frameTurnID(t, fs[2]), frameTurnState(t, fs[2]))
		}
		if book.Stats().WSLateTurnStates != 1 {
			t.Fatalf("WSLateTurnStates = %d", book.Stats().WSLateTurnStates)
		}
		// 旧轮来自会话簿：迟到的 ts 仍交给它绑定的 Sink。
		id := book.Prepare(bgctx, ConversationRequest{Key: wsConvKey, Owner: ownerA, Input: AnalyzeConversationInput([]byte(followPayload))})
		if id.Turn.ID != frameTurnID(t, fs[0]) || id.Turn.State != "late-A" {
			t.Fatalf("会话簿旧轮 = %+v", id.Turn)
		}
	})

	t.Run("ConcurrentSendRecv", func(t *testing.T) {
		book := newTestBook(NewMemoryConversationStore(), nil)
		up := newTSUpstream(t, func(i int, _ []byte) []string {
			return respondTurn("r"+strconv.Itoa(i), "ts-"+strconv.Itoa(i))
		})
		c := dialConversationClient(t, up, book, wsFullFrame)
		const n = 60
		var wg sync.WaitGroup
		var recvErr error
		window := make(chan struct{}, 8) // 在飞帧上限：低于 FIFO 容量
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			for left := n; left > 0; {
				data, err := c.Recv(ctx)
				if err != nil {
					recvErr = err
					return
				}
				if gjson.GetBytes(data, "type").String() == responseEventCompleted {
					left--
					<-window
				}
			}
		}()
		frames := []string{wsFullFrame, wsFollowIncr, wsNewUserIncr, wsFollowIncr, wsPrewarmFrame}
		for i := range n {
			window <- struct{}{}
			mustSend(t, c, frames[i%len(frames)])
		}
		wg.Wait()
		if recvErr != nil {
			t.Fatalf("Recv: %v", recvErr)
		}
		if c.conv.untracked.Load() {
			t.Fatal("并发收发不应触发 FIFO 满")
		}
		if len(c.conv.fifo) != 0 {
			t.Fatalf("FIFO 残留 %d", len(c.conv.fifo))
		}
	})
}

// WS→WS 会话路径哨兵不出网（全量帧、增量帧、客户端预热帧、握手头）。
func TestWSConversationClientValuesNeverEgress(t *testing.T) {
	const sentinel = "SENTINEL-c0ffee"
	meta := `"client_metadata":{"session_id":"` + sentinel + `-sess","thread_id":"` + sentinel + `-thr","x-codex-turn-state":"` + sentinel + `-ts","x-codex-window-id":"` + sentinel + `-win","turn_id":"` + sentinel + `-turn","x-codex-turn-metadata":"{\"session_id\":\"` + sentinel + `\",\"turn_id\":\"` + sentinel + `\",\"workspaces\":{\"/home/` + sentinel + `\":{}}}"}`
	common := `"prompt_cache_key":"` + sentinel + `-pck","session_id":"` + sentinel + `","conversation_id":"` + sentinel + `",` + meta
	prewarm := `{"type":"response.create","model":"gpt-5.5","input":[],"generate":false,` + common + `}`
	full := `{"type":"response.create","model":"gpt-5.5","input":[` + userHi + `],` + common + `}`
	incr := `{"type":"response.create","model":"gpt-5.5","previous_response_id":"r1","input":[` + toolOutput + `],` + common + `}`

	book := newTestBook(NewMemoryConversationStore(), nil)
	up := newTSUpstream(t, nil)
	key := NewConversationKey("dom-"+sentinel, 1, "conv-"+sentinel)
	c, err := Dial(context.Background(), PAT("pat-conv"), Identity{},
		WithTransport(newFixedTransport(t, wsConvURL, up.srv.URL)),
		WithConversation(book, ConversationRequest{Key: key, InstallationID: "inst-ws", Owner: ownerA}, []byte(prewarm)))
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer c.CloseNow()
	for _, f := range []string{prewarm, full, incr, prewarm} {
		mustSend(t, c, f)
	}
	up.waitFrames(t, 4)
	for i, f := range up.sentFrames(t) {
		if strings.Contains(string(f), "SENTINEL") {
			t.Fatalf("帧 %d 含哨兵: %s", i, f)
		}
		if gjson.GetBytes(f, "client_metadata.session_id").String() == "" {
			t.Fatalf("帧 %d 缺合成身份", i)
		}
	}
	for k, vs := range up.handshakeHeaders(t, 0) {
		for _, v := range vs {
			if strings.Contains(v, "SENTINEL") {
				t.Fatalf("握手头 %s 含哨兵: %s", k, v)
			}
		}
	}
}

// stallTransport 把请求改写到测试服务器，并能让下一次网络写在字节交给内核之后、
// Write 返回之前停住（直到给定通道关闭）：模拟"帧已上线、写方尚未返回"时上游
// 立即回包。
type stallTransport struct {
	srv      *url.URL
	rt       *http.Transport
	gate     atomic.Pointer[chan struct{}]
	failNext atomic.Bool // 下一次网络写直接失败
	stalled  atomic.Bool // 确实停住过、且等到了通道关闭
}

func newStallTransport(t *testing.T, srvBase string) *stallTransport {
	t.Helper()
	u, err := url.Parse(srvBase)
	if err != nil {
		t.Fatal(err)
	}
	st := &stallTransport{srv: u}
	var d net.Dialer
	st.rt = &http.Transport{DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
		cn, err := d.DialContext(ctx, network, addr)
		if err != nil {
			return nil, err
		}
		return &stallConn{Conn: cn, st: st}, nil
	}}
	t.Cleanup(st.rt.CloseIdleConnections)
	return st
}

func (st *stallTransport) stallNext(until chan struct{}) { st.gate.Store(&until) }

func (st *stallTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	clone := req.Clone(req.Context())
	u := *req.URL
	u.Scheme, u.Host = st.srv.Scheme, st.srv.Host
	clone.URL = &u
	return st.rt.RoundTrip(clone)
}

type stallConn struct {
	net.Conn
	st *stallTransport
}

func (c *stallConn) Write(p []byte) (int, error) {
	if c.st.failNext.CompareAndSwap(true, false) {
		return 0, errors.New("stallConn: write failed")
	}
	n, err := c.Conn.Write(p)
	if g := c.st.gate.Swap(nil); g != nil {
		select {
		case <-*g:
			c.st.stalled.Store(true)
		case <-time.After(5 * time.Second):
		}
	}
	return n, err
}
