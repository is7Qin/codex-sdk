package codexsdk

import (
	"context"
	"errors"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// ---- 测试替身 ----

// countingStore 包一层 ConversationStore：按方法计数，可注入失败、阻塞与陈旧回填。
type countingStore struct {
	inner                         atomic.Pointer[ConversationStore]
	opens, advances, loads, saves atomic.Int64
	fail                          atomic.Bool   // Open/Advance/Load 返回错误
	block                         atomic.Bool   // Open/Advance/Load 阻塞到 ctx 结束
	hold                          chan struct{} // 非 nil：Open 先等它关闭（或 ctx 结束）
	entered                       chan struct{} // hold 时每次进入 Open 发一个信号
	staleConv                     atomic.Bool   // SaveTurns 返回的对话记录窗口号置 0
	failItems                     atomic.Int64  // SaveTurns 前 N 条结果 OK=false（附无意义的值）
}

func newCountingStore(inner ConversationStore) *countingStore {
	s := &countingStore{}
	s.inner.Store(&inner)
	return s
}

func (s *countingStore) store() ConversationStore { return *s.inner.Load() }
func (s *countingStore) reset(inner ConversationStore) {
	s.inner.Store(&inner)
}
func (s *countingStore) calls() int64 {
	return s.opens.Load() + s.advances.Load() + s.loads.Load()
}

var errStoreDown = errors.New("store down")

func (s *countingStore) gate(ctx context.Context) error {
	if s.hold != nil {
		s.entered <- struct{}{}
		select {
		case <-s.hold:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	if s.block.Load() {
		<-ctx.Done()
		return ctx.Err()
	}
	if s.fail.Load() {
		return errStoreDown
	}
	return nil
}

func (s *countingStore) Open(ctx context.Context, key ConversationKey, fresh ConversationRecord) (ConversationRecord, bool, error) {
	s.opens.Add(1)
	if err := s.gate(ctx); err != nil {
		return ConversationRecord{}, false, err
	}
	return s.store().Open(ctx, key, fresh)
}

func (s *countingStore) Advance(ctx context.Context, key ConversationKey, o, n uint64, w string) (ConversationRecord, error) {
	s.advances.Add(1)
	if err := s.gate(ctx); err != nil {
		return ConversationRecord{}, err
	}
	return s.store().Advance(ctx, key, o, n, w)
}

func (s *countingStore) Load(ctx context.Context, key ConversationKey, turn uint64) (ConversationRecord, bool, TurnRecord, bool, error) {
	s.loads.Add(1)
	if err := s.gate(ctx); err != nil {
		return ConversationRecord{}, false, TurnRecord{}, false, err
	}
	return s.store().Load(ctx, key, turn)
}

func (s *countingStore) SaveTurns(ctx context.Context, batch []TurnWrite) ([]TurnResult, error) {
	s.saves.Add(1)
	res, err := s.store().SaveTurns(ctx, batch)
	if err == nil && s.staleConv.Load() {
		for i := range res {
			res[i].Conv.WindowN = 0
		}
	}
	if n := s.failItems.Swap(0); err == nil && n > 0 {
		// 单条失败：OK=false 时其余字段无意义，这里故意填上看似更新的垃圾值，
		// 会话簿用了就会被测出来。
		for i := range min(int(n), len(res)) {
			res[i] = TurnResult{
				Conv: ConversationRecord{Thread: "garbage-thread", WindowN: 1 << 40, ContextWindowID: "garbage-win"},
				Turn: TurnRecord{TurnID: "garbage-turn", StartedMs: 1, TS: "garbage-ts", TSOwner: ownerA},
			}
		}
	}
	return res, err
}

// testClock 是可推进的时钟。
type testClock struct{ ns atomic.Int64 }

func newTestClock() *testClock {
	c := &testClock{}
	c.ns.Store(time.Unix(1_800_000_000, 0).UnixNano())
	return c
}
func (c *testClock) now() time.Time          { return time.Unix(0, c.ns.Load()) }
func (c *testClock) advance(d time.Duration) { c.ns.Add(int64(d)) }

// 对话输入样本：turnPayload = 一轮的第一个请求；followPayload = 同一轮的后续请求
// （同一锚点 + 工具调用与输出）。
const (
	userHi        = `{"role":"user","content":[{"type":"input_text","text":"hi"}]}`
	toolCall      = `{"type":"function_call","call_id":"c1","name":"f","arguments":"{}"}`
	toolOutput    = `{"type":"function_call_output","call_id":"c1","output":"ok"}`
	assistantMsg  = `{"type":"message","role":"assistant","content":[{"type":"output_text","text":"done"}]}`
	turnPayload   = `{"model":"gpt-5.5","input":[` + userHi + `]}`
	followPayload = `{"model":"gpt-5.5","input":[` + userHi + `,` + toolCall + `,` + toolOutput + `]}`
	// 第二轮说了同样的话：锚点下标不同。
	turn2Payload = `{"model":"gpt-5.5","input":[` + userHi + `,` + toolCall + `,` + toolOutput + `,` + assistantMsg + `,` + userHi + `]}`
)

func compactionPayload(mark string) string {
	return `{"model":"gpt-5.5","input":[{"type":"compaction","encrypted_content":"` + mark + `"},` + userHi + `]}`
}

var (
	ownerA = [16]byte{1}
	ownerB = [16]byte{2}
)

func convReq(key ConversationKey, payload string, owner [16]byte) ConversationRequest {
	return ConversationRequest{Key: key, InstallationID: "inst-1", Owner: owner, Input: AnalyzeConversationInput([]byte(payload))}
}

func newTestBook(s ConversationStore, clk *testClock, opts ...BookOption) *ConversationBook {
	if clk == nil {
		clk = newTestClock()
	}
	return NewConversationBook(s, append([]BookOption{withBookClock(clk.now)}, opts...)...)
}

func mustFlush(t *testing.T, b *ConversationBook) {
	t.Helper()
	if err := b.Flush(context.Background()); err != nil {
		t.Fatalf("Flush: %v", err)
	}
}

var bgctx = context.Background()

// ---- 验收 ----

// 写入落地后后续请求落到另一副本：恰 1 次 Load，轮次号与 ts 相同；再来 0 次；
// 同一副本首请求后的后续请求 0 次读。
func TestBookCrossReplicaFollowup(t *testing.T) {
	mem := NewMemoryConversationStore()
	sa, sb := newCountingStore(mem), newCountingStore(mem)
	a, b := newTestBook(sa, nil), newTestBook(sb, nil)
	key := NewConversationKey("dom", 7, "conv-1")

	idA := a.Prepare(bgctx, convReq(key, turnPayload, ownerA))
	if !idA.Turn.First || idA.Turn.ID == "" || idA.Session.ThreadID == "" || idA.Sink == nil {
		t.Fatalf("首请求身份 = %+v", idA)
	}
	if sa.opens.Load() != 1 || sa.loads.Load() != 0 {
		t.Fatalf("新一轮只应 Open: opens=%d loads=%d", sa.opens.Load(), sa.loads.Load())
	}
	idA.Sink.SetTurnState("ts-1")
	mustFlush(t, a)

	before := sa.calls()
	f := a.Prepare(bgctx, convReq(key, followPayload, ownerA))
	if sa.calls() != before {
		t.Fatalf("同一副本后续请求不应读存储（+%d）", sa.calls()-before)
	}
	if f.Turn.ID != idA.Turn.ID || f.Turn.State != "ts-1" || f.Turn.First {
		t.Fatalf("同副本后续 = %+v", f.Turn)
	}

	got := b.Prepare(bgctx, convReq(key, followPayload, ownerA))
	if sb.loads.Load() != 1 || sb.opens.Load() != 0 || sb.advances.Load() != 0 {
		t.Fatalf("副本 B 应恰 1 次 Load: loads=%d opens=%d advances=%d", sb.loads.Load(), sb.opens.Load(), sb.advances.Load())
	}
	if got.Session != idA.Session || got.Turn.ID != idA.Turn.ID || got.Turn.State != "ts-1" || got.Turn.StartedAtMs != idA.Turn.StartedAtMs {
		t.Fatalf("副本 B 身份 = %+v, 期望与 A 同线程同轮次同 ts", got)
	}
	again := b.Prepare(bgctx, convReq(key, followPayload, ownerA))
	if sb.calls() != 1 || again.Turn.ID != idA.Turn.ID {
		t.Fatalf("副本 B 第二次后续请求应 0 次调用: calls=%d", sb.calls())
	}
}

// 写入未落地时后续请求到 B：临时轮次、不入缓存；Flush 后 A、B 一致。
func TestBookFollowupBeforeFlushConverges(t *testing.T) {
	mem := NewMemoryConversationStore()
	sb := newCountingStore(mem)
	a, b := newTestBook(mem, nil), newTestBook(sb, nil)
	key := NewConversationKey("dom", 7, "conv-3")
	idA := a.Prepare(bgctx, convReq(key, turnPayload, ownerA))

	tmp := b.Prepare(bgctx, convReq(key, followPayload, ownerA))
	if tmp.Turn.ID == idA.Turn.ID || tmp.Turn.First || tmp.Sink != nil || tmp.Turn.State != "" {
		t.Fatalf("未落地时应为临时轮次: %+v", tmp)
	}
	if tmp.Session != idA.Session {
		t.Fatalf("临时轮次仍应在同一线程: %+v vs %+v", tmp.Session, idA.Session)
	}
	if st := b.Stats(); st.TemporaryTurns != 1 || st.TurnEntries != 0 {
		t.Fatalf("临时轮次不入缓存: %+v", st)
	}
	if len(b.queue) != 0 {
		t.Fatal("临时轮次不应写")
	}
	mustFlush(t, a)
	got := b.Prepare(bgctx, convReq(key, followPayload, ownerA))
	if got.Turn.ID != idA.Turn.ID || sb.loads.Load() != 2 {
		t.Fatalf("Flush 后 B 应收敛到 A 的轮次: %+v loads=%d", got.Turn, sb.loads.Load())
	}
}

// 换 API key、换缓存域 → 线程号不同；同域两 installation → 线程号相同、
// installation 不同。
func TestConversationKeyScope(t *testing.T) {
	b := newTestBook(NewMemoryConversationStore(), nil)
	base := NewConversationKey("dom", 1, "c")
	if base != NewConversationKey("dom", 1, "c") {
		t.Fatal("同输入应得同键")
	}
	// 长度前缀：拼接歧义不得撞键。
	if NewConversationKey("ab", 1, "c") == NewConversationKey("a", 1, "bc") {
		t.Fatal("长度前缀缺失")
	}
	thread := func(k ConversationKey, inst string) Identity {
		r := convReq(k, turnPayload, ownerA)
		r.InstallationID = inst
		return b.Prepare(bgctx, r)
	}
	x := thread(base, "inst-1")
	if y := thread(NewConversationKey("dom", 2, "c"), "inst-1"); y.Session.ThreadID == x.Session.ThreadID {
		t.Fatal("换 API key 线程号应不同")
	}
	if y := thread(NewConversationKey("dom2", 1, "c"), "inst-1"); y.Session.ThreadID == x.Session.ThreadID {
		t.Fatal("换缓存域线程号应不同")
	}
	y := thread(base, "inst-2")
	if y.Session.ThreadID != x.Session.ThreadID || y.InstallationID != "inst-2" || x.InstallationID != "inst-1" {
		t.Fatalf("同域两 installation: %+v / %+v", x, y)
	}
}

// ts 同 owner 首值生效、换 owner 覆盖、owner 不同不带；临时轮次 Sink 丢弃。
func TestBookTurnStateOwnerRules(t *testing.T) {
	mem := NewMemoryConversationStore()
	a := newTestBook(mem, nil)
	key := NewConversationKey("dom", 1, "conv-5")
	id := a.Prepare(bgctx, convReq(key, turnPayload, ownerA))
	id.Sink.SetTurnState("s1")
	id.Sink.SetTurnState("s2")
	if st := a.Stats(); st.TurnStateIgnored != 1 {
		t.Fatalf("同 owner 第二个值应被忽略并计数: %+v", st)
	}
	if got := a.Prepare(bgctx, convReq(key, followPayload, ownerA)).Turn.State; got != "s1" {
		t.Fatalf("同 owner 首值 = %q", got)
	}
	fb := a.Prepare(bgctx, convReq(key, followPayload, ownerB))
	if fb.Turn.State != "" {
		t.Fatalf("owner 不同不得带 ts: %q", fb.Turn.State)
	}
	fb.Sink.SetTurnState("o2") // 换 owner：覆盖
	if got := a.Prepare(bgctx, convReq(key, followPayload, ownerB)).Turn.State; got != "o2" {
		t.Fatalf("换 owner 后 = %q", got)
	}
	if got := a.Prepare(bgctx, convReq(key, followPayload, ownerA)).Turn.State; got != "" {
		t.Fatalf("原 owner 不得再带别人的 ts: %q", got)
	}
	// 迟到的旧 Sink（同 owner A，TS 已归 B）：owner 不同 ⇒ 覆盖回 A。
	id.Sink.SetTurnState("s3")
	if got := a.Prepare(bgctx, convReq(key, followPayload, ownerA)).Turn.State; got != "s3" {
		t.Fatalf("A 覆盖回来 = %q", got)
	}
	mustFlush(t, a)
	_, _, rec, tf, _ := mem.Load(bgctx, key, AnalyzeConversationInput([]byte(turnPayload)).TurnKey)
	if !tf || rec.TS != "s3" || rec.TSOwner != ownerA || rec.TurnID != id.Turn.ID {
		t.Fatalf("存储 = %+v", rec)
	}
	// 回填保留本地 ts：SaveTurns 返回 TS=="" 时本地非空不清掉。
	b2 := newTestBook(mem, nil)
	key2 := NewConversationKey("dom", 1, "conv-5b")
	id2 := b2.Prepare(bgctx, convReq(key2, turnPayload, ownerA))
	// 创建写入出队落地之前 Sink 写了本地 ts：返回值 TS=="" 不得清掉本地。
	create := <-b2.queue
	id2.Sink.SetTurnState("late")
	if err := b2.save(bgctx, []TurnWrite{create}); err != nil {
		t.Fatal(err)
	}
	if got := b2.Prepare(bgctx, convReq(key2, followPayload, ownerA)).Turn.State; got != "late" {
		t.Fatalf("回填清掉了本地 ts: %q", got)
	}
	// 临时轮次 Sink 丢弃。
	b3 := newTestBook(mem, nil)
	tmp := b3.Prepare(bgctx, convReq(NewConversationKey("dom", 1, "conv-5c"), followPayload, ownerA))
	if tmp.Sink != nil {
		t.Fatal("临时轮次的 Sink 应为丢弃（nil）")
	}
}

// 窗口推进规则；另一 Book 同压缩项得同窗口；回填不让窗口倒退。
func TestBookWindowAdvance(t *testing.T) {
	mem := NewMemoryConversationStore()
	sa := newCountingStore(mem)
	a := newTestBook(sa, nil)
	key := NewConversationKey("dom", 1, "conv-6")
	id0 := a.Prepare(bgctx, convReq(key, turnPayload, ownerA))
	if id0.Window.Number != 0 || id0.Session.WindowID != id0.Session.ThreadID+":0" {
		t.Fatalf("初始窗口 = %+v / %q", id0.Window, id0.Session.WindowID)
	}
	x := a.Prepare(bgctx, convReq(key, compactionPayload("X"), ownerA))
	if x.Window.Number != 1 || sa.advances.Load() != 1 || x.Window.ContextID == id0.Window.ContextID {
		t.Fatalf("新压缩项应推进: %+v advances=%d", x.Window, sa.advances.Load())
	}
	if x.Session.WindowID != x.Session.ThreadID+":1" {
		t.Fatalf("WindowID = %q", x.Session.WindowID)
	}
	if again := a.Prepare(bgctx, convReq(key, compactionPayload("X"), ownerA)); again.Window != x.Window || sa.advances.Load() != 1 {
		t.Fatalf("重复压缩项不推进、不发命令: %+v advances=%d", again.Window, sa.advances.Load())
	}
	y := a.Prepare(bgctx, convReq(key, compactionPayload("Y"), ownerA))
	if y.Window.Number != 2 || sa.advances.Load() != 2 {
		t.Fatalf("第二个压缩项: %+v", y.Window)
	}
	if old := a.Prepare(bgctx, convReq(key, compactionPayload("X"), ownerA)); old.Window != y.Window || sa.advances.Load() != 2 {
		t.Fatalf("旧压缩项（=Prev）不推进且不发命令: %+v advances=%d", old.Window, sa.advances.Load())
	}
	sb := newCountingStore(mem)
	b := newTestBook(sb, nil)
	if got := b.Prepare(bgctx, convReq(key, compactionPayload("Y"), ownerA)); got.Window != y.Window || got.Session != y.Session || sb.advances.Load() != 0 {
		t.Fatalf("另一 Book 同压缩项: %+v advances=%d", got.Window, sb.advances.Load())
	}
	// 回填不让窗口倒退：SaveTurns 返回旧窗口号。
	sa.staleConv.Store(true)
	mustFlush(t, a)
	if got := a.Prepare(bgctx, convReq(key, turnPayload, ownerA)); got.Window.Number != 2 {
		t.Fatalf("回填把窗口推回 %d", got.Window.Number)
	}
}

// 并发首次 Open 只有一个线程号；存储清空后经 SaveTurns 补回并收敛。
func TestBookConcurrentOpenAndRecovery(t *testing.T) {
	mem := newCountingStore(NewMemoryConversationStore())
	books := []*ConversationBook{newTestBook(mem, nil), newTestBook(mem, nil), newTestBook(mem, nil)}
	key := NewConversationKey("dom", 1, "conv-7")
	var wg sync.WaitGroup
	threads := make([]string, 48)
	for i := range threads {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			threads[i] = books[i%3].Prepare(bgctx, convReq(key, turnPayload, ownerA)).Session.ThreadID
		}(i)
	}
	wg.Wait()
	for _, th := range threads {
		if th != threads[0] || th == "" {
			t.Fatalf("并发首次 Open 得到不同线程号: %v", threads)
		}
	}
	for _, b := range books {
		mustFlush(t, b)
	}
	// 存储数据丢失。
	mem.reset(NewMemoryConversationStore())
	a := books[0]
	a.Prepare(bgctx, convReq(key, turn2Payload, ownerA)) // 新一轮：本地命中，入写入队列
	mustFlush(t, a)                                      // SaveTurns 补回对话记录
	fresh := newTestBook(mem, nil)
	if got := fresh.Prepare(bgctx, convReq(key, turnPayload, ownerA)); got.Session.ThreadID != threads[0] {
		t.Fatalf("补回后新副本线程号 = %q, 期望 %q", got.Session.ThreadID, threads[0])
	}
}

// 存储故障、30ms 超时、熔断与半开、队列满：Prepare 不出错、一次性身份、计数；
// 熔断期间零命令。
func TestBookDegradation(t *testing.T) {
	t.Run("StoreErrors", func(t *testing.T) {
		s := newCountingStore(NewMemoryConversationStore())
		s.fail.Store(true)
		b := newTestBook(s, nil)
		key := NewConversationKey("dom", 1, "c")
		x := b.Prepare(bgctx, convReq(key, turnPayload, ownerA))
		y := b.Prepare(bgctx, convReq(key, followPayload, ownerA))
		for _, id := range []Identity{x, y} {
			if id.Session.ThreadID == "" || id.Turn.ID == "" || id.Window.Number != 0 || id.Sink != nil || id.Window.ContextID == "" {
				t.Fatalf("一次性身份 = %+v", id)
			}
		}
		if x.Session.ThreadID == y.Session.ThreadID || !x.Turn.First || y.Turn.First {
			t.Fatalf("一次性身份应每次新线程: %+v / %+v", x, y)
		}
		st := b.Stats()
		if st.OneShotIdentities != 2 || st.ConversationEntries != 0 || st.TurnEntries != 0 || st.OpenFailures != 1 || st.LoadFailures != 1 {
			t.Fatalf("计数 = %+v", st)
		}
	})
	t.Run("Budget30ms", func(t *testing.T) {
		s := newCountingStore(NewMemoryConversationStore())
		s.block.Store(true)
		b := newTestBook(s, nil)
		start := time.Now()
		id := b.Prepare(bgctx, convReq(NewConversationKey("dom", 1, "c"), turnPayload, ownerA))
		if el := time.Since(start); el < 25*time.Millisecond || el > time.Second {
			t.Fatalf("预算 30ms，实际 %v", el)
		}
		if id.Session.ThreadID == "" || b.Stats().OneShotIdentities != 1 {
			t.Fatalf("超时应降级: %+v", b.Stats())
		}
	})
	t.Run("BreakerAndHalfOpen", func(t *testing.T) {
		s := newCountingStore(NewMemoryConversationStore())
		s.fail.Store(true)
		clk := newTestClock()
		b := newTestBook(s, clk)
		b.cfg.budget = 10 * time.Second // 半开探测被 hold 住期间不因预算超时
		p := func(i int) Identity {
			return b.Prepare(bgctx, convReq(NewConversationKey("dom", int64(i), "c"), turnPayload, ownerA))
		}
		s.block.Store(false)
		for i := range 5 {
			p(i)
		}
		if s.calls() != 5 || b.Stats().BreakerOpens != 1 {
			t.Fatalf("5 次失败后应打开: calls=%d %+v", s.calls(), b.Stats())
		}
		for i := range 10 {
			if id := p(100 + i); id.Session.ThreadID == "" {
				t.Fatal("熔断期间应降级")
			}
		}
		if s.calls() != 5 || b.Stats().BreakerRejects != 10 {
			t.Fatalf("熔断期间应零命令: calls=%d %+v", s.calls(), b.Stats())
		}
		// 半开：探测失败 → 再开 5 秒。
		clk.advance(5 * time.Second)
		p(200)
		if s.calls() != 6 || b.Stats().BreakerOpens != 2 {
			t.Fatalf("半开探测失败: calls=%d %+v", s.calls(), b.Stats())
		}
		p(201)
		if s.calls() != 6 {
			t.Fatal("再开期间应零命令")
		}
		// 半开只放行一个探测：探测在飞时其余调用被拒。
		clk.advance(5 * time.Second)
		s.fail.Store(false)
		s.hold, s.entered = make(chan struct{}), make(chan struct{}, 1)
		done := make(chan Identity)
		go func() { done <- p(300) }()
		<-s.entered
		for i := range 5 {
			p(400 + i)
		}
		if s.calls() != 7 {
			t.Fatalf("半开期间只应有 1 个探测: calls=%d", s.calls())
		}
		close(s.hold)
		<-done
		s.hold = nil
		// 探测成功 → 关闭：后续调用照常放行。
		before := s.calls()
		if id := p(500); id.Sink == nil {
			t.Fatalf("熔断关闭后应走正常路径: %+v", id)
		}
		if s.calls() != before+1 {
			t.Fatalf("关闭后应放行: %d → %d", before, s.calls())
		}
	})
	t.Run("LoadFailsKeepsLocalConversation", func(t *testing.T) {
		// 本地已有对话记录、只是轮次未命中且 Load 失败：沿用本地对话记录（不换线程），
		// 给临时轮次（First=false、无 Sink），不退回一次性身份。
		s := newCountingStore(NewMemoryConversationStore())
		b := newTestBook(s, nil)
		key := NewConversationKey("dom", 1, "c")
		x := b.Prepare(bgctx, convReq(key, turnPayload, ownerA))
		if x.Sink == nil || !x.Turn.First {
			t.Fatalf("首请求应走正常路径: %+v", x)
		}
		before := b.Stats()
		s.fail.Store(true)
		// 第二轮（turn2Payload）的后续请求：本副本没见过这一轮 ⇒ 轮次缓存未命中 ⇒ Load。
		follow2 := `{"model":"gpt-5.5","input":[` + userHi + `,` + toolCall + `,` + toolOutput + `,` + assistantMsg + `,` + userHi + `,` +
			`{"type":"function_call","call_id":"c2","name":"f","arguments":"{}"},{"type":"function_call_output","call_id":"c2","output":"ok"}]}`
		y := b.Prepare(bgctx, convReq(key, follow2, ownerA))
		if y.Session.ThreadID != x.Session.ThreadID || y.Window != x.Window {
			t.Fatalf("Load 失败应沿用本地对话记录: x=%+v y=%+v", x.Session, y.Session)
		}
		if y.Turn.First || y.Sink != nil || y.Turn.ID == "" || y.Turn.ID == x.Turn.ID {
			t.Fatalf("应给临时轮次（First=false、无 Sink、新轮次号）: %+v", y.Turn)
		}
		st := b.Stats()
		if st.LoadFailures != before.LoadFailures+1 || st.TemporaryTurns != before.TemporaryTurns+1 || st.OneShotIdentities != before.OneShotIdentities {
			t.Fatalf("计数: before=%+v after=%+v", before, st)
		}
	})
	t.Run("CallerCancelNotCountedByBreaker", func(t *testing.T) {
		// 调用方 ctx 已取消导致的存储失败不计入熔断；半开探测被取消时归还探测名额。
		s := newCountingStore(NewMemoryConversationStore())
		s.fail.Store(true)
		clk := newTestClock()
		b := newTestBook(s, clk)
		cctx, cancel := context.WithCancel(context.Background())
		cancel()
		p := func(ctx context.Context, i int) Identity {
			return b.Prepare(ctx, convReq(NewConversationKey("dom", int64(i), "c"), turnPayload, ownerA))
		}
		for i := range 2 * bookBreakerFails {
			if id := p(cctx, i); id.Session.ThreadID == "" {
				t.Fatal("取消也应降级出身份")
			}
		}
		if st := b.Stats(); st.BreakerOpens != 0 || st.BreakerRejects != 0 || s.calls() != 2*bookBreakerFails {
			t.Fatalf("取消不得计入熔断: calls=%d %+v", s.calls(), st)
		}
		// 真实失败打开熔断，到期后半开：被取消的探测不得把熔断器再开，名额要归还。
		for i := range bookBreakerFails {
			p(bgctx, 100+i)
		}
		if b.Stats().BreakerOpens != 1 {
			t.Fatalf("真实失败应打开熔断: %+v", b.Stats())
		}
		clk.advance(5 * time.Second)
		calls := s.calls()
		p(cctx, 200) // 半开探测，调用方已取消
		if s.calls() != calls+1 || b.Stats().BreakerOpens != 1 {
			t.Fatalf("被取消的探测不得再开熔断: calls=%d→%d %+v", calls, s.calls(), b.Stats())
		}
		s.fail.Store(false)
		if id := p(bgctx, 201); id.Sink == nil || s.calls() != calls+2 {
			t.Fatalf("探测名额应已归还，下一次作为探测放行并关闭熔断: calls=%d %+v", s.calls(), id)
		}
	})
	t.Run("QueueFull", func(t *testing.T) {
		b := newTestBook(NewMemoryConversationStore(), nil, WithBookQueueSize(2))
		for i := range 3 {
			if id := b.Prepare(bgctx, convReq(NewConversationKey("dom", int64(i), "c"), turnPayload, ownerA)); !id.Turn.First {
				t.Fatal("队列满不影响 Prepare")
			}
		}
		if st := b.Stats(); st.QueueDrops != 1 {
			t.Fatalf("QueueDrops = %d", st.QueueDrops)
		}
	})
}

// 两轮同样的话 → 不同轮次号；同一副本并发重试一轮首请求 → 同轮次号且 First。
func TestBookTurnIdentity(t *testing.T) {
	b := newTestBook(NewMemoryConversationStore(), nil)
	key := NewConversationKey("dom", 1, "conv-9")
	t1 := b.Prepare(bgctx, convReq(key, turnPayload, ownerA))
	t2 := b.Prepare(bgctx, convReq(key, turn2Payload, ownerA))
	if t1.Turn.ID == t2.Turn.ID || !t2.Turn.First {
		t.Fatalf("两轮同样的话应是不同轮次: %q %q", t1.Turn.ID, t2.Turn.ID)
	}
	key2 := NewConversationKey("dom", 1, "conv-9b")
	b.Prepare(bgctx, convReq(key2, `{"model":"m","input":[]}`, ownerA)) // 先建对话记录
	var wg sync.WaitGroup
	ids := make([]Turn, 32)
	for i := range ids {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			ids[i] = b.Prepare(bgctx, convReq(key2, turnPayload, ownerA)).Turn
		}(i)
	}
	wg.Wait()
	for _, tr := range ids {
		if tr.ID != ids[0].ID || !tr.First {
			t.Fatalf("并发重试应同轮次且 First: %+v vs %+v", tr, ids[0])
		}
	}
	// 无锚点：按新的一轮，但不写、不入缓存。
	before := len(b.queue)
	na := b.Prepare(bgctx, convReq(key2, `{"model":"m","input":[]}`, ownerA))
	if !na.Turn.First || na.Sink != nil || len(b.queue) != before {
		t.Fatalf("无锚点: %+v queue %d→%d", na, before, len(b.queue))
	}
}

// 清扫 30s 节流、TTL 与容量淘汰。
func TestBookCacheEviction(t *testing.T) {
	clk := newTestClock()
	b := newTestBook(NewMemoryConversationStore(), clk, WithBookCapacity(3, 2))
	keys := make([]ConversationKey, 5)
	for i := range keys {
		keys[i] = NewConversationKey("dom", int64(i), "c")
		b.Prepare(bgctx, convReq(keys[i], turnPayload, ownerA))
		clk.advance(2 * time.Second)
	}
	mustFlush(t, b)
	if st := b.Stats(); st.Sweeps != 0 || st.ConversationEntries != 5 || st.TurnEntries != 5 {
		t.Fatalf("30s 内不应清扫: %+v", st)
	}
	clk.advance(25 * time.Second) // 距创建 35s
	mustFlush(t, b)
	st := b.Stats()
	if st.Sweeps != 1 || st.ConversationEntries != 3 || st.TurnEntries != 2 || st.SweepDeleted != 5 {
		t.Fatalf("容量淘汰: %+v", st)
	}
	// 留下的是最近使用的。
	for i, k := range keys {
		_, ok := b.convs.Load(k)
		if want := i >= 2; ok != want {
			t.Fatalf("对话条目 %d 存在=%v, 期望 %v", i, ok, want)
		}
	}
	mustFlush(t, b)
	if b.Stats().Sweeps != 1 {
		t.Fatal("第二次 Flush 不应清扫（节流）")
	}
	// TTL：轮次 10min、对话 1h。
	clk.advance(11 * time.Minute)
	b.Prepare(bgctx, convReq(keys[4], followPayload, ownerA)) // 摸一下 keys[4] 的两个条目
	mustFlush(t, b)
	if st := b.Stats(); st.TurnEntries != 1 || st.ConversationEntries != 3 {
		t.Fatalf("轮次 TTL: %+v", st)
	}
	clk.advance(61 * time.Minute)
	mustFlush(t, b)
	if st := b.Stats(); st.TurnEntries != 0 || st.ConversationEntries != 0 {
		t.Fatalf("对话 TTL: %+v", st)
	}
}

// Prepare 命中路径分配 ≤ 4、零存储调用；book 文件无 Mutex。
func TestBookPrepareHitBounds(t *testing.T) {
	s := newCountingStore(NewMemoryConversationStore())
	b := newTestBook(s, nil)
	key := NewConversationKey("dom", 1, "hit")
	b.Prepare(bgctx, convReq(key, turnPayload, ownerA)).Sink.SetTurnState("ts")
	req := convReq(key, followPayload, ownerA)
	if id := b.Prepare(bgctx, req); id.Turn.State != "ts" {
		t.Fatalf("预热命中失败: %+v", id)
	}
	before := s.calls()
	allocs := testing.AllocsPerRun(1000, func() { _ = b.Prepare(bgctx, req) })
	if allocs > 4 {
		t.Fatalf("命中路径分配 %.1f > 4", allocs)
	}
	if s.calls() != before {
		t.Fatalf("命中路径不应调用存储（+%d）", s.calls()-before)
	}
	for _, f := range []string{"conversation_book.go", "conversation_store.go", "ws_conversation.go"} {
		src, err := os.ReadFile(f)
		if err != nil {
			t.Fatal(err)
		}
		for _, bad := range []string{"sync.Mutex", "sync.RWMutex"} {
			if strings.Contains(string(src), bad) {
				t.Fatalf("%s 含 %s", f, bad)
			}
		}
	}
}

// BenchmarkBookPrepareHit：后续请求两缓存命中（-cpu 1,8,20 看多核扩展）。
func BenchmarkBookPrepareHit(b *testing.B) {
	book := NewConversationBook(NewMemoryConversationStore())
	const nkeys = 64
	reqs := make([]ConversationRequest, nkeys)
	for i := range reqs {
		key := NewConversationKey("dom", int64(i), "bench")
		book.Prepare(bgctx, convReq(key, turnPayload, ownerA))
		reqs[i] = convReq(key, followPayload, ownerA)
	}
	b.ReportAllocs()
	b.ResetTimer()
	var seq atomic.Uint64
	b.RunParallel(func(pb *testing.PB) {
		r := reqs[seq.Add(1)%nkeys]
		for pb.Next() {
			_ = book.Prepare(bgctx, r)
		}
	})
}

// Flush 回填：TurnResult.OK=false 的条目只计数，不得回填本地缓存。
func TestBookFlushSkipsFailedItems(t *testing.T) {
	s := newCountingStore(NewMemoryConversationStore())
	b := newTestBook(s, nil)
	key := NewConversationKey("dom", 3, "flush-fail")
	id := b.Prepare(bgctx, convReq(key, turnPayload, ownerA))
	id.Sink.SetTurnState("ts-1")
	if len(b.queue) != 2 {
		t.Fatalf("写入队列 = %d", len(b.queue))
	}
	before := b.Stats()
	s.failItems.Store(2)
	mustFlush(t, b)
	st := b.Stats()
	if st.SaveItemFailures != 2 || st.SaveTurnsFailures != 0 {
		t.Fatalf("单条失败计数: %+v", st)
	}
	if st.ConversationEntries != before.ConversationEntries || st.TurnEntries != before.TurnEntries {
		t.Fatalf("条目数变化: %+v → %+v", before, st)
	}
	calls := s.calls()
	f := b.Prepare(bgctx, convReq(key, followPayload, ownerA))
	if s.calls() != calls {
		t.Fatal("本地缓存应仍命中")
	}
	if f.Session != id.Session || f.Window != id.Window || f.Turn.ID != id.Turn.ID || f.Turn.State != "ts-1" {
		t.Fatalf("失败条目回填了本地缓存: %+v / %+v", f, id)
	}
}
