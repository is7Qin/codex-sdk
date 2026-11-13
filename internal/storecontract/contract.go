// Package storecontract 是 codexsdk.ConversationStore 的契约测试套件：内存实现与
// redisstore 跑同一套（只供测试使用）。
package storecontract

import (
	"context"
	"math"
	"testing"
	"time"

	codexsdk "github.com/is7Qin/codex-sdk"
)

// Harness 是一个待测实现。Advance 把存储的时钟向前推 d（用于过期断言）。
type Harness struct {
	Store   codexsdk.ConversationStore
	Advance func(d time.Duration)
}

// Run 对 newHarness 产出的每个新实例跑全部契约用例。
func Run(t *testing.T, newHarness func(t *testing.T) Harness) {
	ctx := context.Background()
	key := func(s string) codexsdk.ConversationKey { return codexsdk.NewConversationKey("dom", 1, s) }
	rec := func(thread string) codexsdk.ConversationRecord {
		return codexsdk.ConversationRecord{Thread: thread, ContextWindowID: thread + "-w0"}
	}
	owner := func(b byte) [16]byte { var o [16]byte; o[0] = b; o[15] = b; return o }

	t.Run("OpenCreatesOnceAndReturnsExisting", func(t *testing.T) {
		h := newHarness(t)
		k := key("open")
		got, created, err := h.Store.Open(ctx, k, rec("t1"))
		if err != nil || !created || got != rec("t1") {
			t.Fatalf("first Open = %+v created=%v err=%v", got, created, err)
		}
		got, created, err = h.Store.Open(ctx, k, rec("t2"))
		if err != nil || created || got != rec("t1") {
			t.Fatalf("second Open = %+v created=%v err=%v, want existing t1", got, created, err)
		}
	})

	t.Run("OpenRenews24hAndLoadDoesNotRenew", func(t *testing.T) {
		h := newHarness(t)
		k := key("ttl")
		mustOpen(t, h, k, rec("t1"))
		h.Advance(23 * time.Hour)
		mustOpen(t, h, k, rec("tX")) // 续期
		h.Advance(23 * time.Hour)
		if _, cf, _, _, err := h.Store.Load(ctx, k, 1); err != nil || !cf {
			t.Fatalf("46h 后（中间 Open 续期）记录应仍在: cf=%v err=%v", cf, err)
		}
		h.Advance(2 * time.Hour) // 距上次 Open 25h；Load 不续期
		if _, cf, _, _, err := h.Store.Load(ctx, k, 1); err != nil || cf {
			t.Fatalf("Load 不续期：距上次写 25h 后应过期: cf=%v err=%v", cf, err)
		}
		got, created, _ := h.Store.Open(ctx, k, rec("t3"))
		if !created || got.Thread != "t3" {
			t.Fatalf("过期后 Open 应新建: %+v created=%v", got, created)
		}
	})

	t.Run("AdvanceRules", func(t *testing.T) {
		h := newHarness(t)
		k := key("adv")
		if got, err := h.Store.Advance(ctx, k, 0, 7, "w-x"); err != nil || got.Thread != "" {
			t.Fatalf("记录不存在：Advance = %+v err=%v, 期望零值", got, err)
		}
		mustOpen(t, h, k, rec("t1"))
		got, err := h.Store.Advance(ctx, k, 0, 7, "w1")
		want := codexsdk.ConversationRecord{Thread: "t1", WindowN: 1, ContextWindowID: "w1", Compaction: 7, PrevCompaction: 0}
		if err != nil || got != want {
			t.Fatalf("Advance(0→7) = %+v err=%v, want %+v", got, err, want)
		}
		// 重复（Compaction 已不是 0）：不变。
		if got, _ := h.Store.Advance(ctx, k, 0, 7, "w-dup"); got != want {
			t.Fatalf("重复 Advance 改变了记录: %+v", got)
		}
		// newMark == PrevCompaction：不变。
		if got, _ := h.Store.Advance(ctx, k, 7, 0, "w-back"); got != want {
			t.Fatalf("newMark==Prev 的 Advance 改变了记录: %+v", got)
		}
		want2 := codexsdk.ConversationRecord{Thread: "t1", WindowN: 2, ContextWindowID: "w2", Compaction: 9, PrevCompaction: 7}
		if got, _ := h.Store.Advance(ctx, k, 7, 9, "w2"); got != want2 {
			t.Fatalf("Advance(7→9) = %+v want %+v", got, want2)
		}
		// 迟到请求带旧压缩项（=Prev）：不推回去。
		if got, _ := h.Store.Advance(ctx, k, 9, 7, "w-late"); got != want2 {
			t.Fatalf("旧压缩项把窗口推动了: %+v", got)
		}
		// oldMark 不等于记录：不变。
		if got, _ := h.Store.Advance(ctx, k, 1, 11, "w-stale"); got != want2 {
			t.Fatalf("oldMark 不符却推进: %+v", got)
		}
		if conv, cf, _, _, _ := h.Store.Load(ctx, k, 1); !cf || conv != want2 {
			t.Fatalf("Load 后 = %+v", conv)
		}
	})

	t.Run("Uint64RoundTrip", func(t *testing.T) {
		h := newHarness(t)
		k := key("big")
		mustOpen(t, h, k, rec("t1"))
		big := uint64(math.MaxUint64)
		got, err := h.Store.Advance(ctx, k, 0, big, "wb")
		if err != nil || got.Compaction != big || got.WindowN != 1 {
			t.Fatalf("大数 Advance = %+v err=%v", got, err)
		}
		got, _ = h.Store.Advance(ctx, k, big, big-1, "wc")
		if got.Compaction != big-1 || got.PrevCompaction != big || got.WindowN != 2 {
			t.Fatalf("相邻大数应可区分: %+v", got)
		}
		res := mustSave(t, h, codexsdk.TurnWrite{Key: k, Turn: math.MaxUint64,
			Rec: codexsdk.TurnRecord{TurnID: "u1", StartedMs: math.MaxInt64}})
		if res[0].Turn.StartedMs != math.MaxInt64 {
			t.Fatalf("StartedMs 回读 = %d", res[0].Turn.StartedMs)
		}
	})

	t.Run("LoadReportsEachRecord", func(t *testing.T) {
		h := newHarness(t)
		k := key("load")
		if _, cf, _, tf, err := h.Store.Load(ctx, k, 5); err != nil || cf || tf {
			t.Fatalf("空存储 Load: cf=%v tf=%v err=%v", cf, tf, err)
		}
		mustOpen(t, h, k, rec("t1"))
		conv, cf, _, tf, _ := h.Store.Load(ctx, k, 5)
		if !cf || tf || conv != rec("t1") {
			t.Fatalf("仅对话记录: conv=%+v cf=%v tf=%v", conv, cf, tf)
		}
		tr := codexsdk.TurnRecord{TurnID: "u1", StartedMs: 1700000000123, TS: "ts-1", TSOwner: owner(1)}
		mustSave(t, h, codexsdk.TurnWrite{Key: k, Turn: 5, Conv: rec("t1"), Rec: tr, HasTS: true})
		conv, cf, got, tf, _ := h.Store.Load(ctx, k, 5)
		if !cf || !tf || conv != rec("t1") || got != tr {
			t.Fatalf("两条记录: conv=%+v cf=%v rec=%+v tf=%v", conv, cf, got, tf)
		}
		if _, _, _, tf, _ := h.Store.Load(ctx, k, 6); tf {
			t.Fatal("别的轮次键不应命中")
		}
		// 对话记录缺失、轮次记录存在。
		k2 := key("load-turn-only")
		mustSave(t, h, codexsdk.TurnWrite{Key: k2, Turn: 1, Conv: rec("t9"), Rec: codexsdk.TurnRecord{TurnID: "u9"}})
		h.Advance(25 * time.Hour) // 对话（24h）与轮次（1h）都过期
		if _, cf, _, tf, _ := h.Store.Load(ctx, k2, 1); cf || tf {
			t.Fatalf("过期后 Load: cf=%v tf=%v", cf, tf)
		}
	})

	t.Run("SaveTurnsRestoresConversationOnlyWhenMissing", func(t *testing.T) {
		h := newHarness(t)
		k := key("save-conv")
		w := codexsdk.TurnWrite{Key: k, Turn: 1,
			Conv: codexsdk.ConversationRecord{Thread: "local", WindowN: 3, ContextWindowID: "lw3", Compaction: 5, PrevCompaction: 4},
			Rec:  codexsdk.TurnRecord{TurnID: "u1", StartedMs: 10}}
		res := mustSave(t, h, w)
		if res[0].Conv != w.Conv {
			t.Fatalf("缺失时应以 Conv 补写（含窗口字段）: %+v", res[0].Conv)
		}
		w.Conv = rec("other")
		w.Turn = 2
		res = mustSave(t, h, w)
		if res[0].Conv.Thread != "local" || res[0].Conv.WindowN != 3 {
			t.Fatalf("线程号存在时不得覆盖: %+v", res[0].Conv)
		}
		// 续期 24h：23h 后再写一次，再过 23h 仍在。
		h.Advance(23 * time.Hour)
		w.Turn = 3
		mustSave(t, h, w)
		h.Advance(23 * time.Hour)
		if _, cf, _, _, _ := h.Store.Load(ctx, k, 0); !cf {
			t.Fatal("SaveTurns 应续期对话记录 24h")
		}
	})

	t.Run("SaveTurnsTurnIDWriteOnce", func(t *testing.T) {
		h := newHarness(t)
		k := key("save-turn")
		first := mustSave(t, h, codexsdk.TurnWrite{Key: k, Turn: 1, Conv: rec("t1"), Rec: codexsdk.TurnRecord{TurnID: "uA", StartedMs: 100}})
		if first[0].Turn != (codexsdk.TurnRecord{TurnID: "uA", StartedMs: 100}) {
			t.Fatalf("首写: %+v", first[0].Turn)
		}
		second := mustSave(t, h, codexsdk.TurnWrite{Key: k, Turn: 1, Conv: rec("t1"), Rec: codexsdk.TurnRecord{TurnID: "uB", StartedMs: 200}})
		if second[0].Turn != (codexsdk.TurnRecord{TurnID: "uA", StartedMs: 100}) {
			t.Fatalf("TurnID/StartedMs 已存在时不得覆盖: %+v", second[0].Turn)
		}
	})

	t.Run("SaveTurnsTurnStateRules", func(t *testing.T) {
		h := newHarness(t)
		k := key("save-ts")
		base := codexsdk.TurnWrite{Key: k, Turn: 1, Conv: rec("t1"), Rec: codexsdk.TurnRecord{TurnID: "uA", StartedMs: 100}}
		ts := func(id, v string, o byte) codexsdk.TurnWrite {
			w := base
			w.Rec = codexsdk.TurnRecord{TurnID: id, StartedMs: 100, TS: v, TSOwner: owner(o)}
			w.HasTS = true
			return w
		}
		// 轮次记录不存在 + HasTS：TurnID 补写后条件成立，TS 一并写入。
		res := mustSave(t, h, ts("uA", "s1", 1))
		if res[0].Turn.TS != "s1" || res[0].Turn.TSOwner != owner(1) {
			t.Fatalf("首个 ts 未写: %+v", res[0].Turn)
		}
		// 同 owner：首值生效。
		if res = mustSave(t, h, ts("uA", "s2", 1)); res[0].Turn.TS != "s1" {
			t.Fatalf("同 owner 第二个值覆盖了首值: %+v", res[0].Turn)
		}
		// 不带 HasTS 的写入不动 TS。
		if res = mustSave(t, h, base); res[0].Turn.TS != "s1" {
			t.Fatalf("HasTS=false 改动了 TS: %+v", res[0].Turn)
		}
		// TurnID 不等：不写。
		if res = mustSave(t, h, ts("uZ", "sz", 2)); res[0].Turn.TS != "s1" || res[0].Turn.TurnID != "uA" {
			t.Fatalf("TurnID 不符却写了 TS: %+v", res[0].Turn)
		}
		// 换 owner：覆盖。
		if res = mustSave(t, h, ts("uA", "s3", 2)); res[0].Turn.TS != "s3" || res[0].Turn.TSOwner != owner(2) {
			t.Fatalf("换 owner 未覆盖: %+v", res[0].Turn)
		}
		// 再换回 owner 1：覆盖（owner 不同即覆盖）。
		if res = mustSave(t, h, ts("uA", "s4", 1)); res[0].Turn.TS != "s4" || res[0].Turn.TSOwner != owner(1) {
			t.Fatalf("owner 换回未覆盖: %+v", res[0].Turn)
		}
		_, _, got, tf, _ := h.Store.Load(ctx, k, 1)
		if !tf || got != res[0].Turn {
			t.Fatalf("Load 与返回值不一致: %+v vs %+v", got, res[0].Turn)
		}
	})

	t.Run("SaveTurnsTurnTTL1h", func(t *testing.T) {
		h := newHarness(t)
		k := key("turn-ttl")
		w := codexsdk.TurnWrite{Key: k, Turn: 1, Conv: rec("t1"), Rec: codexsdk.TurnRecord{TurnID: "uA"}}
		mustSave(t, h, w)
		h.Advance(59 * time.Minute)
		mustSave(t, h, w) // 续期
		h.Advance(59 * time.Minute)
		if _, _, _, tf, _ := h.Store.Load(ctx, k, 1); !tf {
			t.Fatal("续期后 59min 轮次记录应仍在")
		}
		h.Advance(2 * time.Minute)
		if _, _, _, tf, _ := h.Store.Load(ctx, k, 1); tf {
			t.Fatal("距上次写 61min 轮次记录应过期")
		}
	})

	t.Run("SaveTurnsBatchAligned", func(t *testing.T) {
		h := newHarness(t)
		var batch []codexsdk.TurnWrite
		for i := range 300 {
			k := key("batch-" + string(rune('a'+i%26)) + string(rune('a'+i/26)))
			batch = append(batch, codexsdk.TurnWrite{Key: k, Turn: uint64(i + 1), Conv: rec("t" + string(rune('a'+i%26))),
				Rec: codexsdk.TurnRecord{TurnID: "u" + string(rune('a'+i%26)) + string(rune('a'+i/26)), StartedMs: int64(i)}})
		}
		res, err := h.Store.SaveTurns(ctx, batch)
		if err != nil || len(res) != len(batch) {
			t.Fatalf("SaveTurns: len=%d err=%v", len(res), err)
		}
		for i, r := range res {
			if !r.OK || r.Turn.TurnID != batch[i].Rec.TurnID || r.Turn.StartedMs != int64(i) || r.Conv != batch[i].Conv {
				t.Fatalf("第 %d 条结果错位: %+v", i, r)
			}
		}
	})
}

func mustOpen(t *testing.T, h Harness, k codexsdk.ConversationKey, r codexsdk.ConversationRecord) codexsdk.ConversationRecord {
	t.Helper()
	got, _, err := h.Store.Open(context.Background(), k, r)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	return got
}

func mustSave(t *testing.T, h Harness, ws ...codexsdk.TurnWrite) []codexsdk.TurnResult {
	t.Helper()
	res, err := h.Store.SaveTurns(context.Background(), ws)
	if err != nil || len(res) != len(ws) {
		t.Fatalf("SaveTurns: len=%d err=%v", len(res), err)
	}
	for i, r := range res {
		if !r.OK {
			t.Fatalf("SaveTurns 第 %d 条失败", i)
		}
	}
	return res
}
