package codexsdk

import (
	"context"
	"errors"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

// 会话簿默认参数（设计 §2）。
const (
	bookStoreBudget    = 30 * time.Millisecond // Open / Advance / Load
	bookFlushBudget    = 2 * time.Second       // 每次 SaveTurns
	bookFlushBatch     = 256
	bookQueueSize      = 8192
	bookConvCapacity   = 100_000
	bookTurnCapacity   = 50_000
	bookConvTTL        = time.Hour
	bookTurnTTL        = 10 * time.Minute
	bookSweepInterval  = 30 * time.Second
	bookBreakerFails   = 5
	bookBreakerOpenFor = 5 * time.Second
	// bookTouchGranularity：lastUsed 只在变化超过 1s 时写回，热条目在多核并发
	// 命中时不争抢同一缓存行（TTL 是分钟级，秒级精度足够）。
	bookTouchGranularity = int64(time.Second / time.Millisecond)
)

// ConversationRequest 是一次会话簿查询的输入。
type ConversationRequest struct {
	Key            ConversationKey
	InstallationID string
	Owner          [16]byte          // 凭据 owner（turn-state 归属）
	Input          ConversationInput // AnalyzeConversationInput(payload)；WS→WS 会话由 SDK 逐帧计算
	Now            time.Time         // 零值 = time.Now()
}

// ConversationBook 合成会话身份（线程、窗口、轮次、turn-state），跨副本经
// ConversationStore 共享。
//
// 热路径（Prepare 命中）只用 sync.Map.Load、原子操作与非阻塞 channel 发送；
// 本地缓存是两个 sync.Map，值为不可变条目指针（整体替换）。写入经有界队列，由
// 调用方的 worker 周期调用 Flush 批量落地；SDK 自己不起 goroutine。
type ConversationBook struct {
	store ConversationStore
	cfg   bookConfig

	convs sync.Map // ConversationKey → *convEntry
	turns sync.Map // turnCacheKey → *turnEntry
	convN atomic.Int64
	turnN atomic.Int64

	queue     chan TurnWrite
	breaker   bookBreaker
	lastSweep atomic.Int64 // UnixNano
	stats     bookCounters
}

type bookConfig struct {
	clock       func() time.Time
	budget      time.Duration
	flushBudget time.Duration
	queueSize   int
	convCap     int
	turnCap     int
	convTTL     time.Duration
	turnTTL     time.Duration
	breakerOpen time.Duration
}

// BookOption 配置 ConversationBook。
type BookOption func(*bookConfig)

// WithBookCapacity 设置本地缓存容量（默认对话 10 万、轮次 5 万；<=0 保持默认）。
func WithBookCapacity(conversations, turns int) BookOption {
	return func(c *bookConfig) {
		if conversations > 0 {
			c.convCap = conversations
		}
		if turns > 0 {
			c.turnCap = turns
		}
	}
}

// WithBookTTL 设置本地缓存闲置过期（默认对话 1h、轮次 10min；<=0 保持默认）。
func WithBookTTL(conversation, turn time.Duration) BookOption {
	return func(c *bookConfig) {
		if conversation > 0 {
			c.convTTL = conversation
		}
		if turn > 0 {
			c.turnTTL = turn
		}
	}
}

// WithBookQueueSize 设置写入队列容量（默认 8192；<=0 保持默认）。满则丢弃并计数。
func WithBookQueueSize(n int) BookOption {
	return func(c *bookConfig) {
		if n > 0 {
			c.queueSize = n
		}
	}
}

// withBookClock 替换时钟（测试用）。
func withBookClock(now func() time.Time) BookOption {
	return func(c *bookConfig) { c.clock = now }
}

// NewConversationBook 创建会话簿。store 不能为 nil。
func NewConversationBook(store ConversationStore, opts ...BookOption) *ConversationBook {
	if store == nil {
		panic("codexsdk: NewConversationBook 的 store 不能为 nil")
	}
	cfg := bookConfig{
		clock:       time.Now,
		budget:      bookStoreBudget,
		flushBudget: bookFlushBudget,
		queueSize:   bookQueueSize,
		convCap:     bookConvCapacity,
		turnCap:     bookTurnCapacity,
		convTTL:     bookConvTTL,
		turnTTL:     bookTurnTTL,
		breakerOpen: bookBreakerOpenFor,
	}
	for _, o := range opts {
		o(&cfg)
	}
	b := &ConversationBook{store: store, cfg: cfg, queue: make(chan TurnWrite, cfg.queueSize)}
	b.lastSweep.Store(cfg.clock().UnixNano())
	return b
}

// turnCacheKey 是轮次记录的键：对话键 + 轮次键。
type turnCacheKey struct {
	key  ConversationKey
	turn uint64
}

// convEntry 是本地对话缓存条目：rec 与 windowID 不可变（整体替换），lastUsed
// 是 Unix 毫秒。
type convEntry struct {
	rec      ConversationRecord
	windowID string // Session.WindowID = Thread + ":" + WindowN（建条目时算一次）
	lastUsed atomic.Int64
}

// turnEntry 是本地轮次缓存条目：rec 不可变（整体替换）。
type turnEntry struct {
	rec      TurnRecord
	lastUsed atomic.Int64
}

func newConvEntry(rec ConversationRecord, nowMs int64) *convEntry {
	e := &convEntry{rec: rec, windowID: rec.Thread + ":" + strconv.FormatUint(rec.WindowN, 10)}
	e.lastUsed.Store(nowMs)
	return e
}

func newTurnEntry(rec TurnRecord, nowMs int64) *turnEntry {
	e := &turnEntry{rec: rec}
	e.lastUsed.Store(nowMs)
	return e
}

func touch(v *atomic.Int64, nowMs int64) {
	if nowMs-v.Load() >= bookTouchGranularity {
		v.Store(nowMs)
	}
}

// session 返回该对话的会话标识（根线程语义：SessionID == ThreadID）。
func (e *convEntry) session() Session {
	return Session{SessionID: e.rec.Thread, ThreadID: e.rec.Thread, WindowID: e.windowID}
}

// Prepare 为一次请求（每次尝试一次）合成身份，永不返回错误：存储失败、超时
// （30ms）或熔断打开时降级为一次性身份（新线程、窗口 0、新轮次、Sink 为 nil），
// 不进缓存，计数。
//
// 存储调用：一轮的后续请求（Input.FollowUp()）本地两缓存都命中则零调用，否则只
// 调 Load（对话记录缺失才再 Open）；新的一轮本地对话缓存命中则零调用，否则只调
// Open。新压缩项（≠ 本地 Compaction 且 ≠ PrevCompaction）同步调 Advance。新一轮
// 的轮次记录异步写（Flush）。
//
// Identity.Sink 绑定（对话键、轮次键、轮次号、Owner）：同一 owner 首值生效，换
// owner 覆盖；临时轮次与一次性身份的 Sink 为 nil（丢弃）。
func (b *ConversationBook) Prepare(ctx context.Context, req ConversationRequest) Identity {
	id, _ := b.prepare(ctx, req)
	return id
}

// prepare 是 Prepare 的本体，另返回对话条目（WS→WS 会话状态用）。
func (b *ConversationBook) prepare(ctx context.Context, req ConversationRequest) (Identity, *convEntry) {
	now := req.Now
	if now.IsZero() {
		now = b.cfg.clock()
	}
	nowMs := now.UnixMilli()
	in := req.Input
	follow := in.FollowUp()
	tk := turnCacheKey{key: req.Key, turn: in.TurnKey}

	ce := b.cachedConv(req.Key, nowMs)
	var te *turnEntry
	loadFailed := false
	if follow {
		if ce != nil && in.TurnKey != 0 {
			te = b.cachedTurn(tk, nowMs)
		}
		if te == nil && in.TurnKey != 0 {
			conv, cf, rec, tf, err := b.callLoad(ctx, req.Key, in.TurnKey)
			switch {
			case err != nil:
				loadFailed = true
			case cf:
				ce = b.mergeConv(req.Key, conv, nowMs, true)
				if tf {
					te = b.mergeTurn(tk, rec, nowMs, true)
				}
			}
		}
	}
	if ce == nil {
		if loadFailed {
			return b.oneShot(req, now, !follow), nil
		}
		fresh := ConversationRecord{Thread: NewUUIDv7(), ContextWindowID: NewUUIDv7()}
		rec, _, err := b.callOpen(ctx, req.Key, fresh)
		if err != nil || rec.Thread == "" {
			return b.oneShot(req, now, !follow), nil
		}
		ce = b.mergeConv(req.Key, rec, nowMs, true)
	}
	ce = b.maybeAdvance(ctx, req.Key, ce, in.Compaction, nowMs)

	var turn Turn
	var sink TurnStateSink
	switch {
	case follow && te != nil:
		turn = Turn{ID: te.rec.TurnID, StartedAtMs: te.rec.StartedMs}
		sink = b.newSink(tk, te.rec, req.Owner, ce)
	case follow:
		// 临时轮次：不入缓存、不写，Sink 丢弃。
		b.stats.temporaryTurns.Add(1)
		turn = Turn{ID: NewUUIDv7(), StartedAtMs: nowMs}
	case in.TurnKey == 0:
		// 无锚点：按新的一轮，但不写、不入缓存。
		turn = Turn{ID: NewUUIDv7(), StartedAtMs: nowMs, First: true}
	default:
		te = b.cachedTurn(tk, nowMs)
		if te == nil {
			ne := newTurnEntry(TurnRecord{TurnID: NewUUIDv7(), StartedMs: nowMs}, nowMs)
			if v, loaded := b.turns.LoadOrStore(tk, ne); loaded {
				te = v.(*turnEntry) // 并发重试：收敛到先存入的那份
				touch(&te.lastUsed, nowMs)
			} else {
				b.turnN.Add(1)
				te = ne
				b.enqueue(TurnWrite{Key: req.Key, Turn: in.TurnKey, Conv: ce.rec, Rec: ne.rec})
			}
		}
		turn = Turn{ID: te.rec.TurnID, StartedAtMs: te.rec.StartedMs, First: true}
		sink = b.newSink(tk, te.rec, req.Owner, ce)
	}
	if te != nil && sink != nil && te.rec.TS != "" && te.rec.TSOwner == req.Owner {
		turn.State = te.rec.TS
	}
	return Identity{
		InstallationID: req.InstallationID,
		Session:        ce.session(),
		Window:         Window{Number: ce.rec.WindowN, ContextID: ce.rec.ContextWindowID},
		Turn:           turn,
		Sink:           sink,
	}, ce
}

// prepareConversation 只取对话记录（缓存或 Open），不碰轮次：WS→WS 客户端预热
// 首帧与增量首帧用。失败降级为不进缓存的一次性对话记录（与命中无法区分；该连接
// 随后的全量帧会重新 Prepare，可能换成真实线程号——降级本就不保证一致）。
func (b *ConversationBook) prepareConversation(ctx context.Context, req ConversationRequest) *convEntry {
	now := req.Now
	if now.IsZero() {
		now = b.cfg.clock()
	}
	nowMs := now.UnixMilli()
	if ce := b.cachedConv(req.Key, nowMs); ce != nil {
		return ce
	}
	fresh := ConversationRecord{Thread: NewUUIDv7(), ContextWindowID: NewUUIDv7()}
	rec, _, err := b.callOpen(ctx, req.Key, fresh)
	if err != nil || rec.Thread == "" {
		b.stats.oneShot.Add(1)
		return newConvEntry(fresh, nowMs)
	}
	return b.mergeConv(req.Key, rec, nowMs, true)
}

// oneShot 是降级身份：新线程、窗口 0、新轮次、Sink 为 nil；不进缓存。First 为
// !FollowUp()（按请求本身判断，不因换了线程而恒为 true）。
func (b *ConversationBook) oneShot(req ConversationRequest, now time.Time, first bool) Identity {
	b.stats.oneShot.Add(1)
	thread := NewUUIDv7()
	return Identity{
		InstallationID: req.InstallationID,
		Session:        Session{SessionID: thread, ThreadID: thread, WindowID: thread + ":0"},
		Window:         Window{Number: 0, ContextID: NewUUIDv7()},
		Turn:           Turn{ID: NewUUIDv7(), StartedAtMs: now.UnixMilli(), First: first},
	}
}

func (b *ConversationBook) cachedConv(key ConversationKey, nowMs int64) *convEntry {
	v, ok := b.convs.Load(key)
	if !ok {
		b.stats.convMisses.Add(1)
		return nil
	}
	b.stats.convHits.add(&key)
	e := v.(*convEntry)
	touch(&e.lastUsed, nowMs)
	return e
}

func (b *ConversationBook) cachedTurn(k turnCacheKey, nowMs int64) *turnEntry {
	v, ok := b.turns.Load(k)
	if !ok {
		b.stats.turnMisses.Add(1)
		return nil
	}
	b.stats.turnHits.add(&k.key)
	e := v.(*turnEntry)
	touch(&e.lastUsed, nowMs)
	return e
}

// mergeConv 把存储返回的对话记录合入本地缓存：仅当 WindowN ≥ 本地才替换
// （回填不让窗口倒退）。use=false（Flush 回填）不刷新 lastUsed。返回合并后的条目。
func (b *ConversationBook) mergeConv(key ConversationKey, rec ConversationRecord, nowMs int64, use bool) *convEntry {
	for {
		v, ok := b.convs.Load(key)
		if !ok {
			ne := newConvEntry(rec, nowMs)
			if _, loaded := b.convs.LoadOrStore(key, ne); !loaded {
				b.convN.Add(1)
				return ne
			}
			continue
		}
		cur := v.(*convEntry)
		if cur.rec == rec || rec.WindowN < cur.rec.WindowN {
			if use {
				touch(&cur.lastUsed, nowMs)
			}
			return cur
		}
		used := cur.lastUsed.Load()
		if use {
			used = max(nowMs, used)
		}
		ne := newConvEntry(rec, used)
		if b.convs.CompareAndSwap(key, cur, ne) {
			return ne
		}
	}
}

// mergeTurn 把存储返回的轮次记录合入本地缓存：整体替换，但同一 TurnID 下返回
// TS=="" 而本地非空时保留本地 TS/TSOwner。use 同 mergeConv。
func (b *ConversationBook) mergeTurn(k turnCacheKey, rec TurnRecord, nowMs int64, use bool) *turnEntry {
	for {
		v, ok := b.turns.Load(k)
		if !ok {
			ne := newTurnEntry(rec, nowMs)
			if _, loaded := b.turns.LoadOrStore(k, ne); !loaded {
				b.turnN.Add(1)
				return ne
			}
			continue
		}
		cur := v.(*turnEntry)
		if rec.TurnID == cur.rec.TurnID && rec.TS == "" && cur.rec.TS != "" {
			rec.TS, rec.TSOwner = cur.rec.TS, cur.rec.TSOwner
		}
		if rec == cur.rec {
			if use {
				touch(&cur.lastUsed, nowMs)
			}
			return cur
		}
		used := cur.lastUsed.Load()
		if use {
			used = max(nowMs, used)
		}
		ne := newTurnEntry(rec, used)
		if b.turns.CompareAndSwap(k, cur, ne) {
			return ne
		}
	}
}

// maybeAdvance：压缩标记非 0、≠ 本地 Compaction、≠ 本地 PrevCompaction 时同步
// Advance；失败沿用旧窗口。
func (b *ConversationBook) maybeAdvance(ctx context.Context, key ConversationKey, ce *convEntry, mark uint64, nowMs int64) *convEntry {
	if mark == 0 || mark == ce.rec.Compaction || mark == ce.rec.PrevCompaction {
		return ce
	}
	rec, err := b.callAdvance(ctx, key, ce.rec.Compaction, mark, NewUUIDv7())
	if err != nil || rec.Thread == "" {
		return ce
	}
	return b.mergeConv(key, rec, nowMs, true)
}

// enqueue 非阻塞入写入队列；满则丢弃并计数。
func (b *ConversationBook) enqueue(w TurnWrite) {
	select {
	case b.queue <- w:
	default:
		b.stats.queueDrops.Add(1)
	}
}

// ---- 存储调用（预算 + 熔断） ----

var errBookBreakerOpen = errors.New("codexsdk: 会话簿存储熔断打开")

// call 包装一次 Open/Advance/Load：熔断判定、30ms 预算、结果记入熔断器。
// 调用方 ctx 自身已取消导致的失败不计入熔断。
func (b *ConversationBook) call(ctx context.Context, fn func(context.Context) error) error {
	probe, ok := b.breaker.allow(b.cfg.clock().UnixNano())
	if !ok {
		b.stats.breakerRejects.Add(1)
		return errBookBreakerOpen
	}
	cctx, cancel := context.WithTimeout(ctx, b.cfg.budget)
	err := fn(cctx)
	cancel()
	if err != nil && ctx.Err() != nil {
		b.breaker.abandon(probe)
		return err
	}
	if b.breaker.record(err == nil, probe, b.cfg.clock().UnixNano(), b.cfg.breakerOpen) {
		b.stats.breakerOpens.Add(1)
	}
	return err
}

func (b *ConversationBook) callOpen(ctx context.Context, key ConversationKey, fresh ConversationRecord) (rec ConversationRecord, created bool, err error) {
	b.stats.opens.Add(1)
	err = b.call(ctx, func(c context.Context) (e error) {
		rec, created, e = b.store.Open(c, key, fresh)
		return e
	})
	if err != nil {
		b.stats.openFailures.Add(1)
	}
	return rec, created, err
}

func (b *ConversationBook) callAdvance(ctx context.Context, key ConversationKey, oldMark, newMark uint64, newWindowID string) (rec ConversationRecord, err error) {
	b.stats.advances.Add(1)
	err = b.call(ctx, func(c context.Context) (e error) {
		rec, e = b.store.Advance(c, key, oldMark, newMark, newWindowID)
		return e
	})
	if err != nil {
		b.stats.advanceFailures.Add(1)
	}
	return rec, err
}

func (b *ConversationBook) callLoad(ctx context.Context, key ConversationKey, turn uint64) (conv ConversationRecord, cf bool, rec TurnRecord, tf bool, err error) {
	b.stats.loads.Add(1)
	err = b.call(ctx, func(c context.Context) (e error) {
		conv, cf, rec, tf, e = b.store.Load(c, key, turn)
		return e
	})
	if err != nil {
		b.stats.loadFailures.Add(1)
	}
	return conv, cf, rec, tf, err
}

// bookBreaker 是原子熔断器：连续失败 bookBreakerFails 次 → 打开；到期后半开，
// 只放行一个探测调用（CAS 抢占），成功关闭、失败再开。
type bookBreaker struct {
	fails     atomic.Int32
	openUntil atomic.Int64 // UnixNano；0 = 关闭
	probing   atomic.Bool
}

// allow 报告能否放行；probe=true 表示本次是半开探测。
func (br *bookBreaker) allow(now int64) (probe, ok bool) {
	until := br.openUntil.Load()
	if until == 0 {
		return false, true
	}
	if now < until {
		return false, false
	}
	if br.probing.CompareAndSwap(false, true) {
		return true, true
	}
	return false, false
}

// record 记一次结果；返回本次是否把熔断器打开。
func (br *bookBreaker) record(success, probe bool, now int64, openFor time.Duration) bool {
	if success {
		br.fails.Store(0)
		if probe {
			br.openUntil.Store(0)
			br.probing.Store(false)
		}
		return false
	}
	if probe {
		br.openUntil.Store(now + int64(openFor))
		br.probing.Store(false)
		return true
	}
	if br.fails.Add(1) >= bookBreakerFails && br.openUntil.CompareAndSwap(0, now+int64(openFor)) {
		br.fails.Store(0)
		return true
	}
	return false
}

// abandon 归还未得出结论的探测名额（调用方 ctx 已取消）。
func (br *bookBreaker) abandon(probe bool) {
	if probe {
		br.probing.Store(false)
	}
}

// ---- Sink ----

// bookSink 是会话簿轮次的 TurnStateSink，绑定（对话键、轮次键、轮次号、Owner）。
type bookSink struct {
	b         *ConversationBook
	conv      *convEntry
	tk        turnCacheKey
	turnID    string
	startedMs int64
	owner     [16]byte
}

func (b *ConversationBook) newSink(tk turnCacheKey, rec TurnRecord, owner [16]byte, ce *convEntry) *bookSink {
	return &bookSink{b: b, conv: ce, tk: tk, turnID: rec.TurnID, startedMs: rec.StartedMs, owner: owner}
}

// SetTurnState：本地轮次记录 TS=="" 或 TSOwner!=Owner 时整体替换本地条目并非阻塞
// 入队（HasTS）；否则忽略并计数。本地条目已被换成别的轮次号时忽略；条目已被
// 清扫时只入队（存储按条件判定）。
func (s *bookSink) SetTurnState(v string) {
	if v == "" {
		return
	}
	b := s.b
	rec := TurnRecord{TurnID: s.turnID, StartedMs: s.startedMs, TS: v, TSOwner: s.owner}
	for {
		cur, ok := b.turns.Load(s.tk)
		if !ok {
			break
		}
		e := cur.(*turnEntry)
		if e.rec.TurnID != s.turnID || (e.rec.TS != "" && e.rec.TSOwner == s.owner) {
			b.stats.turnStateIgnored.Add(1)
			return
		}
		rec.StartedMs = e.rec.StartedMs
		ne := newTurnEntry(rec, e.lastUsed.Load())
		if b.turns.CompareAndSwap(s.tk, cur, ne) {
			break
		}
	}
	b.enqueue(TurnWrite{Key: s.tk.key, Turn: s.tk.turn, Conv: s.conv.rec, Rec: rec, HasTS: true})
}

// ---- 写入与清扫 ----

// Flush 排空写入队列（本次调用开始时的长度），按 256 一批调 SaveTurns（每批
// 预算 2s），用返回值回填本地缓存；距上次清扫 ≥30s 时顺带清扫本地缓存。调用方
// worker 每 50ms 调一次；同一时刻只应有一个 Flush 在跑。返回各批失败的合并错误
// （失败的批次已丢弃并计数）。2s 是每批预算：队列满（8192 = 32 批）且存储持续
// 卡住时一次 Flush 最坏约 64s，调用方可用 ctx 给整次 Flush 设上限（ctx 取消即停）。
func (b *ConversationBook) Flush(ctx context.Context) error {
	n := len(b.queue)
	var errs error
	if n > 0 {
		batch := make([]TurnWrite, 0, min(n, bookFlushBatch))
		for n > 0 && ctx.Err() == nil {
			batch = batch[:0]
		fill:
			for len(batch) < bookFlushBatch && n > 0 {
				select {
				case w := <-b.queue:
					batch = append(batch, w)
					n--
				default:
					n = 0
					break fill
				}
			}
			if len(batch) == 0 {
				break
			}
			errs = errors.Join(errs, b.save(ctx, batch))
		}
	}
	b.maybeSweep()
	return errs
}

func (b *ConversationBook) save(ctx context.Context, batch []TurnWrite) error {
	cctx, cancel := context.WithTimeout(ctx, b.cfg.flushBudget)
	res, err := b.store.SaveTurns(cctx, batch)
	cancel()
	b.stats.saveCalls.Add(1)
	if err != nil {
		b.stats.saveFailures.Add(1)
		return err
	}
	nowMs := b.cfg.clock().UnixMilli()
	for i := range min(len(res), len(batch)) {
		r := &res[i]
		if !r.OK {
			b.stats.saveItemFailures.Add(1)
			continue
		}
		w := &batch[i]
		if r.Conv.Thread != "" {
			b.mergeConv(w.Key, r.Conv, nowMs, false)
		}
		if r.Turn.TurnID != "" {
			b.mergeTurn(turnCacheKey{key: w.Key, turn: w.Turn}, r.Turn, nowMs, false)
		}
	}
	return nil
}

// maybeSweep：距上次 ≥30s 才全量 Range 一次：删 TTL 过期（按 lastUsed），仍超
// 容量时按 lastUsed 删最旧的超出部分。
func (b *ConversationBook) maybeSweep() {
	now := b.cfg.clock()
	nowNs := now.UnixNano()
	last := b.lastSweep.Load()
	if nowNs-last < int64(bookSweepInterval) || !b.lastSweep.CompareAndSwap(last, nowNs) {
		return
	}
	start := time.Now()
	nowMs := now.UnixMilli()
	deleted := sweepMap(&b.convs, &b.convN, nowMs-b.cfg.convTTL.Milliseconds(), b.cfg.convCap,
		func(v any) int64 { return v.(*convEntry).lastUsed.Load() })
	deleted += sweepMap(&b.turns, &b.turnN, nowMs-b.cfg.turnTTL.Milliseconds(), b.cfg.turnCap,
		func(v any) int64 { return v.(*turnEntry).lastUsed.Load() })
	b.stats.sweeps.Add(1)
	b.stats.sweepDeleted.Add(int64(deleted))
	b.stats.sweepNanos.Add(int64(time.Since(start)))
}

type sweepItem struct {
	k, v any
	used int64
}

func sweepMap(m *sync.Map, count *atomic.Int64, expireBefore int64, capacity int, lastUsed func(any) int64) int {
	deleted := 0
	m.Range(func(k, v any) bool {
		if lastUsed(v) < expireBefore && m.CompareAndDelete(k, v) {
			count.Add(-1)
			deleted++
		}
		return true
	})
	excess := count.Load() - int64(capacity)
	if excess <= 0 {
		return deleted
	}
	items := make([]sweepItem, 0, count.Load())
	m.Range(func(k, v any) bool {
		items = append(items, sweepItem{k: k, v: v, used: lastUsed(v)})
		return true
	})
	slices.SortFunc(items, func(a, b sweepItem) int {
		switch {
		case a.used < b.used:
			return -1
		case a.used > b.used:
			return 1
		}
		return 0
	})
	for _, it := range items {
		if excess <= 0 {
			break
		}
		if m.CompareAndDelete(it.k, it.v) {
			count.Add(-1)
			deleted++
			excess--
		}
	}
	return deleted
}

// ---- 计数 ----

// ConversationStats 是会话簿计数快照（单调计数，Entries 为当前条目数）。
type ConversationStats struct {
	ConversationCacheHits   int64
	ConversationCacheMisses int64
	TurnCacheHits           int64
	TurnCacheMisses         int64
	ConversationEntries     int64 // 当前本地对话缓存条目数
	TurnEntries             int64 // 当前本地轮次缓存条目数

	Opens           int64
	OpenFailures    int64 // 含熔断拒绝与超时
	Advances        int64
	AdvanceFailures int64
	Loads           int64
	LoadFailures    int64
	BreakerOpens    int64 // 熔断打开次数
	BreakerRejects  int64 // 熔断打开期间被拒的存储调用数（零命令）

	OneShotIdentities int64 // 一次性身份数
	TemporaryTurns    int64 // 后续请求查不到轮次时的临时轮次数
	QueueDrops        int64 // 写入队列满丢弃数
	SaveTurnsCalls    int64
	SaveTurnsFailures int64 // 整批失败数
	SaveItemFailures  int64 // 单条失败数（TurnResult.OK=false）
	TurnStateIgnored  int64 // 被丢弃的 SetTurnState 次数（该轮已有同 owner 的 ts，或本地轮次号已不同）

	WSUntrackedConnections int64 // WS FIFO 满、整条连接停止捕获 ts 的次数
	WSIncrementalInjects   int64 // WS 增量帧因指令/工具变化而"重新插入"的次数（连接上的第一次插入不计）
	WSLocalTemporaryTurns  int64 // WS 增量后续帧在连接内无当前轮次时的临时轮次数
	WSLateTurnStates       int64 // 迟到的旧轮 ts（连接快照已换轮，不更新快照；含客户端在响应结束前就开新轮的正常情形）

	Sweeps       int64
	SweepDeleted int64 // 清扫删除条数（TTL + 容量）
	SweepNanos   int64 // 清扫累计耗时
}

// stripedCounter 是按对话键分条的计数器：命中路径每次请求都要加一，多核并发时
// 单个原子量会成为共享缓存行的争用点；按键首字节分到 16 条（各占一条缓存行）。
type stripedCounter struct {
	s [16]struct {
		n atomic.Int64
		_ [56]byte
	}
}

func (c *stripedCounter) add(k *ConversationKey) { c.s[k[0]&15].n.Add(1) }

func (c *stripedCounter) load() int64 {
	var n int64
	for i := range c.s {
		n += c.s[i].n.Load()
	}
	return n
}

type bookCounters struct {
	convHits, turnHits                                            stripedCounter
	convMisses, turnMisses                                        atomic.Int64
	opens, openFailures, advances, advanceFailures, loads         atomic.Int64
	loadFailures, breakerOpens, breakerRejects                    atomic.Int64
	oneShot, temporaryTurns, queueDrops                           atomic.Int64
	saveCalls, saveFailures, saveItemFailures, turnStateIgnored   atomic.Int64
	wsUntracked, wsIncrementalInjects, wsLocalTemporary, wsLateTS atomic.Int64
	sweeps, sweepDeleted, sweepNanos                              atomic.Int64
}

// Stats 返回计数快照。
func (b *ConversationBook) Stats() ConversationStats {
	c := &b.stats
	return ConversationStats{
		ConversationCacheHits:   c.convHits.load(),
		ConversationCacheMisses: c.convMisses.Load(),
		TurnCacheHits:           c.turnHits.load(),
		TurnCacheMisses:         c.turnMisses.Load(),
		ConversationEntries:     b.convN.Load(),
		TurnEntries:             b.turnN.Load(),
		Opens:                   c.opens.Load(),
		OpenFailures:            c.openFailures.Load(),
		Advances:                c.advances.Load(),
		AdvanceFailures:         c.advanceFailures.Load(),
		Loads:                   c.loads.Load(),
		LoadFailures:            c.loadFailures.Load(),
		BreakerOpens:            c.breakerOpens.Load(),
		BreakerRejects:          c.breakerRejects.Load(),
		OneShotIdentities:       c.oneShot.Load(),
		TemporaryTurns:          c.temporaryTurns.Load(),
		QueueDrops:              c.queueDrops.Load(),
		SaveTurnsCalls:          c.saveCalls.Load(),
		SaveTurnsFailures:       c.saveFailures.Load(),
		SaveItemFailures:        c.saveItemFailures.Load(),
		TurnStateIgnored:        c.turnStateIgnored.Load(),
		WSUntrackedConnections:  c.wsUntracked.Load(),
		WSIncrementalInjects:    c.wsIncrementalInjects.Load(),
		WSLocalTemporaryTurns:   c.wsLocalTemporary.Load(),
		WSLateTurnStates:        c.wsLateTS.Load(),
		Sweeps:                  c.sweeps.Load(),
		SweepDeleted:            c.sweepDeleted.Load(),
		SweepNanos:              c.sweepNanos.Load(),
	}
}
