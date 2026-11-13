package codexsdk

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"sync"
	"sync/atomic"
	"time"
)

// ConversationKey 是对话键：SHA-256(缓存域 ‖ API key ID ‖ 对话标识)，三段各带
// 8 字节大端长度前缀。对话标识只作哈希输入，不落盘、不出网。
type ConversationKey [32]byte

// NewConversationKey 计算对话键。apiKeyID 按 8 字节大端编码（长度前缀为 8）。
// 同一缓存域、同一 API key、同一对话标识得到同一个键；installation 不参与（同域
// 两个账号共用同一线程号，installation 由 ConversationRequest.InstallationID
// 单独给出）。
func NewConversationKey(cacheDomain string, apiKeyID int64, conversation string) ConversationKey {
	h := sha256.New()
	var n [8]byte
	binary.BigEndian.PutUint64(n[:], uint64(len(cacheDomain)))
	h.Write(n[:])
	h.Write([]byte(cacheDomain))
	binary.BigEndian.PutUint64(n[:], 8)
	h.Write(n[:])
	binary.BigEndian.PutUint64(n[:], uint64(apiKeyID))
	h.Write(n[:])
	binary.BigEndian.PutUint64(n[:], uint64(len(conversation)))
	h.Write(n[:])
	h.Write([]byte(conversation))
	var k ConversationKey
	h.Sum(k[:0])
	return k
}

// ConversationRecord 是对话记录（跨副本共享）。
type ConversationRecord struct {
	Thread  string // 线程号（UUIDv7）
	WindowN uint64 // 窗口号
	// ContextWindowID 是窗口 UUID（UUIDv7，即 context_window_id）；注意与
	// Session.WindowID（"thread:n"）不同。
	ContextWindowID string
	Compaction      uint64 // 当前压缩项标记（FNV-64a；0 = 无）
	PrevCompaction  uint64 // 上一个压缩项标记（迟到请求不会把窗口推回去）
}

// TurnRecord 是轮次记录（跨副本共享）。
type TurnRecord struct {
	TurnID    string   // 轮次号（UUIDv7）
	StartedMs int64    // 轮次开始时刻（Unix 毫秒）
	TS        string   // 上游签发的 turn-state（"" = 无）
	TSOwner   [16]byte // 拿到 TS 的凭据 owner
}

// TurnWrite 是一条轮次写入（SaveTurns 的批量元素）。
type TurnWrite struct {
	Key   ConversationKey
	Turn  uint64             // 轮次键（ConversationInput.TurnKey）
	Conv  ConversationRecord // 对话记录线程号不存在时以它补写
	Rec   TurnRecord         // TurnID/StartedMs 不存在才写；TS 见 ConversationStore.SaveTurns
	HasTS bool
}

// TurnResult 是一条写入后两条记录的最终值。
type TurnResult struct {
	Conv ConversationRecord
	Turn TurnRecord
	OK   bool // false = 本条失败（Conv/Turn 无意义，会话簿不回填）；SaveTurns 返回 error 时整批失败
}

// ConversationStore 是会话簿的共享存储（跨副本）。实现必须尊重 ctx 的期限：
// 会话簿给 Open/Advance/Load 的预算是 30ms，给 SaveTurns 的预算是 2s。
//
//   - Open：记录不存在则原子写入 fresh（created=true）；存在则返回已有的。两种
//     情况都把对话记录续期 24h。
//   - Advance：记录的 Compaction==oldMark 且 newMark!=PrevCompaction 时
//     WindowN+1、ContextWindowID=newWindowID、PrevCompaction=oldMark、
//     Compaction=newMark（并续期 24h）；否则不变。返回最终记录；记录不存在
//     返回零值记录（Thread==""）与 nil。
//   - Load：一次往返读对话记录与轮次记录；缺失分别报 convFound/turnFound=false。
//     读不续期。
//   - SaveTurns：逐条 ①对话记录线程号不存在则以 Conv 补写（含窗口字段），续期
//     24h；②轮次记录 TurnID/StartedMs 不存在才写；③HasTS 时，仅当记录
//     TurnID==Rec.TurnID 且（TS=="" 或 TSOwner!=Rec.TSOwner）才写 TS/TSOwner；
//     ④轮次记录续期 1h；⑤返回两条记录的最终值（与 batch 一一对应）。
type ConversationStore interface {
	Open(ctx context.Context, key ConversationKey, fresh ConversationRecord) (rec ConversationRecord, created bool, err error)
	Advance(ctx context.Context, key ConversationKey, oldMark, newMark uint64, newWindowID string) (ConversationRecord, error)
	Load(ctx context.Context, key ConversationKey, turn uint64) (conv ConversationRecord, convFound bool, rec TurnRecord, turnFound bool, err error)
	SaveTurns(ctx context.Context, batch []TurnWrite) ([]TurnResult, error)
}

// 存储侧过期时间（设计 §2）。
const (
	StoreConversationTTL = 24 * time.Hour // 对话记录闲置过期
	StoreTurnTTL         = time.Hour      // 轮次记录过期
)

// NewMemoryConversationStore 返回进程内的 ConversationStore（单进程部署与测试
// 用；多副本必须用共享存储，如 redisstore）。过期按访问惰性判定，另在
// SaveTurns 中每分钟至多一次清掉已过期条目。
func NewMemoryConversationStore() ConversationStore { return newMemoryStore(time.Now) }

// memoryStore 是无锁实现：两个 sync.Map，值为不可变条目指针，用
// CompareAndSwap 整体替换（与 Redis 脚本的单键原子语义等价）。
type memoryStore struct {
	now       func() time.Time
	convs     sync.Map // ConversationKey → *memConv
	turns     sync.Map // turnCacheKey → *memTurn
	lastSweep atomic.Int64
}

type memConv struct {
	rec ConversationRecord
	exp int64 // UnixNano
}

type memTurn struct {
	rec TurnRecord
	exp int64
}

func newMemoryStore(now func() time.Time) *memoryStore {
	s := &memoryStore{now: now}
	s.lastSweep.Store(now().UnixNano())
	return s
}

func (s *memoryStore) loadConv(key ConversationKey, now int64) (*memConv, bool) {
	v, ok := s.convs.Load(key)
	if !ok {
		return nil, false
	}
	m := v.(*memConv)
	return m, m.exp > now
}

func (s *memoryStore) loadTurn(k turnCacheKey, now int64) (*memTurn, bool) {
	v, ok := s.turns.Load(k)
	if !ok {
		return nil, false
	}
	m := v.(*memTurn)
	return m, m.exp > now
}

// swapConv 用 next 替换 old（old==nil 表示不存在），成功返回 true。
func (s *memoryStore) swapConv(key ConversationKey, old, next *memConv) bool {
	if old == nil {
		_, loaded := s.convs.LoadOrStore(key, next)
		return !loaded
	}
	return s.convs.CompareAndSwap(key, old, next)
}

func (s *memoryStore) swapTurn(k turnCacheKey, old, next *memTurn) bool {
	if old == nil {
		_, loaded := s.turns.LoadOrStore(k, next)
		return !loaded
	}
	return s.turns.CompareAndSwap(k, old, next)
}

func (s *memoryStore) Open(ctx context.Context, key ConversationKey, fresh ConversationRecord) (ConversationRecord, bool, error) {
	if err := ctx.Err(); err != nil {
		return ConversationRecord{}, false, err
	}
	for {
		now := s.now().UnixNano()
		exp := now + int64(StoreConversationTTL)
		old, live := s.loadConv(key, now)
		if live {
			if s.swapConv(key, old, &memConv{rec: old.rec, exp: exp}) {
				return old.rec, false, nil
			}
			continue
		}
		if s.swapConv(key, old, &memConv{rec: fresh, exp: exp}) {
			return fresh, true, nil
		}
	}
}

func (s *memoryStore) Advance(ctx context.Context, key ConversationKey, oldMark, newMark uint64, newWindowID string) (ConversationRecord, error) {
	if err := ctx.Err(); err != nil {
		return ConversationRecord{}, err
	}
	for {
		now := s.now().UnixNano()
		old, live := s.loadConv(key, now)
		if !live {
			return ConversationRecord{}, nil
		}
		r := old.rec
		if r.Compaction != oldMark || newMark == r.PrevCompaction {
			return r, nil
		}
		r.WindowN++
		r.ContextWindowID = newWindowID
		r.PrevCompaction = oldMark
		r.Compaction = newMark
		if s.swapConv(key, old, &memConv{rec: r, exp: now + int64(StoreConversationTTL)}) {
			return r, nil
		}
	}
}

func (s *memoryStore) Load(ctx context.Context, key ConversationKey, turn uint64) (ConversationRecord, bool, TurnRecord, bool, error) {
	if err := ctx.Err(); err != nil {
		return ConversationRecord{}, false, TurnRecord{}, false, err
	}
	now := s.now().UnixNano()
	var conv ConversationRecord
	var rec TurnRecord
	c, cf := s.loadConv(key, now)
	if cf {
		conv = c.rec
	}
	t, tf := s.loadTurn(turnCacheKey{key: key, turn: turn}, now)
	if tf {
		rec = t.rec
	}
	return conv, cf, rec, tf, nil
}

func (s *memoryStore) SaveTurns(ctx context.Context, batch []TurnWrite) ([]TurnResult, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	out := make([]TurnResult, len(batch))
	for i := range batch {
		out[i] = s.saveOne(&batch[i])
	}
	s.maybeSweep()
	return out, nil
}

func (s *memoryStore) saveOne(w *TurnWrite) TurnResult {
	var res TurnResult
	for {
		now := s.now().UnixNano()
		old, live := s.loadConv(w.Key, now)
		r := w.Conv
		if live && old.rec.Thread != "" {
			r = old.rec
		}
		if s.swapConv(w.Key, old, &memConv{rec: r, exp: now + int64(StoreConversationTTL)}) {
			res.Conv = r
			break
		}
	}
	tk := turnCacheKey{key: w.Key, turn: w.Turn}
	for {
		now := s.now().UnixNano()
		old, live := s.loadTurn(tk, now)
		var r TurnRecord
		if live {
			r = old.rec
		}
		if r.TurnID == "" {
			r.TurnID, r.StartedMs = w.Rec.TurnID, w.Rec.StartedMs
		}
		if w.HasTS && r.TurnID == w.Rec.TurnID && (r.TS == "" || r.TSOwner != w.Rec.TSOwner) {
			r.TS, r.TSOwner = w.Rec.TS, w.Rec.TSOwner
		}
		if s.swapTurn(tk, old, &memTurn{rec: r, exp: now + int64(StoreTurnTTL)}) {
			res.Turn = r
			break
		}
	}
	res.OK = true
	return res
}

// maybeSweep 每分钟至多一次删除已过期条目（惰性判定之外的内存回收）。
func (s *memoryStore) maybeSweep() {
	now := s.now().UnixNano()
	last := s.lastSweep.Load()
	if now-last < int64(time.Minute) || !s.lastSweep.CompareAndSwap(last, now) {
		return
	}
	s.convs.Range(func(k, v any) bool {
		if v.(*memConv).exp <= now {
			s.convs.CompareAndDelete(k, v)
		}
		return true
	})
	s.turns.Range(func(k, v any) bool {
		if v.(*memTurn).exp <= now {
			s.turns.CompareAndDelete(k, v)
		}
		return true
	})
}
