// Package redisstore 是 codexsdk.ConversationStore 的 Redis 实现。
//
// 只依赖 redis.Scripter：所有操作都是一段 Lua 脚本，先 EVALSHA，遇到 NOSCRIPT
// 回退 EVAL（脚本随即进入服务端缓存）。
//
// 键（只含哈希，不含对话标识原文）：
//
//	codex:conv:<hex(对话键)>               HASH thread / window_n / window_id / compaction / prev_compaction
//	codex:turn:<hex(对话键)>:<hex16(轮次键)> HASH turn_id / started_ms / ts / ts_owner
//
// 64 位数值（窗口号、压缩标记、开始时刻）以十进制字符串存放，脚本只做字符串
// 相等比较（窗口号加一除外），不经过 Lua 的双精度数。
//
// 只支持单节点 Redis：SaveTurns 一批一次脚本（KEYS 为 2N 个），跨哈希槽的
// Cluster 部署会被服务端拒绝；键不加哈希标签（设计如此）。
package redisstore

import (
	"context"
	"crypto/sha1"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	codexsdk "github.com/is7Qin/codex-sdk"
	"github.com/redis/go-redis/v9"
)

// Option 配置 Redis 存储。
type Option func(*store)

// WithTTL 设置对话记录与轮次记录的过期（默认 24h、1h；<=0 保持默认）。
func WithTTL(conversation, turn time.Duration) Option {
	return func(s *store) {
		if conversation > 0 {
			s.convTTL = conversation
		}
		if turn > 0 {
			s.turnTTL = turn
		}
	}
}

// New 返回基于 rdb 的 ConversationStore。
func New(rdb redis.Scripter, opts ...Option) codexsdk.ConversationStore {
	if rdb == nil {
		panic("redisstore: rdb 不能为 nil")
	}
	s := &store{rdb: rdb, convTTL: codexsdk.StoreConversationTTL, turnTTL: codexsdk.StoreTurnTTL}
	for _, o := range opts {
		o(s)
	}
	s.convTTLms = strconv.FormatInt(s.convTTL.Milliseconds(), 10)
	s.turnTTLms = strconv.FormatInt(s.turnTTL.Milliseconds(), 10)
	return s
}

type store struct {
	rdb                  redis.Scripter
	convTTL, turnTTL     time.Duration
	convTTLms, turnTTLms string
}

// script 是一段 Lua 脚本与其 SHA1（构造时算好，运行期无锁）。
type script struct {
	src, sha string
}

func newScript(src string) script {
	h := sha1.Sum([]byte(src))
	return script{src: src, sha: hex.EncodeToString(h[:])}
}

// run 先 EVALSHA，NOSCRIPT 时回退 EVAL。
func (sc script) run(ctx context.Context, rdb redis.Scripter, keys []string, args ...any) ([]any, error) {
	v, err := rdb.EvalSha(ctx, sc.sha, keys, args...).Result()
	if err != nil && strings.HasPrefix(err.Error(), "NOSCRIPT") {
		v, err = rdb.Eval(ctx, sc.src, keys, args...).Result()
	}
	if err != nil {
		return nil, err
	}
	arr, ok := v.([]any)
	if !ok {
		return nil, fmt.Errorf("redisstore: 脚本返回类型 %T", v)
	}
	return arr, nil
}

// 对话记录字段读出（缺失字段补默认值，避免 Lua 表中的 nil 截断数组）。
const luaReadConv = `
local function conv(k)
  local v = redis.call('HMGET', k, 'thread', 'window_n', 'window_id', 'compaction', 'prev_compaction')
  if not v[1] then return nil end
  return {v[1], v[2] or '0', v[3] or '', v[4] or '0', v[5] or '0'}
end
`

// Open：KEYS[1]=对话键；ARGV = thread, window_n, window_id, compaction, prev, ttl_ms。
// 返回 {created(0/1), 5 个字段}。
var scriptOpen = newScript(luaReadConv + `
local c = conv(KEYS[1])
local created = 0
if not c then
  redis.call('HSET', KEYS[1], 'thread', ARGV[1], 'window_n', ARGV[2], 'window_id', ARGV[3], 'compaction', ARGV[4], 'prev_compaction', ARGV[5])
  c = {ARGV[1], ARGV[2], ARGV[3], ARGV[4], ARGV[5]}
  created = 1
end
redis.call('PEXPIRE', KEYS[1], ARGV[6])
return {created, c[1], c[2], c[3], c[4], c[5]}
`)

// Advance：KEYS[1]=对话键；ARGV = old_mark, new_mark, new_window_id, ttl_ms。
// 记录不存在返回空数组；否则返回最终 5 个字段。
var scriptAdvance = newScript(luaReadConv + `
local c = conv(KEYS[1])
if not c then return {} end
if c[4] == ARGV[1] and ARGV[2] ~= c[5] then
  local n = string.format('%.0f', tonumber(c[2]) + 1)
  redis.call('HSET', KEYS[1], 'window_n', n, 'window_id', ARGV[3], 'compaction', ARGV[2], 'prev_compaction', ARGV[1])
  redis.call('PEXPIRE', KEYS[1], ARGV[4])
  return {c[1], n, ARGV[3], ARGV[2], ARGV[1]}
end
return c
`)

// Load：KEYS[1]=对话键，KEYS[2]=轮次键。返回
// {conv_found, 5 个对话字段, turn_found, 4 个轮次字段}；读不续期。
var scriptLoad = newScript(luaReadConv + `
local out = {0, '', '0', '', '0', '0', 0, '', '0', '', ''}
local c = conv(KEYS[1])
if c then
  out[1] = 1
  for i = 1, 5 do out[i + 1] = c[i] end
end
local t = redis.call('HMGET', KEYS[2], 'turn_id', 'started_ms', 'ts', 'ts_owner')
if t[1] then
  out[7] = 1
  out[8] = t[1]
  out[9] = t[2] or '0'
  out[10] = t[3] or ''
  out[11] = t[4] or ''
end
return out
`)

// SaveTurns：KEYS = 每条 (对话键, 轮次键)；ARGV[1]=对话 ttl_ms，ARGV[2]=轮次
// ttl_ms，之后每条 10 个参数：thread, window_n, window_id, compaction, prev,
// turn_id, started_ms, has_ts(0/1), ts, ts_owner。每条返回 9 个字段（对话 5 +
// 轮次 4）。
var scriptSave = newScript(luaReadConv + `
local out = {}
local n = #KEYS / 2
for i = 0, n - 1 do
  local ck, tk = KEYS[2 * i + 1], KEYS[2 * i + 2]
  local a = 2 + i * 10
  local c = conv(ck)
  if not c then
    redis.call('HSET', ck, 'thread', ARGV[a + 1], 'window_n', ARGV[a + 2], 'window_id', ARGV[a + 3], 'compaction', ARGV[a + 4], 'prev_compaction', ARGV[a + 5])
    c = {ARGV[a + 1], ARGV[a + 2], ARGV[a + 3], ARGV[a + 4], ARGV[a + 5]}
  end
  redis.call('PEXPIRE', ck, ARGV[1])
  local t = redis.call('HMGET', tk, 'turn_id', 'started_ms', 'ts', 'ts_owner')
  local tid, sms, ts, owner = t[1], t[2] or '0', t[3] or '', t[4] or ''
  if not tid or tid == '' then
    tid, sms = ARGV[a + 6], ARGV[a + 7]
    redis.call('HSET', tk, 'turn_id', tid, 'started_ms', sms)
  end
  if ARGV[a + 8] == '1' and tid == ARGV[a + 6] and (ts == '' or owner ~= ARGV[a + 10]) then
    ts, owner = ARGV[a + 9], ARGV[a + 10]
    redis.call('HSET', tk, 'ts', ts, 'ts_owner', owner)
  end
  redis.call('PEXPIRE', tk, ARGV[2])
  for _, v in ipairs({c[1], c[2], c[3], c[4], c[5], tid, sms, ts, owner}) do
    out[#out + 1] = v
  end
end
return out
`)

func convKey(k codexsdk.ConversationKey) string {
	return "codex:conv:" + hex.EncodeToString(k[:])
}

func turnKey(k codexsdk.ConversationKey, turn uint64) string {
	return fmt.Sprintf("codex:turn:%s:%016x", hex.EncodeToString(k[:]), turn)
}

func u64(v uint64) string { return strconv.FormatUint(v, 10) }

func ownerHex(o [16]byte) string {
	if o == ([16]byte{}) {
		return ""
	}
	return hex.EncodeToString(o[:])
}

// reply 是脚本返回数组的顺序读取器（第一处错误之后的读取都返回零值）。
type reply struct {
	arr []any
	i   int
	err error
}

func (r *reply) str() string {
	if r.err != nil {
		return ""
	}
	if r.i >= len(r.arr) {
		r.err = errors.New("redisstore: 脚本返回字段不足")
		return ""
	}
	v := r.arr[r.i]
	r.i++
	switch x := v.(type) {
	case string:
		return x
	case int64:
		return strconv.FormatInt(x, 10)
	case nil:
		return ""
	}
	r.err = fmt.Errorf("redisstore: 脚本返回字段类型 %T", v)
	return ""
}

func (r *reply) u64() uint64 {
	s := r.str()
	if s == "" || r.err != nil {
		return 0
	}
	v, err := strconv.ParseUint(s, 10, 64)
	if err != nil {
		r.err = fmt.Errorf("redisstore: 数值字段 %q: %w", s, err)
	}
	return v
}

func (r *reply) i64() int64 {
	s := r.str()
	if s == "" || r.err != nil {
		return 0
	}
	v, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		r.err = fmt.Errorf("redisstore: 数值字段 %q: %w", s, err)
	}
	return v
}

func (r *reply) flag() bool { return r.str() == "1" }

func (r *reply) conv() codexsdk.ConversationRecord {
	return codexsdk.ConversationRecord{
		Thread:          r.str(),
		WindowN:         r.u64(),
		ContextWindowID: r.str(),
		Compaction:      r.u64(),
		PrevCompaction:  r.u64(),
	}
}

func (r *reply) turn() codexsdk.TurnRecord {
	t := codexsdk.TurnRecord{TurnID: r.str(), StartedMs: r.i64(), TS: r.str()}
	if o := r.str(); o != "" && r.err == nil {
		b, err := hex.DecodeString(o)
		if err != nil || len(b) != 16 {
			r.err = fmt.Errorf("redisstore: ts_owner %q 无效", o)
			return t
		}
		copy(t.TSOwner[:], b)
	}
	return t
}

func (s *store) Open(ctx context.Context, key codexsdk.ConversationKey, fresh codexsdk.ConversationRecord) (codexsdk.ConversationRecord, bool, error) {
	arr, err := scriptOpen.run(ctx, s.rdb, []string{convKey(key)},
		fresh.Thread, u64(fresh.WindowN), fresh.ContextWindowID, u64(fresh.Compaction), u64(fresh.PrevCompaction), s.convTTLms)
	if err != nil {
		return codexsdk.ConversationRecord{}, false, err
	}
	r := reply{arr: arr}
	created := r.flag()
	rec := r.conv()
	return rec, created, r.err
}

func (s *store) Advance(ctx context.Context, key codexsdk.ConversationKey, oldMark, newMark uint64, newWindowID string) (codexsdk.ConversationRecord, error) {
	arr, err := scriptAdvance.run(ctx, s.rdb, []string{convKey(key)}, u64(oldMark), u64(newMark), newWindowID, s.convTTLms)
	if err != nil || len(arr) == 0 {
		return codexsdk.ConversationRecord{}, err
	}
	r := reply{arr: arr}
	rec := r.conv()
	return rec, r.err
}

func (s *store) Load(ctx context.Context, key codexsdk.ConversationKey, turn uint64) (codexsdk.ConversationRecord, bool, codexsdk.TurnRecord, bool, error) {
	arr, err := scriptLoad.run(ctx, s.rdb, []string{convKey(key), turnKey(key, turn)})
	if err != nil {
		return codexsdk.ConversationRecord{}, false, codexsdk.TurnRecord{}, false, err
	}
	r := reply{arr: arr}
	cf := r.flag()
	conv := r.conv()
	tf := r.flag()
	rec := r.turn()
	if r.err != nil {
		return codexsdk.ConversationRecord{}, false, codexsdk.TurnRecord{}, false, r.err
	}
	if !cf {
		conv = codexsdk.ConversationRecord{}
	}
	if !tf {
		rec = codexsdk.TurnRecord{}
	}
	return conv, cf, rec, tf, nil
}

func (s *store) SaveTurns(ctx context.Context, batch []codexsdk.TurnWrite) ([]codexsdk.TurnResult, error) {
	if len(batch) == 0 {
		return nil, nil
	}
	keys := make([]string, 0, 2*len(batch))
	args := make([]any, 0, 2+10*len(batch))
	args = append(args, s.convTTLms, s.turnTTLms)
	for i := range batch {
		w := &batch[i]
		keys = append(keys, convKey(w.Key), turnKey(w.Key, w.Turn))
		hasTS := "0"
		if w.HasTS {
			hasTS = "1"
		}
		args = append(args,
			w.Conv.Thread, u64(w.Conv.WindowN), w.Conv.ContextWindowID, u64(w.Conv.Compaction), u64(w.Conv.PrevCompaction),
			w.Rec.TurnID, strconv.FormatInt(w.Rec.StartedMs, 10), hasTS, w.Rec.TS, ownerHex(w.Rec.TSOwner))
	}
	arr, err := scriptSave.run(ctx, s.rdb, keys, args...)
	if err != nil {
		return nil, err
	}
	r := reply{arr: arr}
	out := make([]codexsdk.TurnResult, len(batch))
	for i := range out {
		out[i].Conv = r.conv()
		out[i].Turn = r.turn()
		out[i].OK = r.err == nil
	}
	if r.err != nil {
		return nil, r.err
	}
	return out, nil
}
