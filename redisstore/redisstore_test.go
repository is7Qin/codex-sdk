package redisstore

import (
	"context"
	"encoding/hex"
	"strings"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	codexsdk "github.com/is7Qin/codex-sdk"
	"github.com/is7Qin/codex-sdk/internal/storecontract"
	"github.com/redis/go-redis/v9"
)

func newMini(t *testing.T) (*miniredis.Miniredis, *redis.Client) {
	t.Helper()
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	return mr, rdb
}

// TestConversationStoreContract：Redis 实现跑与内存实现相同的契约（miniredis）。
func TestConversationStoreContract(t *testing.T) {
	storecontract.Run(t, func(t *testing.T) storecontract.Harness {
		mr, rdb := newMini(t)
		return storecontract.Harness{Store: New(rdb), Advance: mr.FastForward}
	})
}

// TestRedisKeysAreHashed：键只含对话键哈希与轮次键十六进制，不含对话标识原文。
func TestRedisKeysAreHashed(t *testing.T) {
	mr, rdb := newMini(t)
	s := New(rdb)
	ctx := context.Background()
	const secret = "conversation-SENTINEL-7f3a"
	k := codexsdk.NewConversationKey("domain-SENTINEL", 42, secret)
	if _, _, err := s.Open(ctx, k, codexsdk.ConversationRecord{Thread: "t1", ContextWindowID: "w"}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.SaveTurns(ctx, []codexsdk.TurnWrite{{Key: k, Turn: 0xabc, Conv: codexsdk.ConversationRecord{Thread: "t1"},
		Rec: codexsdk.TurnRecord{TurnID: "u1"}}}); err != nil {
		t.Fatal(err)
	}
	keys := mr.Keys()
	want := map[string]bool{
		"codex:conv:" + hex.EncodeToString(k[:]):                       true,
		"codex:turn:" + hex.EncodeToString(k[:]) + ":0000000000000abc": true,
	}
	if len(keys) != len(want) {
		t.Fatalf("keys = %v", keys)
	}
	for _, key := range keys {
		if !want[key] {
			t.Fatalf("意外的键 %q", key)
		}
		if strings.Contains(key, "SENTINEL") {
			t.Fatalf("键含原文: %q", key)
		}
		fields, err := mr.HKeys(key)
		if err != nil {
			t.Fatal(err)
		}
		for _, f := range fields {
			if strings.Contains(mr.HGet(key, f), "SENTINEL") {
				t.Fatalf("值含原文: %s.%s", key, f)
			}
		}
	}
}

// TestRedisScriptFallbackAfterFlush：脚本缓存被清空（SCRIPT FLUSH / 重启）后经
// NOSCRIPT 回退 EVAL 仍可用。
func TestRedisScriptFallbackAfterFlush(t *testing.T) {
	_, rdb := newMini(t)
	s := New(rdb)
	ctx := context.Background()
	k := codexsdk.NewConversationKey("d", 1, "c")
	for i := 0; i < 2; i++ {
		if err := rdb.ScriptFlush(ctx).Err(); err != nil {
			t.Fatal(err)
		}
		if _, _, err := s.Open(ctx, k, codexsdk.ConversationRecord{Thread: "t1"}); err != nil {
			t.Fatalf("Open after flush: %v", err)
		}
	}
	ttl := rdb.PTTL(ctx, "codex:conv:"+hex.EncodeToString(k[:])).Val()
	if ttl <= 23*time.Hour || ttl > 24*time.Hour {
		t.Fatalf("对话记录 PTTL = %v, 期望约 24h", ttl)
	}
}
