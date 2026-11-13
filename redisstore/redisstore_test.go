package redisstore

import (
	"context"
	"encoding/hex"
	"io"
	"net"
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
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr(), ContextTimeoutEnabled: true})
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

// TestNewRequiresContextTimeout：go-redis 客户端未开 ContextTimeoutEnabled → 构造期
// panic（否则 30ms 预算失效）。
func TestNewRequiresContextTimeout(t *testing.T) {
	for name, rdb := range map[string]redis.Scripter{
		"Client":        redis.NewClient(&redis.Options{Addr: "127.0.0.1:1"}),
		"ClusterClient": redis.NewClusterClient(&redis.ClusterOptions{Addrs: []string{"127.0.0.1:1"}}),
		"Ring":          redis.NewRing(&redis.RingOptions{Addrs: map[string]string{"a": "127.0.0.1:1"}}),
	} {
		func() {
			defer func() {
				if recover() == nil {
					t.Fatalf("%s: 未开 ContextTimeoutEnabled 应 panic", name)
				}
			}()
			New(rdb)
		}()
	}
	ok := redis.NewClusterClient(&redis.ClusterOptions{Addrs: []string{"127.0.0.1:1"}, ContextTimeoutEnabled: true})
	defer ok.Close()
	New(ok) // 不 panic
}

// TestStalledServerHonorsBudget：服务端只接受连接、从不回包；ContextTimeoutEnabled
// 下 Prepare 约 30ms 降级为一次性身份，而不是等 go-redis 的读超时。
func TestStalledServerHonorsBudget(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go func() { _, _ = io.Copy(io.Discard, c); _ = c.Close() }() // 只读不回
		}
	}()
	rdb := redis.NewClient(&redis.Options{Addr: ln.Addr().String(), ContextTimeoutEnabled: true, MaxRetries: -1})
	t.Cleanup(func() { _ = rdb.Close() })
	book := codexsdk.NewConversationBook(New(rdb))
	in := codexsdk.AnalyzeConversationInput([]byte(`{"input":[{"role":"user","content":"hi"}]}`))
	start := time.Now()
	id := book.Prepare(context.Background(), codexsdk.ConversationRequest{Key: codexsdk.NewConversationKey("d", 1, "c"), Input: in})
	el := time.Since(start)
	if el < 25*time.Millisecond || el > time.Second {
		t.Fatalf("Prepare 耗时 %v，期望约 30ms", el)
	}
	if st := book.Stats(); st.OpenFailures != 1 || st.OneShotIdentities != 1 || id.Session.ThreadID == "" || id.Sink != nil {
		t.Fatalf("应降级为一次性身份: %+v %+v", st, id)
	}
}
