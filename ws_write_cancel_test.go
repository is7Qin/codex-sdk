package codexsdk

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
)

// TestWSWriteCanceledKeepsCtxErr：调用方取消与写真实帧赛跑（取消守卫已终止连接、
// 写返回 errConnClosing / 连接已关闭类错误）时，返回的错误链必须包含 ctx.Err()。
func TestWSWriteCanceledKeepsCtxErr(t *testing.T) {
	payload := []byte(`{"model":"gpt-6.1-sol","input":[{"type":"message","role":"user","content":[{"type":"input_text","text":"hi"}]}]}`)
	for _, c := range []struct {
		name  string
		cache bool
	}{{"拨号路径", false}, {"缓存命中路径", true}} {
		t.Run(c.name, func(t *testing.T) {
			up := newPrewarmUpstream(t)
			var hc *HTTPClient
			if c.cache {
				hc = up.client(t, WithWSConnCache(2))
				doStreamBodyPayload(t, hc, followUpIdentity("thread-wc-"+c.name, "", nil), payload) // 归还，供下次命中
			} else {
				hc = up.client(t)
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			var once sync.Once
			hook := func() { // 写闸内、写出前：取消并等守卫把连接关掉
				once.Do(func() {
					cancel()
					up.waitConnClosed(t, 0, "取消守卫应终止连接")
				})
			}
			writeStampHook.Store(&hook)
			t.Cleanup(func() { writeStampHook.Store(nil) })
			resp, err := hc.StreamBody(ctx, payload, followUpIdentity("thread-wc-"+c.name, "", nil))
			writeStampHook.Store(nil)
			if err == nil {
				resp.Body.Close()
				t.Fatal("期望写失败")
			}
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("错误链缺 context.Canceled: %v", err)
			}
		})
	}
}

// TestCtxWriteErr：赛跑窗口里写返回 errConnClosing 时补上 ctx 错误；ctx 未结束或
// 原错误链已含 ctx 错误时原样返回。
func TestCtxWriteErr(t *testing.T) {
	live := context.Background()
	if got := ctxWriteErr(live, errConnClosing); got != errConnClosing {
		t.Fatalf("ctx 未结束必须原样返回: %v", got)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	got := ctxWriteErr(ctx, errConnClosing)
	if !errors.Is(got, context.Canceled) || !errors.Is(got, errConnClosing) {
		t.Fatalf("错误链须同时含 Canceled 与原错误: %v", got)
	}
	wrapped := fmt.Errorf("x: %w", context.Canceled)
	if got := ctxWriteErr(ctx, wrapped); got != wrapped {
		t.Fatalf("已含 ctx 错误时原样返回: %v", got)
	}
}
