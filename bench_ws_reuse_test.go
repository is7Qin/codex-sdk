package codexsdk

import (
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	websocket "github.com/coder/websocket"
)

// reusableUpstream 是"一条连接上循环服务多次请求"的 bench 假上游。
//
// 与 newBenchWSUpstream 的区别只有一个：它在同一条连接上反复「读一帧 create →
// 回放脚本」，这正是连接复用要服务的形态；旧夹具一条连接只回放一次。
type reusableUpstream struct {
	srv     *httptest.Server
	script  []string
	accepts atomic.Int64
}

func newReusableUpstream(tb testing.TB, script []string) *reusableUpstream {
	tb.Helper()
	u := &reusableUpstream{script: script}
	u.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		u.accepts.Add(1)
		defer c.CloseNow()
		for {
			if _, _, rerr := c.Read(r.Context()); rerr != nil { // 等下一条 create
				return
			}
			for _, f := range u.script {
				if werr := c.Write(r.Context(), websocket.MessageText, []byte(f)); werr != nil {
					return
				}
			}
		}
	}))
	tb.Cleanup(func() {
		u.srv.CloseClientConnections()
		u.srv.Close()
	})
	return u
}

// BenchmarkWSStreamBodyReused：spec §9 P5/P6 —— 同一 client 的连续请求复用一条连接
// （连接身份不含 turn-state，跨轮也命中）。
//
// 客户端除 WithWSConnCache(2) 外与 P3 逐项同配置（含默认开启的 trace 自动注入）。
// 口径：预热一次（建立并归还第一条连接，不计入 b.N），此后每个 b.N 只付"发一帧 +
// 读尽 6 帧 + 关 body"的成本。P6 的两条断言写在这里，不成立直接 Fatal：
//   - 整个 bench 只拨一次号（accepts == 1）；
//   - 除预热外每个请求都命中（hits >= b.N）。
func BenchmarkWSStreamBodyReused(b *testing.B) {
	up := newReusableUpstream(b, []string{
		wsCreatedEv, wsDeltaEv, wsDeltaEv, wsDeltaEv, wsDeltaEv, wsCompletedEv,
	})
	hc := NewHTTPClient(PAT("pat-bench"),
		WithResponsesTransport(TransportWS),
		WithTurnAuto(false),
		WithTransport(benchFixedTransport(b, "https://chatgpt.com/backend-api/codex/responses", up.srv.URL)),
		WithWSConnCache(2),
	)
	b.Cleanup(hc.CloseIdleConnections)
	ctx := b.Context()

	round := func() error {
		resp, err := hc.StreamBody(ctx, wsBenchPayload)
		if err != nil {
			return err
		}
		n, cerr := io.Copy(io.Discard, resp.Body)
		if cerr != nil {
			_ = resp.Body.Close()
			return cerr
		}
		if cerr := resp.Body.Close(); cerr != nil {
			return cerr
		}
		benchSink = int(n)
		return nil
	}

	// 预热：建立第一条连接并在终态后归还（这一步付握手，不计入 b.N）。
	if err := round(); err != nil {
		b.Fatalf("预热失败: %v", err)
	}
	if got := up.accepts.Load(); got != 1 {
		b.Fatalf("预热后 accepts = %d，期望 1", got)
	}
	baseHits := hc.wsCache.hits.Load()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := round(); err != nil {
			b.Fatalf("StreamBody: %v", err)
		}
	}
	b.StopTimer()

	// P6：整个 bench 只拨一次号，且每个请求都命中。
	if got := up.accepts.Load(); got != 1 {
		b.Fatalf("accepts = %d，期望 1（连接被复用了）", got)
	}
	if hits := hc.wsCache.hits.Load() - baseHits; hits < int64(b.N) {
		b.Fatalf("hits = %d，期望 >= %d（有请求没命中）", hits, b.N)
	}
	b.Logf("P6: accepts=%d hits=%d b.N=%d", up.accepts.Load(), hc.wsCache.hits.Load()-baseHits, b.N)
}

// TestWSReuseR18Counters 核对与 P5 相同场景的观测计数。
func TestWSReuseR18Counters(t *testing.T) {
	up := newReusableUpstream(t, []string{
		wsCreatedEv, wsDeltaEv, wsDeltaEv, wsDeltaEv, wsDeltaEv, wsCompletedEv,
	})
	hc := NewHTTPClient(PAT("pat-bench"),
		WithResponsesTransport(TransportWS),
		WithTurnAuto(false),
		WithTransport(newFixedTransport(t, DefaultResponsesURL, up.srv.URL)),
		WithWSConnCache(2),
	)
	t.Cleanup(hc.CloseIdleConnections)
	const requests = 8
	for i := 0; i < requests; i++ {
		resp, err := hc.StreamBody(t.Context(), wsBenchPayload)
		if err != nil {
			t.Fatal(err)
		}
		_, err = io.Copy(io.Discard, resp.Body)
		cerr := resp.Body.Close()
		if err != nil {
			t.Fatal(err)
		}
		if cerr != nil {
			t.Fatal(cerr)
		}
		accepts, hits, misses := up.accepts.Load(), hc.wsCache.hits.Load(), hc.wsCache.misses.Load()
		if accepts != misses || hits+misses != int64(i+1) {
			t.Fatalf("R18: accepts=%d hits=%d misses=%d requests=%d", accepts, hits, misses, i+1)
		}
	}
	if up.accepts.Load() != 1 || hc.wsCache.hits.Load() != requests-1 {
		t.Fatal("R18: scenario did not reuse one connection")
	}
	t.Logf("R18: accepts=%d hits=%d misses=%d requests=%d", up.accepts.Load(), hc.wsCache.hits.Load(), hc.wsCache.misses.Load(), requests)
}
