package codexsdk

import (
	"bytes"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/coder/websocket"
)

// 本文件是 HTTP→WS 预热路径的每请求成本基准：不开连接缓存 ⇒ 每个请求新建连接；
// Turn.First 决定是否预热（预热 = 多一帧预热、多读一次预热响应、真实帧在预热后组装）。
// 两个基准只差 First，差值就是预热的增量成本（含进程内假上游两侧的分配）。

// newBenchPrewarmUpstream 起一个"每帧都回应"的假上游：预热帧（generate:false）回
// created / metadata / completed，真实帧回放 benchWSUpstream 的固定脚本。
func newBenchPrewarmUpstream(b *testing.B) *benchWSUpstream {
	b.Helper()
	u := &benchWSUpstream{reps: []string{
		wsCreatedEv, wsDeltaEv, wsDeltaEv, wsDeltaEv, wsDeltaEv, wsCompletedEv,
	}}
	prewarmReps := [][]byte{
		[]byte(tsCreatedFrame("resp_pw")),
		[]byte(tsMetadataFrame("resp_pw", "T-bench")),
		[]byte(tsCompletedFrame("resp_pw")),
	}
	generateFalse := []byte(`"generate":false`)
	u.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer c.CloseNow()
		for {
			_, data, rerr := c.Read(r.Context())
			if rerr != nil {
				return
			}
			if bytes.Contains(data, generateFalse) {
				for _, f := range prewarmReps {
					if werr := c.Write(r.Context(), websocket.MessageText, f); werr != nil {
						return
					}
				}
				continue
			}
			for _, f := range u.reps {
				if werr := c.Write(r.Context(), websocket.MessageText, []byte(f)); werr != nil {
					return
				}
			}
		}
	}))
	b.Cleanup(func() {
		u.srv.CloseClientConnections()
		u.srv.Close()
	})
	return u
}

func benchNewConnRequest(b *testing.B, first bool) {
	up := newBenchPrewarmUpstream(b)
	hc := benchWSClient(b, up) // 不开缓存：每个请求新建连接
	ctx := b.Context()
	id := Identity{
		InstallationID: "bench-installation",
		Session:        Session{SessionID: "bench-thread", ThreadID: "bench-thread", WindowID: "bench-thread:0"},
		Turn:           Turn{First: first},
	}
	before := ShapeStatsSnapshot().Prewarms
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		resp, err := hc.StreamBody(ctx, wsBenchPayload, id)
		if err != nil {
			b.Fatalf("StreamBody: %v", err)
		}
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
	}
	b.StopTimer()
	want := int64(0)
	if first {
		want = int64(b.N)
	}
	if got := ShapeStatsSnapshot().Prewarms - before; got != want {
		b.Fatalf("Prewarms 增量 = %d, 期望 %d（基准没走到预期路径）", got, want)
	}
}

// BenchmarkWSNewConnNoPrewarm：新建连接、非一轮首请求（不预热）。
func BenchmarkWSNewConnNoPrewarm(b *testing.B) { benchNewConnRequest(b, false) }

// BenchmarkWSNewConnPrewarm：新建连接、一轮首请求（预热 + 真实帧）。
func BenchmarkWSNewConnPrewarm(b *testing.B) { benchNewConnRequest(b, true) }
