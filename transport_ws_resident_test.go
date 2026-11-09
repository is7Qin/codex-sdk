// SPDX-License-Identifier: AGPL-3.0-or-later
// Dual-licensed: AGPL-3.0-or-later (open source) or commercial license (closed-source
// deployment exemption); see LICENSE and LICENSE.commercial. Copyright (c) 2026 is7Qin.

package codexsdk

import (
	"context"
	"crypto/tls"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	coderws "github.com/coder/websocket"
)

// ---- 假上游：常驻读 + 主动发帧 + 主动升级拒绝 ----

// wsResidentUpstream 假 Responses WS 上游：升级成功后**保持一条常驻 Read**
// （coder/websocket 只在读循环在途时自动回 pong），并按 interval 持续写帧。
//
// 常驻读是这类用例的前提：只写不读的上游收不到 ping、也不会回 pong，客户端
// 心跳必然超时——那测的就不是客户端读模型了。
type wsResidentUpstream struct {
	srv      *httptest.Server
	frames   []string
	interval time.Duration

	// done 在脚本帧写完后关闭：用例靠它等"生产侧跑完"，不用 sleep。
	done   chan struct{}
	writes atomic.Int64
}

func newWSResidentUpstream(t *testing.T, frames []string, interval time.Duration) *wsResidentUpstream {
	t.Helper()
	u := &wsResidentUpstream{frames: frames, interval: interval, done: make(chan struct{})}
	u.srv = httptest.NewServer(http.HandlerFunc(u.handle))
	t.Cleanup(func() {
		u.srv.CloseClientConnections()
		u.srv.Close()
	})
	return u
}

func (u *wsResidentUpstream) handle(w http.ResponseWriter, r *http.Request) {
	c, err := coderws.Accept(w, r, nil)
	if err != nil {
		return
	}
	defer c.CloseNow()
	// 常驻读：自动回 pong，同时吃掉客户端发来的 create 帧。
	go func() {
		for {
			if _, _, rerr := c.Read(r.Context()); rerr != nil {
				return
			}
		}
	}()
	defer close(u.done)
	if u.interval > 0 {
		for i, f := range u.frames {
			if i > 0 {
				select {
				case <-time.After(u.interval):
				case <-r.Context().Done():
					return
				}
			}
			if werr := c.Write(r.Context(), coderws.MessageText, []byte(f)); werr != nil {
				return
			}
			u.writes.Add(1)
		}
		_ = c.Close(coderws.StatusNormalClosure, "")
		return
	}
	for _, f := range u.frames {
		if werr := c.Write(r.Context(), coderws.MessageText, []byte(f)); werr != nil {
			return
		}
		u.writes.Add(1)
	}
	_ = c.Close(coderws.StatusNormalClosure, "")
}

// newWSClient 指向假上游的 WS 客户端（固定 URL 重写到 httptest）。
func newWSClient(t *testing.T, up *wsResidentUpstream, opts ...Option) *HTTPClient {
	t.Helper()
	base := []Option{
		WithResponsesTransport(TransportWS),
		WithTransport(newFixedTransport(t, "https://chatgpt.com/backend-api/codex/responses", up.srv.URL)),
		WithTurnAuto(false),
	}
	return NewHTTPClient(PAT("pat-resident"), append(base, opts...)...)
}

// TestWSReaderKeepsPongsWhileConsumerIdle（spec §7-8 / momus I1）：
// 消费者**故意 3s 不 Read**（> 2s pong 超时），心跳期间连接必须存活、帧不得丢。
//
// 旧实现（纯拉模型）在本用例下必红：没有 Read 在飞 ⇒ pong 没人读 ⇒ 200ms 的 ping
// 在 2s 超时后 CloseNow ⇒ ReadAll 拿到传输错误 / 帧缺失。
func TestWSReaderKeepsPongsWhileConsumerIdle(t *testing.T) {
	frames := []string{wsCreatedEv, wsDeltaEv, wsDeltaEv, wsDeltaEv, wsDeltaEv, wsCompletedEv}
	// 3s 的空闲窗口由生产侧 pacing 产生（600ms × 5 段），用例只等屏障。
	up := newWSResidentUpstream(t, frames, 600*time.Millisecond)
	hc := newWSClient(t, up, WithPingInterval(200*time.Millisecond))

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	resp, err := hc.StreamBody(ctx, []byte(`{"model":"gpt-5.6","input":[]}`), Identity{})
	if err != nil {
		t.Fatalf("StreamBody: %v", err)
	}
	// 消费者不 Read，直到生产侧把脚本帧全部发完（≈3s > 2s pong 超时）。
	select {
	case <-up.done:
	case <-time.After(20 * time.Second):
		t.Fatal("生产侧未在 20s 内完成（假上游卡住？）")
	}

	body, readErr := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if readErr != nil {
		t.Fatalf("消费者空闲 %v 后读 body 失败（常驻读协程没保住连接？）: %v", 3*time.Second, readErr)
	}
	got := string(body)
	for _, want := range []string{`event:response.created`, `event:response.completed`, "data: [DONE]"} {
		if !strings.Contains(got, want) {
			t.Fatalf("body 缺 %q；实际:\n%s", want, got)
		}
	}
	if n := strings.Count(got, `"type":"response.output_text.delta"`); n != 4 {
		t.Fatalf("delta 帧数 = %d，想要 4（空闲期不得丢帧）", n)
	}
}

// TestWSReaderClosePromptAndIdempotent：Close 立即返回（终止强断，不会卡在
// 无消费者的队列上），重复调用幂等。
func TestWSReaderClosePromptAndIdempotent(t *testing.T) {
	up := newWSResidentUpstream(t, []string{wsCreatedEv}, 0)
	hc := newWSClient(t, up)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	resp, err := hc.StreamBody(ctx, []byte(`{"model":"gpt-5.6","input":[]}`), Identity{})
	if err != nil {
		t.Fatalf("StreamBody: %v", err)
	}
	done := make(chan error, 1)
	go func() {
		t0 := time.Now()
		e1 := resp.Body.Close()
		e2 := resp.Body.Close()
		if time.Since(t0) > 3*time.Second {
			done <- errors.New("Close 超过 3s 未返回")
			return
		}
		done <- errors.Join(e1, e2)
	}()
	select {
	case e := <-done:
		if e != nil {
			t.Fatalf("Close: %v", e)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Close 卡住")
	}
}

// TestWSDialErrorCarriesBody（spec §7 / momus I6）：升级被拒时，上游 body 必须
// 进 *HTTPError.Raw——两臂的错误分类面（error_message_contains / 判死）才对称。
func TestWSDialErrorCarriesBody(t *testing.T) {
	const upBody = `{"error":{"message":"instructions must not be empty","type":"invalid_request_error"}}`
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(upBody))
	}))
	t.Cleanup(srv.Close)

	hc := NewHTTPClient(PAT("pat-400"),
		WithResponsesTransport(TransportWS),
		WithTransport(newFixedTransport(t, "https://chatgpt.com/backend-api/codex/responses", srv.URL)),
		WithTurnAuto(false),
	)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, err := hc.StreamBody(ctx, []byte(`{"model":"gpt-5.6","input":[]}`), Identity{})
	if err == nil {
		t.Fatal("期望升级失败")
	}
	var he *HTTPError
	if !errors.As(err, &he) {
		t.Fatalf("错误类型 = %T，想要 *HTTPError: %v", err, err)
	}
	if he.StatusCode != http.StatusBadRequest {
		t.Fatalf("状态码 = %d，想要 400", he.StatusCode)
	}
	if !strings.Contains(string(he.Raw), "instructions must not be empty") {
		t.Fatalf("Raw 未携带上游 body: %q", string(he.Raw))
	}
}

// ---- h2 回归（spec §7-7 / momus C1）----

// rewriteRT 把固定上游 URL 重写到测试服务器，底层 transport 可注入（TLS/h2）。
type rewriteRT struct {
	expected string
	target   *url.URL
	base     http.RoundTripper
}

func (rt *rewriteRT) RoundTrip(req *http.Request) (*http.Response, error) {
	if got := req.URL.String(); got != rt.expected {
		return nil, errors.New("unexpected URL: " + got)
	}
	clone := req.Clone(req.Context())
	u2 := *req.URL
	u2.Scheme = rt.target.Scheme
	u2.Host = rt.target.Host
	clone.URL = &u2
	return rt.base.RoundTrip(clone)
}

// TestWSDialOverHTTP2CapableTransport：**生产默认就是这条路径**——网关把
// `ForceAttemptHTTP2: true` 的出口 transport 交给 SDK（`upstream.force_http2`
// 默认 true）。Go 的 net/http 对带 `Connection: Upgrade` 的请求自动降为 h1，
// 所以升级必须成功；普通请求仍走 h2（本用例先证明 transport 确实是 h2 能力）。
//
// 目的不是"修 bug"，是钉住这个前提：将来若换成自定义 RoundTripper / 手写 ALPN，
// 让升级悄悄跑不起来，ws 臂会 100% 失败而两臂读数看起来只是"WS 更差"。
func TestWSDialOverHTTP2CapableTransport(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/plain", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(r.Proto + "|" + r.TLS.NegotiatedProtocol))
	})
	mux.HandleFunc("/backend-api/codex/responses", func(w http.ResponseWriter, r *http.Request) {
		c, err := coderws.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer c.CloseNow()
		_, _, _ = c.Read(r.Context())
		for _, f := range []string{wsCreatedEv, wsCompletedEv} {
			if werr := c.Write(r.Context(), coderws.MessageText, []byte(f)); werr != nil {
				return
			}
		}
		_ = c.Close(coderws.StatusNormalClosure, "")
	})
	srv := httptest.NewUnstartedServer(mux)
	srv.EnableHTTP2 = true
	srv.StartTLS()
	t.Cleanup(srv.Close)

	base := &http.Transport{
		TLSClientConfig:   &tls.Config{InsecureSkipVerify: true},
		ForceAttemptHTTP2: true,
	}
	// 前置：这条 transport 对普通请求确实协商到 h2（否则本用例什么都没证明）。
	plain, err := (&http.Client{Transport: base}).Get(srv.URL + "/plain")
	if err != nil {
		t.Fatalf("plain GET: %v", err)
	}
	pb, _ := io.ReadAll(plain.Body)
	_ = plain.Body.Close()
	if plain.Proto != "HTTP/2.0" || !strings.HasPrefix(string(pb), "HTTP/2.0|h2") {
		t.Fatalf("transport 没走 h2（proto=%s body=%q），用例失去意义", plain.Proto, pb)
	}

	u, _ := url.Parse(srv.URL)
	rt := &rewriteRT{
		expected: "https://chatgpt.com/backend-api/codex/responses",
		target:   u,
		base:     base,
	}
	hc := NewHTTPClient(PAT("pat-h2"),
		WithResponsesTransport(TransportWS),
		WithTransport(rt),
		WithTurnAuto(false),
	)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	resp, err := hc.StreamBody(ctx, []byte(`{"model":"gpt-5.6","input":[]}`), Identity{})
	if err != nil {
		t.Fatalf("h2 能力 transport 上的 WS 升级失败: %v", err)
	}
	body, err := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if err != nil {
		t.Fatalf("读 body: %v", err)
	}
	if !strings.Contains(string(body), "data: [DONE]") {
		t.Fatalf("body 缺 [DONE]:\n%s", string(body))
	}
}
