package codexsdk

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	coderws "github.com/coder/websocket"
	"github.com/tidwall/gjson"
)

// 本文件是 S1–S5 的验收用例：
//
//	1 跨轮复用（连接身份不含 ts）+ 每轮帧带各自的 ts + 跨轮不回填旧响应头
//	2 provider 逐请求求值（值变了也不重连）
//	3 事件级 ts 捕获（response.metadata → observer + TurnState()）
//	4 fail-safe：created 之前的 metadata 一律不捕获（dropped +1，帧照旧透传）
//	5 归还路径：sseView.Close() → turnStateTap.Release → 连接回缓存
//	6 CloseWSIdleConnections 只清 WS 池，不碰共享 *http.Transport 的空闲连接
//
// 另有一条聚合臂的 S5 判据用例（与流式臂共用同一实现，不许两套）。

// ---- 假上游 ----

// tsCreatedFrame / tsMetadataFrame / tsCompletedFrame 是本文件用的事件样本：
// metadata 的形状对齐真实上游（顶层 response_id + headers 对象里带 ts）。
func tsCreatedFrame(id string) string {
	return `{"type":"response.created","response":{"id":"` + id + `","object":"response","status":"in_progress"}}`
}

func tsMetadataFrame(id, ts string) string {
	return `{"type":"response.metadata","response_id":"` + id + `","headers":{"` + HeaderTurnState + `":"` + ts + `"}}`
}

func tsCompletedFrame(id string) string {
	return `{"type":"response.completed","response":{"id":"` + id + `","object":"response","status":"completed","usage":` + respUsage + `}}`
}

// tsUpstream 是"一条连接上循环服务多次请求"的假上游：
//
//   - 每次连接计数一次 accepts；握手可签发 handshake（"" = 不签发）；
//   - 每读到一帧 create 就回放 respond(全局请求序号, 帧)；
//   - 记录客户端发来的全部帧（按请求顺序）。
type tsUpstream struct {
	srv *httptest.Server

	mu        sync.Mutex
	handshake string
	respond   func(reqIdx int, req []byte) []string
	frames    [][]byte
	accepts   atomic.Int64
	upgrades  atomic.Int64
}

func newTSUpstream(t *testing.T, respond func(int, []byte) []string) *tsUpstream {
	t.Helper()
	u := &tsUpstream{respond: respond}
	u.srv = httptest.NewServer(http.HandlerFunc(u.handle))
	t.Cleanup(func() {
		u.srv.CloseClientConnections()
		u.srv.Close()
	})
	return u
}

func (u *tsUpstream) handle(w http.ResponseWriter, r *http.Request) {
	u.upgrades.Add(1)
	u.mu.Lock()
	hs, respond := u.handshake, u.respond
	u.mu.Unlock()
	if hs != "" {
		w.Header().Set(HeaderTurnState, hs)
	}
	c, err := coderws.Accept(w, r, nil)
	if err != nil {
		return
	}
	u.accepts.Add(1)
	defer c.CloseNow()
	for {
		_, data, rerr := c.Read(r.Context())
		if rerr != nil {
			return
		}
		u.mu.Lock()
		u.frames = append(u.frames, append([]byte(nil), data...))
		idx := len(u.frames) - 1
		u.mu.Unlock()
		var out []string
		if respond != nil {
			out = respond(idx, data)
		}
		for _, f := range out {
			if werr := c.Write(r.Context(), coderws.MessageText, []byte(f)); werr != nil {
				return
			}
		}
	}
}

func (u *tsUpstream) setHandshake(v string) {
	u.mu.Lock()
	u.handshake = v
	u.mu.Unlock()
}

func (u *tsUpstream) sentFrames(t *testing.T) [][]byte {
	t.Helper()
	u.mu.Lock()
	defer u.mu.Unlock()
	out := make([][]byte, len(u.frames))
	copy(out, u.frames)
	return out
}

func (u *tsUpstream) client(t *testing.T, opts ...Option) *HTTPClient {
	t.Helper()
	base := []Option{
		WithResponsesTransport(TransportWS),
		WithTransport(newFixedTransport(t, "https://chatgpt.com/backend-api/codex/responses", u.srv.URL)),
		WithTurnAuto(false), WithTraceAuto(false),
	}
	return NewHTTPClient(PAT("pat-ts"), append(base, opts...)...)
}

// doStreamBodyFull 跑一次 StreamBody 并读尽 body（顺带触发终态后的归还），
// 返回响应（用于断言合成响应头）与 SSE 字节流。
func doStreamBodyFull(t *testing.T, hc *HTTPClient) (*http.Response, string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	resp, err := hc.StreamBody(ctx, []byte(`{"model":"gpt-5.6","input":[]}`))
	if err != nil {
		t.Fatalf("StreamBody: %v", err)
	}
	return resp, readAllBody(t, resp.Body)
}

// frameTurnState 取请求帧 client_metadata 里的 ts（缺失 = ""）。
func frameTurnState(t *testing.T, frame []byte) string {
	t.Helper()
	return gjson.GetBytes(frame, "client_metadata."+codexMetaTurnStateKey).String()
}

// tsProvider 是可变的 per-request provider（模拟网关按会话槽取值）。
type tsProvider struct{ v atomic.Value }

func (p *tsProvider) set(v string) { p.v.Store(v) }
func (p *tsProvider) get() string {
	s, _ := p.v.Load().(string)
	return s
}

// tsObserver 收集观察到的 ts（回调可能来自读路径，加锁）。
type tsObserver struct {
	mu   sync.Mutex
	seen []string
}

func (o *tsObserver) observe(ts string) {
	o.mu.Lock()
	o.seen = append(o.seen, ts)
	o.mu.Unlock()
}

func (o *tsObserver) values() []string {
	o.mu.Lock()
	defer o.mu.Unlock()
	return append([]string(nil), o.seen...)
}

// ---- 1. 跨轮复用 ----

// TestTurnStateCrossRoundReuse（S1 + S5）：同 client 跨轮两次 StreamBody。
// 第 1 轮带 ts（== 握手签发值）⇒ 合成响应头回填；第 2 轮 provider 清空 ⇒ 帧不带
// ts、合成响应头**不得**回填上一轮的旧值，而连接只拨 1 次（连接身份不含 ts）。
func TestTurnStateCrossRoundReuse(t *testing.T) {
	up := newTSUpstream(t, func(i int, _ []byte) []string {
		id := fmt.Sprintf("resp_%d", i+1)
		return []string{tsCreatedFrame(id), tsCompletedFrame(id)}
	})
	up.setHandshake("ts-round-1")
	prov := &tsProvider{}
	prov.set("ts-round-1")
	obs := &tsObserver{}
	hc := up.client(t, WithWSConnCache(2), WithTurnStateProvider(prov.get), WithTurnStateObserver(obs.observe))

	// 第 1 轮：请求带 ts-round-1（与握手签发值相同）。
	resp1, body1 := doStreamBodyFull(t, hc)
	if got := resp1.Header.Get(HeaderTurnState); got != "ts-round-1" {
		t.Fatalf("第 1 轮合成响应头 = %q, 期望 ts-round-1（effective == 最近签发值）", got)
	}
	if !strings.Contains(body1, "[DONE]") {
		t.Fatalf("第 1 轮响应缺少 [DONE]: %q", body1)
	}
	if got := hc.TurnState(); got != "ts-round-1" {
		t.Fatalf("TurnState() = %q, 期望 ts-round-1（握手捕获）", got)
	}

	// 第 2 轮：轮结束（网关按会话清 ts）⇒ provider 返回空。
	prov.set("")
	resp2, body2 := doStreamBodyFull(t, hc)
	if !strings.Contains(body2, "[DONE]") {
		t.Fatalf("第 2 轮响应缺少 [DONE]: %q", body2)
	}
	if got := hc.TurnState(); got != "ts-round-1" {
		t.Fatalf("TurnState() = %q, 期望仍是 ts-round-1（复用没有新签发）", got)
	}

	if got := up.accepts.Load(); got != 1 {
		t.Fatalf("accepts = %d, 期望 1（跨轮必须复用同一连接）", got)
	}
	frames := up.sentFrames(t)
	if len(frames) != 2 {
		t.Fatalf("上游收到 %d 帧, 期望 2 帧", len(frames))
	}
	if got := frameTurnState(t, frames[0]); got != "ts-round-1" {
		t.Fatalf("第 1 轮帧 ts = %q, 期望 ts-round-1", got)
	}
	if got := frameTurnState(t, frames[1]); got != "" {
		t.Fatalf("第 2 轮帧 ts = %q, 期望空（不得重放上一轮 ts）", got)
	}
	if got := resp2.Header.Get(HeaderTurnState); got != "" {
		t.Fatalf("第 2 轮合成响应头 = %q, 期望空（跨轮不得回填旧值）", got)
	}
	// observer 与捕获点共用写入口：握手签发值也会回调（第 2 轮无新签发，故只有一次）。
	if got := obs.values(); len(got) != 1 || got[0] != "ts-round-1" {
		t.Fatalf("observer 收到 %v, 期望 [ts-round-1]（握手捕获）", got)
	}
}

// TestTurnStateIntraTurnReuseBackfills（S5 第 2 支）：命中复用且本次请求带的 ts 仍
// 等于最近签发值（轮内重试）⇒ 合成响应头回填同一个值；连接不重拨。
func TestTurnStateIntraTurnReuseBackfills(t *testing.T) {
	up := newTSUpstream(t, func(i int, _ []byte) []string {
		id := fmt.Sprintf("resp_intra_%d", i+1)
		return []string{tsCreatedFrame(id), tsCompletedFrame(id)}
	})
	up.setHandshake("ts-intra")
	prov := &tsProvider{}
	prov.set("ts-intra")
	hc := up.client(t, WithWSConnCache(2), WithTurnStateProvider(prov.get))

	resp1, _ := doStreamBodyFull(t, hc)
	if got := resp1.Header.Get(HeaderTurnState); got != "ts-intra" {
		t.Fatalf("第 1 次（真握手）合成响应头 = %q, 期望 ts-intra", got)
	}
	resp2, _ := doStreamBodyFull(t, hc) // 命中复用，provider 仍返回同一个 ts
	if got := resp2.Header.Get(HeaderTurnState); got != "ts-intra" {
		t.Fatalf("第 2 次（轮内复用）合成响应头 = %q, 期望 ts-intra（effective == 最近签发值）", got)
	}
	if got := up.accepts.Load(); got != 1 {
		t.Fatalf("accepts = %d, 期望 1（轮内复用不重拨）", got)
	}
	frames := up.sentFrames(t)
	if len(frames) != 2 {
		t.Fatalf("上游收到 %d 帧, 期望 2 帧", len(frames))
	}
	for i := range frames {
		if got := frameTurnState(t, frames[i]); got != "ts-intra" {
			t.Fatalf("第 %d 轮帧 ts = %q, 期望 ts-intra", i+1, got)
		}
	}
}

// TestTurnStateAggregateArmSameCriterion（S5 共用判据）：同一个 client 跨轮两次
// Responses（聚合臂）：连接复用，第二轮的 HTTPResponse.TurnState 不得回填旧值。
func TestTurnStateAggregateArmSameCriterion(t *testing.T) {
	up := newTSUpstream(t, func(i int, _ []byte) []string {
		id := fmt.Sprintf("resp_agg_%d", i+1)
		return []string{tsCreatedFrame(id), tsCompletedFrame(id)}
	})
	up.setHandshake("ts-agg-1")
	prov := &tsProvider{}
	prov.set("ts-agg-1")
	hc := up.client(t, WithWSConnCache(2), WithTurnStateProvider(prov.get))
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	payload := []byte(`{"model":"gpt-5.6","input":[]}`)

	first, err := hc.Responses(ctx, payload)
	if err != nil {
		t.Fatalf("第 1 次 Responses: %v", err)
	}
	if first.TurnState != "ts-agg-1" {
		t.Fatalf("第 1 轮 HTTPResponse.TurnState = %q, 期望 ts-agg-1", first.TurnState)
	}

	prov.set("")
	second, err := hc.Responses(ctx, payload)
	if err != nil {
		t.Fatalf("第 2 次 Responses: %v", err)
	}
	if second.TurnState != "" {
		t.Fatalf("第 2 轮 HTTPResponse.TurnState = %q, 期望空（跨轮不得回填旧值）", second.TurnState)
	}
	if got := up.accepts.Load(); got != 1 {
		t.Fatalf("accepts = %d, 期望 1（聚合臂跨轮复用）", got)
	}
}

// ---- 2. provider 逐请求求值 ----

// TestTurnStateProviderPerRequest（S2）：provider 的返回值在请求之间改变 ⇒ 每帧
// 带当次值、**不重连**；provider 返回空时不得回落 TurnState() 缓存。
func TestTurnStateProviderPerRequest(t *testing.T) {
	up := newTSUpstream(t, func(i int, _ []byte) []string {
		id := fmt.Sprintf("resp_p_%d", i+1)
		return []string{tsCreatedFrame(id), tsCompletedFrame(id)}
	})
	up.setHandshake("hs-1") // 让"回落缓存"的变异有值可重放
	prov := &tsProvider{}
	prov.set("P1")
	hc := up.client(t, WithWSConnCache(2), WithTurnStateProvider(prov.get))

	want := []string{"P1", "P2", ""}
	var last *http.Response
	for _, v := range want {
		prov.set(v)
		last, _ = doStreamBodyFull(t, hc)
	}
	if got := up.accepts.Load(); got != 1 {
		t.Fatalf("accepts = %d, 期望 1（provider 变化不得重连）", got)
	}
	frames := up.sentFrames(t)
	if len(frames) != len(want) {
		t.Fatalf("上游收到 %d 帧, 期望 %d 帧", len(frames), len(want))
	}
	for i, v := range want {
		if got := frameTurnState(t, frames[i]); got != v {
			t.Fatalf("第 %d 轮帧 ts = %q, 期望 %q（provider 逐请求求值）", i+1, got, v)
		}
	}
	if got := last.Header.Get(HeaderTurnState); got != "" {
		t.Fatalf("末轮合成响应头 = %q, 期望空（provider 空 ⇒ 不得回填缓存）", got)
	}
}

// ---- 3. 事件级 ts 捕获 ----

// TestTurnStateTapCapturesMetadata（S3）：created → metadata 顺序发帧 ⇒ observer
// 收到事件 ts，且该值写入 TurnState()；帧本身原样透传。
func TestTurnStateTapCapturesMetadata(t *testing.T) {
	const id = "resp_evt"
	md := tsMetadataFrame(id, "ts-evt")
	up := newTSUpstream(t, func(int, []byte) []string {
		return []string{tsCreatedFrame(id), md, tsCompletedFrame(id)}
	})
	obs := &tsObserver{}
	hc := up.client(t, WithTurnStateObserver(obs.observe))

	_, body := doStreamBodyFull(t, hc)

	if got := hc.TurnState(); got != "ts-evt" {
		t.Fatalf("TurnState() = %q, 期望 ts-evt（事件级捕获）", got)
	}
	if got := obs.values(); len(got) != 1 || got[0] != "ts-evt" {
		t.Fatalf("observer 收到 %v, 期望 [ts-evt]", got)
	}
	if got := hc.turnStateDropped.Load(); got != 0 {
		t.Fatalf("dropped = %d, 期望 0（metadata 归属正确）", got)
	}
	if !strings.Contains(body, md) {
		t.Fatalf("metadata 帧没有被原样透传: %q", body)
	}
}

// ---- 4. fail-safe：created 之前 ----

// TestTurnStateTapFailSafeBeforeCreated（S3 fail-safe）：metadata 先于 created
// 到达 ⇒ observer 不被调用（dropped +1），但帧照旧透传；随后的合法 metadata
// 仍能被捕获。
func TestTurnStateTapFailSafeBeforeCreated(t *testing.T) {
	const id = "resp_x"
	early := tsMetadataFrame(id, "ts-early")
	late := tsMetadataFrame(id, "ts-late")
	up := newTSUpstream(t, func(int, []byte) []string {
		return []string{early, tsCreatedFrame(id), late, tsCompletedFrame(id)}
	})
	obs := &tsObserver{}
	hc := up.client(t, WithTurnStateObserver(obs.observe))

	_, body := doStreamBodyFull(t, hc)

	if got := obs.values(); len(got) != 1 || got[0] != "ts-late" {
		t.Fatalf("observer 收到 %v, 期望只有 [ts-late]（created 之前的 metadata 必须丢弃）", got)
	}
	if got := hc.TurnState(); got != "ts-late" {
		t.Fatalf("TurnState() = %q, 期望 ts-late", got)
	}
	if got := hc.turnStateDropped.Load(); got != 1 {
		t.Fatalf("dropped = %d, 期望 1", got)
	}
	// 帧一帧不少：被丢的只是 ts 候选。
	for _, want := range []string{early, late} {
		if !strings.Contains(body, want) {
			t.Fatalf("metadata 帧没有被透传（丢帧违反帧契约）: %q", body)
		}
	}
}

// ---- 5. 归还路径（Release 转发） ----

// TestTurnStateTapForwardsRelease（S3 帧契约）：读到终态后 sseView.Close() 走
// turnStateTap.Release（连接回缓存）⇒ 下一次请求仍复用同一条连接。
func TestTurnStateTapForwardsRelease(t *testing.T) {
	up := newTSUpstream(t, func(i int, _ []byte) []string {
		id := fmt.Sprintf("resp_rel_%d", i+1)
		return []string{tsCreatedFrame(id), tsCompletedFrame(id)}
	})
	hc := up.client(t, WithWSConnCache(2))

	doStreamBody(t, hc) // readAllBody 里的 Close 触发 Release
	hc.wsCache.mu.Lock()
	idle := len(hc.wsCache.idles)
	hc.wsCache.mu.Unlock()
	if idle != 1 {
		t.Fatalf("归还后缓存条目 = %d, 期望 1（Release 被 tap 吃掉了？）", idle)
	}
	doStreamBody(t, hc)
	if got := up.accepts.Load(); got != 1 {
		t.Fatalf("accepts = %d, 期望 1（Release 未转发则下一次必重拨）", got)
	}
	if got := hc.wsCache.hits.Load(); got < 1 {
		t.Fatalf("hits = %d, 期望 ≥1", got)
	}
}

// ---- 6. CloseWSIdleConnections 只清 WS 池 ----

// TestCloseWSIdleConnectionsOnlyClearsWS（S4）：清空 WS 缓存后下一条 WS 请求必须
// 重新拨号，但**不得**影响共享 *http.Transport 的空闲连接（用真 *http.Transport +
// DialContext 计数断言 dial 不增）。
//
// 这里刻意让 Transport 的静态类型就是 *http.Transport：CloseIdleConnections 的
// 断言才会命中——包装器会让正确实现与"改回 CloseIdleConnections"的变异都绿。
func TestCloseWSIdleConnectionsOnlyClearsWS(t *testing.T) {
	var dials atomic.Int64
	var wsAccepts atomic.Int64

	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.EqualFold(r.Header.Get("Upgrade"), "websocket") {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"id":"resp_http"}`))
			return
		}
		wsAccepts.Add(1)
		c, err := coderws.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer c.CloseNow()
		for {
			if _, _, rerr := c.Read(r.Context()); rerr != nil {
				return
			}
			if werr := c.Write(r.Context(), coderws.MessageText, []byte(tsCreatedFrame("resp_tls"))); werr != nil {
				return
			}
			if werr := c.Write(r.Context(), coderws.MessageText, []byte(tsCompletedFrame("resp_tls"))); werr != nil {
				return
			}
		}
	}))
	t.Cleanup(func() { srv.CloseClientConnections(); srv.Close() })
	target := srv.Listener.Addr().String()

	tr := &http.Transport{
		// 测试专用：目标是本机 TLS 假上游，证书不匹配 chatgpt.com。
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true}, //nolint:gosec
		DialContext: func(ctx context.Context, network, _ string) (net.Conn, error) {
			dials.Add(1)
			var d net.Dialer
			return d.DialContext(ctx, network, target)
		},
	}
	t.Cleanup(tr.CloseIdleConnections)

	hc := NewHTTPClient(PAT("pat-tls"),
		WithResponsesTransport(TransportWS),
		WithTransport(tr),
		WithWSConnCache(2),
		WithTurnAuto(false), WithTraceAuto(false),
	)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	doStreamBody(t, hc) // WS：#1
	if got := wsAccepts.Load(); got != 1 {
		t.Fatalf("WS accepts = %d, 期望 1", got)
	}

	// 建立一条 HTTP 空闲连接（会进 tr 的空闲池）。
	if _, err := hc.Do(ctx, []byte(`{}`)); err != nil {
		t.Fatalf("Do: %v", err)
	}
	before := dials.Load()

	hc.CloseWSIdleConnections()

	doStreamBody(t, hc) // WS 必须重拨（缓存已清）
	if got := wsAccepts.Load(); got != 2 {
		t.Fatalf("WS accepts = %d, 期望 2（CloseWSIdleConnections 必须清 WS 池）", got)
	}
	afterWS := dials.Load()
	if afterWS <= before {
		t.Fatalf("WS 重拨没有新增 TCP 连接（dials %d → %d）", before, afterWS)
	}

	// 共享 Transport 的空闲连接必须还在：紧接着一次 HTTP 请求不得再拨号。
	if _, err := hc.Do(ctx, []byte(`{}`)); err != nil {
		t.Fatalf("Do（清池后）: %v", err)
	}
	if got := dials.Load(); got != afterWS {
		t.Fatalf("清 WS 池后 HTTP 腿重拨了（dials %d → %d）——不得关闭 *http.Transport 的空闲连接", afterWS, got)
	}
}
