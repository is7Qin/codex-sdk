package codexsdk

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	coderws "github.com/coder/websocket"
)

// 本文件是 T5.2（连接缓存）的验收用例：R1/R4/R6/R7/R8/R9/R10/R11/R12/R13/R19/R21/R24/R25。
// R22 在下面两组（旧尾帧 = 普通帧 / error 帧透传）；R5（复用连接上的**只写**失败）没有独立用例——
// 要确定性制造“socket 只坏在读不到”的窗口不现实，见实现报告 §5。

// reuseUpstream 是"一条连接上循环服务多次请求"的假上游：
// 每读到一帧 create 就回放 scripts[min(n, len-1)]，n 是该连接上的第几次请求。
type reuseUpstream struct {
	srv     *httptest.Server
	scripts [][]string
	accepts atomic.Int64

	mu       sync.Mutex
	turnSeq  []string // 第 i 次升级签发什么 ts（越界复用最后一个；"" = 不签发）
	requests atomic.Int64
	// closeAfter 为真时，每次响应写完就主动关连接（模拟"上游不保活"）。
	closeAfter bool
	// gate 非 nil 时，handler 写完响应后先等它关闭再往下走：用来把"请求在飞"变成
	// 确定性屏障（R7 需要两个请求真的重叠）。
	gate chan struct{}
}

func newReuseUpstream(t *testing.T, scripts ...[]string) *reuseUpstream {
	t.Helper()
	if len(scripts) == 0 {
		scripts = [][]string{{wsCreatedEv, wsCompletedEv}}
	}
	u := &reuseUpstream{scripts: scripts}
	u.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		u.mu.Lock()
		idx := int(u.accepts.Load())
		ts := ""
		if len(u.turnSeq) > 0 {
			if idx < len(u.turnSeq) {
				ts = u.turnSeq[idx]
			} else {
				ts = u.turnSeq[len(u.turnSeq)-1]
			}
		}
		u.mu.Unlock()
		if ts != "" {
			w.Header().Set(HeaderTurnState, ts)
		}
		c, err := coderws.Accept(w, r, nil)
		if err != nil {
			return
		}
		u.accepts.Add(1)
		defer c.CloseNow()
		n := 0
		for {
			if _, _, rerr := c.Read(r.Context()); rerr != nil {
				return
			}
			u.requests.Add(1)
			script := u.scripts[len(u.scripts)-1]
			if n < len(u.scripts) {
				script = u.scripts[n]
			}
			n++
			// gate 在**写响应之前**等：把"请求已到达上游但还没回帧"变成确定性屏障。
			if g := u.gate; g != nil {
				select {
				case <-g:
				case <-r.Context().Done():
					return
				}
			}
			for _, f := range script {
				if werr := c.Write(r.Context(), coderws.MessageText, []byte(f)); werr != nil {
					return
				}
			}
			if u.closeAfter {
				_ = c.Close(coderws.StatusNormalClosure, "")
				return
			}
		}
	}))
	t.Cleanup(func() { u.srv.CloseClientConnections(); u.srv.Close() })
	return u
}

func (u *reuseUpstream) setTurnSeq(seq ...string) {
	u.mu.Lock()
	u.turnSeq = seq
	u.mu.Unlock()
}

func (u *reuseUpstream) client(t *testing.T, opts ...Option) *HTTPClient {
	t.Helper()
	base := []Option{
		WithResponsesTransport(TransportWS),
		WithTransport(newFixedTransport(t, "https://chatgpt.com/backend-api/codex/responses", u.srv.URL)),
		WithTraceAuto(false),
	}
	return NewHTTPClient(PAT("pat-reuse"), append(base, opts...)...)
}

// reuseIdentity 是该文件默认的稳定身份：连接复用键固定，window 固定。
func reuseIdentity() Identity { return identityFor("thread-reuse", "thread-reuse:0", "", nil) }

// doStreamBody 跑一次 StreamBody 并读尽 body（失败即 t.Fatal）。
func doStreamBody(t *testing.T, hc *HTTPClient) string {
	t.Helper()
	return doStreamBodyID(t, hc, reuseIdentity())
}

func doStreamBodyID(t *testing.T, hc *HTTPClient, id Identity) string {
	t.Helper()
	body, err := streamBodyOnceID(hc, id)
	if err != nil {
		t.Fatalf("StreamBody: %v", err)
	}
	return body
}

// streamBodyOnce 跑一次 StreamBody 并读尽 body。**可在子 goroutine 里调**：
// 它不碰 *testing.T，错误交给调用方决定怎么报（t.Fatalf 只能用在测试主 goroutine）。
func streamBodyOnce(hc *HTTPClient) (string, error) { return streamBodyOnceID(hc, reuseIdentity()) }

func streamBodyOnceID(hc *HTTPClient, id Identity) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	resp, err := hc.StreamBody(ctx, []byte(`{"model":"gpt-5.6","input":[]}`), id)
	if err != nil {
		return "", err
	}
	defer func() { _ = resp.Body.Close() }()
	b, rerr := io.ReadAll(resp.Body)
	if rerr != nil {
		return "", rerr
	}
	return string(b), nil
}

// ---- R1 / R12：命中与默认关闭 ----

// TestWSConnReuseHits（R1）：同一 thread 的 3 次 StreamBody ⇒ 只拨一次号、每次都读到
// [DONE]，且假上游收到的 response.create 条数 == 请求数（**没有隐式重试**）。
func TestWSConnReuseHits(t *testing.T) {
	up := newReuseUpstream(t)
	hc := up.client(t, WithWSConnCache(2))

	const requests = 3
	bodies := make([]string, 0, requests)
	for i := 0; i < requests; i++ {
		bodies = append(bodies, doStreamBody(t, hc))
	}
	for i, body := range bodies {
		if !strings.Contains(body, "[DONE]") {
			t.Fatalf("第 %d 次响应缺少 [DONE]: %q", i+1, body)
		}
	}
	if got := up.accepts.Load(); got != 1 {
		t.Fatalf("accepts = %d，期望 1（第二/三次应命中缓存）", got)
	}
	if got := up.requests.Load(); got != requests {
		t.Fatalf("上游收到 %d 条 response.create，期望 %d（不得隐式重试）", got, requests)
	}
	if hits := hc.wsCache.hits.Load(); hits < 1 {
		t.Fatalf("hits = %d，期望 ≥1", hits)
	}
}

// TestWSConnReuseDefaultOff（R12）：默认（不传 WithWSConnCache）每请求一条连接。
func TestWSConnReuseDefaultOff(t *testing.T) {
	up := newReuseUpstream(t)
	hc := up.client(t)
	if hc.wsCache != nil {
		t.Fatal("默认不该启用缓存")
	}
	doStreamBody(t, hc)
	doStreamBody(t, hc)
	if got := up.accepts.Load(); got != 2 {
		t.Fatalf("accepts = %d，期望 2（默认每请求一条连接）", got)
	}
}

// ---- R4：非终态 Close ⇒ 不归还 ----

// TestWSConnNotReusedAfterAbandon（R4）：读到一半就 Close（非终态）⇒ 连接不归还，
// 下一个请求必须新拨。
func TestWSConnNotReusedAfterAbandon(t *testing.T) {
	up := newReuseUpstream(t)
	hc := up.client(t, WithWSConnCache(2))

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	resp, err := hc.StreamBody(ctx, []byte(`{"model":"gpt-5.6","input":[]}`), Identity{})
	if err != nil {
		t.Fatalf("StreamBody: %v", err)
	}
	buf := make([]byte, 8)
	if _, rerr := resp.Body.Read(buf); rerr != nil {
		t.Fatalf("首帧读取: %v", rerr)
	}
	_ = resp.Body.Close() // 非终态 ⇒ 放弃

	doStreamBody(t, hc)
	if got := up.accepts.Load(); got != 2 {
		t.Fatalf("accepts = %d，期望 2（被放弃的连接不能复用）", got)
	}
}

// ---- R13：CloseIdleConnections 清缓存 ----

// TestWSConnCacheClearedByCloseIdle（R13）：清空后下一个请求必须新拨。
func TestWSConnCacheClearedByCloseIdle(t *testing.T) {
	up := newReuseUpstream(t)
	hc := up.client(t, WithWSConnCache(2))
	doStreamBody(t, hc)
	hc.CloseIdleConnections()
	doStreamBody(t, hc)
	if got := up.accepts.Load(); got != 2 {
		t.Fatalf("accepts = %d，期望 2（缓存已被清空）", got)
	}
}

// ---- S1：turn-state 变化不影响连接身份 ----

// TestWSConnReuseIgnoresTurnState：连接身份不含 ts——第二个请求带不同
// Identity.Turn.State 仍复用同一条连接；握手响应头的 ts 不进 Sink。
func TestWSConnReuseIgnoresTurnState(t *testing.T) {
	up := newReuseUpstream(t)
	up.setTurnSeq("A", "B")
	hc := up.client(t, WithWSConnCache(2))

	var sink1 turnStateSink
	doStreamBodyID(t, hc, identityFor("thread-reuse", "thread-reuse:0", "", &sink1))
	if got := sink1.values(); len(got) != 0 {
		t.Fatalf("Sink = %v，期望空（握手响应头的 ts 不取）", got)
	}
	var sink2 turnStateSink
	doStreamBodyID(t, hc, identityFor("thread-reuse", "thread-reuse:1", "B", &sink2))
	if got := up.accepts.Load(); got != 1 {
		t.Fatalf("accepts = %d，期望 1（跨 ts/window 必须复用同一连接）", got)
	}
	if got := sink2.values(); len(got) != 0 {
		t.Fatalf("复用没有新握手，Sink 应为空, got %v", got)
	}
}

// ---- R22（改写）：旧尾帧就是普通帧，由消费者自己忽略 ----

// TestWSConnReusedStaleFrameIsOrdinary（R22 改写）：第二次请求在复用连接上收到的
// **首帧**是上一轮的残留 delta ⇒ 不再做任何协议判定，它作为普通事件交付，本轮照常
// 读到终态并成功结束。
//
// 这就是真实 codex 客户端的行为：没有首帧白名单、没有 created 门槛、没有
// response_id 过滤，不认识的事件由各自消费者忽略。SDK 只负责"复用准入干净"
// （空闲期收到帧 ⇒ 该连接作废，见 frameInbox.fence）。
func TestWSConnReusedStaleFrameIsOrdinary(t *testing.T) {
	up := newReuseUpstream(t,
		[]string{wsCreatedEv, wsCompletedEv},
		[]string{wsDeltaEv, wsCompletedEv}, // 第二个请求的首帧是上一轮的残留 delta
	)
	hc := up.client(t, WithWSConnCache(2))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	// 两次都走聚合臂（同一条取数路径 ⇒ 同一个 key，第二次才会命中）。
	noop := func([]byte) error { return nil }
	if err := hc.responsesStreamWS(ctx, []byte(`{"model":"gpt-5.6","input":[]}`), reuseIdentity(), noop); err != nil {
		t.Fatalf("第一次: %v", err)
	}

	var seen []string
	err := hc.responsesStreamWS(ctx, []byte(`{"model":"gpt-5.6","input":[]}`), reuseIdentity(), func(raw []byte) error {
		seen = append(seen, string(raw))
		return nil
	})
	if err != nil {
		t.Fatalf("第二次: %v（旧帧必须当普通事件交付，不能报错）", err)
	}
	if len(seen) != 2 || !strings.Contains(seen[0], `"type":"response.output_text.delta"`) {
		t.Fatalf("交付序列 = %v，期望 [旧 delta, completed]（帧既不能被吞掉，也不能被特判）", seen)
	}
	if got := up.accepts.Load(); got != 1 {
		t.Fatalf("accepts = %d，期望 1（第二次必须命中复用连接）", got)
	}
}

// TestWSConnReusedErrorFramePassesThrough（R22 反向）：首帧是**无 status 的** error
// ⇒ 按普通 in-band error 透传给消费者，本轮随后照常读到终态（带 4xx/5xx status 的
// error 帧走另一条路：翻译成错误并退役连接，见 TestWSErrorFrameBecomesHTTPError429）。
func TestWSConnReusedErrorFramePassesThrough(t *testing.T) {
	up := newReuseUpstream(t,
		[]string{wsCreatedEv, wsCompletedEv},
		[]string{`{"type":"error","error":{"type":"server_error","message":"boom"}}`, wsCompletedEv},
	)
	hc := up.client(t, WithWSConnCache(2))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	noop := func([]byte) error { return nil }
	if err := hc.responsesStreamWS(ctx, []byte(`{"model":"gpt-5.6","input":[]}`), reuseIdentity(), noop); err != nil {
		t.Fatalf("第一次: %v", err)
	}

	var seen []byte
	err := hc.responsesStreamWS(ctx, []byte(`{"model":"gpt-5.6","input":[]}`), reuseIdentity(), func(raw []byte) error {
		seen = append(seen, raw...)
		return nil
	})
	if err != nil {
		t.Fatalf("第二次: %v（error 帧是普通事件，不该变成传输/协议错误）", err)
	}
	if !strings.Contains(string(seen), `"type":"error"`) {
		t.Fatalf("error 帧没有被透传给下游: %q", seen)
	}
}

// ---- R8 / R21 / R25：寿命 ----

// TestWSConnCacheTTLExpires（R8）：注入极短 TTL ⇒ 空闲连接被 errConnExpired 退役，
// 下一个请求新拨。
func TestWSConnCacheTTLExpires(t *testing.T) {
	up := newReuseUpstream(t)
	hc := up.client(t, WithWSConnCache(2), withConnSweepInterval(5*time.Millisecond))
	doStreamBody(t, hc)
	hc.wsCache.mu.Lock()
	cc := hc.wsCache.idles[0].cn
	hc.wsCache.ttl = 20 * time.Millisecond
	hc.wsCache.mu.Unlock()

	waitFor(t, func() bool {
		hc.wsCache.mu.Lock()
		n := len(hc.wsCache.idles)
		hc.wsCache.mu.Unlock()
		return n == 0
	})
	// sweep 在锁内摘表、锁外 terminate：摘表与关闭之间本来就有窗口，测试必须
	// 等关闭完成（有界），不能在看到空表的瞬间非阻塞检查（那是测试自身的竞态）。
	select {
	case <-cc.closeDone:
	case <-time.After(2 * time.Second):
		t.Fatal("空闲条目已摘表但连接 2s 内未关闭")
	}
	if !errors.Is(cc.err(), errConnExpired) {
		t.Fatalf("退役原因 = %v，期望 errConnExpired", cc.err())
	}
	doStreamBody(t, hc)
	if got := up.accepts.Load(); got != 2 {
		t.Fatalf("accepts = %d，期望 2（空闲连接已过期）", got)
	}
}

// TestWSConnCacheMaxAge（R21/R25）：命中不重置 bornAt；到点后必须退役。
func TestWSConnCacheMaxAge(t *testing.T) {
	up := newReuseUpstream(t)
	hc := up.client(t, WithWSConnCache(2), withConnSweepInterval(5*time.Millisecond))
	hc.wsCache.maxAge = 60 * time.Millisecond
	hc.wsCache.ttl = time.Minute // 只看 maxAge

	doStreamBody(t, hc) // 建连
	bornAt := time.Now()
	hc.wsCache.mu.Lock()
	if len(hc.wsCache.idles) != 1 {
		hc.wsCache.mu.Unlock()
		t.Fatal("第一次请求后缓存应为 1 条")
	}
	firstBorn := hc.wsCache.idles[0].bornAt
	hc.wsCache.mu.Unlock()
	if firstBorn.After(bornAt) {
		t.Fatal("bornAt 不该晚于建连时刻")
	}

	// 连续命中两次：bornAt 必须保持最初值。
	for i := 0; i < 2; i++ {
		doStreamBody(t, hc)
		hc.wsCache.mu.Lock()
		got := hc.wsCache.idles[0].bornAt
		hc.wsCache.mu.Unlock()
		if !got.Equal(firstBorn) {
			t.Fatalf("第 %d 次命中后 bornAt 被重置了（%v → %v）", i+1, firstBorn, got)
		}
	}

	waitFor(t, func() bool { // 到点退役
		hc.wsCache.mu.Lock()
		defer hc.wsCache.mu.Unlock()
		return len(hc.wsCache.idles) == 0
	})
	doStreamBody(t, hc)
	if got := up.accepts.Load(); got != 2 {
		t.Fatalf("accepts = %d，期望 2（超龄连接必须退役）", got)
	}
}

// TestWSConnCacheSweepDoesNotKillTaken（R24）：被取用（已摘表）的连接不会被 sweep 误杀。
func TestWSConnCacheSweepDoesNotKillTaken(t *testing.T) {
	up := newReuseUpstream(t)
	hc := up.client(t, WithWSConnCache(2))
	doStreamBody(t, hc)

	// (a) 正对照：**仍在表里**的条目过期 ⇒ sweep 必须真的退役它
	// （否则"不误杀"可能只是因为 sweep 什么都不做）。
	hc.wsCache.mu.Lock()
	cc := hc.wsCache.idles[0]
	hc.wsCache.mu.Unlock()
	hc.wsCache.ttl = -1
	hc.wsCache.sweep(time.Now())
	select {
	case <-cc.cn.closeDone:
	case <-time.After(3 * time.Second):
		t.Fatal("过期的在表条目没有被退役（sweep 没生效）")
	}

	// (b) 摘表即免疫：取用者已独占的连接，sweep 再也看不到它。
	doStreamBody(t, hc)
	hc.wsCache.mu.Lock()
	cc2 := hc.wsCache.idles[0]
	hc.wsCache.idles = nil
	hc.wsCache.mu.Unlock()
	hc.wsCache.sweep(time.Now().Add(time.Hour))
	if err := cc2.cn.err(); err != nil {
		t.Fatalf("sweep 杀掉了已摘表的连接: %v", err)
	}
	cc2.cn.terminate(io.EOF)
}

// ---- R6：token 变化 ----

// rotatingAuth 返回一个可变的 Authorization。
type rotatingAuth struct{ v atomic.Value }

func (a *rotatingAuth) Authorization(context.Context) (string, error) {
	if s, ok := a.v.Load().(string); ok {
		return s, nil
	}
	return "", nil
}
func (a *rotatingAuth) set(v string) { a.v.Store(v) }
func (a *rotatingAuth) Invalidate()  {}
func (a *rotatingAuth) Fatal(error)  {}

// TestWSConnCacheTokenBinding（R6）：token 变了以后旧条目被丢弃，且复用时 token 非空。
func TestWSConnCacheTokenBinding(t *testing.T) {
	up := newReuseUpstream(t)
	auth := &rotatingAuth{}
	auth.set("pat-1")
	hc := NewHTTPClient(auth,
		WithResponsesTransport(TransportWS),
		WithTransport(newFixedTransport(t, "https://chatgpt.com/backend-api/codex/responses", up.srv.URL)),
		WithTraceAuto(false),
		WithWSConnCache(2),
	)
	doStreamBody(t, hc)

	auth.set("pat-2")
	doStreamBody(t, hc) // token 不同 ⇒ 旧条目丢弃 + 新拨
	if got := up.accepts.Load(); got != 2 {
		t.Fatalf("accepts = %d，期望 2（token 变化必须丢弃旧连接）", got)
	}
	doStreamBody(t, hc) // 第三次使用同 token，必须命中第二次归还的连接。
	if got := up.accepts.Load(); got != 2 {
		t.Fatalf("accepts = %d，期望仍为 2（同 token 第三次请求必须命中）", got)
	}
	if got := hc.wsCache.hits.Load(); got < 1 {
		t.Fatalf("hits = %d，期望至少 1（必须走过命中路径）", got)
	}
	// 命中归还后 token 必须仍是新值（回填链没断）。
	hc.wsCache.mu.Lock()
	got := hc.wsCache.idles[0].token
	hc.wsCache.mu.Unlock()
	if got != "pat-2" {
		t.Fatalf("缓存条目 token = %q，期望 pat-2（回填链断了）", got)
	}
}

// ---- R19：两条取数路径不共享连接 ----

// TestWSConnCacheSeparatesArms（R19）：StreamBody（64MiB）与 Responses（16MiB）各用各的。
func TestWSConnCacheSeparatesArms(t *testing.T) {
	up := newReuseUpstream(t)
	hc := up.client(t, WithWSConnCache(2))
	doStreamBody(t, hc)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := hc.Responses(ctx, []byte(`{"model":"gpt-5.6","input":[]}`), reuseIdentity()); err != nil {
		t.Fatalf("Responses: %v", err)
	}
	if got := up.accepts.Load(); got != 2 {
		t.Fatalf("accepts = %d，期望 2（两条取数路径不共享连接）", got)
	}
	hc.wsCache.mu.Lock()
	n := len(hc.wsCache.idles)
	var keys []connKey
	for _, cc := range hc.wsCache.idles {
		keys = append(keys, cc.key)
	}
	hc.wsCache.mu.Unlock()
	if n != 2 {
		t.Fatalf("缓存 = %d 条，期望 2（两个 key 各一条）: %v", n, keys)
	}
	if keys[0].readLimit == keys[1].readLimit {
		t.Fatalf("两条连接 readLimit 相同（%v），说明 key 没分开", keys)
	}
}

// ---- R9：上游关连接 ⇒ 不入缓存 / 下一个请求不受影响 ----

// TestWSConnClosedByUpstreamNotCached（R9）：每次响应后上游都主动关连接。
// 断言：下一个请求仍然成功，并且必须新拨（死连接不会把请求带崩）。
func TestWSConnClosedByUpstreamNotCached(t *testing.T) {
	up := newReuseUpstream(t)
	up.closeAfter = true
	hc := up.client(t, WithWSConnCache(2))

	doStreamBody(t, hc)
	// 等 readLoop 看到 close frame：连接变得不可归还。
	var cc *cachedConn
	hc.wsCache.mu.Lock()
	if len(hc.wsCache.idles) > 0 {
		c := hc.wsCache.idles[0]
		cc = &c
	}
	hc.wsCache.mu.Unlock()
	if cc != nil {
		waitFor(t, func() bool { return !cc.cn.releasable() })
	}

	doStreamBody(t, hc) // 不应被死连接带崩
	if got := up.accepts.Load(); got != 2 {
		t.Fatalf("accepts = %d，期望 2（上游关掉的连接不能复用）", got)
	}
}

// ---- R7：容量 1、并发同 ts ----

// TestWSConnCacheCapacityOne（R7）：两个并发请求，第二个必须新拨；归还后只保留一条。
func TestWSConnCacheCapacityOne(t *testing.T) {
	up := newReuseUpstream(t)
	up.gate = make(chan struct{}) // 响应先扣住：让"请求在飞"变成确定性状态
	hc := up.client(t, WithWSConnCache(1))

	// 请求 1 到达上游、等放行 —— 此时缓存还是空的。
	first := make(chan error, 1)
	go func() { _, err := streamBodyOnce(hc); first <- err }()
	waitFor(t, func() bool { return up.requests.Load() == 1 })

	// 请求 2：缓存里没有任何可用的连接 ⇒ 必须新拨（与调度无关）。
	second := make(chan error, 1)
	go func() { _, err := streamBodyOnce(hc); second <- err }()
	waitFor(t, func() bool { return up.accepts.Load() == 2 })

	close(up.gate)
	if err := <-first; err != nil {
		t.Fatalf("请求 1: %v", err)
	}
	if err := <-second; err != nil {
		t.Fatalf("请求 2: %v", err)
	}
	if got := up.accepts.Load(); got != 2 {
		t.Fatalf("accepts = %d，期望 2（同一条连接同一时刻只能服务一个请求）", got)
	}
	hc.wsCache.mu.Lock()
	n := len(hc.wsCache.idles)
	hc.wsCache.mu.Unlock()
	if n > 1 {
		t.Fatalf("缓存 = %d 条，超过容量 1", n)
	}
}

// ---- R10：并发混跑（-race 下跑） ----

// TestWSConnCacheConcurrentMixed（R10）：8 个 StreamBody + 4 个 Responses 并发。
func TestWSConnCacheConcurrentMixed(t *testing.T) {
	up := newReuseUpstream(t)
	hc := up.client(t, WithWSConnCache(2))

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := streamBodyOnce(hc); err != nil {
				t.Errorf("StreamBody: %v", err)
			}
		}()
	}
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if _, err := hc.Responses(ctx, []byte(`{"model":"gpt-5.6","input":[]}`), reuseIdentity()); err != nil {
				t.Errorf("Responses: %v", err)
			}
		}()
	}
	wg.Wait()
	hc.CloseIdleConnections()
}

// ---- B1：失败终态不得归还（客户端语义：错误即退役）----

// cacheIdleCount 读缓存里的空闲条目数（测试直读，不导出）。
func cacheIdleCount(hc *HTTPClient) int {
	hc.wsCache.mu.Lock()
	defer hc.wsCache.mu.Unlock()
	return len(hc.wsCache.idles)
}

// TestWSConnFailedTerminalNotCached（B1）：以 `response.failed` 结束的交换，连接
// **不进缓存**，且下一个请求必须新拨。判定发生在聚合器（本来就知道事件类型），
// 由 exchange 的 MarkFailed 记录，Release 见到它一律拒绝并终止连接。
func TestWSConnFailedTerminalNotCached(t *testing.T) {
	up := newReuseUpstream(t, []string{wsCreatedEv, wsFailedEv})
	hc := up.client(t, WithWSConnCache(2))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	payload := []byte(`{"model":"gpt-5.6","input":[]}`)

	_, err := hc.Responses(ctx, payload, reuseIdentity())
	var he *HTTPError
	if !errors.As(err, &he) || he.StatusCode != http.StatusInternalServerError {
		t.Fatalf("response.failed 的错误 = %v，期望 *HTTPError{500}", err)
	}
	if n := cacheIdleCount(hc); n != 0 {
		t.Fatalf("失败终态之后缓存仍有 %d 条连接（错误即退役）", n)
	}

	// 下一个请求必须新拨（失败终态的连接不能复用）。
	if _, err := hc.Responses(ctx, payload, reuseIdentity()); !errors.As(err, &he) {
		t.Fatalf("第二次错误 = %v，期望 *HTTPError{500}", err)
	}
	if got := up.accepts.Load(); got != 2 {
		t.Fatalf("accepts = %d，期望 2（失败终态的连接必须退役）", got)
	}
}

// TestWSConnIncompleteTerminalNotCached（B1）：`response.incomplete` 同样是失败终态
// ——聚合器按"事件流截断"报错，连接不得归还。
func TestWSConnIncompleteTerminalNotCached(t *testing.T) {
	const incompleteEv = `{"type":"response.incomplete","response":{"id":"resp_ws","status":"incomplete"}}`
	up := newReuseUpstream(t, []string{wsCreatedEv, incompleteEv})
	hc := up.client(t, WithWSConnCache(2))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	payload := []byte(`{"model":"gpt-5.6","input":[]}`)

	if _, err := hc.Responses(ctx, payload, reuseIdentity()); err == nil {
		t.Fatal("response.incomplete 应当报错")
	}
	if n := cacheIdleCount(hc); n != 0 {
		t.Fatalf("incomplete 终态之后缓存仍有 %d 条连接", n)
	}
	if _, err := hc.Responses(ctx, payload, reuseIdentity()); err == nil {
		t.Fatal("第二次 response.incomplete 也应当报错")
	}
	if got := up.accepts.Load(); got != 2 {
		t.Fatalf("accepts = %d，期望 2（incomplete 终态的连接必须退役）", got)
	}
}

// TestWSConnCompletedTerminalStillReused（B1 对照组）：正常终态**照旧归还**——
// 防止"一刀切不归还"把复用的收益打死。
func TestWSConnCompletedTerminalStillReused(t *testing.T) {
	up := newReuseUpstream(t)
	hc := up.client(t, WithWSConnCache(2))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	payload := []byte(`{"model":"gpt-5.6","input":[]}`)

	if _, err := hc.Responses(ctx, payload, reuseIdentity()); err != nil {
		t.Fatalf("Responses: %v", err)
	}
	if n := cacheIdleCount(hc); n != 1 {
		t.Fatalf("completed 之后缓存 = %d 条，期望 1", n)
	}
	if _, err := hc.Responses(ctx, payload, reuseIdentity()); err != nil {
		t.Fatalf("Responses(第二次): %v", err)
	}
	if got := up.accepts.Load(); got != 1 {
		t.Fatalf("accepts = %d，期望 1（正常终态必须命中复用）", got)
	}
}

// TestWSStreamBodyFailedTerminalNotCached（B1 字节透传路径）：网关 relay 走的是
// `StreamBody` 的原始 SSE 字节——sseView 在渲染时已解析 type，看到 failed 就调
// MarkFailed，所以调用方只调 `Body.Close()` 也不会让连接被复用。
func TestWSStreamBodyFailedTerminalNotCached(t *testing.T) {
	up := newReuseUpstream(t, []string{wsCreatedEv, wsFailedEv})
	hc := up.client(t, WithWSConnCache(2))

	body := doStreamBody(t, hc)
	if !strings.Contains(body, `"type":"response.failed"`) {
		t.Fatalf("SSE 体里没有 failed 帧:\n%s", body)
	}
	if n := cacheIdleCount(hc); n != 0 {
		t.Fatalf("失败终态之后缓存仍有 %d 条连接（字节透传路径也必须退役）", n)
	}
	_ = doStreamBody(t, hc) // 第二次：必须新拨
	if got := up.accepts.Load(); got != 2 {
		t.Fatalf("accepts = %d，期望 2", got)
	}
}

// ---- B2：维护协程的生命周期与"表里有没有东西"绑定 ----

// cacheMaintainRunning 报告缓存当前是否有维护协程在跑（测试直读，不导出）。
func cacheMaintainRunning(c *connCache) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.sweepStop != nil
}

// TestWSConnCacheMaintainIdleBound（B2）：维护协程惰性启动、表空即停、再 put 可重启、
// Close 后回落到基线。不这么做的话，未显式 Close 的短命客户端会留下永久 ticker。
//
// 用**一个共享**假上游 + 显式关服务器：goroutine 计数才可判（每个 httptest.Server
// 自身都占 goroutine，多次 NewServer 会把基线搅乱）。
func TestWSConnCacheMaintainIdleBound(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := coderws.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer c.CloseNow()
		for { // 只读：连接被客户端终止时退出
			if _, _, rerr := c.Read(r.Context()); rerr != nil {
				return
			}
		}
	}))
	t.Cleanup(srv.Close)

	base := runtime.NumGoroutine() // 已含服务器自身的 goroutine（后续对比都在同一基线上）
	c := newConnCache(2, 5*time.Millisecond)

	// 1) 构造后不 put：不得有维护协程，也不得有 goroutine 增长。
	time.Sleep(30 * time.Millisecond)
	if cacheMaintainRunning(c) {
		t.Fatal("空缓存就起了维护协程")
	}
	if n := runtime.NumGoroutine(); n > base {
		t.Fatalf("空缓存让 goroutine 从 %d 涨到 %d", base, n)
	}

	// 2) put 一条真连接 ⇒ 惰性启动。
	dialPumped := func() *conn {
		t.Helper()
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		t.Cleanup(cancel)
		wsURL := "ws" + strings.TrimPrefix(srv.URL, "http")
		wsConn, _, derr := coderws.Dial(ctx, wsURL, nil)
		if derr != nil {
			t.Fatalf("拨号测试连接: %v", derr)
		}
		return newConn(wsConn, connConfig{mode: modePumped, readLimit: defaultReadLimit}, ctx, cancel)
	}
	cn := dialPumped()
	if !c.put(&wsExchange{cn: cn, token: "t", bornAt: time.Now()}) {
		t.Fatal("put 失败")
	}
	if !cacheMaintainRunning(c) {
		t.Fatal("put 之后维护协程没有启动")
	}

	// 3) 到点 sweep 清空 ⇒ 自行停止（不需要任何人调 Close）。
	c.mu.Lock()
	c.ttl = time.Millisecond
	c.mu.Unlock()
	waitFor(t, func() bool {
		c.mu.Lock()
		defer c.mu.Unlock()
		return len(c.idles) == 0 && c.sweepStop == nil
	})
	waitFor(t, func() bool { return runtime.NumGoroutine() <= base })

	// 4) 再 put ⇒ 能重启（生命周期是"每次入表一轮"，不是一次性开关）。
	cn2 := dialPumped()
	if !c.put(&wsExchange{cn: cn2, token: "t", bornAt: time.Now()}) {
		t.Fatal("第二次 put 失败")
	}
	if !cacheMaintainRunning(c) {
		t.Fatal("清空后再次 put 没有重启维护协程")
	}

	// 5) Close 停协程，goroutine 回落基线。
	c.Close()
	if cacheMaintainRunning(c) {
		t.Fatal("Close 之后维护协程仍在")
	}
	waitFor(t, func() bool { return runtime.NumGoroutine() <= base })
}
