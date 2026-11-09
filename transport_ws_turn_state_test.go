package codexsdk

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
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

// 本文件是 S4「身份是请求数据」的 SDK 侧验收用例（spec §2.4）：
//
//	S1 跨窗口复用 / S2 跨 thread 不复用 / S3 退役只摘 idle / S4 ts 只走 Sink
//	S5 请求级身份进帧 / S6 匿名不复用 / S7 并发 / S8 Dial 身份
//	S9 tap 负向 / S10 Close 后在飞归还 / S12 回调时序
//
// S11（保留键 panic）在 identity_reserved_test.go。

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
//   - 每次升级计数一次 upgrades；握手可签发 handshake（"" = 不签发）；
//   - 每读到一帧 create 就回放 respond(全局请求序号, 帧)；
//   - 记录客户端发来的全部帧（按请求顺序）与每次升级的握手头。
type tsUpstream struct {
	srv *httptest.Server

	mu        sync.Mutex
	handshake string
	respond   func(reqIdx int, req []byte) []string
	frames    [][]byte
	headers   []http.Header
	accepts   atomic.Int64
	upgrades  atomic.Int64

	// frameSig 在每次记录请求帧后做一次非阻塞通知：waitFrames 用它等条件，
	// 不轮询、不 sleep。
	frameSig chan struct{}
}

func newTSUpstream(t *testing.T, respond func(int, []byte) []string) *tsUpstream {
	t.Helper()
	u := &tsUpstream{respond: respond, frameSig: make(chan struct{}, 1)}
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
	u.headers = append(u.headers, r.Header.Clone())
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
		select {
		case u.frameSig <- struct{}{}:
		default:
		}
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

func (u *tsUpstream) handshakeHeaders(t *testing.T, i int) http.Header {
	t.Helper()
	u.mu.Lock()
	defer u.mu.Unlock()
	if i >= len(u.headers) {
		t.Fatalf("第 %d 次握手不存在（共 %d 次）", i, len(u.headers))
	}
	return u.headers[i].Clone()
}

// waitFrames 等到上游记录到第 n 个请求帧。用通道通知 + deadline 兜底，
// 不轮询、不 sleep（2ms 轮询在慢机器上既拖时间又可能误判超时）。
func (u *tsUpstream) waitFrames(t *testing.T, n int) {
	t.Helper()
	deadline := time.After(5 * time.Second)
	for {
		u.mu.Lock()
		got := len(u.frames)
		u.mu.Unlock()
		if got >= n {
			return
		}
		select {
		case <-u.frameSig:
		case <-deadline:
			t.Fatalf("等待第 %d 帧超时（已记录 %d）", n, got)
		}
	}
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

// doStreamBodyFull 跑一次 StreamBody 并读尽 body（顺带触发终态后的归还）。
func doStreamBodyFull(t *testing.T, hc *HTTPClient, id Identity) (*http.Response, string) {
	t.Helper()
	return doStreamBodyPayload(t, hc, id, []byte(`{"model":"gpt-5.6","input":[]}`))
}

// doStreamBodyPayload 同 doStreamBodyFull，但由调用方给 payload（用于"payload 自带
// 身份"这类边界：SDK 必须整体覆写/删除，而不是透传）。
func doStreamBodyPayload(t *testing.T, hc *HTTPClient, id Identity, payload []byte) (*http.Response, string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	resp, err := hc.StreamBody(ctx, payload, id)
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

// frameMeta 取请求帧 client_metadata 的某个键（缺失 = ""）。
func frameMeta(t *testing.T, frame []byte, key string) string {
	t.Helper()
	return gjson.GetBytes(frame, "client_metadata."+key).String()
}

// S1：同 thread、window 0→1 ⇒ 只握手一次；两帧的 window 各自正确。
func TestS1ReuseAcrossWindows(t *testing.T) {
	up := newTSUpstream(t, func(i int, _ []byte) []string {
		id := fmt.Sprintf("resp_s1_%d", i+1)
		return []string{tsCreatedFrame(id), tsCompletedFrame(id)}
	})
	hc := up.client(t, WithWSConnCache(2))

	id0 := identityFor("thread-s1", "thread-s1:0", "", nil)
	id1 := identityFor("thread-s1", "thread-s1:1", "", nil)
	doStreamBodyFull(t, hc, id0)
	doStreamBodyFull(t, hc, id1)

	if got := up.upgrades.Load(); got != 1 {
		t.Fatalf("upgrades = %d, 期望 1（window 变化不得重连）", got)
	}
	frames := up.sentFrames(t)
	if len(frames) != 2 {
		t.Fatalf("上游收到 %d 帧, 期望 2 帧", len(frames))
	}
	for i, want := range []string{"thread-s1:0", "thread-s1:1"} {
		if got := frameMeta(t, frames[i], codexMetaWindowKey); got != want {
			t.Fatalf("第 %d 帧 window = %q, 期望 %q", i+1, got, want)
		}
	}
}

// S2：两个 threadKey 交替 ⇒ 各拨一次（upgrades == 2）；同 thread 的第二次
// 请求命中自己的连接。
func TestS2NoReuseAcrossThreads(t *testing.T) {
	up := newTSUpstream(t, func(i int, _ []byte) []string {
		id := fmt.Sprintf("resp_s2_%d", i+1)
		return []string{tsCreatedFrame(id), tsCompletedFrame(id)}
	})
	hc := up.client(t, WithWSConnCache(4))
	idA := identityFor("thread-A", "thread-A:0", "", nil)
	idB := identityFor("thread-B", "thread-B:0", "", nil)
	for _, id := range []Identity{idA, idB, idA, idB} {
		doStreamBodyFull(t, hc, id)
	}
	if got := up.upgrades.Load(); got != 2 {
		t.Fatalf("upgrades = %d, 期望 2（跨 thread 不得复用）", got)
	}
	if got := up.accepts.Load(); got != 2 {
		t.Fatalf("accepts = %d, 期望 2", got)
	}
	if got := hc.wsCache.hits.Load(); got != 2 {
		t.Fatalf("hits = %d, 期望 2（各 thread 的第二次请求命中自己的连接）", got)
	}
}

// S3：RetireThread 只摘该 key 的 idle（锁外终止）；B 不受影响；Close 之后
// RetireThread 为 no-op（无墓碑）。
func TestS3RetireOnlyIdle(t *testing.T) {
	up := newTSUpstream(t, func(i int, _ []byte) []string {
		id := fmt.Sprintf("resp_s3_%d", i+1)
		return []string{tsCreatedFrame(id), tsCompletedFrame(id)}
	})
	hc := up.client(t, WithWSConnCache(4))
	idA := identityFor("thread-retire-A", "thread-retire-A:0", "", nil)
	idB := identityFor("thread-retire-B", "thread-retire-B:0", "", nil)
	doStreamBodyFull(t, hc, idA)
	doStreamBodyFull(t, hc, idB)

	hc.wsCache.mu.Lock()
	if len(hc.wsCache.idles) != 2 {
		hc.wsCache.mu.Unlock()
		t.Fatalf("idle = %d, 期望 2", len(hc.wsCache.idles))
	}
	var retired *conn
	for _, cc := range hc.wsCache.idles {
		if cc.key.threadKey == idA.threadKey() {
			retired = cc.cn
		}
	}
	hc.wsCache.mu.Unlock()
	if retired == nil {
		t.Fatal("未找到 A 的 idle 连接")
	}

	hc.RetireThread(idA.threadKey())
	if err := retired.err(); err == nil {
		t.Fatal("RetireThread 后 A 的连接未被终止")
	}
	hc.wsCache.mu.Lock()
	idles := append([]cachedConn(nil), hc.wsCache.idles...)
	hc.wsCache.mu.Unlock()
	if len(idles) != 1 || idles[0].key.threadKey != idB.threadKey() {
		t.Fatalf("退役后 idle = %+v, 期望只剩 B", idles)
	}

	hc.wsCache.Close()
	hc.RetireThread(idB.threadKey()) // Close 之后 no-op（不 panic、不复活）
	hc.wsCache.mu.Lock()
	got := len(hc.wsCache.idles)
	hc.wsCache.mu.Unlock()
	if got != 0 {
		t.Fatalf("Close 后 idle = %d, 期望 0", got)
	}
}

// S4：握手头 T1 → response.metadata T2 ⇒ Sink 序列 [T1, T2]；Sink == nil
// 不 panic，帧照旧透传。
func TestS4TurnStateOnlyGoesToSink(t *testing.T) {
	const id = "resp_s4"
	up := newTSUpstream(t, func(int, []byte) []string {
		return []string{tsCreatedFrame(id), tsMetadataFrame(id, "T2"), tsCompletedFrame(id)}
	})
	up.setHandshake("T1")
	hc := up.client(t, WithWSConnCache(2))
	var sink turnStateSink
	id4 := identityFor("thread-s4", "thread-s4:0", "", &sink)
	_, body := doStreamBodyFull(t, hc, id4)
	if got := sink.values(); len(got) != 2 || got[0] != "T1" || got[1] != "T2" {
		t.Fatalf("Sink = %v, 期望 [T1 T2]", got)
	}
	if !strings.Contains(body, "T2") {
		t.Fatalf("metadata 帧未被透传: %q", body)
	}

	// nil Sink：事件照旧透传，不 panic。
	hc2 := up.client(t, WithWSConnCache(2))
	_, body2 := doStreamBodyFull(t, hc2, identityFor("thread-s4-nil", "thread-s4-nil:0", "", nil))
	if !strings.Contains(body2, "T2") {
		t.Fatalf("Sink=nil 时 metadata 帧未透传: %q", body2)
	}
}

// S5：同一 client 交替两个 Identity ⇒ 帧内身份与握手头各自正确，无串台。
func TestS5RequestIdentityPerRequest(t *testing.T) {
	up := newTSUpstream(t, func(i int, _ []byte) []string {
		id := fmt.Sprintf("resp_s5_%d", i+1)
		return []string{tsCreatedFrame(id), tsCompletedFrame(id)}
	})
	hc := up.client(t, WithWSConnCache(4))
	idA := identityFor("thread-s5-A", "thread-s5-A:0", "", nil)
	idB := identityFor("thread-s5-B", "thread-s5-B:3", "", nil)
	doStreamBodyFull(t, hc, idA)
	doStreamBodyFull(t, hc, idB)

	frames := up.sentFrames(t)
	if len(frames) != 2 {
		t.Fatalf("上游收到 %d 帧, 期望 2 帧", len(frames))
	}
	checks := []struct {
		frame int
		id    Identity
	}{
		{0, idA}, {1, idB},
	}
	for _, c := range checks {
		if got := frameMeta(t, frames[c.frame], codexMetaSessionKey); got != c.id.Meta.SessionID {
			t.Fatalf("第 %d 帧 session = %q, 期望 %q", c.frame+1, got, c.id.Meta.SessionID)
		}
		if got := frameMeta(t, frames[c.frame], codexMetaThreadKey); got != c.id.Meta.ThreadID {
			t.Fatalf("第 %d 帧 thread = %q, 期望 %q", c.frame+1, got, c.id.Meta.ThreadID)
		}
		if got := frameMeta(t, frames[c.frame], codexMetaWindowKey); got != c.id.Meta.WindowID {
			t.Fatalf("第 %d 帧 window = %q, 期望 %q", c.frame+1, got, c.id.Meta.WindowID)
		}
	}
	for i, want := range []string{"thread-s5-A", "thread-s5-B"} {
		hdr := up.handshakeHeaders(t, i)
		if got := hdr.Get(HeaderThreadID); got != want {
			t.Fatalf("第 %d 次握手 thread-id = %q, 期望 %q", i+1, got, want)
		}
		if got := hdr.Get(HeaderSessionID); got != "sess-"+want {
			t.Fatalf("第 %d 次握手 session-id = %q, 期望 %q", i+1, got, "sess-"+want)
		}
	}
}

// methodCountingTransport 统计经过的 HTTP 方法（S6 断言 POST == 0）。
type methodCountingTransport struct {
	base  http.RoundTripper
	posts atomic.Int64
}

func (m *methodCountingTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	if req.Method == http.MethodPost {
		m.posts.Add(1)
	}
	return m.base.RoundTrip(req)
}

// S6：缓存开启 + 同一 client：两次匿名请求都读尽并 Close ⇒ upgrades == 2、
// HTTP POST == 0；帧内不出现身份键（turn_id 等自动字段允许）。
func TestS6AnonymousNeverReuses(t *testing.T) {
	up := newTSUpstream(t, func(i int, _ []byte) []string {
		id := fmt.Sprintf("resp_s6_%d", i+1)
		return []string{tsCreatedFrame(id), tsCompletedFrame(id)}
	})
	tr := &methodCountingTransport{base: newFixedTransport(t, "https://chatgpt.com/backend-api/codex/responses", up.srv.URL)}
	hc := NewHTTPClient(PAT("pat-s6"),
		WithResponsesTransport(TransportWS),
		WithTransport(tr),
		WithWSConnCache(2),
		WithTraceAuto(false),
	)
	// payload 自带旧身份：匿名请求必须把它整体抹掉（否则等于冒充调用方的老会话）。
	payload := []byte(`{"model":"gpt-5.6","input":[],"client_metadata":{` +
		`"thread_id":"old-thread","x-codex-window-id":"old-window",` +
		`"x-codex-turn-state":"old-ts"}}`)
	doStreamBodyPayload(t, hc, Identity{}, payload)
	doStreamBodyPayload(t, hc, Identity{}, payload)

	if got := up.upgrades.Load(); got != 2 {
		t.Fatalf("upgrades = %d, 期望 2（匿名不复用）", got)
	}
	if got := tr.posts.Load(); got != 0 {
		t.Fatalf("HTTP POST = %d, 期望 0（匿名 WS 请求不得回退 HTTP）", got)
	}
	frames := up.sentFrames(t)
	if len(frames) != 2 {
		t.Fatalf("上游收到 %d 帧, 期望 2 帧", len(frames))
	}
	for i, frame := range frames {
		for _, key := range []string{codexMetaInstallationKey, codexMetaSessionKey, codexMetaThreadKey, codexMetaWindowKey, codexMetaTurnStateKey} {
			if gjson.GetBytes(frame, "client_metadata."+key).Exists() {
				t.Fatalf("第 %d 帧不应出现身份键 %s: %s", i+1, key, frame)
			}
		}
	}
}

// S7：同 client 并发两个 Identity（不同 thread），-race 绿。
func TestS7ConcurrentIdentities(t *testing.T) {
	up := newTSUpstream(t, func(i int, _ []byte) []string {
		id := fmt.Sprintf("resp_s7_%d", i+1)
		return []string{tsCreatedFrame(id), tsCompletedFrame(id)}
	})
	hc := up.client(t, WithWSConnCache(4))
	ids := []Identity{
		identityFor("thread-s7-A", "thread-s7-A:0", "", nil),
		identityFor("thread-s7-B", "thread-s7-B:0", "", nil),
	}
	var wg sync.WaitGroup
	errs := make(chan error, len(ids))
	for _, id := range ids {
		wg.Add(1)
		go func(id Identity) {
			defer wg.Done()
			resp, err := hc.StreamBody(context.Background(), []byte(`{"model":"m","input":[]}`), id)
			if err != nil {
				errs <- err
				return
			}
			_, _ = io.Copy(io.Discard, resp.Body)
			errs <- resp.Body.Close()
		}(id)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("并发请求失败: %v", err)
		}
	}
	if got := up.upgrades.Load(); got != 2 {
		t.Fatalf("upgrades = %d, 期望 2（两个 thread 各一条连接）", got)
	}
}

// S8：Dial(ctx, auth, Identity) ⇒ 握手头与 Identity 一致；握手响应头 ts 进 Sink。
func TestS8DialIdentity(t *testing.T) {
	up := newTSUpstream(t, nil)
	up.setHandshake("T-dial")
	var sink turnStateSink
	id := identityFor("thread-dial", "thread-dial:2", "T-req", &sink)
	c, err := Dial(context.Background(), PAT("pat-dial"), id,
		WithTransport(newFixedTransport(t, "https://chatgpt.com/backend-api/codex/responses", up.srv.URL)),
		WithTurnAuto(false), WithTraceAuto(false))
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer c.Close(StatusGoingAway, "")

	hdr := up.handshakeHeaders(t, 0)
	for key, want := range map[string]string{
		HeaderSessionID:       "sess-thread-dial",
		HeaderThreadID:        "thread-dial",
		HeaderClientRequestID: "thread-dial",
		HeaderWindowID:        "thread-dial:2",
	} {
		if got := hdr.Get(key); got != want {
			t.Fatalf("握手头 %s = %q, 期望 %q", key, got, want)
		}
	}
	if got := sink.values(); len(got) != 1 || got[0] != "T-dial" {
		t.Fatalf("Sink = %v, 期望 [T-dial]", got)
	}
	if err := c.Send(context.Background(), []byte(`{"type":"response.create","model":"m"}`)); err != nil {
		t.Fatalf("Send: %v", err)
	}
	up.waitFrames(t, 1)
	frame := up.sentFrames(t)[0]
	if got := frameTurnState(t, frame); got != "T-req" {
		t.Fatalf("帧内 ts = %q, 期望 T-req（Identity.TurnState）", got)
	}
}

// S9：tap 负向——created 之前的 metadata 丢弃；response_id 不匹配同样丢弃。
func TestS9TapNegative(t *testing.T) {
	const id = "resp_s9"
	up := newTSUpstream(t, func(int, []byte) []string {
		return []string{
			tsMetadataFrame(id, "T-early"),
			tsCreatedFrame(id),
			tsMetadataFrame("other", "T-mismatch"),
			tsMetadataFrame(id, "T-ok"),
			tsCompletedFrame(id),
		}
	})
	hc := up.client(t)
	var sink turnStateSink
	_, body := doStreamBodyFull(t, hc, identityFor("thread-s9", "thread-s9:0", "", &sink))
	if got := sink.values(); len(got) != 1 || got[0] != "T-ok" {
		t.Fatalf("Sink = %v, 期望 [T-ok]", got)
	}
	if got := hc.turnStateDropped.Load(); got != 2 {
		t.Fatalf("dropped = %d, 期望 2（早到 + response_id 不匹配）", got)
	}
	for _, ts := range []string{"T-early", "T-mismatch"} {
		if !strings.Contains(body, ts) {
			t.Fatalf("被丢弃的只是 ts 候选，帧必须透传（缺 %s）: %q", ts, body)
		}
	}
}

// S10：Close 之后在飞 exchange 归还 ⇒ put=false、无 idle 残留（由 Release 终止）。
func TestS10CloseRejectsInFlightReturn(t *testing.T) {
	up := newTSUpstream(t, func(i int, _ []byte) []string {
		id := fmt.Sprintf("resp_s10_%d", i+1)
		return []string{tsCreatedFrame(id), tsCompletedFrame(id)}
	})
	hc := up.client(t, WithWSConnCache(2))
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	resp, err := hc.StreamBody(ctx, []byte(`{"model":"m","input":[]}`), identityFor("thread-s10", "thread-s10:0", "", nil))
	if err != nil {
		t.Fatalf("StreamBody: %v", err)
	}
	hc.wsCache.Close() // 关闭发生在 exchange 仍在飞时
	_, _ = io.Copy(io.Discard, resp.Body)
	_ = resp.Body.Close()

	hc.wsCache.mu.Lock()
	idle := len(hc.wsCache.idles)
	hc.wsCache.mu.Unlock()
	if idle != 0 {
		t.Fatalf("Close 后在飞归还留下了 %d 条 idle，期望 0", idle)
	}
}

// S12：Body.Close 之后 Sink 调用计数不再增长（提前 Close / 取消两种收尾）。
func TestS12NoSinkAfterClose(t *testing.T) {
	for _, tc := range []struct {
		name   string
		cancel bool
	}{
		{"提前 Close", false},
		{"取消", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			release := make(chan struct{})
			lateDone := make(chan struct{})
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				c, err := coderws.Accept(w, r, nil)
				if err != nil {
					return
				}
				defer c.CloseNow()
				if _, _, rerr := c.Read(r.Context()); rerr != nil {
					return
				}
				_ = c.Write(r.Context(), coderws.MessageText, []byte(tsCreatedFrame("resp_s12")))
				<-release
				// 迟到帧：客户端此时已 Close（或已取消）。写完（或写失败）后关闭
				// lateDone，测试用它当同步点——不 sleep、不猜时间。
				_ = c.Write(r.Context(), coderws.MessageText, []byte(tsMetadataFrame("resp_s12", "T-late")))
				_ = c.Write(r.Context(), coderws.MessageText, []byte(tsCompletedFrame("resp_s12")))
				close(lateDone)
			}))
			t.Cleanup(srv.Close)
			hc := NewHTTPClient(PAT("pat-s12"),
				WithResponsesTransport(TransportWS),
				WithTransport(newFixedTransport(t, "https://chatgpt.com/backend-api/codex/responses", srv.URL)),
				WithTurnAuto(false), WithTraceAuto(false))
			var sink turnStateSink
			ctx, cancel := context.WithCancel(context.Background())
			resp, err := hc.StreamBody(ctx, []byte(`{"model":"m","input":[]}`), identityFor("thread-s12", "thread-s12:0", "", &sink))
			if err != nil {
				t.Fatalf("StreamBody: %v", err)
			}
			buf := make([]byte, 4096)
			if _, err := resp.Body.Read(buf); err != nil {
				t.Fatalf("读首帧: %v", err)
			}
			if tc.cancel {
				cancel()
			}
			_ = resp.Body.Close()
			if got := sink.count(); got != 0 {
				t.Fatalf("Body.Close 返回时 Sink 已调用 %d 次，期望 0", got)
			}
			close(release)
			select {
			case <-lateDone:
			case <-time.After(5 * time.Second):
				t.Fatal("上游未在期限内尝试写入迟到帧")
			}
			if got := sink.count(); got != 0 {
				t.Fatalf("迟到帧到达后 Sink 调用 %d 次，期望 0（Close 之后不得再回调）", got)
			}
			cancel()
		})
	}
}

// TestCloseWSIdleConnectionsOnlyClearsWS：清空 WS 缓存后下一条 WS 请求必须
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

	doStreamBodyFull(t, hc, identityFor("thread-tls", "thread-tls:0", "", nil)) // WS：#1
	if got := wsAccepts.Load(); got != 1 {
		t.Fatalf("WS accepts = %d, 期望 1", got)
	}

	// 建立一条 HTTP 空闲连接（会进 tr 的空闲池）。
	if _, err := hc.Do(ctx, []byte(`{}`)); err != nil {
		t.Fatalf("Do: %v", err)
	}
	before := dials.Load()

	hc.CloseWSIdleConnections()

	doStreamBodyFull(t, hc, identityFor("thread-tls", "thread-tls:0", "", nil)) // WS 必须重拨（缓存已关）
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
