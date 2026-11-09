package codexsdk

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/tidwall/gjson"
)

// 本文件覆盖 Responses 出站 WS 传输（WithResponsesTransport(TransportWS)）：
// 帧构造、SSE 渲染、与 HTTP 臂的等价面、turn-state 回传、关闭语义、默认路径。

// ---- 假上游 ----

// wsScript 描述一次升级内假上游做什么：依次写出 frames，然后按 closeCode 收尾
// （abrupt=true 时不发 close frame，直接断 TCP——"无 close frame"路径）。
type wsScript struct {
	frames []string
	code   websocket.StatusCode
	abrupt bool
}

// wsUpstream 是测试用假 Responses WS 上游：httptest + websocket.Accept。
//
//   - 每次升级记录握手头，并记录该连接收到的第一帧（客户端只发一帧）；
//   - 第 i 次升级执行 scripts[i]（越界复用最后一个）；脚本返回后停在读循环上，
//     等客户端关闭，保证 httptest 收尾不挂；
//   - turnState 非空时在升级响应头签发 x-codex-turn-state。
type wsUpstream struct {
	srv *httptest.Server

	mu        sync.Mutex
	scripts   []wsScript
	turnState string
	dials     int
	upgrades  int
	headers   []http.Header
	frames    [][]byte
}

func newWSUpstream(t *testing.T) *wsUpstream {
	t.Helper()
	u := &wsUpstream{}
	u.srv = httptest.NewServer(http.HandlerFunc(u.handle))
	t.Cleanup(func() {
		u.srv.CloseClientConnections()
		u.srv.Close()
	})
	return u
}

func (u *wsUpstream) handle(w http.ResponseWriter, r *http.Request) {
	u.mu.Lock()
	u.dials++
	idx := u.upgrades
	u.upgrades++
	u.headers = append(u.headers, r.Header.Clone())
	script := wsScript{code: websocket.StatusNormalClosure}
	if len(u.scripts) > 0 {
		if idx < len(u.scripts) {
			script = u.scripts[idx]
		} else {
			script = u.scripts[len(u.scripts)-1]
		}
	}
	if u.turnState != "" {
		w.Header().Set(HeaderTurnState, u.turnState)
	}
	u.mu.Unlock()

	c, err := websocket.Accept(w, r, nil)
	if err != nil {
		return
	}
	defer c.CloseNow()
	_, data, err := c.Read(r.Context())
	if err == nil {
		u.mu.Lock()
		u.frames = append(u.frames, append([]byte(nil), data...))
		u.mu.Unlock()
	}
	for _, f := range script.frames {
		if werr := c.Write(r.Context(), websocket.MessageText, []byte(f)); werr != nil {
			return
		}
	}
	if script.abrupt {
		_ = c.CloseNow()
		return
	}
	_ = c.Close(script.code, "")
	// 关闭握手完成后对端会断开；读到错误即退出（上限由 ctx 兜底）。
	for {
		if _, _, rerr := c.Read(r.Context()); rerr != nil {
			return
		}
	}
}

// setScripts 设置第 0..n-1 次升级各自的行为（越界复用最后一个）。
func (u *wsUpstream) setScripts(s ...wsScript) {
	u.mu.Lock()
	u.scripts = s
	u.mu.Unlock()
}

// setTurnState 设置升级响应头签发的 x-codex-turn-state（空 = 不签发）。
func (u *wsUpstream) setTurnState(v string) {
	u.mu.Lock()
	u.turnState = v
	u.mu.Unlock()
}

func (u *wsUpstream) dialCount() int {
	u.mu.Lock()
	defer u.mu.Unlock()
	return u.dials
}

// sentFrames 返回按到达顺序记录的上游收到的帧（逐帧拷贝，可跨帧保留）。
func (u *wsUpstream) sentFrames(t *testing.T) [][]byte {
	t.Helper()
	u.mu.Lock()
	defer u.mu.Unlock()
	out := make([][]byte, len(u.frames))
	copy(out, u.frames)
	return out
}

// handshakeHeaders 返回第 i 次升级的握手头。
func (u *wsUpstream) handshakeHeaders(t *testing.T, i int) http.Header {
	t.Helper()
	u.mu.Lock()
	defer u.mu.Unlock()
	if i >= len(u.headers) {
		t.Fatalf("第 %d 次握手不存在（共 %d 次）", i, len(u.headers))
	}
	return u.headers[i]
}

// waitDial 等待第 n 次升级进入 handler（并发场景用）。
func (u *wsUpstream) waitDial(t *testing.T, n int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if u.dialCount() >= n {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("等待第 %d 次握手超时（实际 %d）", n, u.dialCount())
}

// ---- 事件样本（与 responses_test.go 的聚合样本同族）----

const (
	wsCreatedEv   = `{"type":"response.created","response":{"id":"resp_ws","object":"response","status":"in_progress"}}`
	wsDeltaEv     = `{"type":"response.output_text.delta","item_id":"msg_1","delta":"Hel"}`
	wsItemDoneEv  = `{"type":"response.output_item.done","item":` + respItemMsg + `}`
	wsItemDone2Ev = `{"type":"response.output_item.done","item":` + respItemFC + `}`
	wsCompletedEv = `{"type":"response.completed","response":{"id":"resp_ws","object":"response","status":"completed","usage":` + respUsage + `}}`
	wsFailedEv    = `{"type":"response.failed","error":{"type":"server_error","message":"upstream blew up"}}`
)

// readAllBody 读尽并关闭 body（HTTP / WS 两臂共用的消费方式）。
func readAllBody(t *testing.T, body io.ReadCloser) string {
	t.Helper()
	defer func() { _ = body.Close() }()
	b, err := io.ReadAll(body)
	if err != nil {
		t.Fatalf("读 body: %v", err)
	}
	return string(b)
}

// ---- 1. StreamBody（ws）：帧 → SSE 逐帧形状 ----

// TestStreamBodyWSRendersFramesAsSSE：上游帧序列 → SSE 字节流逐字断言。
// 覆盖：event: 行取自 data.type、帧字节 verbatim、in-band error 帧（created
// 前后各一）原样透传且不中断流、[DONE] 只在 completed 之后出现一次。
func TestStreamBodyWSRendersFramesAsSSE(t *testing.T) {
	errBeforeEv := `{"type":"error","error":{"type":"invalid_request_error","message":"nope"}}`
	errAfterEv := `{"type":"error","error":{"type":"rate_limit_exceeded","message":"slow down"}}`
	up := newWSUpstream(t)
	up.setScripts(wsScript{frames: []string{
		errBeforeEv, wsCreatedEv, wsDeltaEv, errAfterEv, wsItemDoneEv, wsCompletedEv,
	}})

	hc := NewHTTPClient(PAT("pat-ws"),
		WithResponsesTransport(TransportWS),
		WithTransport(newFixedTransport(t, "https://chatgpt.com/backend-api/codex/responses", up.srv.URL)),
		WithSession(Session{SessionID: "sess-1", ThreadID: "thread-1", WindowID: "thread-1:0"}),
		WithHeader("x-custom", "custom-value"),
		WithClientMetadata(MetaResponsesLiteKey, "true"),
		WithTurnAuto(false),
	)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	resp, err := hc.StreamBody(ctx, []byte(`{"model":"gpt-5.6","input":[],"stream":false,"previous_response_id":"resp_old","generate":true,"junk":"drop"}`))
	if err != nil {
		t.Fatalf("StreamBody: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("StatusCode = %d, 期望 200", resp.StatusCode)
	}
	if ct := resp.Header.Get("Content-Type"); ct != "text/event-stream" {
		t.Fatalf("Content-Type = %q, 期望 text/event-stream", ct)
	}
	got := readAllBody(t, resp.Body)

	want := "event:error\ndata:" + errBeforeEv + "\n\n" +
		"event:response.created\ndata:" + wsCreatedEv + "\n\n" +
		"event:response.output_text.delta\ndata:" + wsDeltaEv + "\n\n" +
		"event:error\ndata:" + errAfterEv + "\n\n" +
		"event:response.output_item.done\ndata:" + wsItemDoneEv + "\n\n" +
		"event:response.completed\ndata:" + wsCompletedEv + "\n\n" +
		"data: [DONE]\n\n"
	if got != want {
		t.Fatalf("SSE 字节流不符\n实际 = %q\n期望 = %q", got, want)
	}
	if n := strings.Count(got, "[DONE]"); n != 1 {
		t.Fatalf("[DONE] 出现 %d 次, 期望 1 次", n)
	}

	// 上游帧形状：type 覆写、stream 强制 true、HTTP 专属键被删、白名单外键被滤、
	// store 由 Send 归零、client_metadata 带会话身份 + 透传键。
	frames := up.sentFrames(t)
	if len(frames) != 1 {
		t.Fatalf("上游收到 %d 帧, 期望 1 帧", len(frames))
	}
	frame := frames[0]
	if typ := gjson.GetBytes(frame, "type").String(); typ != responsesCreateType {
		t.Fatalf("帧 type = %q, 期望 %q", typ, responsesCreateType)
	}
	if !gjson.GetBytes(frame, "stream").Bool() {
		t.Fatalf("帧 stream 应为 true: %s", frame)
	}
	if gjson.GetBytes(frame, "store").Bool() {
		t.Fatalf("帧 store 应为 false: %s", frame)
	}
	for _, k := range []string{"previous_response_id", "generate", "junk"} {
		if gjson.GetBytes(frame, k).Exists() {
			t.Fatalf("帧不应携带 %q: %s", k, frame)
		}
	}
	cm := gjson.GetBytes(frame, "client_metadata")
	for key, want := range map[string]string{
		codexMetaSessionKey:  "sess-1",
		codexMetaThreadKey:   "thread-1",
		codexMetaWindowKey:   "thread-1:0",
		MetaResponsesLiteKey: "true",
	} {
		if got := cm.Get(key).String(); got != want {
			t.Fatalf("client_metadata.%s = %q, 期望 %q（帧 %s）", key, got, want, frame)
		}
	}
	if cm.Get(codexMetaTurnKey).Exists() {
		t.Fatalf("WithTurnAuto(false) 时帧不应带 turn_id: %s", frame)
	}
	if cm.Get(codexMetaTurnStateKey).Exists() {
		t.Fatalf("首轮不应回传 turn-state: %s", frame)
	}

	// 握手头：会话标识 + 调用方头 + beta 头（HTTPClient 的 opts 沿用到 WS 握手）。
	hdr := up.handshakeHeaders(t, 0)
	for key, want := range map[string]string{
		"Authorization":       "Bearer pat-ws",
		HeaderSessionID:       "sess-1",
		HeaderThreadID:        "thread-1",
		HeaderClientRequestID: "thread-1",
		HeaderWindowID:        "thread-1:0",
		"x-custom":            "custom-value",
		"OpenAI-Beta":         "responses_websockets=" + DefaultBetaWS,
	} {
		if got := hdr.Get(key); got != want {
			t.Fatalf("握手头 %s = %q, 期望 %q", key, got, want)
		}
	}
}

// TestStreamBodyWSFailedFramePassthroughNotDone：response.failed 作为**帧**透传，
// 之后不补 [DONE]，reader 收尾为 EOF（失败终态不能读成正常收尾）。
func TestStreamBodyWSFailedFramePassthroughNotDone(t *testing.T) {
	up := newWSUpstream(t)
	up.setScripts(wsScript{frames: []string{wsCreatedEv, wsFailedEv}})

	hc := NewHTTPClient(PAT("pat-ws"),
		WithResponsesTransport(TransportWS),
		WithTransport(newFixedTransport(t, "https://chatgpt.com/backend-api/codex/responses", up.srv.URL)),
		WithTurnAuto(false), WithTraceAuto(false))
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	resp, err := hc.StreamBody(ctx, []byte(`{"model":"m","input":[]}`))
	if err != nil {
		t.Fatalf("StreamBody: %v", err)
	}
	got := readAllBody(t, resp.Body)
	want := "event:response.created\ndata:" + wsCreatedEv + "\n\n" +
		"event:response.failed\ndata:" + wsFailedEv + "\n\n"
	if got != want {
		t.Fatalf("SSE 字节流不符\n实际 = %q\n期望 = %q", got, want)
	}
	if strings.Contains(got, "[DONE]") {
		t.Fatalf("失败终态不应补 [DONE]: %q", got)
	}
}

// ---- 2. Responses（ws）与 HTTP 臂逐字段等价 ----

// sseUpstream 起一个 SSE 假上游（HTTP 臂），按事件序列写出 data: 行 + [DONE]。
func sseUpstream(t *testing.T, events ...string) string {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		f := w.(http.Flusher)
		for _, ev := range events {
			_, _ = fmt.Fprintf(w, "data: %s\n\n", ev)
			f.Flush()
		}
		_, _ = io.WriteString(w, "data: [DONE]\n\n")
		f.Flush()
	}))
	t.Cleanup(srv.Close)
	return srv.URL
}

// TestResponsesWSMatchesHTTPComposite：同一事件序列分别经 HTTP（SSE 源）与 WS
// （帧源）聚合，合成体逐字段相等（id / output[] / usage / status），且 TurnState
// 语义一致（本例两边都不签发 → 空）。
func TestResponsesWSMatchesHTTPComposite(t *testing.T) {
	events := []string{wsCreatedEv, wsDeltaEv, wsItemDoneEv, wsItemDone2Ev, wsCompletedEv}

	httpURL := sseUpstream(t, events...)
	httpClient := NewHTTPClient(PAT("pat"),
		WithTransport(newFixedTransport(t, "https://chatgpt.com/backend-api/codex/responses", httpURL)),
		WithTurnAuto(false), WithTraceAuto(false))
	httpResp, err := httpClient.Responses(context.Background(), []byte(`{"model":"gpt-5.6","input":[]}`))
	if err != nil {
		t.Fatalf("HTTP 臂 Responses: %v", err)
	}

	up := newWSUpstream(t)
	up.setScripts(wsScript{frames: events})
	wsClient := NewHTTPClient(PAT("pat"),
		WithResponsesTransport(TransportWS),
		WithTransport(newFixedTransport(t, "https://chatgpt.com/backend-api/codex/responses", up.srv.URL)),
		WithTurnAuto(false), WithTraceAuto(false))
	wsResp, err := wsClient.Responses(context.Background(), []byte(`{"model":"gpt-5.6","input":[]}`))
	if err != nil {
		t.Fatalf("WS 臂 Responses: %v", err)
	}

	if wsResp.StatusCode != httpResp.StatusCode {
		t.Fatalf("StatusCode = %d, HTTP 臂 %d", wsResp.StatusCode, httpResp.StatusCode)
	}
	if wsResp.TurnState != httpResp.TurnState {
		t.Fatalf("TurnState = %q, HTTP 臂 %q", wsResp.TurnState, httpResp.TurnState)
	}
	if string(wsResp.Raw) != string(httpResp.Raw) {
		t.Fatalf("合成体字节不等\nWS   = %s\nHTTP = %s", wsResp.Raw, httpResp.Raw)
	}

	// 逐字段断言（不只比字节：字段级断言能定位"哪一项不等"）。
	var wsBody, httpBody map[string]any
	if err := json.Unmarshal(wsResp.Raw, &wsBody); err != nil {
		t.Fatalf("解析 WS 合成体: %v", err)
	}
	if err := json.Unmarshal(httpResp.Raw, &httpBody); err != nil {
		t.Fatalf("解析 HTTP 合成体: %v", err)
	}
	for _, field := range []string{"id", "object", "status", "output", "usage"} {
		wsJSON, _ := json.Marshal(wsBody[field])
		httpJSON, _ := json.Marshal(httpBody[field])
		if string(wsJSON) != string(httpJSON) {
			t.Fatalf("字段 %s 不等\nWS   = %s\nHTTP = %s", field, wsJSON, httpJSON)
		}
	}
	if gjson.GetBytes(wsResp.Raw, "status").String() != "completed" {
		t.Fatalf("status = %q, 期望 completed", gjson.GetBytes(wsResp.Raw, "status").String())
	}
	if got := gjson.GetBytes(wsResp.Raw, "id").String(); got != "resp_ws" {
		t.Fatalf("id = %q, 期望 resp_ws", got)
	}
	if n := len(gjson.GetBytes(wsResp.Raw, "output").Array()); n != 2 {
		t.Fatalf("output 长度 = %d, 期望 2", n)
	}
	if got := gjson.GetBytes(wsResp.Raw, "usage.total_tokens").Int(); got != 30 {
		t.Fatalf("usage.total_tokens = %d, 期望 30", got)
	}
}

// TestResponsesWSFailedMatchesHTTPError：response.failed 两臂同形——*HTTPError
// {500, Raw: 事件顶层 error 原始 JSON}（逐字复刻 failedError）。
func TestResponsesWSFailedMatchesHTTPError(t *testing.T) {
	httpURL := sseUpstream(t, wsCreatedEv, wsFailedEv)
	httpClient := NewHTTPClient(PAT("pat"),
		WithTransport(newFixedTransport(t, "https://chatgpt.com/backend-api/codex/responses", httpURL)),
		WithTurnAuto(false), WithTraceAuto(false))
	_, httpErr := httpClient.Responses(context.Background(), []byte(`{"model":"m","input":[]}`))

	up := newWSUpstream(t)
	up.setScripts(wsScript{frames: []string{wsCreatedEv, wsFailedEv}})
	wsClient := NewHTTPClient(PAT("pat"),
		WithResponsesTransport(TransportWS),
		WithTransport(newFixedTransport(t, "https://chatgpt.com/backend-api/codex/responses", up.srv.URL)),
		WithTurnAuto(false), WithTraceAuto(false))
	_, wsErr := wsClient.Responses(context.Background(), []byte(`{"model":"m","input":[]}`))

	var httpHE, wsHE *HTTPError
	if !errors.As(httpErr, &httpHE) {
		t.Fatalf("HTTP 臂期望 *HTTPError, got %v", httpErr)
	}
	if !errors.As(wsErr, &wsHE) {
		t.Fatalf("WS 臂期望 *HTTPError, got %v", wsErr)
	}
	if wsHE.StatusCode != httpHE.StatusCode || wsHE.StatusCode != http.StatusInternalServerError {
		t.Fatalf("StatusCode = %d / %d, 期望都是 500", wsHE.StatusCode, httpHE.StatusCode)
	}
	if string(wsHE.Raw) != string(httpHE.Raw) {
		t.Fatalf("Raw 不等\nWS   = %s\nHTTP = %s", wsHE.Raw, httpHE.Raw)
	}
	if want := gjson.Get(wsFailedEv, "error").Raw; string(wsHE.Raw) != want {
		t.Fatalf("Raw = %s, 期望事件 error 原始 JSON %s", wsHE.Raw, want)
	}
}

// TestResponsesWSTruncatedOnIncomplete：response.incomplete 是帧（透传）但聚合器
// 不认它 → 无 completed → 截断错误（普通错误，非 *HTTPError）。
func TestResponsesWSTruncatedOnIncomplete(t *testing.T) {
	up := newWSUpstream(t)
	up.setScripts(wsScript{frames: []string{
		wsCreatedEv,
		`{"type":"response.incomplete","response":{"id":"resp_ws","status":"incomplete"}}`,
	}})
	hc := NewHTTPClient(PAT("pat"),
		WithResponsesTransport(TransportWS),
		WithTransport(newFixedTransport(t, "https://chatgpt.com/backend-api/codex/responses", up.srv.URL)),
		WithTurnAuto(false), WithTraceAuto(false))
	_, err := hc.Responses(context.Background(), []byte(`{"model":"m","input":[]}`))
	if err == nil {
		t.Fatal("期望截断错误, got nil")
	}
	var he *HTTPError
	if errors.As(err, &he) {
		t.Fatalf("截断应为普通错误, got *HTTPError{StatusCode:%d}", he.StatusCode)
	}
	if !strings.Contains(err.Error(), "responses 事件流截断") {
		t.Fatalf("错误文案应与 HTTP 臂一致（截断）, got %v", err)
	}
}

// ---- 3. turn-state 握手签发 → 回写；请求侧不回落缓存 ----

// TestResponsesWSTurnStateCapturedNoReplay：握手响应头签发值回写 HTTPClient.TurnState()
// 并回填到合成响应头（S5 的"真握手"分支：那是 ts 的唯一发现通道），但**不**自动
// 成为下一次请求的 ts——缓存只是观测面，跨轮不得重放（S2）。
func TestResponsesWSTurnStateCapturedNoReplay(t *testing.T) {
	up := newWSUpstream(t)
	up.setTurnState("ts-issued-1")
	up.setScripts(
		wsScript{frames: []string{wsCreatedEv, wsCompletedEv}},
		wsScript{frames: []string{wsCreatedEv, wsCompletedEv}},
		wsScript{frames: []string{wsCreatedEv, wsCompletedEv}},
	)
	hc := NewHTTPClient(PAT("pat-ws"),
		WithResponsesTransport(TransportWS),
		WithTransport(newFixedTransport(t, "https://chatgpt.com/backend-api/codex/responses", up.srv.URL)),
		WithTurnAuto(false), WithTraceAuto(false))
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// 第 1 轮：上游签发 ts-issued-1。请求没带 ts，但本次真的握手了 ⇒ 合成响应头
	// 回填本次签发值（S5 新鲜值分支），同时回写 TurnState()（观测面）。
	resp, err := hc.StreamBody(ctx, []byte(`{"model":"m","input":[]}`))
	if err != nil {
		t.Fatalf("第 1 次 StreamBody: %v", err)
	}
	if got := resp.Header.Get(HeaderTurnState); got != "ts-issued-1" {
		t.Fatalf("合成响应头 %s = %q, 期望 ts-issued-1（真握手必须回填本次签发值）", HeaderTurnState, got)
	}
	_ = readAllBody(t, resp.Body)
	if got := hc.TurnState(); got != "ts-issued-1" {
		t.Fatalf("HTTPClient.TurnState() = %q, 期望 ts-issued-1", got)
	}
	first := up.sentFrames(t)
	if len(first) != 1 {
		t.Fatalf("上游收到 %d 帧, 期望 1 帧", len(first))
	}
	if got := gjson.GetBytes(first[0], "client_metadata."+codexMetaTurnStateKey); got.Exists() {
		t.Fatalf("首轮不应回传 turn-state, got %s", got.Raw)
	}

	// 第 2 轮：**不**回放上一轮签发值（请求侧取值不回落到 TurnState()）；
	// 本轮又拨了一条新连接（缓存关闭）⇒ 真握手 ⇒ 回填它自己的签发值 ts-issued-2。
	up.setTurnState("ts-issued-2")
	resp2, err := hc.StreamBody(ctx, []byte(`{"model":"m","input":[]}`))
	if err != nil {
		t.Fatalf("第 2 次 StreamBody: %v", err)
	}
	if got := resp2.Header.Get(HeaderTurnState); got != "ts-issued-2" {
		t.Fatalf("第 2 轮合成响应头 = %q, 期望 ts-issued-2（本次签发值，不得是上一轮的旧值）", got)
	}
	_ = readAllBody(t, resp2.Body)
	frames := up.sentFrames(t)
	if len(frames) != 2 {
		t.Fatalf("上游收到 %d 帧, 期望 2 帧", len(frames))
	}
	if got := gjson.GetBytes(frames[1], "client_metadata."+codexMetaTurnStateKey); got.Exists() {
		t.Fatalf("第 2 轮帧不应回传上一轮 turn-state, got %s（帧 %s）", got.Raw, frames[1])
	}
	if got := hc.TurnState(); got != "ts-issued-2" {
		t.Fatalf("HTTPClient.TurnState() = %q, 期望 ts-issued-2（本轮签发值）", got)
	}

	// 第 3 轮（兼容入口）：per-request 值以 WithHeader(HeaderTurnState) 下发。
	// WS 臂必须把它搬进帧内 client_metadata，且**不能**出现在升级头里。
	// 本轮握手签发的正是同一个值 ⇒ S5 判据相等 ⇒ 合成响应头回填它。
	up.setTurnState("ts-from-gateway")
	gw := NewHTTPClient(PAT("pat-ws"),
		WithResponsesTransport(TransportWS),
		WithTransport(newFixedTransport(t, "https://chatgpt.com/backend-api/codex/responses", up.srv.URL)),
		WithHeader(HeaderTurnState, "ts-from-gateway"),
		WithTurnAuto(false), WithTraceAuto(false))
	resp3, err := gw.StreamBody(ctx, []byte(`{"model":"m","input":[]}`))
	if err != nil {
		t.Fatalf("第 3 次 StreamBody（WithHeader 注入）: %v", err)
	}
	if got := resp3.Header.Get(HeaderTurnState); got != "ts-from-gateway" {
		t.Fatalf("第 3 轮合成响应头 = %q, 期望 ts-from-gateway（effective == 最近签发值）", got)
	}
	_ = readAllBody(t, resp3.Body)
	all := up.sentFrames(t)
	if len(all) != 3 {
		t.Fatalf("上游收到 %d 帧, 期望 3 帧", len(all))
	}
	if got := gjson.GetBytes(all[2], "client_metadata."+codexMetaTurnStateKey).String(); got != "ts-from-gateway" {
		t.Fatalf("WithHeader 注入的 turn-state = %q, 期望 ts-from-gateway（帧 %s）", got, all[2])
	}
	if got := up.handshakeHeaders(t, 2).Get(HeaderTurnState); got != "" {
		t.Fatalf("升级头不应携带 %s（真实客户端只在帧里带）, got %q", HeaderTurnState, got)
	}
}

// ---- 4. 帧构造五条规则 + 非对象根 ----

// TestBuildResponsesCreateFrame：覆写 type、删 previous_response_id/generate、
// 强制 stream:true、过 WS 白名单；非对象根（null/123/"str"/[1]）与坏 JSON 报错。
func TestBuildResponsesCreateFrame(t *testing.T) {
	t.Run("重写与删除", func(t *testing.T) {
		in := `{"model":"gpt-5.6","type":"wrong.type","stream":false,"previous_response_id":"resp_old","generate":true,"junk":1,"input":[{"type":"message"}]}`
		frame, err := buildResponsesCreateFrame([]byte(in))
		if err != nil {
			t.Fatalf("buildResponsesCreateFrame: %v", err)
		}
		if typ := gjson.GetBytes(frame, "type").String(); typ != responseCreateTypeWant {
			t.Fatalf("type = %q, 期望 %q（覆写而非补缺）", typ, responseCreateTypeWant)
		}
		if !gjson.GetBytes(frame, "stream").Bool() {
			t.Fatalf("stream 应为 true: %s", frame)
		}
		for _, k := range []string{"previous_response_id", "generate", "junk"} {
			if gjson.GetBytes(frame, k).Exists() {
				t.Fatalf("不应携带 %q: %s", k, frame)
			}
		}
		if got := gjson.GetBytes(frame, "model").String(); got != "gpt-5.6" {
			t.Fatalf("model = %q, 期望保留", got)
		}
		if n := len(gjson.GetBytes(frame, "input").Array()); n != 1 {
			t.Fatalf("input 长度 = %d, 期望 1（白名单字段值原样保留）", n)
		}
	})

	t.Run("空对象补齐", func(t *testing.T) {
		frame, err := buildResponsesCreateFrame([]byte(`{}`))
		if err != nil {
			t.Fatalf("buildResponsesCreateFrame: %v", err)
		}
		if got := string(frame); got != `{"type":"response.create","stream":true}` {
			t.Fatalf("帧 = %s, 期望补 type/stream 的最简帧", got)
		}
	})

	t.Run("白名单外全空", func(t *testing.T) {
		frame, err := buildResponsesCreateFrame([]byte(`{"foo":1}`))
		if err != nil {
			t.Fatalf("buildResponsesCreateFrame: %v", err)
		}
		if gjson.GetBytes(frame, "foo").Exists() {
			t.Fatalf("白名单外键应被过滤: %s", frame)
		}
	})

	t.Run("非对象根", func(t *testing.T) {
		for _, in := range []string{`null`, `123`, `"str"`, `[1]`, `true`, `  null  `} {
			frame, err := buildResponsesCreateFrame([]byte(in))
			if err == nil {
				t.Fatalf("输入 %q 应报错, got frame %s", in, frame)
			}
			if !errors.Is(err, errResponsesFrameInvalid) {
				t.Fatalf("输入 %q 期望 errResponsesFrameInvalid, got %v", in, err)
			}
			if frame != nil {
				t.Fatalf("输入 %q 报错时不应返回帧: %s", in, frame)
			}
		}
	})

	t.Run("坏 JSON", func(t *testing.T) {
		for _, in := range []string{``, `{`, `{"a":}`, `{"a":1,}`} {
			if _, err := buildResponsesCreateFrame([]byte(in)); !errors.Is(err, errResponsesFrameInvalid) {
				t.Fatalf("输入 %q 期望 errResponsesFrameInvalid, got %v", in, err)
			}
		}
	})

	t.Run("对象根前导空白", func(t *testing.T) {
		if _, err := buildResponsesCreateFrame([]byte(" \n\t{\"model\":\"m\"}")); err != nil {
			t.Fatalf("带前导空白的对象根应通过: %v", err)
		}
	})

	// StreamBody 的本地输入错误必须在拨号前返回（不能为坏 body 烧一次握手）。
	t.Run("坏输入不烧握手", func(t *testing.T) {
		up := newWSUpstream(t)
		hc := NewHTTPClient(PAT("pat"),
			WithResponsesTransport(TransportWS),
			WithTransport(newFixedTransport(t, "https://chatgpt.com/backend-api/codex/responses", up.srv.URL)))
		if _, err := hc.StreamBody(context.Background(), []byte(`null`)); !errors.Is(err, errResponsesFrameInvalid) {
			t.Fatalf("期望 errResponsesFrameInvalid, got %v", err)
		}
		if n := up.dialCount(); n != 0 {
			t.Fatalf("坏输入不应拨号, 实际握手 %d 次", n)
		}
	})
}

// responseCreateTypeWant 是测试侧的独立字面量（不复用实现的常量——实现写成
// 别的字符串时测试必须红）。
const responseCreateTypeWant = "response.create"

// ---- 5. 关闭语义 ----

// TestStreamBodyWSCloseSemantics：任意 close frame（1011/1005）= EOF；没有 close
// frame（直接断 TCP）= 传输错误（底层错误，不包 *HTTPError）。
func TestStreamBodyWSCloseSemantics(t *testing.T) {
	cases := []struct {
		name    string
		script  wsScript
		wantEOF bool
	}{
		{
			name:    "1011 内部错误关闭帧",
			script:  wsScript{frames: []string{wsCreatedEv}, code: websocket.StatusInternalError},
			wantEOF: true,
		},
		{
			name:    "1005 无状态关闭帧",
			script:  wsScript{frames: []string{wsCreatedEv}, code: websocket.StatusNoStatusRcvd},
			wantEOF: true,
		},
		{
			name:    "1000 正常关闭帧",
			script:  wsScript{frames: []string{wsCreatedEv}, code: websocket.StatusNormalClosure},
			wantEOF: true,
		},
		{
			name:    "无 close 帧直接断 TCP",
			script:  wsScript{frames: []string{wsCreatedEv}, abrupt: true},
			wantEOF: false,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			up := newWSUpstream(t)
			up.setScripts(tc.script)
			hc := NewHTTPClient(PAT("pat-ws"),
				WithResponsesTransport(TransportWS),
				WithTransport(newFixedTransport(t, "https://chatgpt.com/backend-api/codex/responses", up.srv.URL)),
				WithTurnAuto(false), WithTraceAuto(false))
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			resp, err := hc.StreamBody(ctx, []byte(`{"model":"m","input":[]}`))
			if err != nil {
				t.Fatalf("StreamBody: %v", err)
			}
			defer func() { _ = resp.Body.Close() }()
			var rest []byte
			rest, err = io.ReadAll(resp.Body)
			if !strings.Contains(string(rest), "response.created") {
				t.Fatalf("关闭前应已交付 created 帧, got %q", rest)
			}
			if tc.wantEOF {
				if err != nil {
					t.Fatalf("close frame 收尾应为 EOF, got %v", err)
				}
				return
			}
			if err == nil {
				t.Fatal("无 close 帧的断开应为错误, got nil")
			}
			// 注意：不能用 errors.Is(err, io.EOF)——coder/websocket 的读错把
			// io.EOF 作为底层原因包在 "failed to read frame header: EOF" 里，
			// 那是传输错误而非干净 EOF。这里判的是"没有以 io.EOF 收尾"。
			if err == io.EOF {
				t.Fatalf("无 close 帧的断开不应是 EOF: %v", err)
			}
			var he *HTTPError
			if errors.As(err, &he) {
				t.Fatalf("传输读错不应包成 *HTTPError, got %+v", he)
			}
			if !errors.Is(err, io.ErrUnexpectedEOF) && !strings.Contains(strings.ToLower(err.Error()), "eof") &&
				!strings.Contains(strings.ToLower(err.Error()), "reset") &&
				!strings.Contains(strings.ToLower(err.Error()), "closed") {
				t.Fatalf("期望底层断开错误, got %v", err)
			}
		})
	}
}

// ---- 6. 默认路径（HTTP）零 WS 连接 ----

// TestResponsesTransportHTTPKeepsHTTPPath：零值 / TransportHTTP / 未知值都走 HTTP，
// 一条 WS 也不拨（假上游计数）。同一份事件序列产物一致。
func TestResponsesTransportHTTPKeepsHTTPPath(t *testing.T) {
	var wsDials atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.EqualFold(r.Header.Get("Upgrade"), "websocket") {
			wsDials.Add(1)
			c, err := websocket.Accept(w, r, nil)
			if err == nil {
				defer c.CloseNow()
				for {
					if _, _, rerr := c.Read(r.Context()); rerr != nil {
						return
					}
				}
			}
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, "data: "+wsCreatedEv+"\n\n")
		_, _ = io.WriteString(w, "data: "+wsCompletedEv+"\n\n")
		_, _ = io.WriteString(w, "data: [DONE]\n\n")
	}))
	t.Cleanup(srv.Close)
	tr := newFixedTransport(t, "https://chatgpt.com/backend-api/codex/responses", srv.URL)

	cases := []struct {
		name string
		opt  []Option
	}{
		{name: "零值（未设置）", opt: nil},
		{name: "TransportHTTP", opt: []Option{WithResponsesTransport(TransportHTTP)}},
		{name: "空串", opt: []Option{WithResponsesTransport(Transport(""))}},
		{name: "未知值", opt: []Option{WithResponsesTransport(Transport("smtp"))}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			opts := append([]Option{WithTransport(tr), WithTurnAuto(false), WithTraceAuto(false)}, tc.opt...)
			hc := NewHTTPClient(PAT("pat"), opts...)
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()

			resp, err := hc.StreamBody(ctx, []byte(`{"model":"m","input":[]}`))
			if err != nil {
				t.Fatalf("StreamBody: %v", err)
			}
			body := readAllBody(t, resp.Body)
			if !strings.Contains(body, wsCreatedEv) || !strings.Contains(body, "[DONE]") {
				t.Fatalf("HTTP 臂原始 SSE 字节应有 created 与 [DONE], got %q", body)
			}

			agg, err := hc.Responses(ctx, []byte(`{"model":"m","input":[]}`))
			if err != nil {
				t.Fatalf("Responses: %v", err)
			}
			if got := gjson.GetBytes(agg.Raw, "status").String(); got != "completed" {
				t.Fatalf("合成体 status = %q, 期望 completed", got)
			}
		})
	}
	if n := wsDials.Load(); n != 0 {
		t.Fatalf("HTTP 路径不应拨 WS, 实际 %d 次", n)
	}
}

// ---- 7. 并发（配合 -race）----

// TestStreamBodyWSConcurrent：同一 HTTPClient 并发多次 StreamBody，各自独立连接
// 与 reader；共享的只有 opts 与 turn-state 缓存（atomic）。
func TestStreamBodyWSConcurrent(t *testing.T) {
	const n = 4
	up := newWSUpstream(t)
	up.setScripts(wsScript{frames: []string{wsCreatedEv, wsItemDoneEv, wsCompletedEv}})
	hc := NewHTTPClient(PAT("pat-ws"),
		WithResponsesTransport(TransportWS),
		WithTransport(newFixedTransport(t, "https://chatgpt.com/backend-api/codex/responses", up.srv.URL)),
		WithTurnAuto(true), WithTraceAuto(true))
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	errs := make([]error, n)
	bodies := make([]string, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			resp, err := hc.StreamBody(ctx, []byte(`{"model":"m","input":[]}`))
			if err != nil {
				errs[idx] = err
				return
			}
			defer func() { _ = resp.Body.Close() }()
			b, rerr := io.ReadAll(resp.Body)
			if rerr != nil {
				errs[idx] = rerr
				return
			}
			bodies[idx] = string(b)
		}(i)
	}
	wg.Wait()
	for i := 0; i < n; i++ {
		if errs[i] != nil {
			t.Fatalf("goroutine %d: %v", i, errs[i])
		}
		if !strings.Contains(bodies[i], "[DONE]") {
			t.Fatalf("goroutine %d 未读到完整流: %q", i, bodies[i])
		}
	}
	up.waitDial(t, n)
	if got := len(up.sentFrames(t)); got != n {
		t.Fatalf("上游收到 %d 帧, 期望 %d", got, n)
	}
}
