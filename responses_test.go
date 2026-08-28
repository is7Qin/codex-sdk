package codexsdk

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/tidwall/gjson"
)

// ---- 聚合单测（直接喂 SSE 事件序列，对齐真实 codex 事件形态）----

// 事件样本：response.created 嵌套 response.id；output_item.done 携带完整 item
// （非增量）；response.completed 携带 usage。增量事件（output_item.added /
// response.output_text.delta）与 response.in_progress 不收集。
const (
	respCreatedEvent = `{"type":"response.created","response":{"id":"resp_001","object":"response","status":"in_progress","model":"gpt-5.6"}}`
	respItemAddedEv  = `{"type":"output_item.added","item":{"id":"msg_1","status":"in_progress","type":"message"}}`
	respDeltaEv      = `{"type":"response.output_text.delta","item_id":"msg_1","delta":"Hel"}`

	respItemMsg = `{"id":"msg_1","status":"completed","type":"message","role":"assistant","content":[{"type":"output_text","text":"Hello"}]}`
	respItemFC  = `{"id":"fc_1","status":"completed","type":"function_call","name":"get_weather","arguments":"{\"city\":\"SF\"}"}`

	respItemMsgDoneEv = `{"type":"output_item.done","item":` + respItemMsg + `}`
	respItemFCDoneEv  = `{"type":"output_item.done","item":` + respItemFC + `}`

	respUsage        = `{"input_tokens":10,"output_tokens":20,"total_tokens":30}`
	respCompletedEv  = `{"type":"response.completed","response":{"id":"resp_001","object":"response","status":"completed"},"usage":` + respUsage + `}`
	respCompletedNoU = `{"type":"response.completed","response":{"id":"resp_001","object":"response","status":"completed"}}`
)

// TestResponsesAggregatorComposite：response.created → output_item.done ×N →
// response.completed 聚合——合成体 id / output 顺序（流序）/ usage 断言；
// 增量事件（added/delta）不收集。
func TestResponsesAggregatorComposite(t *testing.T) {
	agg := &responsesAggregator{}
	for _, ev := range []string{respCreatedEvent, respItemAddedEv, respDeltaEv, respItemMsgDoneEv, respItemFCDoneEv, respCompletedEv} {
		if err := agg.feed([]byte(ev)); err != nil {
			t.Fatalf("feed %s: %v", ev, err)
		}
	}
	if !agg.completed {
		t.Fatal("completed 应置位")
	}
	body, err := json.Marshal(agg.composite())
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	want := `{"id":"resp_001","object":"response","status":"completed","output":[` +
		respItemMsg + `,` + respItemFC + `],"usage":` + respUsage + `}`
	if string(body) != want {
		t.Fatalf("合成体 = %s\n期望     = %s", body, want)
	}
}

// TestResponsesAggregatorSkipsIncrementalAndUnknown：白名单外事件全部跳过——
// response.in_progress（其 response.id 不污染合成 id）、增量事件、
// response.rejected（终态但非 completed → 由截断语义兜底）、未知事件、
// 非 JSON 行（type 不可提取）。仅白名单事件生效。
func TestResponsesAggregatorSkipsIncrementalAndUnknown(t *testing.T) {
	agg := &responsesAggregator{}
	for _, ev := range []string{
		respCreatedEvent, // 先置 id=resp_001（in_progress 的 response.id 不得覆盖）
		`{"type":"response.in_progress","response":{"id":"evil_id"}}`, // 不污染合成 id
		respItemAddedEv,
		respDeltaEv,
		`{"type":"response.rejected","response":{"id":"resp_001"}}`,
		`{"type":"future.event.v9","payload":{"x":1}}`, // 未知事件——上游扩展兼容
		`this is not json`, // 非 JSON 行跳过不报错
		respItemMsgDoneEv,
		respItemAddedEv, // 重复 added 也不收集
		respCompletedEv,
	} {
		if err := agg.feed([]byte(ev)); err != nil {
			t.Fatalf("feed %s: %v", ev, err)
		}
	}
	if agg.id != "resp_001" || !agg.completed {
		t.Fatalf("id/completed = %q/%v（in_progress 不应污染 id）", agg.id, agg.completed)
	}
	body, _ := json.Marshal(agg.composite())
	want := `{"id":"resp_001","object":"response","status":"completed","output":[` +
		respItemMsg + `],"usage":` + respUsage + `}`
	if string(body) != want {
		t.Fatalf("合成体 = %s\n期望     = %s", body, want)
	}
}

// TestResponsesAggregatorMissingCreatedAndUsage：缺 response.created → id 兜底
// 空；response.completed 无 usage → 合成体 usage 缺省（字段省略）。
func TestResponsesAggregatorMissingCreatedAndUsage(t *testing.T) {
	agg := &responsesAggregator{}
	for _, ev := range []string{respItemMsgDoneEv, respCompletedNoU} {
		if err := agg.feed([]byte(ev)); err != nil {
			t.Fatalf("feed %s: %v", ev, err)
		}
	}
	if agg.id != "" {
		t.Fatalf("缺 response.created 时 id 应兜底空, got %q", agg.id)
	}
	body, err := json.Marshal(agg.composite())
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	want := `{"id":"","object":"response","status":"completed","output":[` + respItemMsg + `]}`
	if string(body) != want {
		t.Fatalf("合成体 = %s\n期望     = %s", body, want)
	}
}

// TestResponsesAggregatorEmptyOutput：无任何 item → output 恒为数组（[] 非 null）。
func TestResponsesAggregatorEmptyOutput(t *testing.T) {
	agg := &responsesAggregator{}
	for _, ev := range []string{respCreatedEvent, respCompletedEv} {
		if err := agg.feed([]byte(ev)); err != nil {
			t.Fatalf("feed %s: %v", ev, err)
		}
	}
	body, err := json.Marshal(agg.composite())
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	want := `{"id":"resp_001","object":"response","status":"completed","output":[],"usage":` + respUsage + `}`
	if string(body) != want {
		t.Fatalf("合成体 = %s\n期望     = %s", body, want)
	}
}

// TestResponsesAggregatorFailedEvent：response.failed → 事件级失败合成
// HTTPError{500, Raw=事件 error JSON}；error 字段缺失 → Raw 兜底整个事件。
func TestResponsesAggregatorFailedEvent(t *testing.T) {
	agg := &responsesAggregator{}
	err := agg.feed([]byte(`{"type":"response.failed","error":{"code":"server_error","message":"boom"}}`))
	var he *HTTPError
	if !errors.As(err, &he) {
		t.Fatalf("期望 *HTTPError, got %T: %v", err, err)
	}
	if he.StatusCode != http.StatusInternalServerError {
		t.Fatalf("StatusCode = %d, 期望 500（事件级失败归网关 5xx 分类）", he.StatusCode)
	}
	if string(he.Raw) != `{"code":"server_error","message":"boom"}` {
		t.Fatalf("Raw = %s, 期望事件 error JSON", he.Raw)
	}

	// error 字段缺失 → Raw 兜底整个事件
	agg2 := &responsesAggregator{}
	err = agg2.feed([]byte(`{"type":"response.failed"}`))
	var he2 *HTTPError
	if !errors.As(err, &he2) || he2.StatusCode != http.StatusInternalServerError {
		t.Fatalf("缺 error 字段应仍合成 500 HTTPError, got %v", err)
	}
	if string(he2.Raw) != `{"type":"response.failed"}` {
		t.Fatalf("缺 error 字段 Raw 应兜底整个事件, got %s", he2.Raw)
	}
}

// ---- 集成（httptest mock 上游）----

// responsesWireCapture 捕获 mock 收到的请求（wire 断言）。
type responsesWireCapture struct {
	mu     sync.Mutex
	auth   string
	ct     string
	ua     string
	origin string
	body   []byte
}

// startResponsesMock 起 responses 端点 mock：按 events 依次发 SSE data: 行，
// sendDone 时尾部追加 [DONE]；hdr 注入响应头（turn-state 等）。
func startResponsesMock(t *testing.T, hdr http.Header, events []string, sendDone bool, cap *responsesWireCapture) string {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if cap != nil {
			cap.mu.Lock()
			cap.auth = r.Header.Get("Authorization")
			cap.ct = r.Header.Get("Content-Type")
			cap.ua = r.Header.Get("User-Agent")
			cap.origin = r.Header.Get("Originator")
			cap.body, _ = io.ReadAll(r.Body)
			cap.mu.Unlock()
		}
		for k, vs := range hdr {
			for _, v := range vs {
				w.Header().Add(k, v)
			}
		}
		w.Header().Set("Content-Type", "text/event-stream")
		f := w.(http.Flusher)
		for _, ev := range events {
			_, _ = io.WriteString(w, "data: "+ev+"\n\n")
			f.Flush()
		}
		if sendDone {
			_, _ = io.WriteString(w, "data: [DONE]\n\n")
			f.Flush()
		}
	}))
	t.Cleanup(srv.Close)
	return srv.URL + "/v1/responses"
}

// TestResponsesAggregateWire：全链路——鉴权头注入 + stream:true 注入（payload
// 未带 stream）+ 其余字段保留 + 事件聚合合成体 + 响应头 turn-state 捕获
// （resp.TurnState 与 c.TurnState() 一致）。
func TestResponsesAggregateWire(t *testing.T) {
	var cap responsesWireCapture
	srv := startResponsesMock(t,
		http.Header{HeaderTurnState: []string{"st-resp"}},
		[]string{respCreatedEvent, respItemAddedEv, respDeltaEv, respItemMsgDoneEv, respItemFCDoneEv, respCompletedEv},
		true, &cap)

	hc := NewHTTPClient(PAT("pat-resp"), WithTransport(newFixedTransport(t, "https://chatgpt.com/backend-api/codex/responses", srv)))
	resp, err := hc.Responses(context.Background(), []byte(`{"model":"gpt-5.6","input":"hi"}`))
	if err != nil {
		t.Fatalf("Responses: %v", err)
	}

	cap.mu.Lock()
	defer cap.mu.Unlock()
	if cap.auth != "Bearer pat-resp" {
		t.Fatalf("Authorization = %q", cap.auth)
	}
	if cap.ct != "application/json" {
		t.Fatalf("Content-Type = %q", cap.ct)
	}
	if cap.ua != DefaultCodexUserAgent || cap.origin != DefaultOriginator {
		t.Fatalf("UA/originator = %q/%q", cap.ua, cap.origin)
	}
	if !gjson.GetBytes(cap.body, "stream").Bool() {
		t.Fatalf("payload 未带 stream 应注入 stream:true, body = %s", cap.body)
	}
	if gjson.GetBytes(cap.body, "model").String() != "gpt-5.6" ||
		gjson.GetBytes(cap.body, "input").String() != "hi" {
		t.Fatalf("注入不应动其余字段: %s", cap.body)
	}

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("StatusCode = %d, 期望 200", resp.StatusCode)
	}
	want := `{"id":"resp_001","object":"response","status":"completed","output":[` +
		respItemMsg + `,` + respItemFC + `],"usage":` + respUsage + `}`
	if string(resp.Raw) != want {
		t.Fatalf("合成体 = %s\n期望     = %s", resp.Raw, want)
	}
	if resp.TurnState != "st-resp" {
		t.Fatalf("HTTPResponse.TurnState = %q, 期望 st-resp", resp.TurnState)
	}
	if hc.TurnState() != "st-resp" {
		t.Fatalf("TurnState() = %q, 期望 st-resp（Stream 内部已捕获）", hc.TurnState())
	}
}

// TestResponsesStreamFalseOverridden：payload 显式 stream:false → wire 请求
// 无条件覆盖为 stream:true（网关非流式消费主场景——冲突 wire 断言）。
func TestResponsesStreamFalseOverridden(t *testing.T) {
	var cap responsesWireCapture
	srv := startResponsesMock(t, nil,
		[]string{respCreatedEvent, respItemMsgDoneEv, respCompletedEv}, true, &cap)

	hc := NewHTTPClient(PAT("p"), WithTransport(newFixedTransport(t, "https://chatgpt.com/backend-api/codex/responses", srv)))
	if _, err := hc.Responses(context.Background(), []byte(`{"model":"m","stream":false}`)); err != nil {
		t.Fatalf("Responses: %v", err)
	}
	cap.mu.Lock()
	defer cap.mu.Unlock()
	if !gjson.GetBytes(cap.body, "stream").Bool() {
		t.Fatalf("显式 stream:false 必须被无条件覆盖为 true, body = %s", cap.body)
	}
	if gjson.GetBytes(cap.body, "model").String() != "m" {
		t.Fatalf("覆盖不应动其余字段: %s", cap.body)
	}
}

// TestResponsesInvalidPayloadAbandonInjection：非法 JSON payload → 放弃注入
// 保持原样（对齐 injectClientMetadataKeys 失败语义）→ 上游 400 原样透传。
func TestResponsesInvalidPayloadAbandonInjection(t *testing.T) {
	errorBody := []byte(`{"error":{"code":"invalid_request","message":"bad json"}}`)
	var gotBody []byte
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotBody, _ = io.ReadAll(r.Body)
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write(errorBody)
	}))
	t.Cleanup(srv.Close)

	hc := NewHTTPClient(PAT("p"), WithTransport(newFixedTransport(t, "https://chatgpt.com/backend-api/codex/responses", srv.URL)))
	_, err := hc.Responses(context.Background(), []byte(`{"broken`))
	var he *HTTPError
	if !errors.As(err, &he) {
		t.Fatalf("期望 *HTTPError, got %T: %v", err, err)
	}
	if he.StatusCode != http.StatusBadRequest {
		t.Fatalf("StatusCode = %d, 期望 400", he.StatusCode)
	}
	if !bytes.Equal(he.Raw, errorBody) {
		t.Fatalf("Raw 应原样交付上游错误体: %s", he.Raw)
	}
	if string(gotBody) != `{"broken` {
		t.Fatalf("非法 payload 应原样上送（放弃注入）, got %s", gotBody)
	}
}

// TestResponsesTruncatedEOF：流 EOF 结束（无 [DONE]）未达 response.completed →
// 错误返回（截断 = 失败，不返回 status=completed 的半截响应）。
func TestResponsesTruncatedEOF(t *testing.T) {
	srv := startResponsesMock(t, nil, []string{respCreatedEvent, respItemMsgDoneEv}, false, nil)
	hc := NewHTTPClient(PAT("p"), WithTransport(newFixedTransport(t, "https://chatgpt.com/backend-api/codex/responses", srv)))
	_, err := hc.Responses(context.Background(), []byte(`{"model":"m"}`))
	if err == nil {
		t.Fatal("EOF 截断应错误返回")
	}
	var he *HTTPError
	if errors.As(err, &he) {
		t.Fatalf("截断错误不应为 *HTTPError 信封（非上游 HTTP 错误）, got %v", err)
	}
	if !strings.Contains(err.Error(), "response.completed") {
		t.Fatalf("截断错误应指明缺失终态事件, got %v", err)
	}
}

// TestResponsesTruncatedDoneWithoutCompleted：[DONE] 正常终止但未达
// response.completed → 同样错误（终态事件缺失 = 截断，[DONE] 与 EOF 统一判定）。
func TestResponsesTruncatedDoneWithoutCompleted(t *testing.T) {
	srv := startResponsesMock(t, nil, []string{respCreatedEvent, respItemMsgDoneEv}, true, nil)
	hc := NewHTTPClient(PAT("p"), WithTransport(newFixedTransport(t, "https://chatgpt.com/backend-api/codex/responses", srv)))
	if _, err := hc.Responses(context.Background(), []byte(`{"model":"m"}`)); err == nil {
		t.Fatal("[DONE] 而无 completed 应错误返回")
	}
}

// TestResponsesFailedEventWire：response.failed 事件 → 合成 HTTPError{500,
// Raw=事件 error JSON}（事件级失败无 HTTP 状态——500 归网关 5xx 分类）。
func TestResponsesFailedEventWire(t *testing.T) {
	failedEv := `{"type":"response.failed","error":{"code":"server_error","message":"upstream exploded"}}`
	srv := startResponsesMock(t, nil, []string{respCreatedEvent, failedEv}, true, nil)
	hc := NewHTTPClient(PAT("p"), WithTransport(newFixedTransport(t, "https://chatgpt.com/backend-api/codex/responses", srv)))
	_, err := hc.Responses(context.Background(), []byte(`{}`))
	var he *HTTPError
	if !errors.As(err, &he) {
		t.Fatalf("期望 *HTTPError, got %T: %v", err, err)
	}
	if he.StatusCode != http.StatusInternalServerError {
		t.Fatalf("StatusCode = %d, 期望 500", he.StatusCode)
	}
	if string(he.Raw) != `{"code":"server_error","message":"upstream exploded"}` {
		t.Fatalf("Raw = %s, 期望事件 error JSON", he.Raw)
	}
}

// TestResponsesZeroCopyBigItem：MB 级大 item（base64）与长交错事件——scanner
// 复用缓冲下不拷贝的实现会在此暴露（跨回调污染合成体）。合成体 output 必须
// 与发送的 item 字节完全一致。
func TestResponsesZeroCopyBigItem(t *testing.T) {
	blob := base64.StdEncoding.EncodeToString(bytes.Repeat([]byte{0xAB}, 900*1024)) // ~1.2MB base64
	bigItem := `{"id":"big_1","status":"completed","type":"message","content":[{"type":"output_image","image_url":"data:image/png;base64,` + blob + `"}]}`
	bigEvent := `{"type":"output_item.done","item":` + bigItem + `}`
	// 300KB 长 delta 行：扫描器复用大缓冲时覆盖其头部字节——未拷贝实现被污染
	filler := `{"type":"response.output_text.delta","item_id":"big_1","delta":"` + strings.Repeat("B", 300*1024) + `"}`
	item2 := `{"id":"small_1","status":"completed","type":"message","content":[{"type":"output_text","text":"C"}]}`

	srv := startResponsesMock(t, nil,
		[]string{respCreatedEvent, bigEvent, filler, `{"type":"output_item.done","item":` + item2 + `}`, respCompletedEv},
		true, nil)
	hc := NewHTTPClient(PAT("p"), WithTransport(newFixedTransport(t, "https://chatgpt.com/backend-api/codex/responses", srv)))
	resp, err := hc.Responses(context.Background(), []byte(`{"model":"m"}`))
	if err != nil {
		t.Fatalf("Responses: %v", err)
	}
	out := gjson.GetBytes(resp.Raw, "output")
	if !out.IsArray() || len(out.Array()) != 2 {
		t.Fatalf("output = %v（期望 2 个 item）", out.Raw)
	}
	if out.Array()[0].Raw != bigItem {
		t.Fatalf("大 item 被污染（零拷贝防线）——output[0] 与发送字节不一致:\n got %s...", truncate(out.Array()[0].Raw, 200))
	}
	if out.Array()[1].Raw != item2 {
		t.Fatalf("item2 被污染: %s...", truncate(out.Array()[1].Raw, 200))
	}
	if gjson.GetBytes(resp.Raw, "id").String() != "resp_001" {
		t.Fatalf("id = %s", gjson.GetBytes(resp.Raw, "id").String())
	}
	if gjson.GetBytes(resp.Raw, "usage").Raw != respUsage {
		t.Fatalf("usage = %s", gjson.GetBytes(resp.Raw, "usage").Raw)
	}
}

// truncate 截断超长字符串（断言失败时的可读输出）。
func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + fmt.Sprintf("...(len=%d)", len(s))
}

// TestResponses403Passthrough：上游 403 → *HTTPError 原样交付（网关透传映射）。
func TestResponses403Passthrough(t *testing.T) {
	errorBody := []byte(`{"detail":"Forbidden"}`)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusForbidden)
		_, _ = w.Write(errorBody)
	}))
	t.Cleanup(srv.Close)

	hc := NewHTTPClient(PAT("p"), WithTransport(newFixedTransport(t, "https://chatgpt.com/backend-api/codex/responses", srv.URL)))
	_, err := hc.Responses(context.Background(), []byte(`{}`))
	var he *HTTPError
	if !errors.As(err, &he) {
		t.Fatalf("期望 *HTTPError, got %T: %v", err, err)
	}
	if he.StatusCode != http.StatusForbidden {
		t.Fatalf("StatusCode = %d, 期望 403", he.StatusCode)
	}
	if !bytes.Equal(he.Raw, errorBody) {
		t.Fatalf("Raw 应原样交付错误体: %s", he.Raw)
	}
}

// TestResponses401Fatal：401 判死码 → *AuthPermanentlyRevokedError 透传
// （不重试，Fatal 态后不再发请求）。
func TestResponses401Fatal(t *testing.T) {
	var reqs int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reqs++
		w.WriteHeader(http.StatusUnauthorized)
		_, _ = w.Write([]byte(`{"error":{"code":"token_revoked"}}`))
	}))
	t.Cleanup(srv.Close)

	auth := OAuthWithRotation("rt-0", WithInitialAccessToken("at-old"))
	hc := NewHTTPClient(auth, WithTransport(newFixedTransport(t, "https://chatgpt.com/backend-api/codex/responses", srv.URL)))
	_, err := hc.Responses(context.Background(), []byte(`{}`))
	var are *AuthPermanentlyRevokedError
	if !errors.As(err, &are) {
		t.Fatalf("期望 *AuthPermanentlyRevokedError 透传, got %T: %v", err, err)
	}
	if are.Code != "token_revoked" {
		t.Fatalf("Code = %q, 期望 token_revoked", are.Code)
	}
	if reqs != 1 {
		t.Fatalf("判死不重试, 请求数 = %d", reqs)
	}
	// Fatal 态：后续调用不再发请求
	_, err = hc.Responses(context.Background(), []byte(`{}`))
	if !errors.As(err, &are) {
		t.Fatalf("Fatal 态后应恒报错, got %T: %v", err, err)
	}
	if reqs != 1 {
		t.Fatalf("Fatal 态后不应发请求, reqs = %d", reqs)
	}
}

// TestResponses401Rotate：非判死 401 → 单飞 refresh → 自动重试一次成功
// （轮转后聚合正常）。
func TestResponses401Rotate(t *testing.T) {
	m := newMockRefresh(t, refreshStep{status: 200, body: `{"access_token":"at-new","refresh_token":"rt-new"}`})
	var mu sync.Mutex
	var auths []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		auths = append(auths, r.Header.Get("Authorization"))
		mu.Unlock()
		if r.Header.Get("Authorization") == "Bearer at-old" {
			w.WriteHeader(http.StatusUnauthorized)
			_, _ = w.Write([]byte(`{"error":{"type":"invalid_request_error"}}`))
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		f := w.(http.Flusher)
		for _, ev := range []string{respCreatedEvent, respItemMsgDoneEv, respCompletedEv} {
			_, _ = io.WriteString(w, "data: "+ev+"\n\n")
			f.Flush()
		}
		_, _ = io.WriteString(w, "data: [DONE]\n\n")
		f.Flush()
	}))
	t.Cleanup(srv.Close)

	auth := OAuthWithRotation("rt-0", WithInitialAccessToken("at-old"))
	hc := NewHTTPClient(auth, WithTransport(newFixedTransport(t, "https://chatgpt.com/backend-api/codex/responses", srv.URL)))
	resp, err := hc.Responses(context.Background(), []byte(`{"model":"m"}`))
	if err != nil {
		t.Fatalf("Responses: %v", err)
	}
	if gjson.GetBytes(resp.Raw, "id").String() != "resp_001" {
		t.Fatalf("轮转后应成功聚合, raw = %s", resp.Raw)
	}
	if m.callCount() != 1 {
		t.Fatalf("refresh 次数 = %d, 期望 1", m.callCount())
	}
	mu.Lock()
	defer mu.Unlock()
	if len(auths) != 2 || auths[0] != "Bearer at-old" || auths[1] != "Bearer at-new" {
		t.Fatalf("请求序列 = %v, 期望 [Bearer at-old, Bearer at-new]", auths)
	}
}

// TestResponsesConnectionReuse：Responses（合成非流式——内部走 Stream）[DONE]
// 后排空 body → 第二轮请求复用同一连接（countingListener 断言 accepts==1）。
// Stream 排空修复自动覆盖非流式路径的连接复用（残余未读使连接不可回池）。
func TestResponsesConnectionReuse(t *testing.T) {
	var accepts atomic.Int32
	var reqs atomic.Int32
	base, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reqs.Add(1)
		w.Header().Set("Content-Type", "text/event-stream")
		f := w.(http.Flusher)
		for _, ev := range []string{respCreatedEvent, respItemMsgDoneEv, respCompletedEv} {
			_, _ = io.WriteString(w, "data: "+ev+"\n\n")
			f.Flush()
		}
		_, _ = io.WriteString(w, "data: [DONE]\n\n")
		_, _ = io.WriteString(w, "trailing-bytes-after-done") // [DONE] 后残余字节
		f.Flush()
	}))
	srv.Listener = &countingListener{Listener: base, accepts: &accepts}
	srv.Start()
	t.Cleanup(srv.Close)

	hc := NewHTTPClient(PAT("p"), WithTransport(newFixedTransport(t, "https://chatgpt.com/backend-api/codex/responses", srv.URL)))
	for i := 0; i < 2; i++ {
		resp, err := hc.Responses(context.Background(), []byte(`{"model":"m"}`))
		if err != nil {
			t.Fatalf("Responses #%d: %v", i+1, err)
		}
		if gjson.GetBytes(resp.Raw, "id").String() != "resp_001" {
			t.Fatalf("合成体 id = %s", gjson.GetBytes(resp.Raw, "id").String())
		}
	}
	if n := reqs.Load(); n != 2 {
		t.Fatalf("服务端请求数 = %d, 期望 2（服务端收到第二轮请求 = 排空完成信号）", n)
	}
	if n := accepts.Load(); n != 1 {
		t.Fatalf("TCP 连接数 = %d, 期望 1（Responses 内部 Stream 排空 → 连接回池复用）", n)
	}
}

// TestResponsesClientMetadataWire：Responses（非流式聚合路径，内部走 Stream
// 统一注入点）——配置 meta + session → wire 请求体带恒 4 key（meta 优先于
// session）+ 自动 turn_id（UUIDv7）+ stream:true 注入共存。
func TestResponsesClientMetadataWire(t *testing.T) {
	var cap responsesWireCapture
	srv := startResponsesMock(t, nil,
		[]string{respCreatedEvent, respItemMsgDoneEv, respCompletedEv}, true, &cap)

	hc := NewHTTPClient(PAT("p"), WithTransport(newFixedTransport(t, "https://chatgpt.com/backend-api/codex/responses", srv)),
		WithCodexMeta(CodexMeta{
			InstallationID: "inst-1",
			SessionID:      "sess-1",
			ThreadID:       "thread-1",
			WindowID:       "win-1:0",
		}),
		WithSession(Session{SessionID: "sess-other", ThreadID: "thread-other", WindowID: "win-other:1"}))
	resp, err := hc.Responses(context.Background(), []byte(`{"model":"gpt-5.6"}`))
	if err != nil {
		t.Fatalf("Responses: %v", err)
	}
	if gjson.GetBytes(resp.Raw, "id").String() != "resp_001" {
		t.Fatalf("合成体 id = %s", resp.Raw)
	}

	cap.mu.Lock()
	defer cap.mu.Unlock()
	if !gjson.GetBytes(cap.body, "stream").Bool() {
		t.Fatalf("stream:true 注入应保留, body = %s", cap.body)
	}
	cm := gjson.GetBytes(cap.body, "client_metadata")
	if !cm.Exists() || len(cm.Map()) != 5 {
		t.Fatalf("client_metadata = %s（期望恒 4 key + turn_id）", cm.Raw)
	}
	for key, want := range map[string]string{
		"x-codex-installation-id": "inst-1",
		"session_id":              "sess-1",
		"thread_id":               "thread-1",
		"x-codex-window-id":       "win-1:0",
	} {
		if got := cm.Get(key).String(); got != want {
			t.Fatalf("%s = %q, 期望 %q（CodexMeta 优先于 WithSession）", key, got, want)
		}
	}
	if turn := cm.Get("turn_id").String(); !uuidv7Re.MatchString(turn) {
		t.Fatalf("turn_id = %q, 期望 UUIDv7 格式（无静态值自动生成）", turn)
	}
	if gjson.GetBytes(cap.body, "model").String() != "gpt-5.6" {
		t.Fatalf("注入不应动其余字段: %s", cap.body)
	}
}

// TestResponsesNilAuth：auth 为 nil 直接报错（不发出请求）。
func TestResponsesNilAuth(t *testing.T) {
	hc := NewHTTPClient(nil, WithTransport(newFixedTransport(t, "https://chatgpt.com/backend-api/codex/responses", "http://127.0.0.1:1")))
	if _, err := hc.Responses(context.Background(), []byte(`{}`)); err == nil {
		t.Fatal("auth 为 nil 应报错")
	}
}
