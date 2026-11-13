package codexsdk

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	coderws "github.com/coder/websocket"
	"github.com/tidwall/gjson"
)

// wireCapture 包住一个 RoundTripper，记录 SDK 交给传输层的原始请求头（键的
// 大小写原样）与请求体——HTTP/1.1 上写出的头名就是这些键。
type wireCapture struct {
	inner http.RoundTripper
	mu    sync.Mutex
	reqs  []capturedReq
}

type capturedReq struct {
	url    string
	header http.Header
	body   []byte
}

func (c *wireCapture) RoundTrip(req *http.Request) (*http.Response, error) {
	var body []byte
	if req.Body != nil {
		body, _ = io.ReadAll(req.Body)
		req.Body = io.NopCloser(bytes.NewReader(body))
	}
	c.mu.Lock()
	c.reqs = append(c.reqs, capturedReq{url: req.URL.String(), header: req.Header.Clone(), body: body})
	c.mu.Unlock()
	return c.inner.RoundTrip(req)
}

func (c *wireCapture) all() []capturedReq {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]capturedReq(nil), c.reqs...)
}

func headerKeys(h http.Header) []string {
	var ks []string
	for k := range h {
		ks = append(ks, k)
	}
	sort.Strings(ks)
	return ks
}

// officialHeaderKeys 取抓包头的键集合（去掉 Go / 传输层自己写的那些）。
func officialHeaderKeys(h map[string]string, drop ...string) []string {
	skip := map[string]bool{}
	for _, d := range drop {
		skip[strings.ToLower(d)] = true
	}
	var ks []string
	for k := range h {
		if skip[strings.ToLower(k)] {
			continue
		}
		if strings.EqualFold(k, "user-agent") {
			k = "User-Agent"
		}
		ks = append(ks, k)
	}
	sort.Strings(ks)
	return ks
}

func sseOK(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "text/event-stream")
	_, _ = io.WriteString(w, "data: {\"type\":\"response.completed\"}\n\ndata: [DONE]\n\n")
}

// TestHTTPResponsesHeadersAndZstd：/responses 的头全表（小写名、accept、无
// SDK 设置的 accept-encoding）；zstd 三档大小帧头均为 28b52ffd0058 且可还原；只有
// /responses 压缩；401 轮转重发字节相同。
func TestHTTPResponsesHeadersAndZstd(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { sseOK(w) }))
	t.Cleanup(srv.Close)
	cap := &wireCapture{inner: newFixedTransport(t, DefaultResponsesURL, srv.URL)}
	hc := NewHTTPClient(PAT("pat-h", WithPATAccountID("account-123")), WithTransport(cap))
	noop := func([]byte) error { return nil }

	id := testID()
	if err := hc.Stream(context.Background(), []byte(`{"model":"gpt-6.1-sol","input":[]}`), id, noop); err != nil {
		t.Fatalf("Stream: %v", err)
	}
	req := cap.all()[0]
	h := req.header
	// 头名集合 = 官方 HTTP 抓包（去掉 host / content-length，这两个由 Go 写）。
	official := loadOfficialHeaders(t, "exec-http-followup.headers.json")
	if got, want := strings.Join(headerKeys(h), ","), strings.Join(officialHeaderKeys(official, "host", "content-length"), ","); got != want {
		t.Fatalf("头名集合\n实际 %s\n期望 %s", got, want)
	}
	for k := range h {
		if k != "User-Agent" && k != strings.ToLower(k) {
			t.Fatalf("头名 %q 应为小写", k)
		}
	}
	sh := mustShape(t, []byte(`{"model":"gpt-6.1-sol","input":[]}`), surfaceHTTP, id)
	for k, want := range map[string]string{
		"authorization":                          "Bearer pat-h",
		"chatgpt-account-id":                     "account-123",
		"User-Agent":                             DefaultCodexUserAgent,
		"originator":                             "codex-tui",
		"version":                                CodexVersion,
		"x-codex-beta-features":                  "remote_compaction_v2",
		"x-codex-routing-hint":                   "model=gpt-6.1-sol",
		"x-openai-internal-codex-responses-lite": "true",
		"session-id":                             id.Session.SessionID,
		"thread-id":                              id.Session.ThreadID,
		"x-client-request-id":                    id.Session.ThreadID,
		"x-codex-window-id":                      id.Session.WindowID,
		"x-codex-turn-state":                     "ts-1",
		"x-codex-turn-metadata":                  string(sh.turnMeta),
		"accept":                                 "text/event-stream",
		"content-type":                           "application/json",
		"content-encoding":                       "zstd",
	} {
		if got := firstHeader(h, k); got != want {
			t.Fatalf("%s = %q，期望 %q", k, got, want)
		}
	}
	for _, v := range []string{"version", "x-codex-beta-features", "accept", "content-type", "content-encoding"} { // originator / UA：exec 抓包按 README 替换为 tui 值，上面已断言
		if official[v] != firstHeader(h, v) {
			t.Fatalf("%s = %q，抓包 %q", v, firstHeader(h, v), official[v])
		}
	}
	if _, ok := h["Accept-Encoding"]; ok {
		t.Fatal("SDK 不应设置 Accept-Encoding")
	}
	if _, ok := h["accept-encoding"]; ok {
		t.Fatal("SDK 不应设置 accept-encoding")
	}
	// 请求体里的 turn-metadata 与头里的是同一份字节。
	body, err := decodeZstd(req.body)
	if err != nil {
		t.Fatalf("解压: %v", err)
	}
	if gjson.GetBytes(body, "client_metadata.x-codex-turn-metadata").String() != firstHeader(h, "x-codex-turn-metadata") {
		t.Fatal("请求体与请求头的 turn-metadata 不同")
	}

	// service_tier → routing-hint 带 tier；匿名不发身份头与 turn-metadata。
	_ = hc.Stream(context.Background(), []byte(`{"model":"gpt-5.5","service_tier":"priority"}`), Identity{}, noop)
	h = cap.all()[1].header
	if got := firstHeader(h, "x-codex-routing-hint"); got != "model=gpt-5.5;tier=priority" {
		t.Fatalf("routing-hint = %q", got)
	}
	for _, k := range []string{"session-id", "thread-id", "x-client-request-id", "x-codex-window-id", "x-codex-turn-metadata", "x-codex-turn-state", "x-openai-internal-codex-responses-lite"} {
		if _, ok := h[k]; ok {
			t.Fatalf("匿名 / 非 lite 不应带 %s", k)
		}
	}

	// zstd 三档：<256B、1–128KB、>128KB。
	for _, size := range []int{0, 10 << 10, 200 << 10} {
		payload := []byte(`{"model":"m","input":"` + strings.Repeat("x", size) + `"}`)
		n := len(cap.all())
		if err := hc.Stream(context.Background(), payload, Identity{}, noop); err != nil {
			t.Fatalf("Stream(%d): %v", size, err)
		}
		got := cap.all()[n].body
		sh := mustShape(t, payload, surfaceHTTP, Identity{})
		plain := assembleHTTP(&sh)
		if size == 0 && len(plain) >= 256 || size > 0 && size < 128<<10 && (len(plain) < 1<<10 || len(plain) > 128<<10) || size > 128<<10 && len(plain) <= 128<<10 {
			t.Fatalf("档位构造错误：明文 %d 字节", len(plain))
		}
		if !bytes.HasPrefix(got, []byte{0x28, 0xb5, 0x2f, 0xfd, 0x00, 0x58}) {
			t.Fatalf("明文 %d 字节：帧头 % x，期望 28 b5 2f fd 00 58", len(plain), got[:min(6, len(got))])
		}
		dec, err := decodeZstd(got)
		if err != nil || !bytes.Equal(dec, plain) {
			t.Fatalf("明文 %d 字节：解压不能还原（err=%v）", len(plain), err)
		}
	}
	// 官方原始 zstd 体同样是这个帧头（夹具自检）。
	if raw := loadOfficial(t, "http-body-turn.zst"); !bytes.HasPrefix(raw, []byte{0x28, 0xb5, 0x2f, 0xfd, 0x00, 0x58}) {
		t.Fatalf("官方帧头 % x", raw[:6])
	}

	// Do / Search 不压缩、不带官方形态头。
	n := len(cap.all())
	_, _ = hc.Do(context.Background(), []byte(`{"model":"m"}`))
	if r := cap.all()[n]; firstHeader(r.header, "content-encoding") != "" || string(r.body) != `{"model":"m"}` || firstHeader(r.header, "version") != "" {
		t.Fatalf("Do 不应整形/压缩: %v %s", r.header, r.body)
	}
	scap := &wireCapture{inner: newFixedTransport(t, DefaultSearchURL, srv.URL)}
	shc := NewHTTPClient(PAT("pat-h"), WithTransport(scap))
	_, _ = shc.Search(context.Background(), []byte(`{"q":"x"}`))
	if r := scap.all()[0]; firstHeader(r.header, "content-encoding") != "" || string(r.body) != `{"q":"x"}` {
		t.Fatalf("Search 不应压缩: %v %s", r.header, r.body)
	}

	// 401 轮转重发：同一份已压缩字节、同一组头（authorization 除外）。
	var calls int
	var mu sync.Mutex
	srv401 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		calls++
		first := calls == 1
		mu.Unlock()
		if first {
			w.WriteHeader(http.StatusUnauthorized)
			_, _ = io.WriteString(w, `{"error":{"type":"invalid_request_error"}}`)
			return
		}
		sseOK(w)
	}))
	t.Cleanup(srv401.Close)
	newMockRefresh(t, refreshStep{status: 200, body: `{"access_token":"at-new","refresh_token":"rt-new"}`})
	rcap := &wireCapture{inner: newFixedTransport(t, DefaultResponsesURL, srv401.URL)}
	rhc := NewHTTPClient(OAuthWithRotation("rt-0", WithInitialAccessToken("at-old")), WithTransport(rcap))
	if err := rhc.Stream(context.Background(), []byte(`{"model":"gpt-6.1-sol","input":"retry me"}`), testID(), noop); err != nil {
		t.Fatalf("Stream(401): %v", err)
	}
	rs := rcap.all()
	if len(rs) != 2 || !bytes.Equal(rs[0].body, rs[1].body) {
		t.Fatalf("401 重发应复用同一份字节（%d 次请求）", len(rs))
	}
	for k, v := range rs[0].header {
		if k == "authorization" {
			continue
		}
		if strings.Join(rs[1].header[k], "\x00") != strings.Join(v, "\x00") {
			t.Fatalf("重发头 %s 不同: %q vs %q", k, v, rs[1].header[k])
		}
	}
	if firstHeader(rs[1].header, "authorization") != "Bearer at-new" {
		t.Fatalf("重发应换新 token: %q", firstHeader(rs[1].header, "authorization"))
	}

	// WithHeader 覆盖：两种写法都删，只以小写发一份。
	ocap := &wireCapture{inner: newFixedTransport(t, DefaultResponsesURL, srv.URL)}
	ohc := NewHTTPClient(PAT("p"), WithTransport(ocap), WithHeader("Originator", "codex_exec"), WithHeader("user-agent", "ua-x"))
	_ = ohc.Stream(context.Background(), []byte(`{"model":"m"}`), Identity{}, noop)
	oh := ocap.all()[0].header
	if len(oh["Originator"]) != 0 || strings.Join(oh["originator"], ",") != "codex_exec" || strings.Join(oh["User-Agent"], ",") != "ua-x" || len(oh["user-agent"]) != 0 {
		t.Fatalf("覆盖结果: %v", oh)
	}
}

// sniffListener 记录服务端读到的原始字节（握手请求 + 后续 WS 帧）。
type sniffListener struct {
	net.Listener
	mu  sync.Mutex
	buf bytes.Buffer
}

func (l *sniffListener) Accept() (net.Conn, error) {
	c, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}
	return &sniffConn{Conn: c, l: l}, nil
}

func (l *sniffListener) bytes() []byte {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]byte(nil), l.buf.Bytes()...)
}

type sniffConn struct {
	net.Conn
	l *sniffListener
}

func (c *sniffConn) Read(p []byte) (int, error) {
	n, err := c.Conn.Read(p)
	c.l.mu.Lock()
	c.l.buf.Write(p[:n])
	c.l.mu.Unlock()
	return n, err
}

// TestWSHandshakeHeaders：WS 握手头全表（小写名、permessage-deflate、
// turn-metadata 头），出站帧不压缩（RSV1 未置位）；静态身份 WithFirstFrame 的
// routing-hint 与 turn-metadata 取自首帧，未设置时两头不发。
func TestWSHandshakeHeaders(t *testing.T) {
	var mu sync.Mutex
	var frames [][]byte
	var exts []string
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		exts = append(exts, r.Header.Get("Sec-WebSocket-Extensions"))
		mu.Unlock()
		c, err := coderws.Accept(w, r, &coderws.AcceptOptions{CompressionMode: coderws.CompressionContextTakeover})
		if err != nil {
			return
		}
		defer c.CloseNow()
		for {
			_, data, err := c.Read(r.Context())
			if err != nil {
				return
			}
			mu.Lock()
			frames = append(frames, data)
			mu.Unlock()
			rid := "resp_hs_" + strconv.Itoa(len(frames))
			for _, f := range []string{tsCreatedFrame(rid), tsCompletedFrame(rid)} {
				if c.Write(r.Context(), coderws.MessageText, []byte(f)) != nil {
					return
				}
			}
		}
	})
	srv := httptest.NewUnstartedServer(handler)
	sniff := &sniffListener{Listener: srv.Listener}
	srv.Listener = sniff
	srv.Start()
	t.Cleanup(func() { srv.CloseClientConnections(); srv.Close() })

	// HTTP→WS。
	cap := &wireCapture{inner: newFixedTransport(t, DefaultResponsesURL, srv.URL)}
	hc := NewHTTPClient(PAT("pat-ws", WithPATAccountID("account-123")), WithResponsesTransport(TransportWS), WithTransport(cap))
	id := testID()
	resp, err := hc.StreamBody(context.Background(), []byte(`{"model":"gpt-6.1-sol","input":[]}`), id)
	if err != nil {
		t.Fatalf("StreamBody: %v", err)
	}
	_ = readAllBody(t, resp.Body)
	h := cap.all()[0].header
	official := loadOfficialHeaders(t, "tui-handshake.headers.json")
	// 官方握手头里 Host/Connection/Upgrade/Sec-WebSocket-* 由 WS 库写；其余 SDK 写。
	ours := func(ks []string) []string {
		var out []string
		for _, k := range ks {
			if !strings.HasPrefix(strings.ToLower(k), "sec-websocket") && k != "Connection" && k != "Upgrade" && k != "Host" {
				out = append(out, k)
			}
		}
		return out
	}
	if got, want := strings.Join(ours(headerKeys(h)), ","), strings.Join(ours(officialHeaderKeys(official)), ","); got != want {
		t.Fatalf("握手头名\n实际 %s\n期望 %s", got, want)
	}
	mu.Lock()
	frame0 := frames[0]
	ext0 := exts[0]
	mu.Unlock()
	for k, want := range map[string]string{
		"authorization":         "Bearer pat-ws",
		"User-Agent":            DefaultCodexUserAgent,
		"originator":            "codex-tui",
		"openai-beta":           "responses_websockets=" + DefaultBetaWS,
		"version":               CodexVersion,
		"x-codex-beta-features": "remote_compaction_v2",
		"x-codex-routing-hint":  "model=gpt-6.1-sol",
		"x-codex-turn-metadata": gjson.GetBytes(frame0, "client_metadata.x-codex-turn-metadata").String(),
		"session-id":            id.Session.SessionID,
		"chatgpt-account-id":    "account-123",
	} {
		if got := firstHeader(h, k); got != want {
			t.Fatalf("握手头 %s = %q，期望 %q", k, got, want)
		}
	}
	if !strings.Contains(firstHeader(h, "x-codex-turn-metadata"), `"request_kind":"turn"`) {
		t.Fatal("HTTP→WS 不预热：握手 turn-metadata 应为 turn 形态")
	}
	if ext0 != "permessage-deflate" {
		t.Fatalf("Sec-WebSocket-Extensions = %q，期望 permessage-deflate（缺 client_max_window_bits 是已知偏差）", ext0)
	}
	if _, ok := h[HeaderTurnState]; ok {
		t.Fatal("握手不带 turn-state")
	}
	// 出站帧不压缩：握手请求之后第一个帧头字节 = 0x81（FIN + text，RSV1=0）。
	raw := sniff.bytes()
	i := bytes.Index(raw, []byte("\r\n\r\n"))
	if i < 0 || i+4 >= len(raw) {
		t.Fatal("没有嗅探到握手之后的帧")
	}
	if b := raw[i+4]; b&0x40 != 0 || b != 0x81 {
		t.Fatalf("首帧头字节 %#x：RSV1 应为 0（出站不压缩）", b)
	}

	// WS→WS 静态身份 + WithFirstFrame：握手头取自首帧，首次 Send 复用同一个 turn_id。
	first := []byte(`{"type":"response.create","model":"gpt-5.5","service_tier":"flex","input":[{"role":"user","content":"hi"}]}`)
	dcap := &wireCapture{inner: newFixedTransport(t, DefaultResponsesURL, srv.URL)}
	cid := testID()
	cid.Turn.ID = ""
	c, err := Dial(context.Background(), PAT("pat-ws"), cid, WithTransport(dcap), WithFirstFrame(first))
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer c.CloseNow()
	if err := c.Send(context.Background(), first); err != nil {
		t.Fatalf("Send: %v", err)
	}
	if _, err := c.Recv(context.Background()); err != nil {
		t.Fatalf("Recv: %v", err)
	}
	dh := dcap.all()[0].header
	mu.Lock()
	sent := frames[len(frames)-1]
	mu.Unlock()
	if got := firstHeader(dh, "x-codex-routing-hint"); got != "model=gpt-5.5;tier=flex" {
		t.Fatalf("WS→WS routing-hint = %q", got)
	}
	if got := firstHeader(dh, "x-codex-turn-metadata"); got == "" || got != gjson.GetBytes(sent, "client_metadata.x-codex-turn-metadata").String() {
		t.Fatalf("WS→WS 握手 turn-metadata 应等于首帧的（同一 turn_id）:\n%s\n%s", got, gjson.GetBytes(sent, "client_metadata.x-codex-turn-metadata").String())
	}
	// 首帧是 generate:false 时为 prewarm 形态。
	pre := []byte(`{"type":"response.create","model":"gpt-5.5","input":[],"generate":false}`)
	pcap := &wireCapture{inner: newFixedTransport(t, DefaultResponsesURL, srv.URL)}
	pc, err := Dial(context.Background(), PAT("pat-ws"), testID(), WithTransport(pcap), WithFirstFrame(pre))
	if err != nil {
		t.Fatalf("Dial(prewarm): %v", err)
	}
	pc.CloseNow()
	if got := firstHeader(pcap.all()[0].header, "x-codex-turn-metadata"); !strings.Contains(got, `"request_kind":"prewarm"`) {
		t.Fatalf("预热首帧的握手 turn-metadata 应为 prewarm 形态: %s", got)
	}
	// 未设置 WithFirstFrame：两头不发。
	ncap := &wireCapture{inner: newFixedTransport(t, DefaultResponsesURL, srv.URL)}
	nc, err := Dial(context.Background(), PAT("pat-ws"), testID(), WithTransport(ncap))
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	nc.CloseNow()
	nh := ncap.all()[0].header
	if _, ok := nh["x-codex-routing-hint"]; ok {
		t.Fatal("未设置 WithFirstFrame 时不发 routing-hint")
	}
	if _, ok := nh["x-codex-turn-metadata"]; ok {
		t.Fatal("未设置 WithFirstFrame 时不发 turn-metadata")
	}
	if firstHeader(nh, "version") != CodexVersion || firstHeader(nh, "session-id") == "" {
		t.Fatalf("其余握手头照发: %v", nh)
	}
	// 无效首帧：Dial 本地拒绝。
	if _, err := Dial(context.Background(), PAT("pat-ws"), testID(), WithTransport(ncap), WithFirstFrame([]byte(`{}`))); !errors.Is(err, ErrInvalidRequestBody) {
		t.Fatalf("无效首帧应返回 ErrInvalidRequestBody, got %v", err)
	}
}

// TestTurnStateOnlyFromMetadataEvents：握手响应头的 ts 不进 Sink；属于本次
// response 的 response.metadata 进；WS→WS 静态身份的 Recv 也捕获；HTTP 响应头进。
func TestTurnStateOnlyFromMetadataEvents(t *testing.T) {
	respond := func(i int, _ []byte) []string {
		rid := "resp_ev_" + strconv.Itoa(i)
		return []string{
			tsMetadataFrame(rid, "EARLY"), // created 之前：不认
			tsCreatedFrame(rid),
			tsMetadataFrame("resp_other", "OTHER"), // 别的 response：不认
			tsMetadataFrame(rid, "EV"),
			tsCompletedFrame(rid),
		}
	}
	up := newTSUpstream(t, respond)
	up.setHandshake("HANDSHAKE")

	var sink turnStateSink
	hc := up.client(t)
	doStreamBodyFull(t, hc, identityFor("thread-ev", "thread-ev:0", "", &sink))
	if got := sink.values(); len(got) != 1 || got[0] != "EV" {
		t.Fatalf("HTTP→WS Sink = %v，期望 [EV]", got)
	}

	var dsink turnStateSink
	c, err := Dial(context.Background(), PAT("p"), identityFor("thread-dial-ev", "thread-dial-ev:0", "", &dsink),
		WithTransport(newFixedTransport(t, DefaultResponsesURL, up.srv.URL)))
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer c.CloseNow()
	if got := dsink.values(); len(got) != 0 {
		t.Fatalf("握手响应头的 ts 不应进 Sink: %v", got)
	}
	if err := c.Send(context.Background(), []byte(`{"model":"m","input":[]}`)); err != nil {
		t.Fatalf("Send: %v", err)
	}
	for {
		ev, err := c.Recv(context.Background())
		if err != nil {
			t.Fatalf("Recv: %v", err)
		}
		if gjson.GetBytes(ev, "type").String() == "response.completed" {
			break
		}
	}
	if got := dsink.values(); len(got) != 1 || got[0] != "EV" {
		t.Fatalf("WS→WS Recv Sink = %v，期望 [EV]", got)
	}

	// HTTP→HTTP：响应头。
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set(HeaderTurnState, "HTTP-TS")
		sseOK(w)
	}))
	t.Cleanup(srv.Close)
	var hsink turnStateSink
	h := NewHTTPClient(PAT("p"), WithTransport(newFixedTransport(t, DefaultResponsesURL, srv.URL)))
	if err := h.Stream(context.Background(), []byte(`{"model":"m"}`), identityFor("t-h", "t-h:0", "", &hsink), func([]byte) error { return nil }); err != nil {
		t.Fatalf("Stream: %v", err)
	}
	if got := hsink.values(); len(got) != 1 || got[0] != "HTTP-TS" {
		t.Fatalf("HTTP Sink = %v", got)
	}
}

// sentinelRequest 是带满客户端身份哨兵的请求体。
const sentinelRequest = `{"model":"gpt-6.1-sol","input":[{"role":"user","content":"hi"}],` +
	`"prompt_cache_key":"SENTINEL-PCK","previous_response_id":"SENTINEL-PREV","conversation_id":"SENTINEL-CONV",` +
	`"session_id":"SENTINEL-SID","type":"SENTINEL-TYPE","client_metadata":{"session_id":"SENTINEL-S","thread_id":"SENTINEL-T",` +
	`"turn_id":"SENTINEL-TURN","x-codex-window-id":"SENTINEL-W","x-codex-turn-state":"SENTINEL-TS","x-codex-installation-id":"SENTINEL-I",` +
	`"x-codex-turn-metadata":"{\"session_id\":\"SENTINEL-TM\",\"sandbox\":\"seatbelt\"}","mcp_attribution":"SENTINEL-MCP"}}`

func assertNoSentinel(t *testing.T, label string, parts ...[]byte) {
	t.Helper()
	for _, p := range parts {
		if bytes.Contains(p, []byte("SENTINEL")) {
			t.Fatalf("%s: 哨兵出网: %s", label, p)
		}
	}
}

func headerBytes(h http.Header) []byte {
	var b bytes.Buffer
	_ = h.Write(&b)
	return b.Bytes()
}

// TestClientIdentityValuesNeverEgress：客户端身份值（会话号、线程号、窗口号、
// 轮次号、client_metadata 全部内容、prompt_cache_key、turn-state、对话标识）不出现在
// 出网字节里（头 + 解压后的体；WS 帧 + 握手头）。
func TestClientIdentityValuesNeverEgress(t *testing.T) {
	// HTTP→HTTP。
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { sseOK(w) }))
	t.Cleanup(srv.Close)
	cap := &wireCapture{inner: newFixedTransport(t, DefaultResponsesURL, srv.URL)}
	hc := NewHTTPClient(PAT("p"), WithTransport(cap))
	for _, id := range []Identity{testID(), {}} {
		if err := hc.Stream(context.Background(), []byte(sentinelRequest), id, func([]byte) error { return nil }); err != nil {
			t.Fatalf("Stream: %v", err)
		}
	}
	for _, r := range cap.all() {
		body, err := decodeZstd(r.body)
		if err != nil {
			t.Fatal(err)
		}
		assertNoSentinel(t, "HTTP→HTTP", headerBytes(r.header), body)
	}

	// HTTP→WS 与 WS→WS（静态身份，首帧经 WithFirstFrame 进握手头）。
	up := newTSUpstream(t, func(i int, _ []byte) []string {
		rid := "resp_s_" + strconv.Itoa(i)
		return []string{tsCreatedFrame(rid), tsCompletedFrame(rid)}
	})
	wcap := &wireCapture{inner: newFixedTransport(t, DefaultResponsesURL, up.srv.URL)}
	whc := NewHTTPClient(PAT("p"), WithResponsesTransport(TransportWS), WithTransport(wcap))
	for _, id := range []Identity{testID(), {}} {
		doStreamBodyPayload(t, whc, id, []byte(sentinelRequest))
	}
	native := []byte(strings.Replace(sentinelRequest, `"previous_response_id":"SENTINEL-PREV",`, ``, 1))
	c, err := Dial(context.Background(), PAT("p"), testID(), WithTransport(wcap), WithFirstFrame(native))
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer c.CloseNow()
	for i := 0; i < 2; i++ { // 第一次复用 Dial 的整形，第二次重新整形
		if err := c.Send(context.Background(), native); err != nil {
			t.Fatalf("Send: %v", err)
		}
	}
	up.waitFrames(t, 4)
	for _, r := range wcap.all() {
		assertNoSentinel(t, "握手头", headerBytes(r.header))
	}
	for _, f := range up.sentFrames(t) {
		assertNoSentinel(t, "WS 帧", f)
	}
}

// TestStreamStartStampedAtWrite：x-codex-ws-stream-request-start-ms 在取得
// 写闸之后覆写：值不早于写闸内的时刻、单调、接近写出时刻。
func TestStreamStartStampedAtWrite(t *testing.T) {
	up := newTSUpstream(t, nil)
	c, err := Dial(context.Background(), PAT("p"), testID(), WithTransport(newFixedTransport(t, DefaultResponsesURL, up.srv.URL)))
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer c.CloseNow()
	var gateAt []time.Time
	writeStampHook = func() {
		time.Sleep(40 * time.Millisecond) // 模拟在写闸后、写出前的延迟
		gateAt = append(gateAt, time.Now())
	}
	t.Cleanup(func() { writeStampHook = nil })
	var before []time.Time
	for i := 0; i < 2; i++ {
		before = append(before, time.Now())
		if err := c.Send(context.Background(), []byte(`{"model":"m","input":[]}`)); err != nil {
			t.Fatalf("Send: %v", err)
		}
	}
	writeStampHook = nil
	up.waitFrames(t, 2)
	var prev int64
	for i, f := range up.sentFrames(t) {
		v, err := strconv.ParseInt(gjson.GetBytes(f, "client_metadata.x-codex-ws-stream-request-start-ms").String(), 10, 64)
		if err != nil {
			t.Fatalf("第 %d 帧发送时刻: %v", i, err)
		}
		if v < gateAt[i].UnixMilli() {
			t.Fatalf("第 %d 帧发送时刻 %d 早于写闸内时刻 %d（应在取得写闸后覆写）", i, v, gateAt[i].UnixMilli())
		}
		if v-before[i].UnixMilli() < 40 {
			t.Fatalf("第 %d 帧发送时刻 %d 与组装时刻 %d 相差不足 40ms：没有覆写", i, v, before[i].UnixMilli())
		}
		if v > time.Now().UnixMilli() || v < prev {
			t.Fatalf("发送时刻不单调或超前: %d (prev %d)", v, prev)
		}
		prev = v
	}
}
