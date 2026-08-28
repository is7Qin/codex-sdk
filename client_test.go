package codexsdk

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/tidwall/gjson"
)

// echoState 记录 echo server 观测到的连接事件。
type echoState struct {
	mu              sync.Mutex
	authHeader      string
	userAgent       string
	originator      string
	betaHeader      string
	customHeader    string
	traceparent     string
	tracestate      string
	sessionID       string
	threadID        string
	clientRequestID string
	windowID        string
	texts           [][]byte
	binaryCount     int
	closeCode       websocket.StatusCode
	closeReason     string
}

// startEchoServer 起本地 WS echo server（收什么回什么）。
// wantAuth 非空时校验 Authorization 头，不匹配返回 401。
func startEchoServer(t *testing.T, wantAuth string) (string, *echoState) {
	t.Helper()
	st := &echoState{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		st.mu.Lock()
		st.authHeader = r.Header.Get("Authorization")
		st.userAgent = r.Header.Get("User-Agent")
		st.originator = r.Header.Get("Originator")
		st.betaHeader = r.Header.Get("OpenAI-Beta")
		st.customHeader = r.Header.Get("x-custom")
		st.traceparent = r.Header.Get("traceparent")
		st.tracestate = r.Header.Get("tracestate")
		st.sessionID = r.Header.Get(HeaderSessionID)
		st.threadID = r.Header.Get(HeaderThreadID)
		st.clientRequestID = r.Header.Get(HeaderClientRequestID)
		st.windowID = r.Header.Get(HeaderWindowID)
		st.mu.Unlock()
		if wantAuth != "" && r.Header.Get("Authorization") != wantAuth {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		c, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer c.CloseNow()
		for {
			typ, data, err := c.Read(r.Context())
			if err != nil {
				var ce websocket.CloseError
				if errors.As(err, &ce) {
					st.mu.Lock()
					st.closeCode = ce.Code
					st.closeReason = ce.Reason
					st.mu.Unlock()
					// 回关闭帧完成握手，让客户端 Close 立即返回。
					_ = c.Close(ce.Code, "")
				}
				return
			}
			st.mu.Lock()
			if typ == websocket.MessageText {
				st.texts = append(st.texts, append([]byte(nil), data...))
			} else {
				st.binaryCount++
			}
			st.mu.Unlock()
			if err := c.Write(r.Context(), typ, data); err != nil {
				return
			}
		}
	}))
	t.Cleanup(srv.Close)
	return srv.URL, st
}

func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("条件等待超时")
}

// startReader 启动常驻读循环（网关透传编排的标准形态）：
// 处理 pong/close 等控制帧并把数据帧送入 chan。
func startReader(t *testing.T, c *Client) chan []byte {
	t.Helper()
	frames := make(chan []byte, 8)
	go func() {
		for {
			data, err := c.Recv(context.Background())
			if err != nil {
				return
			}
			frames <- data
		}
	}()
	// 不主动关闭 chan：连接关闭后读循环自行退出，缓冲 chan 随测试结束被回收。
	return frames
}

// TestDialPATAuthAndRoundtrip：PAT 鉴权头注入 + 字节 roundtrip
// （关闭伪装层，纯传输路径字节全等断言）。
func TestDialPATAuthAndRoundtrip(t *testing.T) {
	url, st := startEchoServer(t, "")
	c, err := Dial(context.Background(), PAT("test-pat"), WithTransport(newFixedTransport(t, "https://chatgpt.com/backend-api/codex/responses", url)),
		WithPayloadFiltering(false), WithTraceAuto(false), WithTurnAuto(false))
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer c.Close(StatusGoingAway, "")

	waitFor(t, func() bool {
		st.mu.Lock()
		defer st.mu.Unlock()
		return st.authHeader == "Bearer test-pat"
	})

	event := []byte(`{"type":"response.create","model":"gpt-5","input":"hi"}`)
	if err := c.Send(context.Background(), event); err != nil {
		t.Fatalf("Send: %v", err)
	}
	data, err := c.Recv(context.Background())
	if err != nil {
		t.Fatalf("Recv: %v", err)
	}
	if !bytes.Equal(data, event) {
		t.Fatalf("echo 不一致: %s", data)
	}
}

// TestDialRejectsBadAuth：升级 401 时返回 DialError（携带状态码）。
func TestDialRejectsBadAuth(t *testing.T) {
	url, _ := startEchoServer(t, "Bearer correct")
	_, err := Dial(context.Background(), PAT("wrong"), WithTransport(newFixedTransport(t, "https://chatgpt.com/backend-api/codex/responses", url)))
	var de *DialError
	if !errors.As(err, &de) {
		t.Fatalf("期望 *DialError, got %T: %v", err, err)
	}
	if de.StatusCode != http.StatusUnauthorized {
		t.Fatalf("StatusCode = %d, 期望 401", de.StatusCode)
	}
}

// TestOAuthProviderCalledPerDial：provider 每次 Dial 恰好调用一次并注入头。
func TestOAuthProviderCalledPerDial(t *testing.T) {
	url, st := startEchoServer(t, "Bearer oauth-1")
	var calls atomic.Int32
	provider := func(ctx context.Context) (string, error) {
		calls.Add(1)
		return "oauth-1", nil
	}
	c1, err := Dial(context.Background(), OAuth(provider), WithTransport(newFixedTransport(t, "https://chatgpt.com/backend-api/codex/responses", url)))
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer c1.Close(StatusGoingAway, "")
	waitFor(t, func() bool {
		st.mu.Lock()
		defer st.mu.Unlock()
		return st.authHeader == "Bearer oauth-1"
	})
	if calls.Load() != 1 {
		t.Fatalf("provider 调用次数 = %d, 期望 1", calls.Load())
	}

	c2, err := Dial(context.Background(), OAuth(provider), WithTransport(newFixedTransport(t, "https://chatgpt.com/backend-api/codex/responses", url)))
	if err != nil {
		t.Fatalf("第二次 Dial: %v", err)
	}
	defer c2.Close(StatusGoingAway, "")
	if calls.Load() != 2 {
		t.Fatalf("provider 调用次数 = %d, 期望 2（每次 Dial 取一次）", calls.Load())
	}
}

// TestOAuthProviderErrorAbortsDial：provider 错误透传且不发起升级。
func TestOAuthProviderErrorAbortsDial(t *testing.T) {
	url, _ := startEchoServer(t, "")
	_, err := Dial(context.Background(), OAuth(func(ctx context.Context) (string, error) {
		return "", errors.New("token expired")
	}), WithTransport(newFixedTransport(t, "https://chatgpt.com/backend-api/codex/responses", url)))
	if err == nil || !strings.Contains(err.Error(), "token expired") {
		t.Fatalf("期望鉴权错误透传, got %v", err)
	}
}

// TestDialNilAuth：auth 为 nil 直接报错。
func TestDialNilAuth(t *testing.T) {
	url, _ := startEchoServer(t, "")
	if _, err := Dial(context.Background(), nil, WithTransport(newFixedTransport(t, "https://chatgpt.com/backend-api/codex/responses", url))); err == nil {
		t.Fatal("auth 为 nil 应报错")
	}
}

// TestWSDefaultHeadersAndOverride：默认 codex UA / originator / beta 头注入，
// WithHeader / WithBeta 可覆盖。
func TestWSDefaultHeadersAndOverride(t *testing.T) {
	url, st := startEchoServer(t, "")
	c, err := Dial(context.Background(), PAT("t"), WithTransport(newFixedTransport(t, "https://chatgpt.com/backend-api/codex/responses", url)))
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer c.Close(StatusGoingAway, "")
	waitFor(t, func() bool {
		st.mu.Lock()
		defer st.mu.Unlock()
		return st.userAgent == DefaultCodexUserAgent &&
			st.originator == DefaultOriginator &&
			st.betaHeader == "responses_websockets="+DefaultBetaWS
	})

	// 覆盖：自定义 UA + beta 任意值（现役唯一真实值 DefaultBetaWS=2026-02-06）
	url2, st2 := startEchoServer(t, "")
	c2, err := Dial(context.Background(), PAT("t"), WithTransport(newFixedTransport(t, "https://chatgpt.com/backend-api/codex/responses", url2)),
		WithHeader("User-Agent", "custom-ua"), WithBeta("2026-02-04"))
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer c2.Close(StatusGoingAway, "")
	waitFor(t, func() bool {
		st2.mu.Lock()
		defer st2.mu.Unlock()
		return st2.userAgent == "custom-ua" && st2.betaHeader == "responses_websockets=2026-02-04"
	})
}

// TestBetaAndCustomHeaders：OpenAI-Beta 与自定义头注入升级请求。
func TestBetaAndCustomHeaders(t *testing.T) {
	url, st := startEchoServer(t, "")
	c, err := Dial(context.Background(), PAT("t"), WithTransport(newFixedTransport(t, "https://chatgpt.com/backend-api/codex/responses", url)),
		WithBeta("2026-02-04"), WithHeader("x-custom", "abc"))
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer c.Close(StatusGoingAway, "")
	waitFor(t, func() bool {
		st.mu.Lock()
		defer st.mu.Unlock()
		return st.betaHeader == "responses_websockets=2026-02-04" && st.customHeader == "abc"
	})
}

// TestPing：WS 层 ping 有 pong 应答（常驻读循环处理 pong），ping 后连接仍可用。
func TestPing(t *testing.T) {
	url, _ := startEchoServer(t, "")
	c, err := Dial(context.Background(), PAT("t"), WithTransport(newFixedTransport(t, "https://chatgpt.com/backend-api/codex/responses", url)),
		WithPayloadFiltering(false), WithTraceAuto(false), WithTurnAuto(false))
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer c.Close(StatusGoingAway, "")
	frames := startReader(t, c)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := c.Ping(ctx); err != nil {
		t.Fatalf("Ping: %v", err)
	}
	if err := c.Send(ctx, []byte(`{"type":"ping"}`)); err != nil {
		t.Fatalf("ping 后 Send: %v", err)
	}
	select {
	case got := <-frames:
		if string(got) != `{"type":"ping"}` {
			t.Fatalf("echo = %s", got)
		}
	case <-ctx.Done():
		t.Fatal("ping 后未收到 echo")
	}
}

// TestHeartbeatKeepalive：短间隔心跳下连接保持存活（常驻读循环处理 pong）。
func TestHeartbeatKeepalive(t *testing.T) {
	url, _ := startEchoServer(t, "")
	c, err := Dial(context.Background(), PAT("t"), WithTransport(newFixedTransport(t, "https://chatgpt.com/backend-api/codex/responses", url)), WithPingInterval(50*time.Millisecond))
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer c.Close(StatusGoingAway, "")
	startReader(t, c)

	time.Sleep(250 * time.Millisecond) // 覆盖多个心跳周期
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := c.Ping(ctx); err != nil {
		t.Fatalf("心跳期间连接应存活: %v", err)
	}
}

// TestHeartbeatDeathPath：心跳失败（对端不响应 ping）→ CloseNow →
// 阻塞中的 Recv 返回错误。
func TestHeartbeatDeathPath(t *testing.T) {
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		// 挂住不读不写：不处理控制帧 → 不响应 ping。
		<-release
		_ = c.CloseNow()
	}))
	t.Cleanup(srv.Close)
	t.Cleanup(func() { close(release) }) // LIFO：先释放 handler，再关 server

	c, err := Dial(context.Background(), PAT("t"), WithTransport(newFixedTransport(t, "https://chatgpt.com/backend-api/codex/responses", srv.URL)), WithPingInterval(50*time.Millisecond))
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer c.Close(StatusGoingAway, "")

	errCh := make(chan error, 1)
	go func() {
		_, err := c.Recv(context.Background())
		errCh <- err
	}()
	select {
	case err := <-errCh:
		if err == nil {
			t.Fatal("心跳死亡后 Recv 应返回错误")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("心跳失败后 Recv 未在时限内返回")
	}
}

// TestReadLimit：单帧超 ReadLimit 返回 ErrMessageTooBig。
func TestReadLimit(t *testing.T) {
	url, _ := startEchoServer(t, "")
	c, err := Dial(context.Background(), PAT("t"), WithTransport(newFixedTransport(t, "https://chatgpt.com/backend-api/codex/responses", url)), WithReadLimit(8))
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer c.Close(StatusGoingAway, "")

	big := bytes.Repeat([]byte("x"), 100)
	if err := c.Send(context.Background(), big); err != nil {
		t.Fatalf("Send: %v", err)
	}
	_, err = c.Recv(context.Background())
	if !errors.Is(err, ErrMessageTooBig) {
		t.Fatalf("Recv 应返回 ErrMessageTooBig, got %v", err)
	}
}

// TestCloseCodePassthrough：关闭码与原因透传 + Close 幂等。
func TestCloseCodePassthrough(t *testing.T) {
	url, st := startEchoServer(t, "")
	c, err := Dial(context.Background(), PAT("t"), WithTransport(newFixedTransport(t, "https://chatgpt.com/backend-api/codex/responses", url)))
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	if err := c.Close(StatusNormalClosure, "done"); err != nil {
		t.Fatalf("Close: %v", err)
	}
	waitFor(t, func() bool {
		st.mu.Lock()
		defer st.mu.Unlock()
		return st.closeCode == StatusNormalClosure && st.closeReason == "done"
	})
	if err := c.Close(StatusNormalClosure, ""); err != nil {
		t.Fatalf("重复 Close 应幂等: %v", err)
	}
}

// TestCloseNow：CloseNow 立即断开且幂等，CloseNow 后 Close 为 no-op。
func TestCloseNow(t *testing.T) {
	url, _ := startEchoServer(t, "")
	c, err := Dial(context.Background(), PAT("t"), WithTransport(newFixedTransport(t, "https://chatgpt.com/backend-api/codex/responses", url)))
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	c.CloseNow()
	c.CloseNow() // 幂等
	if _, err := c.Recv(context.Background()); err == nil {
		t.Fatal("CloseNow 后 Recv 应返回错误")
	}
	if err := c.Close(StatusNormalClosure, ""); err != nil {
		t.Fatalf("CloseNow 后 Close 应幂等: %v", err)
	}
}

// TestBinaryFrameRecv：二进制帧原样透传（Recv 返回帧字节，不区分帧类型）。
func TestBinaryFrameRecv(t *testing.T) {
	payload := []byte{0x00, 0x01, 0xfe, 0xff}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer c.CloseNow()
		_ = c.Write(r.Context(), websocket.MessageBinary, payload)
		for {
			_, _, err := c.Read(r.Context())
			if err != nil {
				var ce websocket.CloseError
				if errors.As(err, &ce) {
					_ = c.Close(ce.Code, "")
				}
				return
			}
		}
	}))
	t.Cleanup(srv.Close)

	c, err := Dial(context.Background(), PAT("t"), WithTransport(newFixedTransport(t, "https://chatgpt.com/backend-api/codex/responses", srv.URL)))
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer c.Close(StatusGoingAway, "")

	got, err := c.Recv(context.Background())
	if err != nil {
		t.Fatalf("Recv: %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("二进制帧内容不一致: %v", got)
	}
}

// TestRecvBufferOwnership：Recv 返回的缓冲归调用方所有（跨次调用有效）。
func TestRecvBufferOwnership(t *testing.T) {
	url, _ := startEchoServer(t, "")
	c, err := Dial(context.Background(), PAT("t"), WithTransport(newFixedTransport(t, "https://chatgpt.com/backend-api/codex/responses", url)),
		WithPayloadFiltering(false), WithTraceAuto(false), WithTurnAuto(false))
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer c.Close(StatusGoingAway, "")

	first := []byte(`{"type":"response.create","model":"gpt-5","input":"a"}`)
	second := []byte(`{"type":"response.create","model":"gpt-5","input":"b"}`)
	if err := c.Send(context.Background(), first); err != nil {
		t.Fatalf("Send: %v", err)
	}
	buf1, err := c.Recv(context.Background())
	if err != nil {
		t.Fatalf("Recv: %v", err)
	}
	if err := c.Send(context.Background(), second); err != nil {
		t.Fatalf("Send: %v", err)
	}
	buf2, err := c.Recv(context.Background())
	if err != nil {
		t.Fatalf("Recv: %v", err)
	}
	if !bytes.Equal(buf1, first) {
		t.Fatalf("第一次 Recv 缓冲被后续 Recv 污染: %s", buf1)
	}
	if !bytes.Equal(buf2, second) {
		t.Fatalf("第二次 Recv 内容不符: %s", buf2)
	}
}

// traceparentRe 是 W3C traceparent 格式（00-32hex-16hex-01）。
var traceparentRe = regexp.MustCompile(`^00-[0-9a-f]{32}-[0-9a-f]{16}-01$`)

// TestWSTraceHeaders：WS 握手不发 trace 头（trace 只进帧内 client_metadata）。
func TestWSTraceHeaders(t *testing.T) {
	url, st := startEchoServer(t, "")
	c, err := Dial(context.Background(), PAT("t"), WithTransport(newFixedTransport(t, "https://chatgpt.com/backend-api/codex/responses", url)))
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer c.Close(StatusGoingAway, "")
	// 等握手请求被服务端处理：发一帧等 echo
	if err := c.Send(context.Background(), []byte(`{"type":"response.create","model":"gpt-5"}`)); err != nil {
		t.Fatalf("Send: %v", err)
	}
	if _, err := c.Recv(context.Background()); err != nil {
		t.Fatalf("Recv: %v", err)
	}
	st.mu.Lock()
	defer st.mu.Unlock()
	if st.traceparent != "" || st.tracestate != "" {
		t.Fatalf("WS 握手不应带 trace 头: %q %q", st.traceparent, st.tracestate)
	}
}

// TestSessionHeaders：WithSession 注入握手头 + 帧内 client_metadata
// （session_id/thread_id/x-codex-window-id）。
func TestSessionHeaders(t *testing.T) {
	url, st := startEchoServer(t, "")
	sess := Session{SessionID: "s-1", ThreadID: "t-1", WindowID: "t-1:0"}
	c, err := Dial(context.Background(), PAT("t"), WithTransport(newFixedTransport(t, "https://chatgpt.com/backend-api/codex/responses", url)), WithSession(sess))
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer c.Close(StatusGoingAway, "")
	waitFor(t, func() bool {
		st.mu.Lock()
		defer st.mu.Unlock()
		return st.sessionID == "s-1" && st.threadID == "t-1" &&
			st.clientRequestID == "t-1" && st.windowID == "t-1:0"
	})

	if err := c.Send(context.Background(), []byte(`{"type":"response.create","model":"gpt-5"}`)); err != nil {
		t.Fatalf("Send: %v", err)
	}
	waitFor(t, func() bool {
		st.mu.Lock()
		defer st.mu.Unlock()
		return len(st.texts) >= 1
	})
	st.mu.Lock()
	got := st.texts[0]
	st.mu.Unlock()
	if v := gjson.GetBytes(got, "client_metadata.session_id").String(); v != "s-1" {
		t.Fatalf("metadata session_id = %q, 期望 s-1", v)
	}
	if v := gjson.GetBytes(got, "client_metadata.thread_id").String(); v != "t-1" {
		t.Fatalf("metadata thread_id = %q, 期望 t-1", v)
	}
	if v := gjson.GetBytes(got, "client_metadata.x-codex-window-id").String(); v != "t-1:0" {
		t.Fatalf("metadata x-codex-window-id = %q, 期望 t-1:0", v)
	}

	// ClientRequestID 显式提供
	url2, st2 := startEchoServer(t, "")
	sess2 := Session{SessionID: "s-2", ThreadID: "t-2", ClientRequestID: "cr-2"}
	c2, err := Dial(context.Background(), PAT("t"), WithTransport(newFixedTransport(t, "https://chatgpt.com/backend-api/codex/responses", url2)), WithSession(sess2))
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer c2.Close(StatusGoingAway, "")
	waitFor(t, func() bool {
		st2.mu.Lock()
		defer st2.mu.Unlock()
		return st2.clientRequestID == "cr-2"
	})
}

// TestWSTurnState：握手响应头 x-codex-turn-state → SDK 缓存 → 帧内回传；
// 显式 SetTurnState 同效；SetTurnState("") 清除（跨轮不得回传）。
// 对齐真实 codex：WS 发送路径在 build_ws_client_metadata 后显式追加 turn-state
// 进 client_metadata（client.rs:1626-1631，帧体携带 :1702-1711）——TurnState
// 非空时帧 metadata 恒带该键，勿反转回"无键"断言。
func TestWSTurnState(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set(HeaderTurnState, "st-1")
		c, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer c.CloseNow()
		for {
			typ, data, err := c.Read(r.Context())
			if err != nil {
				var ce websocket.CloseError
				if errors.As(err, &ce) {
					_ = c.Close(ce.Code, "")
				}
				return
			}
			_ = c.Write(r.Context(), typ, data)
		}
	}))
	t.Cleanup(srv.Close)

	c, err := Dial(context.Background(), PAT("t"), WithTransport(newFixedTransport(t, "https://chatgpt.com/backend-api/codex/responses", srv.URL)))
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer c.Close(StatusGoingAway, "")

	if got := c.TurnState(); got != "st-1" {
		t.Fatalf("TurnState = %q, 期望 st-1（握手响应头自动捕获）", got)
	}
	if err := c.Send(context.Background(), []byte(`{"type":"response.create","model":"gpt-5"}`)); err != nil {
		t.Fatalf("Send: %v", err)
	}
	ev, err := c.Recv(context.Background())
	if err != nil {
		t.Fatalf("Recv: %v", err)
	}
	if v := gjson.GetBytes(ev, "client_metadata.x-codex-turn-state").String(); v != "st-1" {
		t.Fatalf("帧内应回传 x-codex-turn-state=st-1, got %q", v)
	}

	// 显式设置（网关在别处观测到该值时手动回传）：TurnState 非空 → 帧
	// metadata 恒带该键（对齐真实 :1626-1631 追加注入）
	c.SetTurnState("st-manual")
	if err := c.Send(context.Background(), []byte(`{"type":"response.create","model":"gpt-5"}`)); err != nil {
		t.Fatalf("Send: %v", err)
	}
	ev3, err := c.Recv(context.Background())
	if err != nil {
		t.Fatalf("Recv: %v", err)
	}
	if v := gjson.GetBytes(ev3, "client_metadata.x-codex-turn-state").String(); v != "st-manual" {
		t.Fatalf("显式设置后帧内应回传 x-codex-turn-state=st-manual, got %q", v)
	}

	// 轮结束清除：不再回传
	c.SetTurnState("")
	if err := c.Send(context.Background(), []byte(`{"type":"response.create","model":"gpt-5"}`)); err != nil {
		t.Fatalf("Send: %v", err)
	}
	ev2, err := c.Recv(context.Background())
	if err != nil {
		t.Fatalf("Recv: %v", err)
	}
	if gjson.GetBytes(ev2, "client_metadata.x-codex-turn-state").Exists() {
		t.Fatal("清除后不应回传 x-codex-turn-state")
	}
}
