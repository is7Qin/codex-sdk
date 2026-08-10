package codexsdk

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"
)

// echoState 记录 echo server 观测到的连接事件。
type echoState struct {
	mu           sync.Mutex
	authHeader   string
	betaHeader   string
	customHeader string
	texts        [][]byte
	binaryCount  int
	closeCode    websocket.StatusCode
	closeReason  string
}

// startEchoServer 起本地 WS echo server（收什么回什么）。
// wantAuth 非空时校验 Authorization 头，不匹配返回 401。
func startEchoServer(t *testing.T, wantAuth string) (string, *echoState) {
	t.Helper()
	st := &echoState{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		st.mu.Lock()
		st.authHeader = r.Header.Get("Authorization")
		st.betaHeader = r.Header.Get("OpenAI-Beta")
		st.customHeader = r.Header.Get("x-custom")
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

// TestDialPATAuthAndRoundtrip：PAT 鉴权头注入 + 字节 roundtrip。
func TestDialPATAuthAndRoundtrip(t *testing.T) {
	url, st := startEchoServer(t, "")
	c, err := Dial(context.Background(), url, PAT("test-pat"))
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer c.Close(StatusGoingAway, "")

	waitFor(t, func() bool {
		st.mu.Lock()
		defer st.mu.Unlock()
		return st.authHeader == "Bearer test-pat"
	})

	event := []byte(`{"type":"response.created","id":"evt_1"}`)
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
	_, err := Dial(context.Background(), url, PAT("wrong"))
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
	c1, err := Dial(context.Background(), url, OAuth(provider))
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

	c2, err := Dial(context.Background(), url, OAuth(provider))
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
	_, err := Dial(context.Background(), url, OAuth(func(ctx context.Context) (string, error) {
		return "", errors.New("token expired")
	}))
	if err == nil || !strings.Contains(err.Error(), "token expired") {
		t.Fatalf("期望鉴权错误透传, got %v", err)
	}
}

// TestDialNilAuth：auth 为 nil 直接报错。
func TestDialNilAuth(t *testing.T) {
	url, _ := startEchoServer(t, "")
	if _, err := Dial(context.Background(), url, nil); err == nil {
		t.Fatal("auth 为 nil 应报错")
	}
}

// TestBetaAndCustomHeaders：OpenAI-Beta 与自定义头注入升级请求。
func TestBetaAndCustomHeaders(t *testing.T) {
	url, st := startEchoServer(t, "")
	c, err := Dial(context.Background(), url, PAT("t"),
		WithBeta("2026-02-06"), WithHeader("x-custom", "abc"))
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer c.Close(StatusGoingAway, "")
	waitFor(t, func() bool {
		st.mu.Lock()
		defer st.mu.Unlock()
		return st.betaHeader == "responses_websockets=2026-02-06" && st.customHeader == "abc"
	})
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

// TestPing：WS 层 ping 有 pong 应答（常驻读循环处理 pong），ping 后连接仍可用。
func TestPing(t *testing.T) {
	url, _ := startEchoServer(t, "")
	c, err := Dial(context.Background(), url, PAT("t"))
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
	c, err := Dial(context.Background(), url, PAT("t"), WithPingInterval(50*time.Millisecond))
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

// TestReadLimit：单帧超 ReadLimit 返回 ErrMessageTooBig。
func TestReadLimit(t *testing.T) {
	url, _ := startEchoServer(t, "")
	c, err := Dial(context.Background(), url, PAT("t"), WithReadLimit(8))
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
	c, err := Dial(context.Background(), url, PAT("t"))
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

	c, err := Dial(context.Background(), srv.URL, PAT("t"))
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
