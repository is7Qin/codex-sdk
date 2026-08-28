package codexsdk

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/coder/websocket"
)

type captureTransport struct {
	mu   sync.Mutex
	url  string
	auth string
}

func (t *captureTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	t.mu.Lock()
	t.url = req.URL.String()
	t.auth = req.Header.Get("Authorization")
	t.mu.Unlock()
	return &http.Response{
		StatusCode: http.StatusOK,
		Header:     make(http.Header),
		Body:       io.NopCloser(bytes.NewReader([]byte(`{}`))),
		Request:    req,
	}, nil
}

func (t *captureTransport) snapshot() (string, string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.url, t.auth
}

func TestURLDefaultResponsesEndpoint(t *testing.T) {
	ct := &captureTransport{}
	hc := NewHTTPClient(PAT("t"), WithTransport(ct))
	if _, err := hc.Do(context.Background(), []byte(`{}`)); err != nil {
		t.Fatalf("Do: %v", err)
	}
	got, _ := ct.snapshot()
	want := "https://chatgpt.com/backend-api/codex/responses"
	if got != want {
		t.Fatalf("请求 URL = %q, 期望 %q", got, want)
	}
	if DefaultResponsesURL != "https://chatgpt.com/backend-api/codex/responses" {
		t.Fatalf("DefaultResponsesURL = %q", DefaultResponsesURL)
	}
}

func TestWSDefaultURLConstant(t *testing.T) {
	want := "wss://chatgpt.com/backend-api/codex/responses"
	if DefaultResponsesWSURL != want {
		t.Fatalf("DefaultResponsesWSURL = %q, 期望 %q", DefaultResponsesWSURL, want)
	}
}

func TestWSDialFixedURLViaTransport(t *testing.T) {
	srvURL := newWSEchoServer(t, "")
	expectedHTTPS := "https://chatgpt.com/backend-api/codex/responses"
	tr := newFixedTransport(t, expectedHTTPS, srvURL)
	c, err := Dial(context.Background(), PAT("t"), WithTransport(tr))
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer c.Close(StatusGoingAway, "")
	got := tr.capturedURL()
	if got != expectedHTTPS {
		t.Fatalf("升级 URL = %q, 期望 %q", got, expectedHTTPS)
	}
}

func newWSEchoServer(t *testing.T, wantAuth string) string {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
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
					_ = c.Close(ce.Code, "")
				}
				return
			}
			_ = c.Write(r.Context(), typ, data)
		}
	}))
	t.Cleanup(srv.Close)
	return srv.URL
}
