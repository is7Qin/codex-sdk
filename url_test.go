package codexsdk

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/url"
	"sync"
	"testing"
)

// captureTransport 记录请求 URL 与 Authorization 头，返回固定 200。
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

// TestURLDefaultResponsesEndpoint：无 WithBaseURL 时请求内置默认端点
// （完整 responses 端点直用，不再拼 /responses）。
func TestURLDefaultResponsesEndpoint(t *testing.T) {
	ct := &captureTransport{}
	hc := NewHTTPClient(PAT("t"), WithTransport(ct))
	if _, err := hc.Do(context.Background(), []byte(`{}`)); err != nil {
		t.Fatalf("Do: %v", err)
	}
	got, _ := ct.snapshot()
	if got != DefaultResponsesURL {
		t.Fatalf("请求 URL = %q, 期望内置默认 %q", got, DefaultResponsesURL)
	}
	if DefaultResponsesURL != "https://chatgpt.com/backend-api/codex/responses" {
		t.Fatalf("DefaultResponsesURL 应为完整 responses 端点, got %q", DefaultResponsesURL)
	}
}

// TestURLWithQueryHTTP：WithQuery 追加到 HTTP 请求 URL；
// base URL 自带 query 时追加而非覆盖（多值追加）。
func TestURLWithQueryHTTP(t *testing.T) {
	ct := &captureTransport{}
	hc := NewHTTPClient(PAT("t"), WithTransport(ct),
		WithQuery("beta", "x"), WithQuery("a", "1"))
	if _, err := hc.Do(context.Background(), []byte(`{}`)); err != nil {
		t.Fatalf("Do: %v", err)
	}
	got, _ := ct.snapshot()
	want := "https://chatgpt.com/backend-api/codex/responses?a=1&beta=x" // url.Values.Encode 按键排序
	if got != want {
		t.Fatalf("请求 URL = %q, 期望 %q", got, want)
	}

	// base 自带 query + WithQuery 追加
	ct2 := &captureTransport{}
	hc2 := NewHTTPClient(PAT("t"), WithTransport(ct2),
		WithBaseURL("https://selfhost.example/v1/responses?t=1"), WithQuery("b", "2"))
	if _, err := hc2.Do(context.Background(), []byte(`{}`)); err != nil {
		t.Fatalf("Do: %v", err)
	}
	got2, _ := ct2.snapshot()
	want2 := "https://selfhost.example/v1/responses?b=2&t=1"
	if got2 != want2 {
		t.Fatalf("请求 URL = %q, 期望 %q（base query 保留 + WithQuery 追加）", got2, want2)
	}
}

// TestWSURLDerivation：WS 端点由 HTTP 端点派生——scheme 替换
// （http→ws / https→wss；ws/wss 原样），path/query 保留 + WithQuery 追加。
func TestWSURLDerivation(t *testing.T) {
	cases := []struct {
		name  string
		base  string
		query map[string]string
		want  string
	}{
		{"https 默认端点", "https://chatgpt.com/backend-api/codex/responses", nil,
			"wss://chatgpt.com/backend-api/codex/responses"},
		{"http 派生 + query 保留", "http://host:8080/v1/responses?beta=old", map[string]string{"beta": "new"},
			"ws://host:8080/v1/responses?beta=old&beta=new"},
		{"ws 原样", "ws://host/x", nil, "ws://host/x"},
		{"wss 原样", "wss://host/x", nil, "wss://host/x"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var q url.Values
			for k, v := range tc.query {
				if q == nil {
					q = make(url.Values)
				}
				q.Add(k, v)
			}
			got, err := wsURLOf(tc.base, q)
			if err != nil {
				t.Fatalf("wsURLOf: %v", err)
			}
			if got != tc.want {
				t.Fatalf("派生 URL = %q, 期望 %q", got, tc.want)
			}
		})
	}
	// 不支持 scheme 报错
	if _, err := wsURLOf("ftp://host/x", nil); err == nil {
		t.Fatal("ftp scheme 应报错")
	}
	// 非法 URL 报错
	if _, err := wsURLOf("://bad", nil); err == nil {
		t.Fatal("非法 URL 应报错")
	}
}

// TestWSDialDerivedURL：Dial 端到端——默认端点派生 wss 升级（coderws 实际以
// http/https 发送，断言 host/path/query 一致）；WithQuery 与 base query 拼接。
func TestWSDialDerivedURL(t *testing.T) {
	ct := &captureTransport{}
	_, err := Dial(context.Background(), PAT("t"), WithTransport(ct),
		WithBaseURL("https://example.com/backend-api/codex/responses"), WithQuery("beta", "x"))
	var de *DialError
	if !errors.As(err, &de) {
		t.Fatalf("期望 *DialError（capture transport 非 101）got %T: %v", err, err)
	}
	if de.StatusCode != http.StatusOK {
		t.Fatalf("StatusCode = %d, 期望 200", de.StatusCode)
	}
	got, _ := ct.snapshot()
	// coderws 将 wss 换回 https 发送；断言 host/path/query 由 SDK 派生且完整保留
	want := "https://example.com/backend-api/codex/responses?beta=x"
	if got != want {
		t.Fatalf("升级 URL = %q, 期望 %q", got, want)
	}
}
