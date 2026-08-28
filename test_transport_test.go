package codexsdk

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"
)

type fixedTransport struct {
	expected string
	srvURL   *url.URL
	mu       sync.Mutex
	captured string
	mismatch string
}

func newFixedTransport(t *testing.T, expected, srvBase string) *fixedTransport {
	t.Helper()
	u, err := url.Parse(srvBase)
	if err != nil {
		t.Fatalf("parse srvBase %q: %v", srvBase, err)
	}
	return &fixedTransport{expected: expected, srvURL: u}
}

func (tr *fixedTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	got := req.URL.String()
	tr.mu.Lock()
	tr.captured = got
	if got != tr.expected {
		tr.mismatch = fmt.Sprintf("transport URL = %q, want %q", got, tr.expected)
		msg := tr.mismatch
		tr.mu.Unlock()
		return nil, fmt.Errorf("codexsdk test: %s", msg)
	}
	tr.mu.Unlock()
	clone := req.Clone(req.Context())
	u2 := *req.URL
	u2.Scheme = tr.srvURL.Scheme
	u2.Host = tr.srvURL.Host
	clone.URL = &u2
	return http.DefaultTransport.RoundTrip(clone)
}

func (tr *fixedTransport) capturedURL() string {
	tr.mu.Lock()
	defer tr.mu.Unlock()
	return tr.captured
}

func (tr *fixedTransport) mismatchString() string {
	tr.mu.Lock()
	defer tr.mu.Unlock()
	return tr.mismatch
}

func (tr *fixedTransport) assertNoMismatch(t *testing.T) {
	t.Helper()
	if s := tr.mismatchString(); s != "" {
		t.Fatalf("unexpected transport mismatch: %s", s)
	}
}

func TestFixedTransportMismatchConcurrentNoHang(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{}`))
	}))
	t.Cleanup(srv.Close)
	tr := newFixedTransport(t, "https://chatgpt.com/backend-api/codex/alpha/searchWRONG", srv.URL)
	const n = 16
	errs := make([]error, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			hc := NewHTTPClient(PAT("pat-search"), WithTransport(tr))
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			_, err := hc.Search(ctx, []byte(`{}`))
			errs[idx] = err
		}(i)
	}
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("concurrent mismatch did not return boundedly (hang)")
	}
	for i, err := range errs {
		if err == nil {
			t.Fatalf("goroutine %d: expected mismatch error, got nil (captured=%q mismatch=%q)", i, tr.capturedURL(), tr.mismatchString())
		}
		if !strings.Contains(err.Error(), "transport URL") {
			t.Fatalf("goroutine %d: error should contain mismatch, got %v", i, err)
		}
	}
	if got := tr.mismatchString(); got == "" {
		t.Fatal("expected mismatch recorded")
	}
}
