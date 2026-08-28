package codexsdk

import (
	"net/http"
	"net/url"
	"sync"
	"testing"
)

type fixedTransport struct {
	t        *testing.T
	expected string
	srvURL   *url.URL
	mu       sync.Mutex
	captured string
}

func newFixedTransport(t *testing.T, expected, srvBase string) *fixedTransport {
	t.Helper()
	u, err := url.Parse(srvBase)
	if err != nil {
		t.Fatalf("parse srvBase %q: %v", srvBase, err)
	}
	return &fixedTransport{t: t, expected: expected, srvURL: u}
}

func (tr *fixedTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	got := req.URL.String()
	tr.mu.Lock()
	tr.captured = got
	tr.mu.Unlock()
	if got != tr.expected {
		tr.t.Fatalf("transport URL = %q, want %q", got, tr.expected)
	}
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
