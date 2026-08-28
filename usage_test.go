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
)

// TestUsageDefaultURLConst：DefaultUsageURL 常量值断言（无网络）。
func TestUsageDefaultURLConst(t *testing.T) {
	if DefaultUsageURL != "https://chatgpt.com/backend-api/wham/usage" {
		t.Fatalf("DefaultUsageURL 应为完整 usage 端点, got %q", DefaultUsageURL)
	}
}


// TestUsageGetMethodAndAuth：GetUsage 请求形态断言——method=GET + 无请求体 +
// 鉴权头注入（固定官方端点路径 /backend-api/wham/usage 一并断言）。
func TestUsageGetMethodAndAuth(t *testing.T) {
	var gotMethod, gotPath, gotAuth string
	var gotBody []byte
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotMethod = r.Method
		gotPath = r.URL.Path
		gotAuth = r.Header.Get("Authorization")
		gotBody, _ = io.ReadAll(r.Body)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{}`))
	}))
	t.Cleanup(srv.Close)

	hc := NewHTTPClient(PAT("pat-usage"), WithTransport(newFixedTransport(t, "https://chatgpt.com/backend-api/wham/usage", srv.URL)))
	if _, err := hc.GetUsage(context.Background()); err != nil {
		t.Fatalf("GetUsage: %v", err)
	}
	if gotMethod != http.MethodGet {
		t.Fatalf("method = %q, 期望 GET", gotMethod)
	}
	if gotPath != "/backend-api/wham/usage" {
		t.Fatalf("路径 = %q, 期望 /backend-api/wham/usage", gotPath)
	}
	if len(gotBody) != 0 {
		t.Fatalf("GET 应无请求体, got %s", gotBody)
	}
	if gotAuth != "Bearer pat-usage" {
		t.Fatalf("Authorization = %q", gotAuth)
	}
}

// TestUsageDecode：2xx 响应解码断言——全字段形态（含 credits.balance /
// spend_control.individual_limit 有值）+ 未知字段静默忽略（白名单解码，
// user_id/account_id/email/rate_limit_upsell 等键不报错）。
func TestUsageDecode(t *testing.T) {
	usageJSON := `{
		"plan_type": "team",
		"rate_limit": {"allowed": true, "limit_reached": false, "primary_window": {"used_percent": 42, "limit_window_seconds": 3600, "reset_after_seconds": 900, "reset_at": 1750000000}},
		"credits": {"has_credits": true, "unlimited": false, "overage_limit_reached": false, "balance": "12.50", "approx_local_messages": [{"id":"m1"}], "approx_cloud_messages": ["c1"]},
		"spend_control": {"reached": false, "individual_limit": {"limit": "50.00", "used": "12.50", "remaining": "37.50", "used_percent": 25, "remaining_percent": 75}},
		"rate_limit_reached_type": {"type": "rate_limit_reached", "details": "default"},
		"user_id": "u-1",
		"account_id": "a-1",
		"email": "x@y.z",
		"rate_limit_upsell": {"ctas": [{"action": "open_pricing_dialog"}]}
	}`
	var gotPath string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(usageJSON))
	}))
	t.Cleanup(srv.Close)

	hc := NewHTTPClient(PAT("pat-usage"), WithTransport(newFixedTransport(t, "https://chatgpt.com/backend-api/wham/usage", srv.URL)))
	usage, err := hc.GetUsage(context.Background())
	if err != nil {
		t.Fatalf("GetUsage: %v", err)
	}
	if gotPath != "/backend-api/wham/usage" {
		t.Fatalf("路径 = %q, 期望 /backend-api/wham/usage", gotPath)
	}
	if usage.PlanType != "team" {
		t.Fatalf("PlanType = %q, 期望 team", usage.PlanType)
	}
	if usage.RateLimit == nil || !usage.RateLimit.Allowed || usage.RateLimit.LimitReached {
		t.Fatalf("RateLimit = %+v, 期望 Allowed=true 且 LimitReached=false", usage.RateLimit)
	}
	if w := usage.RateLimit.PrimaryWindow; w == nil || w.UsedPercent != 42 || w.LimitWindowSeconds != 3600 || w.ResetAfterSeconds != 900 || w.ResetAt != 1750000000 {
		t.Fatalf("PrimaryWindow = %+v", w)
	}
	if usage.Credits == nil || !usage.Credits.HasCredits || usage.Credits.Unlimited || usage.Credits.OverageLimitReached {
		t.Fatalf("Credits = %+v", usage.Credits)
	}
	if usage.Credits.Balance == nil || *usage.Credits.Balance != "12.50" {
		t.Fatalf("Credits.Balance = %v, 期望 12.50", usage.Credits.Balance)
	}
	if len(usage.Credits.ApproxLocalMessages) != 1 || len(usage.Credits.ApproxCloudMessages) != 1 {
		t.Fatalf("ApproxLocalMessages/ApproxCloudMessages = %v / %v",
			usage.Credits.ApproxLocalMessages, usage.Credits.ApproxCloudMessages)
	}
	if usage.SpendControl == nil || usage.SpendControl.Reached || usage.SpendControl.IndividualLimit == nil {
		t.Fatalf("SpendControl = %+v", usage.SpendControl)
	}
	if l := usage.SpendControl.IndividualLimit; l.Limit != "50.00" || l.Used != "12.50" || l.Remaining != "37.50" || l.UsedPercent != 25 || l.RemainingPercent != 75 {
		t.Fatalf("IndividualLimit = %+v", l)
	}
	if usage.RateLimitReachedType == nil || usage.RateLimitReachedType.Type != "rate_limit_reached" || usage.RateLimitReachedType.Details != "default" {
		t.Fatalf("RateLimitReachedType = %+v", usage.RateLimitReachedType)
	}

	// 可空字段 null 形态（实测两样本：balance/individual_limit 恒 null、
	// rate_limit_reached_type team=null）→ 指针为 nil 不报错。
	t.Run("null 形态指针容忍", func(t *testing.T) {
		nullJSON := `{"plan_type":"free","rate_limit":{"allowed":false,"limit_reached":true,"primary_window":{"used_percent":100,"limit_window_seconds":3600,"reset_after_seconds":0,"reset_at":1750000000}},"credits":{"has_credits":false,"unlimited":false,"overage_limit_reached":false,"balance":null,"approx_local_messages":null,"approx_cloud_messages":null},"spend_control":{"reached":false,"individual_limit":null},"rate_limit_reached_type":null}`
		srv2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(nullJSON))
		}))
		t.Cleanup(srv2.Close)

		hc2 := NewHTTPClient(PAT("pat-usage"), WithTransport(newFixedTransport(t, "https://chatgpt.com/backend-api/wham/usage", srv2.URL)))
		usage, err := hc2.GetUsage(context.Background())
		if err != nil {
			t.Fatalf("GetUsage(null 形态): %v", err)
		}
		if usage.PlanType != "free" || usage.RateLimit == nil || !usage.RateLimit.LimitReached || usage.RateLimit.Allowed {
			t.Fatalf("free 限流触达形态 = %+v", usage)
		}
		if usage.Credits == nil || usage.Credits.Balance != nil || usage.SpendControl.IndividualLimit != nil || usage.RateLimitReachedType != nil {
			t.Fatalf("null 字段应解码为 nil 指针: credits=%v individual_limit=%v rlrt=%v",
				usage.Credits, usage.SpendControl.IndividualLimit, usage.RateLimitReachedType)
		}
	})
}

// TestUsageErrorStatus：非 2xx（>= 400）→ *HTTPError（状态码 + 错误体原样交付）。
func TestUsageErrorStatus(t *testing.T) {
	errorBody := []byte(`{"error":{"type":"invalid_request_error","message":"bad request"}}`)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write(errorBody)
	}))
	t.Cleanup(srv.Close)

	hc := NewHTTPClient(PAT("wrong"), WithTransport(newFixedTransport(t, "https://chatgpt.com/backend-api/wham/usage", srv.URL)))
	_, err := hc.GetUsage(context.Background())
	var he *HTTPError
	if !errors.As(err, &he) {
		t.Fatalf("期望 *HTTPError, got %T: %v", err, err)
	}
	if he.StatusCode != http.StatusBadRequest {
		t.Fatalf("StatusCode = %d, 期望 400", he.StatusCode)
	}
	if !bytes.Equal(he.Raw, errorBody) {
		t.Fatalf("Raw 应原样交付错误体: %s", he.Raw)
	}
}

// TestUsage401Rotate：401 自动轮转经 GetUsage 方法全链路生效——非判死 401 →
// 单飞 refresh → 自动重试一次成功（复用 doURL 通道既有测试语义）。
func TestUsage401Rotate(t *testing.T) {
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
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{}`))
	}))
	t.Cleanup(srv.Close)

	auth := OAuthWithRotation("rt-0", WithInitialAccessToken("at-old"))
	hc := NewHTTPClient(auth, WithTransport(newFixedTransport(t, "https://chatgpt.com/backend-api/wham/usage", srv.URL)))
	usage, err := hc.GetUsage(context.Background())
	if err != nil {
		t.Fatalf("GetUsage: %v", err)
	}
	if usage == nil {
		t.Fatal("usage 应为非 nil（轮转后应成功）")
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
