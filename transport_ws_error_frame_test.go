package codexsdk

import (
	"context"
	"errors"
	"io"
	"net/http"
	"testing"
	"time"
)

// 上游"本轮不可服务"的真实形态：一条带 status 的 error 事件帧。
// 429 + usage_limit_reached 就是生产"号额度耗尽"现场（repro 报告 Case C/D 的帧）。
const wsErrorFrame429 = `{"type":"error","status":429,"error":{"type":"usage_limit_reached","message":"You've reached your usage limit."}}`

// 同一帧的上游别名形态（codex 的 serde alias = "status_code"）。
const wsErrorFrame429Alias = `{"type":"error","status_code":429,"error":{"type":"usage_limit_reached","message":"You've reached your usage limit."}}`

// runPumpedOnce 跑一次 StreamBody（pumped/sseView 臂）并读尽 body。
func runPumpedOnce(hc *HTTPClient) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	resp, err := hc.StreamBody(ctx, []byte(`{"model":"gpt-5.6","input":[]}`), reuseIdentity())
	if err != nil {
		return "", err
	}
	defer func() { _ = resp.Body.Close() }()
	b, rerr := io.ReadAll(resp.Body)
	return string(b), rerr
}

// TestWSErrorFrameBecomesHTTPError429（T1）：pumped 臂必须把带 status 的错误帧
// 翻译成 *HTTPError{429} 交付给 Read，而不是渲染成 SSE 字节当流内容；且该连接
// **不入 wsConnCache**（下一次请求重拨 = accepts 计数 +1）。
func TestWSErrorFrameBecomesHTTPError429(t *testing.T) {
	up := newReuseUpstream(t, []string{wsErrorFrame429})
	hc := up.client(t, WithWSConnCache(1))

	body, readErr := runPumpedOnce(hc)
	var he *HTTPError
	if !errors.As(readErr, &he) {
		t.Fatalf("错误帧的读结束原因 = %v（body=%q），期望 *HTTPError{429}", readErr, body)
	}
	if he.StatusCode != http.StatusTooManyRequests {
		t.Fatalf("StatusCode = %d，期望 429（帧里的 status）", he.StatusCode)
	}
	if string(he.Raw) != wsErrorFrame429 {
		t.Fatalf("Raw = %q，期望原帧 %q", he.Raw, wsErrorFrame429)
	}
	if len(body) != 0 {
		t.Fatalf("错误帧不得再作为流内容交付（收到 %d 字节: %q）", len(body), body)
	}

	// 连接未回缓存 ⇒ 下一次请求必须重拨（accepts +1）。这是"错误即退役"的行为判据。
	if _, err := runPumpedOnce(hc); !errors.As(err, &he) {
		t.Fatalf("第二次错误 = %v，期望仍是 *HTTPError", err)
	}
	if got := up.accepts.Load(); got != 2 {
		t.Fatalf("accepts = %d，期望 2（错误帧结束的连接不得复用，必须重拨）", got)
	}
	if n := cacheIdleCount(hc); n != 0 {
		t.Fatalf("错误帧结束的交换之后缓存有 %d 条空闲连接（错误即退役）", n)
	}
}

// TestWSErrorFrameStatusAliasIs429（T1b）：`status_code` 别名与 `status` 等价
// （参考实现同 serde alias 口径）。
func TestWSErrorFrameStatusAliasIs429(t *testing.T) {
	up := newReuseUpstream(t, []string{wsErrorFrame429Alias})
	up.closeAfter = true
	hc := up.client(t, WithWSConnCache(1))

	body, readErr := runPumpedOnce(hc)
	var he *HTTPError
	if !errors.As(readErr, &he) {
		t.Fatalf("别名帧的读结束原因 = %v（body=%q），期望 *HTTPError{429}", readErr, body)
	}
	if he.StatusCode != http.StatusTooManyRequests {
		t.Fatalf("StatusCode = %d，期望 429（status_code 别名）", he.StatusCode)
	}
	if string(he.Raw) != wsErrorFrame429Alias {
		t.Fatalf("Raw = %q，期望原帧 %q", he.Raw, wsErrorFrame429Alias)
	}
}

// TestErrorFrameHTTPErrorBoundaries（T0）：映射函数的全边界表驱动。
//
// 预期：只有 status ∈ [400,599] 才产出 *HTTPError；缺失 / 2xx / 越界一律 nil
// （保持透传 —— 与参考实现 codex-api/src/endpoint/responses_websocket.rs:752-763
// 同口径：`is_success()`、越界与缺失都回到 None）。这是 M2/M3/M5a/M5b 的最短
// 反馈回路，也覆盖 2xx 与 399/600 两个越界格。
func TestErrorFrameHTTPErrorBoundaries(t *testing.T) {
	cases := []struct {
		name  string
		frame string
		want  int // 0 = 期望 nil
	}{
		{"无 status", `{"type":"error","error":{"type":"server_error","message":"boom"}}`, 0},
		{"status 是 2xx", `{"type":"error","status":200,"error":{"type":"x"}}`, 0},
		{"status 是 399（下越界）", `{"type":"error","status":399,"error":{"type":"x"}}`, 0},
		{"status 是 400（下界）", `{"type":"error","status":400,"error":{"type":"x"}}`, 400},
		{"status 是 429", `{"type":"error","status":429,"error":{"type":"x"}}`, 429},
		{"status 是 599（上界）", `{"type":"error","status":599,"error":{"type":"x"}}`, 599},
		{"status 是 600（上越界）", `{"type":"error","status":600,"error":{"type":"x"}}`, 0},
		{"status_code 别名 429", `{"type":"error","status_code":429,"error":{"type":"x"}}`, 429},
		// 双键并存：top-level `status` 赢。这是与本仓 codex 参考（serde alias 在两键
		// 并存时反而取 status_code，见 responses_websocket.rs:693-702）的**刻意分叉**，
		// 不是疏漏：SDK 的映射对每种输入只能有一个答案，r3/r4 钉住"缺失才用别名"。
		{"status 与 status_code 并存 ⇒ status 优先", `{"type":"error","status":503,"status_code":429}`, 503},
		// 非顶层 `error.status` 不认（gjson 路径只取顶层键），避免把上游嵌套结构
		// 随意升格成状态码。
		{"嵌套 error.status（非顶层）", `{"type":"error","error":{"status":429}}`, 0},
		// 不做字符串解析、不猜：非整数形态一律当"没有可用 status"。
		{"status 是字符串 429", `{"type":"error","status":"429"}`, 0},
		{"status 是 null", `{"type":"error","status":null}`, 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := errorFrameHTTPError([]byte(tc.frame))
			if tc.want == 0 {
				if err != nil {
					t.Fatalf("期望 nil（保持透传），实际 = %v", err)
				}
				return
			}
			var he *HTTPError
			if !errors.As(err, &he) {
				t.Fatalf("期望 *HTTPError{%d}，实际 = %v", tc.want, err)
			}
			if he.StatusCode != tc.want {
				t.Fatalf("StatusCode = %d，期望 %d", he.StatusCode, tc.want)
			}
			if string(he.Raw) != tc.frame {
				t.Fatalf("Raw = %q，期望原帧 %q", he.Raw, tc.frame)
			}
		})
	}
}

// TestWSErrorFrameWithoutUsableStatusStaysContent（§5.2 第一条）：缺 status / status
// 是 2xx 的错误帧**保持今天的透传**（与参考实现 :752-763 同口径）。
//
// 这条是"不新增分类"的钉子：谁把 502 兜底加回来，这里立刻红。
func TestWSErrorFrameWithoutUsableStatusStaysContent(t *testing.T) {
	cases := []struct {
		name  string
		frame string
	}{
		{"缺 status", `{"type":"error","error":{"type":"server_error","message":"boom"}}`},
		{"status 是 2xx", `{"type":"error","status":200,"error":{"type":"usage_limit_reached","message":"fake success"}}`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			up := newReuseUpstream(t, []string{tc.frame})
			up.closeAfter = true // 一帧后正常关闭：把"之后没有帧"变成确定性 EOF
			hc := up.client(t, WithWSConnCache(1))

			body, readErr := runPumpedOnce(hc)
			if readErr != nil {
				t.Fatalf("不可翻译的错误帧必须按内容透传，读结束原因 = %v", readErr)
			}
			want := "event:error\ndata:" + tc.frame + "\n\n"
			if body != want {
				t.Fatalf("SSE 字节流不符\n实际 = %q\n期望 = %q", body, want)
			}
		})
	}
}
