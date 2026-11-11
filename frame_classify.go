package codexsdk

import (
	"bytes"
	"strings"

	"github.com/tidwall/gjson"
)

// ClassifyAuthFatalFrame 判定 WS 错误事件帧是否携带账号级致命码。
// 复用 isATFatalCode 码集（单一真相）；帧形不含致命码 → nil。
// 热路径契约：未命中零分配（bytes.Contains 预筛 + 仅命中才 gjson）。
func ClassifyAuthFatalFrame(frame []byte) *AuthPermanentlyRevokedError {
	if !bytes.Contains(frame, []byte(`"type":"error"`)) {
		return nil
	}
	if !strings.EqualFold(gjson.GetBytes(frame, "type").String(), "error") {
		return nil
	}
	if code, ok := scanATFatalCode(frame); ok {
		return &AuthPermanentlyRevokedError{Code: strings.ToLower(code), Raw: frame}
	}
	return nil
}

// errorFrameHTTPError 把上游 WS 错误事件帧的 status 翻译成 HTTP 面同形的错误
// 信封（与握手失败的 wsDialError 同形：StatusCode + 原帧）。
//
// 只有 **4xx/5xx** 才翻译：上游在 socket 建起来之后没有 4xx/429 可发，它只能发
// `{"type":"error","status":N}`——把这条帧当流内容渲染成 SSE 就等于把失败当成功
// 交付（状态码/规则/429 节流/终止语义全部失效）。
//
// 缺失 / 越界 / 2xx ⇒ nil（保持今天的透传，与参考实现
// codex-api/src/endpoint/responses_websocket.rs:752-763 同口径）。**不猜状态码**：
// 502 兜底是自造分类，会把"未知形状"永久钉成"上游坏了"。
//
// 只认 **JSON number**：`"429"` / `null` / 对象一律当"没有可用 status"。这一点必须
// 显式判型——`gjson.Result.Int()` 对 String 会 parseInt，`"429"` 会被静默当成 429，
// 而参考实现的 `Option<u16>` 不接受字符串（serde 不强制转换）⇒ 那边反序列化失败即
// 不映射，我们同义。
//
// 调用点已确认 type == "error"（sse.go / responses_ws.go 都先过 frameEventType），
// 所以这里不做预筛、不做 type 判定、不做大小写折叠——两套纪律只会分叉。
func errorFrameHTTPError(frame []byte) error {
	r := gjson.GetBytes(frame, "status")
	if r.Type != gjson.Number {
		r = gjson.GetBytes(frame, "status_code") // 上游别名（serde alias = "status_code"）
	}
	if r.Type != gjson.Number {
		return nil
	}
	status := int(r.Int())
	if status < 400 || status > 599 {
		return nil
	}
	return &HTTPError{StatusCode: status, Raw: frame}
}
