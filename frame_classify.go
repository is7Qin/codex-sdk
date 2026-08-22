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
	for _, p := range []string{"error.code", "error.type"} {
		if code := gjson.GetBytes(frame, p).String(); isATFatalCode(code) {
			return &AuthPermanentlyRevokedError{Code: strings.ToLower(code), Raw: frame}
		}
	}
	return nil
}
