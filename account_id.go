package codexsdk

import (
	"encoding/base64"
	"encoding/json"
	"net/http"
	"strings"
)

// ChatGPTAccountIDHeader 是账号标识请求头（真客户端由 BearerAuthProvider
// 对 ChatGPT 系 auth 恒发；SDK 以小写 chatgpt-account-id 写出，与官方抓包一致）。
const ChatGPTAccountIDHeader = "ChatGPT-Account-ID"

// applyDefaultHeaders 注入伪装层默认头（codex UA + originator；断言值见
// DefaultCodexUserAgent / DefaultOriginator）。调用方 WithHeader 经
// applyHeaderOverrides 可覆盖，故本助手须在各面默认头之后、覆盖循环之前应用。
func applyDefaultHeaders(h http.Header) {
	h["User-Agent"] = []string{DefaultCodexUserAgent}
	h["originator"] = []string{DefaultOriginator}
}

// applyHeaderOverrides 应用调用方 WithHeader 注入项：按小写键覆盖——覆盖前同时
// 删除规范写法与小写写法（同一头不会以两种写法重复发出），同名多次调用为扩展
// （多个值）。User-Agent 例外保持规范写法（否则 Go 会补默认 UA）。WS 升级头与
// HTTP 请求头共用。
func applyHeaderOverrides(h http.Header, overrides http.Header) {
	for k, vals := range overrides {
		lk := strings.ToLower(k)
		delete(h, http.CanonicalHeaderKey(k))
		delete(h, lk)
		if lk == "user-agent" {
			lk = "User-Agent"
		}
		h[lk] = append([]string(nil), vals...)
	}
}

// applyAccountID 在 auth 携带账号标识时注入 ChatGPT-Account-ID。
//
// 注入时机在各面默认头之后、调用方 WithHeader 覆盖循环之前 ——
// 与既有「WithHeader 可覆盖默认头」约定一致（Del+Add 后写赢）。
func applyAccountID(h http.Header, auth Auth) {
	p, ok := auth.(AccountIDProvider)
	if !ok {
		return
	}
	id := strings.TrimSpace(p.AccountID())
	if id == "" || hasControlByte(id) {
		return // 空 = 不发（向后兼容）；任何控制字节都会让 Transport 直接拒绝整请求，跳过
	}
	h["chatgpt-account-id"] = []string{id}
}

// hasControlByte 值卫生：<0x20（TAB 除外）与 0x7f 一律拒绝——对齐 net/http
// httpguts.ValidHeaderFieldByte 口径（CR/LF/NUL 最小集之上的加固）。
func hasControlByte(s string) bool {
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c == '\t' {
			continue
		}
		if c < 0x20 || c == 0x7f {
			return true
		}
	}
	return false
}

// AccountIDFromToken 从 ChatGPT 身份 JWT（access_token 或 id_token——两者共享
// 同一 claims 命名空间）提取 chatgpt_account_id。只解 payload 的
// "https://api.openai.com/auth" claims——不验签、不校验 exp（与真客户端
// decode_jwt_payload 同语义：本地提取而非鉴权）。任何解析失败返回
// ("", false)，不 panic。
//
// 值经 TrimSpace 后非空才算命中。refresh_token 为不透明串（无 claims），
// 传它只会得到 ("", false)。
func AccountIDFromToken(token string) (string, bool) {
	raw, ok := jwtPayload(token)
	if !ok {
		return "", false
	}
	var payload struct {
		Auth struct {
			AccountID string `json:"chatgpt_account_id"`
		} `json:"https://api.openai.com/auth"`
	}
	if err := json.Unmarshal(raw, &payload); err != nil {
		return "", false
	}
	id := strings.TrimSpace(payload.Auth.AccountID)
	if id == "" {
		return "", false
	}
	return id, true
}

// jwtPayload 解出 JWT payload 原始字节（三段式校验 + base64url 无填充解码；
// 不验签、不校验 exp）。畸形 token → ("", false)，不 panic。
func jwtPayload(token string) ([]byte, bool) {
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return nil, false
	}
	raw, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return nil, false
	}
	return raw, true
}
