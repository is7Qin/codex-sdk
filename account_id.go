package codexsdk

import (
	"encoding/base64"
	"encoding/json"
	"net/http"
	"strings"
)

// ChatGPTAccountIDHeader 是账号标识请求头（真客户端由 BearerAuthProvider
// 对 ChatGPT 系 auth 恒发；Go 端经 textproto 规范化后上线形态为
// Chatgpt-Account-Id，头名大小写不敏感，见 §2.4）。
const ChatGPTAccountIDHeader = "ChatGPT-Account-ID"

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
	if id == "" || strings.ContainsAny(id, "\r\n\x00") {
		return // 空 = 不发（向后兼容）；含控制字符的值会让 Transport 直接拒绝整请求，跳过
	}
	h.Set(ChatGPTAccountIDHeader, id)
}

// chatGPTAuthClaimsNamespace 是 ChatGPT 身份 JWT 的 claims 命名空间
// （access_token 与 id_token 共享同一命名空间）。
const chatGPTAuthClaimsNamespace = "https://api.openai.com/auth"

// AccountIDFromToken 从 ChatGPT 身份 JWT（access_token 或 id_token——两者共享
// 同一 claims 命名空间）提取 chatgpt_account_id。只解 payload 的
// "https://api.openai.com/auth" claims——不验签、不校验 exp（与真客户端
// decode_jwt_payload 同语义：本地提取而非鉴权）。任何解析失败返回
// ("", false)，不 panic。
//
// 值经 TrimSpace 后非空才算命中。refresh_token 为不透明串（无 claims），
// 传它只会得到 ("", false)。
func AccountIDFromToken(token string) (string, bool) {
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return "", false
	}
	raw, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
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
