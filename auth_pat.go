package codexsdk

import "context"

// PAT 构造静态 PAT 鉴权：Authorization: Bearer <token>。
func PAT(token string) Auth {
	return patAuth{token: token}
}

// patAuth 是 PAT 鉴权实现（值类型，零分配）。
type patAuth struct {
	token string
}

// Authorization 返回 "Bearer <token>"。PAT 不随上下文/时间变化。
func (a patAuth) Authorization(context.Context) (string, error) {
	return "Bearer " + a.token, nil
}
