package codexsdk

import (
	"context"
	"fmt"
)

// OAuth 构造动态令牌鉴权：每次建连/请求前调用 tokenProvider 取最新 token
// （返回裸 token，SDK 补 "Bearer " 前缀），刷新与缓存逻辑由调用方实现。
func OAuth(tokenProvider func(ctx context.Context) (string, error)) Auth {
	return oauthAuth{provider: tokenProvider}
}

// oauthAuth 是 OAuth 鉴权实现（值类型，零分配）。
type oauthAuth struct {
	provider func(ctx context.Context) (string, error)
}

// Authorization 取最新 token 并组装 "Bearer <token>"。
func (a oauthAuth) Authorization(ctx context.Context) (string, error) {
	if a.provider == nil {
		return "", fmt.Errorf("codexsdk: OAuth tokenProvider 不能为 nil")
	}
	token, err := a.provider(ctx)
	if err != nil {
		return "", err
	}
	return "Bearer " + token, nil
}
