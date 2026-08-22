package codexsdk

import (
	"context"
	"sync/atomic"
)

// PAT 构造静态 PAT 鉴权：Authorization: Bearer <token>。
// 可选 PATOption 配置账号级终止回调（WithPATOnAuthFatal）；无回调时判死仅毒化不通知。
func PAT(token string, opts ...PATOption) Auth {
	a := &patAuth{token: token}
	for _, o := range opts {
		if o != nil {
			o(a)
		}
	}
	return a
}

// PATOption 配置 PAT 鉴权。
type PATOption func(*patAuth)

// WithPATOnAuthFatal 设置 PAT 账号级终止回调（at-most-once）：SDK 判定
// 账号级不可重试（AT 判死码命中）时通知调用方标记账号失效。Fatal 显式终止
// 不触发本回调（调用方已获知）。
func WithPATOnAuthFatal(fn func(error)) PATOption {
	return func(a *patAuth) { a.onAuthFatal = fn }
}

// patAuth 是 PAT 鉴权实现（指针状态，毒化后 fail-closed）。
type patAuth struct {
	token       string
	fatal       atomic.Pointer[fatalState]
	onAuthFatal func(error)
}

// Authorization 返回 "Bearer <token>"；毒化后恒返回毒化错误（fail-closed）。
func (a *patAuth) Authorization(context.Context) (string, error) {
	if f := a.fatal.Load(); f != nil {
		return "", f.err
	}
	return "Bearer " + a.token, nil
}

// Invalidate：PAT 无轮转状态，no-op。
func (a *patAuth) Invalidate() {}

// Fatal 显式终止（网关解析到 WS 判死事件帧时调用）：置账号级终止状态，
// 后续 Authorization 恒返回该错误。不触发 OnAuthFatal（调用方已获知）。
// nil 忽略。
func (a *patAuth) Fatal(err error) {
	if err == nil {
		return
	}
	a.fatal.CompareAndSwap(nil, &fatalState{err: err})
}

// setFatal 置终止态并回调 OnAuthFatal（至多一次：CAS 胜者回调）。
func (a *patAuth) setFatal(err error) {
	if a.fatal.CompareAndSwap(nil, &fatalState{err: err}) {
		if a.onAuthFatal != nil {
			a.onAuthFatal(err)
		}
	}
}

// authFatal 是 AT 401 判死路径的终止入口（私有接口 authFatalTrigger）：
// 走 setFatal——触发 OnAuthFatal 至多一次。
func (a *patAuth) authFatal(err error) {
	a.setFatal(err)
}
