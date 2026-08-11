package codexsdk

import "context"

// Auth 抽象上游鉴权：向 WS 升级请求 / HTTP 请求注入 Authorization 头。
//
// 实现为零分配值类型（PAT / OAuth 均为小型结构体），由 SDK 在
// Dial / Do / Stream 时各调用一次取头值。
type Auth interface {
	// Authorization 返回 Authorization 请求头值（如 "Bearer xxx"）。
	// 每次建连/请求调用一次；OAuth 场景的 token 刷新逻辑由调用方在
	// tokenProvider 内实现（网关侧接 OAuth 刷新后在此注入）。
	Authorization(ctx context.Context) (string, error)

	// Invalidate 显式失效：标记当前 access token 失效，下次 Authorization
	// 前刷新。OAuthWithRotation 实现为置空 at 缓存；PAT / oauthAuth
	// 无轮转状态，实现为 no-op。
	Invalidate()

	// Fatal 终止：置账号级终止状态，后续 Authorization 恒返回该错误。
	// 网关解析到 WS 判死错误事件（token_invalidated 等业务事件帧）时调用
	// （唯一跨边界点：SDK 不解析业务事件帧）。PAT / oauthAuth 实现为 no-op。
	Fatal(err error)
}
