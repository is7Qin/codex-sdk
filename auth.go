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
}
