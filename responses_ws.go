package codexsdk

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
)

// Responses 出站 WS 臂的读上限（按取数路径对齐 HTTP 面，不是一条全局规则）：
//   - 流式：HTTP 臂交付的是**未读**的原始 SSE 字节，单行上限由下游决定
//     （网关 sserelay 的单行上限 64MiB）；
//   - 非流式：HTTP 臂走响应聚合器，单行上限是 defaultMaxLineSize（16MiB）。
//
// 同一份事件在两条路径上的"最大可承载帧"必须一致，否则 16–64MiB 的单帧会让
// 某一臂独败——那是配置差异，不是传输差异。
const (
	wsStreamReadLimit    = 64 << 20
	wsResponsesReadLimit = defaultMaxLineSize
)

// responseEventIncomplete 是上游的"未完成"终态事件。它**不在** responsesAggregator
// 的白名单里（聚合器不解析它 → 无 response.completed → 截断错误）——本常量只
// 供 SSE 视图判定流已终结（不再取帧、也不补 [DONE]）。
const responseEventIncomplete = "response.incomplete"

// streamBodyWS 是 StreamBody 的 ws 臂：帧构造 → 拨号 → 发一帧 → 合成 200
// 响应。对外形状与 HTTP 臂一致（StatusCode / Body / Header 三项——网关侧
// sdkbridge 与 pipeline 只读这三项，零改动）。
//
// 顺序是契约：**帧构造先于拨号**（见 openWSExchange）——本地坏请求体是确定性
// 输入错误，不能为它烧一次上游握手（否则拨号失败会盖掉本地错误，两臂状态面
// 不等价）。
//
// turn-state：本方法认两个来源（前者优先）：
//  1. 调用方以 WithHeader(HeaderTurnState, ...) 下发的 per-request 值——网关既有的
//     注入手法（HTTP 面它落在请求头上，WS 面落在帧内 client_metadata）；该头
//     **不进升级头**（真实客户端只在帧里带，见 wsUpgradeHeaders）；
//  2. HTTPClient 缓存的"上一次响应/握手签发值"（c.TurnState()）。
//
// ctx 同时是 exchange 的寿命（连接 ctx 带 WithTimeout 的 deadline）与读 ctx：
// 调用方取消能终止后续对 Body 的读取；已入队的帧仍先交付（取帧即提交）。
func (c *HTTPClient) streamBodyWS(ctx context.Context, payload []byte, readLimit int64) (*http.Response, error) {
	ex, err := c.openWSExchange(ctx, payload, readLimit)
	if err != nil {
		return nil, err
	}
	hdr := make(http.Header, 2)
	hdr.Set("Content-Type", "text/event-stream")
	if ex.issued != "" {
		hdr.Set(HeaderTurnState, ex.issued)
	}
	return &http.Response{
		Status:        "200 OK",
		StatusCode:    http.StatusOK,
		Proto:         "HTTP/1.1",
		ProtoMajor:    1,
		ProtoMinor:    1,
		Header:        hdr,
		Body:          newSSEView(ctx, ex, readLimit),
		ContentLength: -1,
	}, nil
}

// responsesStreamWS 是 Responses 的 ws 臂取数：**直接从 exchange 取帧**喂给
// responsesAggregator（HTTP 臂走 Stream，同样只做 scanner + feed）。合成体因此
// 与 HTTP 臂逐字段同构——这不是巧合，而是同一份 feed/composite 代码的结果。
//
// 为什么不走 StreamBody：SSE 编码只为"以 SSE 字节流对外"存在（StreamBody 的
// 契约）。Responses 只要事件帧，经 sseView 渲染成字节再逐行扫回来是纯回路——
// 每帧多一次帧体复制 + 每行一次 scanner 分配，且中间没有消费者受益。直取帧后
// 帧体在本次回调里只被引用，feed 里的解析（gjson 零拷贝路径）原样适用。
//
// 终态即提交：终态帧 feed 完即返回（completed / failed / incomplete），不再
// Next——已完成的请求不会被消费侧随后的取消/超时改写。帧自身的事件语义不变：
// failed 的 error、incomplete 的截断判定仍归聚合器与调用方。
func (c *HTTPClient) responsesStreamWS(ctx context.Context, payload []byte, feed func(raw []byte) error) error {
	ex, err := c.openWSExchange(ctx, payload, wsResponsesReadLimit)
	if err != nil {
		return err
	}
	defer func() { _ = ex.Close() }()
	for {
		raw, nextErr := ex.Next(ctx)
		if nextErr == io.EOF { // close frame / 正常收尾：截断与否由调用方按终态判定
			return nil
		}
		if nextErr != nil {
			return fmt.Errorf("codexsdk: 读取响应流失败: %w", nextErr)
		}
		if ferr := feed(raw); ferr != nil {
			return ferr
		}
		if isTerminalEvent(raw) { // 终态帧已 feed ⇒ 本次取数完成
			return nil
		}
	}
}

// isTerminalEvent 报告 raw 是否为本次请求的终态事件（completed / failed /
// incomplete）。与 sseView 的渲染判定共用同一套零分配 type 扫描与同一组常量，
// 两条路径不会对"什么算终结"产生分歧；协议解析仍只在这里与 sseView，conn /
// exchange 的取帧路径不认识事件类型。
func isTerminalEvent(raw []byte) bool {
	typ := frameEventType(raw)
	return bytesAre(typ, responseEventCompleted) ||
		bytesAre(typ, responseEventFailed) ||
		bytesAre(typ, responseEventIncomplete)
}

// effectiveTurnState 解析本次调用的生效 turn-state：WithHeader(HeaderTurnState, ...)
// 优先（调用方 per-request 注入），否则 HTTPClient 缓存的最近签发值。
//
// 为什么两条：网关面 per-request 的 turn-state 一直是用 WithHeader 下发的（HTTP
// 臂就靠它成为请求头），而 WS 面的同一个值必须进帧内 client_metadata——不认这条
// 来源的话，"网关零改动"下 WS 臂永远不会回传 turn-state（HTTP 臂会）。
func (c *HTTPClient) effectiveTurnState() string {
	if v := c.opts.headers.Get(HeaderTurnState); v != "" {
		return v
	}
	return c.TurnState()
}

// wsUpgradeHeaders 算出升级头里的"调用方覆盖集"（构造期一次，避免每请求重建）：
// 与 WithHeader 一致，唯一例外是 x-codex-turn-state——真实 codex 客户端的 WS
// 面只在帧内 client_metadata 里带它，升级头只有 Authorization / UA / Originator /
// OpenAI-Beta / 会话标识 / 账号标识。
func wsUpgradeHeaders(h http.Header) http.Header {
	if len(h) == 0 {
		return nil
	}
	out := make(http.Header, len(h))
	turnStateKey := http.CanonicalHeaderKey(HeaderTurnState)
	for k, vals := range h {
		if http.CanonicalHeaderKey(k) == turnStateKey {
			continue
		}
		out[k] = vals
	}
	return out
}

// wsDialError 把 WS 升级失败翻译成 HTTP 面同形的错误信封：状态码沿用升级
// 响应，body 用 DialError.Body（≤1024B 的截断响应体），底层原因挂在 cause
// 上供 errors.As 回溯。
//
// 为什么要带 body：HTTP 臂的 4xx 带 body，网关的 error_message_contains 规则
// 与判死分类都靠它；升级失败同样有 body（coder/websocket 会把非 101 的响应体
// 留回来），不带就会让同一个错误在 WS 臂命中不了规则，成功率之差混进了"规则
// 引擎差异"而不是传输差异。
//
// 非 DialError 的错误原样透传：无响应的拨号失败、auth 为 nil、OAuth refresh 的
// fatal 类错误（AuthPermanentlyRevokedError 等）必须留在 errors.Is/As 链上，
// 包成 HTTPError 会让网关的判死分类与 ctx 判定失效（D3）。
func wsDialError(err error) error {
	var de *DialError
	if errors.As(err, &de) {
		return &HTTPError{StatusCode: de.StatusCode, Raw: de.Body, cause: err}
	}
	return err
}
