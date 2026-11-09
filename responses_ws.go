package codexsdk

import (
	"bytes"
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

// 复用连接首帧白名单（I8）用到的其余事件名。
const (
	responseEventInProgress = "response.in_progress"
	responseEventError      = "error"
	// responsesTimingEvent 是上游的观测事件（codex-api/src/sse/responses.rs 的
	// "responsesapi.websocket_timing"）。它可能作为某次响应的最后一帧出现，
	// 因此必须能合法地当"下一请求的第一个帧"。
	responsesTimingEvent = "responsesapi.websocket_timing"
)

// frameStartsResponse 报告 raw 能不能作为一条复用连接上"新请求的第一帧"。
//
// 白名单：response.created / response.in_progress / error / 任何 *.metadata /
// responsesapi.websocket_timing。白名单外的一切（.delta / .completed / .failed …）
// 都是上一个请求的残留或串台，拿它们当新请求的事件会静默算错 usage。
//
// 零分配：frameEventType 只扫顶层 type 值，bytes.HasSuffix 不复制。
func frameStartsResponse(raw []byte) bool {
	typ := frameEventType(raw)
	switch {
	case bytesAre(typ, responseEventCreated),
		bytesAre(typ, responseEventInProgress),
		bytesAre(typ, responseEventError),
		bytesAre(typ, responsesTimingEvent):
		return true
	case bytes.HasSuffix(typ, []byte(".metadata")):
		return true
	}
	return false
}

// streamBodyWS 是 StreamBody 的 ws 臂：帧构造 → 拨号 → 发一帧 → 合成 200
// 响应。对外形状与 HTTP 臂一致（StatusCode / Body / Header 三项——网关侧
// sdkbridge 与 pipeline 只读这三项，零改动）。
//
// 顺序是契约：**帧构造先于拨号**（见 openWSExchange）——本地坏请求体是确定性
// 输入错误，不能为它烧一次上游握手（否则拨号失败会盖掉本地错误，两臂状态面
// 不等价）。
//
// turn-state：请求侧只来自 Identity.TurnState（"" = 不带）；捕获到的签发值
// 只回调 Identity.Sink。ts 不进升级头（真实客户端只在帧里带），也不改连接身份。
//
// ctx 只是 exchange 的构造输入（含超时预算）：**取帧用 ex.ctx**，连接 ctx 已与
// 请求解耦。调用方取消能终止后续对 Body 的读取；已入队的帧仍先交付（取帧即提交）。
func (c *HTTPClient) streamBodyWS(ctx context.Context, payload []byte, id Identity, readLimit int64) (*http.Response, error) {
	ex, err := c.openWSExchange(ctx, payload, id, readLimit)
	if err != nil {
		return nil, err
	}
	hdr := make(http.Header, 2)
	hdr.Set("Content-Type", "text/event-stream")
	return &http.Response{
		Status:        "200 OK",
		StatusCode:    http.StatusOK,
		Proto:         "HTTP/1.1",
		ProtoMajor:    1,
		ProtoMinor:    1,
		Header:        hdr,
		Body:          newSSEView(ex.ctx, c.turnStateTapFor(ex, id.Sink), readLimit),
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
//
// 终态一成立就 Release（T5 起）：连接停在干净帧边界，可以交回缓存给同一个
// HTTPClient 的下一个请求（跨轮同样复用——连接身份不含 ts）。feed 报错但帧本身
// 是终态时 **照样** Release——连接状态与下游解析失败无关，杀掉它只会让下一个
// 请求多付一次握手。
func (c *HTTPClient) responsesStreamWS(ctx context.Context, payload []byte, id Identity, feed func(raw []byte) error) error {
	ex, err := c.openWSExchange(ctx, payload, id, wsResponsesReadLimit)
	if err != nil {
		return err
	}
	// 事件级 ts 捕获（S3）与流式臂共用同一个装饰器；Release 之后这个 defer 是
	// no-op（wsExchange.closed）。
	tap := c.turnStateTapFor(ex, id.Sink)
	defer func() { _ = tap.Close() }()
	for {
		raw, nextErr := tap.Next(ex.ctx)
		if nextErr == io.EOF { // close frame / 正常收尾：截断与否由调用方按终态判定
			return nil
		}
		if nextErr != nil {
			return fmt.Errorf("codexsdk: 读取响应流失败: %w", nextErr)
		}
		term := isTerminalEvent(raw)
		if ferr := feed(raw); ferr != nil {
			if term {
				tap.Release()
			}
			return ferr
		}
		if term { // 终态帧已 feed ⇒ 本次取数完成
			tap.Release()
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
