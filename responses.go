package codexsdk

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/tidwall/gjson"
)

// responses 聚合白名单事件类型（白名单外跳过——上游扩展兼容）。
// response.rejected 不在白名单：其为终态事件 → 无 response.completed →
// 截断错误兜底（与"不做流中断恢复"边界一致）。
const (
	responseEventCreated   = "response.created"
	responseEventItemDone  = "response.output_item.done"
	responseEventCompleted = "response.completed"
	responseEventFailed    = "response.failed"
	// responseEventError 是上游的错误事件帧（`{"type":"error","status":N}`）。
	// 它不是内容帧：命中即翻译成 *HTTPError 终止本流（见 errorFrameHTTPError）。
	responseEventError = "error"
)

// Responses 在 WS 臂可用 WithWSConnCache(n) 开启默认关闭的连接复用：连接身份 =
// (threadKey, readLimit)（不含 turn-state/window），同一 HTTPClient 上同一
// thread 的连续请求——包括跨轮跨窗口——都可命中；消费终态后自动归还；匿名不
// 复用；与 StreamBody 不共享连接。收益边界见 TRANSPORT.md。
//
// Responses 合成非流式 responses 调用：内部无条件覆盖 stream:true（上游硬性
// 要求——payload 任意 stream 值（含显式 false）均覆盖为 true，非流式语义由
// 聚合保证），SSE 事件流聚合重组为完整响应体返回。
// 注入机制：sjson.SetBytes 同款（对齐 injectClientMetadataKeys 先例——非法
// JSON payload 放弃注入保持原样，上游 400 透传）；client_metadata 注入
// （恒 4 key + turn_id + 条件键，对齐真实 client_metadata()——
// responses_metadata.rs:255-288）在 Stream 发送前统一执行
// （injectResponsesClientMetadata——非流式聚合与流式路径同一注入点）。
// 与 Do 的区别：Do 是通用非流式 POST（对 codex responses 端点 400 不可用）；
// 本方法走 Stream 合成——网关以非流式语义消费（一次性响应）。
// 返回 HTTPResponse{StatusCode, Raw}（与 Do 同形态）。上游签发的 turn-state
// 只回调 Identity.Sink，不出现在响应对象上。
// 错误透传同 Stream（HTTPError 信封 / fatal 五类 / 401 判死轮转）。
//
// TransportWS 时取数改走上游 Responses WS（StreamBody 的 ws 臂 + 同一个
// responsesAggregator），合成体与 HTTP 臂逐字段同构；错误语义见 TRANSPORT.md。
//
// 终态提交（WS 臂）：终态帧一旦交给聚合器，本次 exchange 即视为完成——此后
// 消费侧的取消/超时不得把已完成的请求改写成失败（帧自身的事件语义不变：
// failed / incomplete 仍按聚合器与截断规则报错）。这也覆盖"终态帧已入队、
// deadline 随即到点"的次序：已拿到的帧先被交付。
func (c *HTTPClient) Responses(ctx context.Context, payload []byte, id Identity) (*HTTPResponse, error) {
	wire := payload // stream:true 由整形强制
	agg := &responsesAggregator{}
	if c.opts.responsesTransport == TransportWS {
		// ws 臂：responsesStreamWS 直接从 exchange 取帧喂聚合器（不经 sseView
		// 渲染 + scanSSE 回扫）。终态帧喂进聚合器后不再取帧，所以只有"终态之后
		// 才到"的取消/超时可以豁免，其余错误原样透传。
		err := c.responsesStreamWS(ctx, wire, id, agg.feed)
		if err != nil {
			if !agg.completed || !isContextError(err) {
				return nil, err // 上游/传输错误透传（response.failed 事件级 HTTPError 同样原样）
			}
		}
	} else {
		if err := c.streamIssued(ctx, wire, id, agg.feed); err != nil {
			return nil, err // 上游错误（HTTPError/fatal 五类/401 轮转）/ response.failed 事件级错误透传
		}
	}
	if !agg.completed {
		// 截断：流结束（[DONE] 与 EOF 统一）未达 response.completed → 错误返回，
		// 不返回 status=completed 的半截响应（不做流中断恢复）。
		return nil, fmt.Errorf("codexsdk: responses 事件流截断：未收到 response.completed 终态事件")
	}
	body, err := json.Marshal(agg.composite())
	if err != nil {
		return nil, fmt.Errorf("codexsdk: 合成 responses 响应失败: %w", err)
	}
	return &HTTPResponse{StatusCode: http.StatusOK, Raw: body}, nil
}

// responsesAggregator 是 SSE 事件 → 合成响应体的聚合态。
// 零拷贝语义（http.go Stream 回调——raw 为 scanner 复用缓冲，仅回调执行期间
// 有效）：事件 JSON 必须在回调内立即解析/拷贝；item/usage 经 gjson .Raw
// （字符串转换即拷贝）落地为 json.RawMessage，可跨回调保留（大 item 达 MB 级
// base64，未拷贝则缓冲复用交叉污染，合成体静默损坏）。
type responsesAggregator struct {
	id        string            // response.created 的 response.id（缺失 → 合成体 id 兜底空）
	output    []json.RawMessage // output_item.done 收集的完整 item（流序；added 不收集避免重复）
	usage     json.RawMessage   // response.completed 的 usage（缺失 → 合成体 usage 缺省）
	completed bool              // 是否收到 response.completed 终态事件
}

// feed 处理单条 SSE 事件（白名单外跳过——增量事件 / 未知事件不解析）。
// response.failed → 事件级失败：合成 HTTPError（StatusCode=500 + Raw=事件
// error JSON；事件级失败无 HTTP 状态，500 归网关 5xx 分类）。
func (a *responsesAggregator) feed(raw []byte) error {
	switch gjson.GetBytes(raw, "type").String() {
	case responseEventCreated:
		a.id = gjson.GetBytes(raw, "response.id").String()
	case responseEventItemDone:
		if item := gjson.GetBytes(raw, "item"); item.Exists() {
			a.output = append(a.output, json.RawMessage(item.Raw))
		}
	case responseEventCompleted:
		a.completed = true
		if usage := responseUsage(raw); len(usage) > 0 {
			a.usage = usage
		}
	case responseEventFailed:
		return a.failedError(raw)
	}
	return nil
}

// responseUsage 返回事件内 response.usage 的原始 JSON（缺失或显式 null → nil）。
func responseUsage(raw []byte) json.RawMessage {
	usage := gjson.GetBytes(raw, "response.usage")
	if usage.Exists() && usage.Type != gjson.Null {
		return json.RawMessage(usage.Raw)
	}
	return nil
}

// failedError 把 response.failed 事件合成为错误（Raw=事件 error 字段 JSON，
// error 字段缺失时兜底整个事件 JSON）。
func (a *responsesAggregator) failedError(raw []byte) error {
	rawErr := raw
	if e := gjson.GetBytes(raw, "error"); e.Exists() {
		rawErr = []byte(e.Raw)
	}
	return &HTTPError{StatusCode: http.StatusInternalServerError, Raw: rawErr}
}

// responsesComposite 是合成非流式响应体（与 OpenAI responses 非流式响应同构；
// 最小字段集即终态——与"白名单外不解析"一致，response.created 的
// model/created_at 等不保留）。
type responsesComposite struct {
	ID     string            `json:"id"`
	Object string            `json:"object"`
	Status string            `json:"status"`
	Output []json.RawMessage `json:"output"`
	Usage  json.RawMessage   `json:"usage,omitempty"`
}

// composite 组装合成体（仅 completed 终态保证后调用；output 恒为数组——
// 空输出 [] 而非 null）。
func (a *responsesAggregator) composite() *responsesComposite {
	out := a.output
	if out == nil {
		out = []json.RawMessage{}
	}
	return &responsesComposite{
		ID:     a.id,
		Object: "response",
		Status: "completed",
		Output: out,
		Usage:  a.usage,
	}
}
