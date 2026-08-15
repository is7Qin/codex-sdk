package codexsdk

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

// responses 聚合白名单事件类型（白名单外跳过——上游扩展兼容）。
// response.rejected 不在白名单：其为终态事件 → 无 response.completed →
// 截断错误兜底（与"不做流中断恢复"边界一致）。
const (
	responseEventCreated   = "response.created"
	responseEventItemDone  = "output_item.done"
	responseEventCompleted = "response.completed"
	responseEventFailed    = "response.failed"
)

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
// 返回 HTTPResponse{StatusCode, Raw, TurnState}（与 Do 同形态；TurnState 读
// c.TurnState()——Stream 内部已捕获（http.go captureTurnState），非 2xx 路径
// 本就为空）。
// 错误透传同 Stream（HTTPError 信封 / fatal 五类 / 401 判死轮转）。
func (c *HTTPClient) Responses(ctx context.Context, payload []byte) (*HTTPResponse, error) {
	wire := payload
	// 无条件覆盖 stream:true（含显式 stream:false）；非法 JSON 放弃注入保持原样
	// （对齐 injectClientMetadataKeys 失败语义 + FilterCodexPayload 的
	// gjson.ValidBytes 先例——sjson.SetBytes 对非法 JSON 不报错而是静默产出
	// 损坏字节，须前置有效性校验）。
	if gjson.ValidBytes(payload) {
		if injected, err := sjson.SetBytes(payload, "stream", true); err == nil {
			wire = injected
		}
	}
	agg := &responsesAggregator{}
	err := c.Stream(ctx, wire, agg.feed)
	if err != nil {
		return nil, err // 上游错误（HTTPError/fatal 五类/401 轮转）/ response.failed 事件级错误透传
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
	return &HTTPResponse{
		StatusCode: http.StatusOK,
		Raw:        body,
		TurnState:  c.TurnState(),
	}, nil
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
		if usage := gjson.GetBytes(raw, "usage"); usage.Exists() {
			a.usage = json.RawMessage(usage.Raw)
		}
	case responseEventFailed:
		return a.failedError(raw)
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
