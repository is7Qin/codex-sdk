package codexsdk

import (
	"bytes"
	"context"
	"strings"
	"sync/atomic"

	"github.com/tidwall/gjson"
)

// metadataEventSuffix 是 metadata 类事件的类型后缀（本 SDK 认 `*.metadata`；
// 真实上游当前只发 `response.metadata`）。
var metadataEventSuffix = []byte(".metadata")

// turnStateHeaderKey 是快筛用的字节形态（避免每帧把常量串转成 []byte）。
var turnStateHeaderKey = []byte(HeaderTurnState)

// turnStateTap 逐帧旁路观测 turn-state：命中才解析，**绝不改帧内容**。
//
// 为什么要它：跨轮复用连接时没有新握手，新 ts 只能从事件来（spec §1 推论）。
//
// 判定顺序（写死，见 spec §3.1-S3）：
//  1. **每帧**先取事件类型（frameEventType，零分配）；非 `*.metadata` 的帧只做
//     一件事：类型是 `response.created` 就解析并记下本轮 `response_id`，然后
//     原样透传（v1.2 的"先看子串"会永远记不到 id，事件 ts 全被丢）；
//  2. `*.metadata` 才做 `x-codex-turn-state` 子串快筛，未命中透传；
//  3. 归属判定：`metadata.response_id == seenID && seenID != ""` ⇒ 接受；否则
//     dropped+1（**只丢 ts 候选，不丢帧**）。这条防的是复用连接上残留下来的旧
//     `*.metadata` 把旧 ts 覆盖成新值（exchange 不再做首帧判定，所以这里必须自己
//     持有归属条件）。fail-safe：`response.created` 到达前的任何 metadata 一律不
//     捕获——漏掉一个 ts 远好于写进一个错误的值；
//  4. 接受 ⇒ 写入 id.Sink（nil = 忽略），然后原样透传该帧。
//
// 帧契约：一帧进、一帧出，不复制、不改写、不重排、不丢弃任何帧。三个方法必须
// 全部转发——`Release` 是**唯一**把连接交回缓存的出口，漏了复用直接失效。
type turnStateTap struct {
	inner frameStream
	obs   turnStateObserver
}

// turnStateObserver 是 turn-state 的逐帧观测状态（HTTP→WS 的 tap 与 WS→WS 的
// Client.Recv 共用）。只在单个读 goroutine 内使用，无锁。
type turnStateObserver struct {
	sink    TurnStateSink // 捕获到本次 response 的值（非空）时的落点；nil = 忽略
	seenID  string        // 从 response.created 记下的本次 response_id
	dropped *atomic.Int64 // 归属判定丢弃计数（可空）
}

var _ frameStream = (*turnStateTap)(nil)

// Next 转发取帧并在命中时旁路观测。
func (t *turnStateTap) Next(ctx context.Context) ([]byte, error) {
	raw, err := t.inner.Next(ctx)
	if err != nil {
		return raw, err
	}
	t.obs.observe(raw)
	return raw, nil
}

// MarkFailed 转发"失败终态"标记（Release 的归还准入，判定由外层做）。
func (t *turnStateTap) MarkFailed() { t.inner.MarkFailed() }

// Release 转发归还（唯一归还出口）。
func (t *turnStateTap) Release() bool { return t.inner.Release() }

// Close 转发放弃（幂等语义由内层保证）。
func (t *turnStateTap) Close() error { return t.inner.Close() }

// observe 按上面的判定顺序观测一帧；不修改 raw。
func (t *turnStateObserver) observe(raw []byte) {
	typ := frameEventType(raw)
	if !bytes.HasSuffix(typ, metadataEventSuffix) {
		if bytesAre(typ, responseEventCreated) {
			t.seenID = gjson.GetBytes(raw, "response.id").String()
		}
		return
	}
	if !bytes.Contains(raw, turnStateHeaderKey) {
		return
	}
	id, ts := metadataTurnState(raw)
	if ts == "" {
		return
	}
	if t.seenID != "" && id == t.seenID {
		if t.sink != nil {
			t.sink.SetTurnState(ts)
		}
		return
	}
	if t.dropped != nil {
		t.dropped.Add(1)
	}
}

// metadataTurnState 从 `*.metadata` 事件里取 (response_id, ts)：
// 顶层 `response_id` + `headers` 对象里键名大小写不敏感匹配 x-codex-turn-state
// （对齐真实 codex 的 header_turn_state_value_from_json）。无 ts ⇒ 空串。
func metadataTurnState(raw []byte) (id, ts string) {
	id = gjson.GetBytes(raw, "response_id").String()
	headers := gjson.GetBytes(raw, "headers")
	if !headers.IsObject() {
		return id, ""
	}
	headers.ForEach(func(k, v gjson.Result) bool {
		if strings.EqualFold(k.String(), HeaderTurnState) {
			ts = v.String()
			return false
		}
		return true
	})
	return id, ts
}

// turnStateTapFor 给一次 exchange 装观测装饰器（两条臂共用这一处构造）。
// sink 是本次请求的 Identity.Sink（nil-safe）。
//
// dropped 挂在 HTTPClient 上：一次 exchange 的 tap 是短命的，观测计数要在
// 请求结束后仍可读（测试与运维诊断都不必持有 exchange）。
func (c *HTTPClient) turnStateTapFor(inner frameStream, sink TurnStateSink) *turnStateTap {
	return &turnStateTap{
		inner: inner,
		obs:   turnStateObserver{sink: sink, dropped: &c.turnStateDropped},
	}
}
