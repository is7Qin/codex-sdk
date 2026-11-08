package codexsdk

import (
	"context"
	"errors"
	"io"
	"net/http"
	"sync"
	"sync/atomic"

	coderws "github.com/coder/websocket"
	"github.com/tidwall/gjson"
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

// wsSSELineOverhead 是 wsReader 把一帧渲染成 SSE 时附加的字节余量：`event:`
// 行（含事件名）+ `data:` 前缀 + 帧尾空行。聚合扫描上限按它放大，使"恰好顶到
// WS ReadLimit 的合法帧"不会因为多出的 SSE 包装字节在扫描阶段撞上同一档上限
// （否则 16MiB 边界上 WS 臂比 HTTP 臂严苛数十字节）。
const wsSSELineOverhead = 1024

// wsReader 常驻读协程的队列参数（见 wsReader 类型注释）。
//
//   - wsQueueFrames：队列深度（帧数）。阻塞只会发生在"上千帧小对象"这种极端形态，
//     正常大帧先撞字节预算；
//   - wsQueueBytesBudget：队列字节预算。超限 ⇒ ErrWSBackpressure（快速失败，
//     **不**静默丢帧、也**不**无限缓冲）。
const (
	wsQueueFrames      = 256
	wsQueueBytesBudget = 8 << 20
)

// ErrWSBackpressure 下游消费慢于上游产出、队列字节预算被撑爆。
//
// 它是**传输层错误**而不是协议终态：调用方据此判失败/重试，不能当正常 EOF
// （否则截断会被读成"响应正常结束"，成功率统计失真）。
var ErrWSBackpressure = errors.New("codexsdk: ws 出站背压超队列预算")

// responseEventIncomplete 是上游的"未完成"终态事件。它**不在** responsesAggregator
// 的白名单里（聚合器不解析它 → 无 response.completed → 截断错误）——本常量只
// 供 WS reader 判定流已终结（不再读、也不补 [DONE]）。
const responseEventIncomplete = "response.incomplete"

// streamBodyWS 是 StreamBody 的 ws 臂：帧构造 → Dial → Send 一帧 → 合成 200
// 响应。对外形状与 HTTP 臂一致（StatusCode / Body / Header 三项——网关侧
// sdkbridge 与 pipeline 只读这三项，零改动）。
//
// 顺序是契约：**帧构造先于拨号**——本地坏请求体是确定性输入错误，不能为它烧
// 一次上游握手（否则拨号失败会盖掉本地错误，两臂状态面不等价）。
//
// turn-state：本方法认两个来源（前者优先）：
//  1. 调用方以 WithHeader(HeaderTurnState, ...) 下发的 per-request 值——网关既有的
//     注入手法（HTTP 面它落在请求头上，WS 面落在帧内 client_metadata）；该头
//     **不进升级头**（真实客户端只在帧里带，见 wsDialOptions）；
//  2. HTTPClient 缓存的"上一次响应/握手签发值"（c.TurnState()）。
//
// Dial 之后：先读 ws.TurnState()（本次握手签发值）回写 HTTPClient（HTTP 臂在这里
// 读的是 resp.Header.Get(HeaderTurnState)，语义相同），再 SetTurnState(生效值)
// 注入 WS client（SDK 在每帧 client_metadata 里回传）。顺序不能反：SetTurnState
// 会覆盖握手缓存，反了上游本次签发值就永久丢失（HTTP 臂靠响应头回写，WS 面只有
// 这一个观测点）。
//
// ctx 同时是 Recv 的 ctx（返回的 Body 每次读都用它），也是上游读写的唯一预算：
// 调用方取消能终止后续对 Body 的读取。
func (c *HTTPClient) streamBodyWS(ctx context.Context, payload []byte, readLimit int64) (*http.Response, error) {
	frame, err := buildResponsesCreateFrame(payload)
	if err != nil {
		return nil, err
	}
	effective := c.effectiveTurnState()
	ws, err := Dial(ctx, c.auth, c.wsDialOptions(readLimit)...)
	if err != nil {
		return nil, wsDialHTTPError(err)
	}
	issued := ws.TurnState()
	c.captureTurnStateValue(issued)
	// 无条件注入生效值（空串 = 清除 ws client 在 Dial 时捕获的**本次**握手签发值）。
	// 这一句是 HTTP 臂等价的关键：HTTP 面的 turn-state 只从"上一次响应头"流向
	// "下一次请求头"，本轮握手刚签发的值不会出现在本轮请求里（首轮更是完全没有）。
	// 不显式清空的话，Dial 的既有语义会让首帧带上刚签发的值——两臂的上游请求
	// 立刻不同。跨轮清除的责任仍在调用方（HTTPClient 的 TurnState 缓存按其生命周期）。
	_ = ws.SetTurnState(effective)
	if err := ws.Send(ctx, frame); err != nil {
		_ = ws.CloseNow()
		return nil, err
	}
	hdr := make(http.Header, 2)
	hdr.Set("Content-Type", "text/event-stream")
	if issued != "" {
		hdr.Set(HeaderTurnState, issued)
	}
	return &http.Response{
		Status:        "200 OK",
		StatusCode:    http.StatusOK,
		Proto:         "HTTP/1.1",
		ProtoMajor:    1,
		ProtoMinor:    1,
		Header:        hdr,
		Body:          newWSReader(ctx, ws, readLimit),
		ContentLength: -1,
	}, nil
}

// responsesStreamWS 是 Responses 的 ws 臂取数：把 StreamBody（ws）交付的 SSE
// 字节喂给**同一个** responsesAggregator（HTTP 臂走 Stream，同样只做
// scanner + feed）。合成体因此与 HTTP 臂逐字段同构——这不是巧合，而是同一份
// feed/composite 代码的结果。
func (c *HTTPClient) responsesStreamWS(ctx context.Context, payload []byte, feed func(raw []byte) error) error {
	resp, err := c.streamBodyWS(ctx, payload, wsResponsesReadLimit)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	// 扫描上限 = WS 单帧上限 + SSE 包装余量（见 wsSSELineOverhead）。
	_, err = scanSSE(resp.Body, wsResponsesReadLimit+wsSSELineOverhead, feed)
	return err
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

// wsDialOptions 把 HTTPClient 的配置沿用到 WS 拨号与帧组装——这是"网关零
// 改动"的前提：同一个 codex 模板的身份 / 出口 / 伪装配置在两个传输上等价。
//
// 转发的项与理由：
//   - WithTransport：出口（代理 / RoundTripper）与 HTTP 面同一注入点；
//   - WithSession / WithCodexMeta：握手头 + 帧内 client_metadata 身份；
//   - WithHeader：调用方头（含 UA/originator 覆盖、OpenAI-Beta、responses-lite
//     标记头……）——与 HTTP 面的请求头是同一份配置；唯一例外是
//     x-codex-turn-state（见下）；
//   - WithClientMetadata / WithTraceContext / WithTraceAuto / WithTurnAuto /
//     WithTurnMetadata：帧面扩展（HTTP 面本就忽略这些项，WS 面不该丢）；
//   - WithCompression / WithPingInterval / WithBeta：传输层参数（默认值原本就
//     一致，显式配置时以调用方为准）；
//   - WithReadLimit：本路径按取数形态给定（64MiB 流式 / 16MiB 非流式）。
//
// x-codex-turn-state 头**不转发到升级头**：真实 codex 客户端的 WS 面只在帧内
// client_metadata 里带它（doc.go："WS：升级响应头签发 → 帧内回传"），升级头只
// 有 Authorization / UA / Originator / OpenAI-Beta / 会话标识 / 账号标识。该值
// 由 effectiveTurnState 取出后注入 ws client（帧面），语义与 HTTP 臂的请求头一致。
//
// 不转发 WithTimeout / WithMaxLineSize：前者是 http.Client 总超时（WS 的预算
// 是调用方 ctx，等效约束由 ctx 承担），后者只作用于 SSE 行扫描。
func (c *HTTPClient) wsDialOptions(readLimit int64) []Option {
	opts := make([]Option, 0, 8+len(c.opts.headers)+len(c.opts.metaPassthrough))
	opts = append(opts,
		WithTransport(c.opts.transport),
		WithCompression(c.opts.compression),
		WithPingInterval(c.opts.pingInterval),
		WithBeta(c.opts.beta),
		WithTraceAuto(c.opts.traceAuto),
		WithTurnAuto(c.opts.turnAuto),
	)
	if c.opts.session != nil {
		opts = append(opts, WithSession(*c.opts.session))
	}
	if c.opts.meta != nil {
		opts = append(opts, WithCodexMeta(*c.opts.meta))
	}
	if c.opts.trace != nil {
		opts = append(opts, WithTraceContext(*c.opts.trace))
	}
	if c.opts.turnProvider != nil {
		opts = append(opts, WithTurnMetadata(c.opts.turnProvider))
	}
	for _, e := range c.opts.metaPassthrough {
		opts = append(opts, WithClientMetadata(e.key, e.value))
	}
	for key, values := range c.opts.headers {
		if http.CanonicalHeaderKey(key) == http.CanonicalHeaderKey(HeaderTurnState) {
			continue // turn-state 只进帧内 client_metadata（见上）
		}
		for _, v := range values {
			opts = append(opts, WithHeader(key, v))
		}
	}
	return append(opts, WithReadLimit(readLimit))
}

// wsDialHTTPError 把 WS 升级失败翻译成 HTTP 面同形的错误信封：状态码沿用升级
// 响应，body 用 DialError.Body（≤1024B 的截断响应体）。
//
// 为什么要带 body：HTTP 臂的 4xx 带 body，网关的 error_message_contains 规则
// 与判死分类都靠它；升级失败同样有 body（coder/websocket 会把非 101 的响应体
// 留回来），不带就会让同一个错误在 WS 臂命中不了规则，成功率之差混进了"规则
// 引擎差异"而不是传输差异。
//
// 非 DialError 的错误原样透传：auth 为 nil、OAuth refresh 的 fatal 类错误
// （AuthPermanentlyRevokedError 等）必须留在 errors.As 链上，包成 HTTPError 会
// 让网关的判死分类失效（与 HTTP 臂 doURL 的透传语义一致）。
func wsDialHTTPError(err error) error {
	var de *DialError
	if errors.As(err, &de) {
		return &HTTPError{StatusCode: de.StatusCode, Raw: de.Body}
	}
	return err
}

// wsReader 把上游 WS 帧拉取成 SSE 字节流（io.ReadCloser）。
//
// **常驻读协程，不是纯拉模型**。构造时起一个 goroutine 持续 Recv，把渲染好的 SSE
// 块压进有界队列；Read 只从队列取。这不是性能优化，是正确性：
// coder/websocket 的 Ping **自己不读连接**，它只是在 ping 之后等"某次 Reader 调用
// 把 pong 读上来"（conn.go:218-220 原文：Ping must be called concurrently with
// Reader as it does not read from the connection but instead waits for a Reader
// call to read the pong）。纯拉模型下，下游一慢（网关把字节写给客户端被阻塞）就
// 没有 Read 在飞，30s 心跳的 ping 等到 2s 超时就会 CloseNow——这是一条**只属于
// WS 臂**的截断源，HTTP 臂没有，两臂的成功率差里会混进它。常驻读协程让"上游保活"
// 与"下游背压"解耦。
//
// 渲染规则（与 HTTP 裸 body 逐字节等价）：
//   - 每帧产出 `event:<data.type>\n` + `data:<frame>\n\n`，帧字节 **verbatim**
//     （不再序列化：字段名集合与顺序与上游逐字节一致）；
//   - `response.completed` → 本帧后补 `data: [DONE]\n\n` 并终止；
//   - `response.failed` / `response.incomplete` → 作为**帧**透传后终止（不补
//     [DONE]：失败终态不该被读成"正常收尾"）；
//   - 其余帧（含 `type:"error"`——**不分** created 前后）原样透传继续读：
//     HTTP 臂的 200 流里 in-band error 也是普通帧，由下游按终态事件判定；
//   - 无 type 的帧只出 data 行（不写空的 `event:` 名）。
//
// 关闭语义（与 HTTP 裸 body 的 EOF 等价）：收到**任意** close frame
// （CloseStatus >= 0，含 1000/1001/1005/1011）= EOF，交给下游按"是否见过终态
// 事件"判定截断；只有 CloseStatus == -1（对端没有发 close frame：直接断 TCP /
// 连接重置 / 超 ReadLimit）才是传输错误，原样返回底层错误（ErrMessageTooBig /
// net.OpError 等）。
//
// 背压：队列字节预算 wsQueueBytesBudget。超限 ⇒ ErrWSBackpressure——**不是**静默
// 丢弃，也不是无限缓冲。单帧大于预算但队列为空时照常通过：64MiB 的合法巨帧不能
// 因为预算比它小就被判死。
//
// 并发契约：Read 与 Close 由**同一个** goroutine 串行调用（网关读循环 + 循环结束
// 后的 Close）；pending/err/finished 因此无锁，err 的可见性由 frames 关闭建立
// happens-before。Close 会等读协程退出（返回后不再有并发 Recv）。
type wsReader struct {
	ctx    context.Context
	cancel context.CancelFunc
	ws     *Client
	limit  int64
	frames chan []byte
	done   chan struct{}
	queued atomic.Int64

	// 以下只被消费侧（Read/Close 的调用者）访问。
	pending  []byte
	err      error
	finished bool

	closeOnce sync.Once
}

var _ io.ReadCloser = (*wsReader)(nil)

// newWSReader 构造 WS→SSE reader 并**立即**启动常驻读协程。
// readLimit 与 Dial 的 WithReadLimit 同值：conn 层已按它截断，reader 再显式守
// 一道（错误统一为 ErrMessageTooBig，不依赖底层实现细节）。
func newWSReader(ctx context.Context, ws *Client, readLimit int64) *wsReader {
	cctx, cancel := context.WithCancel(ctx)
	r := &wsReader{
		ctx:    cctx,
		cancel: cancel,
		ws:     ws,
		limit:  readLimit,
		frames: make(chan []byte, wsQueueFrames),
		done:   make(chan struct{}),
	}
	go r.pump()
	return r
}

// pump 常驻读协程：Recv → 渲染 → 入队。终止时关闭 frames/done，终止原因留在 r.err。
func (r *wsReader) pump() {
	defer close(r.done)
	defer close(r.frames)
	for {
		raw, err := r.ws.Recv(r.ctx)
		if err != nil {
			// 调用方主动 Close（ctx 取消）⇒ 不是错误，Read 侧按 EOF 处理。
			if r.ctx.Err() == nil {
				r.err = wsRecvError(err)
			}
			return
		}
		block, terminal, berr := renderWSBlock(raw, r.limit)
		if berr != nil {
			r.err = berr
			return
		}
		// 队列里还有货时才检查预算：空队列允许任意大的单帧通过（见类型注释）。
		if q := r.queued.Load(); q > 0 && q+int64(len(block)) > wsQueueBytesBudget {
			r.err = ErrWSBackpressure
			return
		}
		select {
		case r.frames <- block:
			r.queued.Add(int64(len(block)))
		case <-r.ctx.Done():
			return
		}
		if terminal {
			return
		}
	}
}

// Read 实现 io.Reader：一次最多消费当前块剩余字节。
func (r *wsReader) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	for len(r.pending) == 0 {
		if r.finished {
			return 0, io.EOF
		}
		block, ok := <-r.frames
		if !ok {
			r.finished = true
			if r.err != nil {
				return 0, r.err
			}
			return 0, io.EOF
		}
		r.queued.Add(-int64(len(block)))
		r.pending = block
	}
	n := copy(p, r.pending)
	r.pending = r.pending[n:]
	return n, nil
}

// Close 关闭上游连接：取消读协程 + CloseNow（跳过关闭握手：终态已到 / 出错 /
// 取消，等 5s 关闭帧没有收益），并等读协程退出（保证返回后不再有并发 Recv）。
// 幂等：重复调用只等同一个 done。
func (r *wsReader) Close() error {
	r.closeOnce.Do(func() {
		r.cancel()
		_ = r.ws.CloseNow()
	})
	<-r.done
	return nil
}

// renderWSBlock 把一帧渲染成 SSE 块，并报告它是否终结了本流。
func renderWSBlock(raw []byte, limit int64) ([]byte, bool, error) {
	if limit > 0 && int64(len(raw)) > limit {
		return nil, false, ErrMessageTooBig
	}
	typ := gjson.GetBytes(raw, "type").String()
	block := make([]byte, 0, len(typ)+len(raw)+8)
	if typ != "" {
		block = append(block, "event:"...)
		block = append(block, typ...)
		block = append(block, '\n')
	}
	block = append(block, "data:"...)
	block = append(block, raw...)
	block = append(block, '\n', '\n')
	switch typ {
	case responseEventCompleted:
		return append(block, "data: [DONE]\n\n"...), true, nil
	case responseEventFailed, responseEventIncomplete:
		return block, true, nil
	}
	return block, false, nil
}

// wsRecvError 把 Recv 的终止错误翻译成"流结束"语义：任意 close frame = EOF（nil）。
func wsRecvError(err error) error {
	if coderws.CloseStatus(err) >= 0 {
		return nil
	}
	return err
}
