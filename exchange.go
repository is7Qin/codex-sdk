package codexsdk

import (
	"context"
	"io"
	"time"

	coderws "github.com/coder/websocket"
)

// frameStream 是"一次请求 = 一条入站帧流"的取帧入口。sseView 只认这个接口，
// 具体实现是 wsExchange（连接内核之上的一层生命周期）。
type frameStream interface {
	// Next 交付下一帧；流结束后返回 EOF 或终止原因。
	Next(ctx context.Context) ([]byte, error)
	// Close 放弃本次请求（幂等）。
	Close() error
}

// wsExchange 是一次 Responses WS 请求的生命周期：一条连接 + 一份 turn-state
// 快照 + 取帧入口。conn 不知道"请求"，exchange 不知道"socket"。
type wsExchange struct {
	cn        *conn
	turnState string // 生效值（拨号前的快照）
	issued    string // 本次握手签发值
}

// openWSExchange 建立一次请求级 WS exchange。
//
// 创建次序是契约（写死）：取 turn-state 快照 → **编码**（本地校验在此，
// 坏请求体不烧握手）→ 拨号 → 回写本次签发值 → 发一帧。effective 必须在编码
// 前取：握手刚签发的值不出现在本轮请求里（与 HTTP 臂的请求/响应头时序一致）。
func (c *HTTPClient) openWSExchange(ctx context.Context, wire []byte, readLimit int64) (*wsExchange, error) {
	effective := c.effectiveTurnState()
	turn := uint64(0)
	if c.opts.turnProvider != nil {
		turn = 1 // 一次 exchange 只发一帧 ⇒ 恒为第 1 轮
	}
	frame, err := c.codec.encode(wire, encodeWSResponses, frameInput{
		TurnState: effective,
		Turn:      turn,
		Now:       time.Now(),
	})
	if err != nil {
		return nil, err
	}
	cn, hs, err := dialConn(ctx, dialSpec{
		auth:      c.auth,
		opts:      c.opts,
		readLimit: readLimit,
		mode:      modePumped,
		headers:   c.wsHeaders,
	})
	if err != nil {
		return nil, wsDialError(err)
	}
	// 本次握手签发值回写（HTTP 臂在这里读 resp.Header；WS 面只有这一个观测点）。
	// 顺序不能反：帧已按 effective 编码，回写不会污染本轮帧。
	c.captureTurnStateValue(hs.turnState)
	e := &wsExchange{cn: cn, turnState: effective, issued: hs.turnState}
	if err := cn.write(ctx, frame); err != nil {
		cn.terminate(err)
		return nil, err
	}
	return e, nil
}

// Next 交付下一帧。
//
//   - 入口 ctx 已结束：先尝试交付**已入队**的帧（上游已完成的结果不能被消费侧
//     超时改写），队列空才把取消当作本次请求的放弃；
//   - 有帧立即返回（取帧即提交：已取出的帧不再受 ctx 影响）；
//   - 流结束：close frame 归一为 io.EOF，其它终止原因原样返回。
func (e *wsExchange) Next(ctx context.Context) ([]byte, error) {
	if ctx.Err() != nil {
		if data, ok := e.cn.inbox.tryPop(); ok {
			return data, nil
		}
		e.cn.terminate(ctx.Err())
		return nil, ctx.Err()
	}
	data, err := e.cn.inbox.pop(ctx)
	if err == nil {
		return data, nil
	}
	if cerr := ctx.Err(); cerr != nil {
		e.cn.terminate(cerr) // 阻塞等待期间被取消：请求已放弃
		return nil, cerr
	}
	if coderws.CloseStatus(err) >= 0 {
		return nil, io.EOF
	}
	return nil, err
}

// Close 放弃本次 exchange（幂等）：收尾一律强断，pumped 模式不做关闭握手。
func (e *wsExchange) Close() error {
	e.cn.terminate(io.EOF)
	return nil
}

var _ frameStream = (*wsExchange)(nil)
