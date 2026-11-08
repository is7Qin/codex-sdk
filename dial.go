package codexsdk

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"

	coderws "github.com/coder/websocket"
)

// dialErrBodyLimit 是升级失败响应体的保留上限。与 HTTP 臂的错误体截断量级一致：
// 网关只从里面取 error.message / error_code，不需要整篇 body。
const dialErrBodyLimit = 1024

// dialSpec 是一次拨号所需的全部输入。headers 是升级头的调用方覆盖集
// （构造期已按面算好——pumped 面必须去掉 x-codex-turn-state，见 http.go）。
type dialSpec struct {
	auth      Auth
	opts      options
	readLimit int64
	mode      connMode
	headers   http.Header
}

// handshake 是握手成功后从上游取到的东西（目前只有 turn-state 签发值）。
type handshake struct {
	turnState string
}

// dialConn 拨号 + 升级 + 401 轮转，返回连接内核与握手结果。
//
// ctx 只是**握手预算**；连接寿命 ctx 在这里定：pull = Background + 自身 cancel，
// pumped = 请求 ctx（已按 WithTimeout 设好 deadline）+ 自身 cancel。
func dialConn(ctx context.Context, spec dialSpec) (*conn, handshake, error) {
	if spec.auth == nil {
		return nil, handshake{}, errors.New("codexsdk: auth 不能为 nil（用 PAT 或 OAuth）")
	}
	wsURL := DefaultResponsesWSURL

	// 升级头组装（重连时以新 at 重取）。
	buildHeaders := func(ctx context.Context) (http.Header, error) {
		hdr := make(http.Header, 10)
		token, err := spec.auth.Authorization(ctx)
		if err != nil {
			return nil, fmt.Errorf("codexsdk: 获取鉴权信息失败: %w", err)
		}
		hdr.Set("Authorization", token)
		// 伪装层默认头（WithHeader 可覆盖）。
		applyDefaultHeaders(hdr)
		hdr.Set("OpenAI-Beta", "responses_websockets="+spec.opts.beta)
		// 会话标识握手头（真实客户端恒带；trace 不进握手头，只进帧内 metadata）。
		if s := spec.opts.session; s != nil {
			if s.SessionID != "" {
				hdr.Set(HeaderSessionID, s.SessionID)
			}
			if s.ThreadID != "" {
				hdr.Set(HeaderThreadID, s.ThreadID)
			}
			clientRequestID := s.ClientRequestID
			if clientRequestID == "" {
				clientRequestID = s.ThreadID
			}
			if clientRequestID != "" {
				hdr.Set(HeaderClientRequestID, clientRequestID)
			}
			if s.WindowID != "" {
				hdr.Set(HeaderWindowID, s.WindowID)
			}
		}
		// 账号标识头（ChatGPT 系 auth 恒发；空 = 不发，向后兼容）。
		applyAccountID(hdr, spec.auth)
		// 调用方头：覆盖默认头（先删后加），同名多次调用为扩展。
		applyHeaderOverrides(hdr, spec.headers)
		return hdr, nil
	}

	dialWS := func(ctx context.Context) (*coderws.Conn, *http.Response, error) {
		hdr, err := buildHeaders(ctx)
		if err != nil {
			return nil, nil, err
		}
		dopts := &coderws.DialOptions{HTTPHeader: hdr}
		if spec.opts.transport != nil {
			dopts.HTTPClient = &http.Client{Transport: spec.opts.transport}
		}
		if spec.opts.compression != CompressionDisabled {
			dopts.CompressionMode = spec.opts.compression.coder()
			dopts.CompressionThreshold = defaultCompressionThreshold
		}
		return coderws.Dial(ctx, wsURL, dopts)
	}

	wsConn, resp, err := dialWS(ctx)
	if err != nil {
		if resp == nil {
			// 无响应（连接拒绝 / 拨号中取消 / DNS 失败）：原始错误链透传，
			// 不构造 DialError —— 否则 errors.Is(ctx.Canceled) 失效，且
			// *HTTPError{0} 会被读成"上游 0 状态码"。
			return nil, handshake{}, dialNoResponse(err)
		}
		status := dialStatus(resp)
		if status == http.StatusUnauthorized {
			// WS 升级 401 → 可轮转：单飞 refresh → 自动重连一次。
			rotated, rerr := rotateOn401(ctx, spec.auth)
			if rerr != nil {
				return nil, handshake{}, rerr // refresh 失败（fatal / RefreshError）透传
			}
			if !rotated {
				return nil, handshake{}, dialUpgradeError(status, err, resp) // PAT/oauthAuth：原样返回
			}
			wsConn, resp, err = dialWS(ctx)
			if err != nil {
				if resp == nil {
					return nil, handshake{}, dialNoResponse(err)
				}
				de := dialUpgradeError(dialStatus(resp), err, resp)
				de.Refreshed = true
				return nil, handshake{}, de
			}
		} else {
			return nil, handshake{}, dialUpgradeError(status, err, resp)
		}
	}
	wsConn.SetReadLimit(spec.readLimit)

	hs := handshake{}
	if resp != nil {
		hs.turnState = resp.Header.Get(HeaderTurnState)
	}

	// 连接寿命 ctx：
	//   - pumped：请求 ctx，WithTimeout>0 时在同一处设 deadline（=0 时就是调用方 ctx）；
	//   - pull：与调用方 ctx 解耦（今日语义：Dial 的 ctx 只约束握手）。
	// handshake 用的是调用方 ctx：它就是握手预算。
	baseCtx := ctx
	if spec.mode == modePull {
		baseCtx = context.Background()
	}
	var connCtx context.Context
	var stop context.CancelFunc
	if spec.mode == modePumped && spec.opts.timeout > 0 {
		connCtx, stop = context.WithTimeout(baseCtx, spec.opts.timeout)
	} else {
		connCtx, stop = context.WithCancel(baseCtx)
	}

	cfg := connConfig{
		mode:         spec.mode,
		readLimit:    spec.readLimit,
		pingInterval: spec.opts.pingInterval,
		pongTimeout:  defaultPongTimeout,
		queueFrames:  defaultQueueFrames,
		queueBytes:   defaultQueueBytes,
	}
	return newConn(wsConn, cfg, connCtx, stop), hs, nil
}

// dialNoResponse 包装"拨号失败但没有升级响应"的原始错误链（保留 errors.Is/As）。
func dialNoResponse(err error) error {
	return fmt.Errorf("codexsdk: websocket 升级失败: %w", err)
}

// dialUpgradeError 构造"上游拒绝升级"的 *DialError（状态码 + 截断 body）。
func dialUpgradeError(status int, err error, resp *http.Response) *DialError {
	return &DialError{StatusCode: status, Err: err, Body: dialErrBody(resp)}
}

// dialStatus 提取升级失败响应状态码（coderws 非 101 时返回响应）。
func dialStatus(resp *http.Response) int {
	if resp != nil {
		return resp.StatusCode
	}
	return 0
}

// dialErrBody 读出并关闭升级失败响应的截断响应体（nil/空 → nil）。
//
// 调用点只在上游拒绝升级（非 101）时：此时 coder/websocket 已把 resp 交回，
// 但**没有**读过 body——不读就是永久丢弃（两臂错误分类面不对称）。
func dialErrBody(resp *http.Response) []byte {
	if resp == nil || resp.Body == nil {
		return nil
	}
	defer func() { _ = resp.Body.Close() }()
	b, err := io.ReadAll(io.LimitReader(resp.Body, dialErrBodyLimit))
	if err != nil || len(b) == 0 {
		return nil
	}
	return b
}
