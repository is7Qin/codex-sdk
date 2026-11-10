package codexsdk

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"

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
	identity  Identity // 握手头来源（帧内身份由编码器直接取）
}

// handshake 是握手成功后从上游取到的东西。
//
// token 是本次升级头里实际使用的 Authorization（401 轮转后是新的那一份）：
// 连接复用的取用判定要拿它跟当次请求的 Authorization 比对，OAuth 轮转后旧连接
// 不能将就。bornAt 是拨号起始时刻，是本地寿命上限（connMaxAge）的起点——
// 它随连接在缓存里逐次传递，命中不重置。
type handshake struct {
	turnState string
	token     string
	bornAt    time.Time
}

// dialConn 拨号 + 升级 + 401 轮转，返回连接内核与握手结果。
//
// ctx 只是**握手预算**；连接寿命 ctx 在这里定（两支都是 Background + 自身 cancel）：
// 连接要跨请求复用，不能再让请求结束时的 defer cancel() 杀掉 socket。单次请求的
// 时限由 openWSExchange 的 exchangeCtx 负责（含拨号/升级）。
func dialConn(ctx context.Context, spec dialSpec) (*conn, handshake, error) {
	if spec.auth == nil {
		return nil, handshake{}, errors.New("codexsdk: auth 不能为 nil（用 PAT 或 OAuth）")
	}
	wsURL := DefaultResponsesWSURL
	bornAt := time.Now()
	// dialedToken 是最后一次成功升级所用的 Authorization（dialWS 填入）。
	var dialedToken string

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
		if s := spec.identity.Session; s.SessionID != "" || s.ThreadID != "" || s.ClientRequestID != "" || s.WindowID != "" {
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
		dialedToken = hdr.Get("Authorization")
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

	hs := handshake{token: dialedToken, bornAt: bornAt}
	if resp != nil {
		hs.turnState = resp.Header.Get(HeaderTurnState)
	}

	// 连接寿命 ctx：
	//   - pumped：**与请求 ctx 解耦**（T5 起）。连接要跨请求复用，所以不能再让
	//     请求结束时的 defer cancel() 杀掉 socket；请求 ctx 只作握手预算。
	//     单次请求的时限由 openWSExchange 的 exchangeCtx 负责（含拨号/升级）。
	//   - pull：同今日语义（Dial 的 ctx 只约束握手）。
	// handshake 用的是调用方 ctx：它就是握手预算。
	connCtx, stop := context.WithCancel(context.Background())

	cfg := connConfig{
		mode:        spec.mode,
		readLimit:   spec.readLimit,
		queueFrames: defaultQueueFrames,
		queueBytes:  defaultQueueBytes,
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
