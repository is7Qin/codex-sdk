package codexsdk

import (
	"io"
	"net/http"
	"strings"
	"sync"

	"github.com/klauspost/compress/zstd"
)

// client_metadata 键名与帧 type（测试断言用）。
const (
	codexMetaInstallationKey = "x-codex-installation-id"
	codexMetaSessionKey      = "session_id"
	codexMetaThreadKey       = "thread_id"
	codexMetaTurnKey         = "turn_id"
	codexMetaWindowKey       = "x-codex-window-id"
	codexMetaTurnStateKey    = "x-codex-turn-state"
	responsesCreateType      = "response.create"
)

// turnStateSink 是测试用 TurnStateSink：记录每次回调（读路径可能来自不同
// goroutine，加锁）。
type turnStateSink struct {
	mu   sync.Mutex
	seen []string
}

func (s *turnStateSink) SetTurnState(ts string) {
	s.mu.Lock()
	s.seen = append(s.seen, ts)
	s.mu.Unlock()
}

func (s *turnStateSink) values() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.seen...)
}

func (s *turnStateSink) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.seen)
}

// identityFor 构造一个稳定的请求级身份：thread 决定连接复用键，window 决定
// 帧内 window_id，ts 是本次请求下发的 turn-state（"" = 不带），sink 可为 nil。
// thread 为空 = 匿名（匿名不带 turn-state）。
func identityFor(thread, window, ts string, sink TurnStateSink) Identity {
	if thread == "" {
		return Identity{Turn: Turn{State: ts}, Sink: sink}
	}
	return Identity{
		InstallationID: "inst-test",
		Session:        Session{SessionID: "sess-" + thread, ThreadID: thread, WindowID: window},
		Turn:           Turn{State: ts},
		Sink:           sink,
	}
}

// readRequestBody 读出请求体；Content-Encoding: zstd 时先解压（/responses 请求体
// 恒为 zstd）。
func readRequestBody(r *http.Request) ([]byte, error) {
	raw, err := io.ReadAll(r.Body)
	if err != nil || !strings.EqualFold(r.Header.Get("Content-Encoding"), "zstd") {
		return raw, err
	}
	return decodeZstd(raw)
}

// decodeZstd 解压一个 zstd 帧。
func decodeZstd(raw []byte) ([]byte, error) {
	dec, err := zstd.NewReader(nil)
	if err != nil {
		return nil, err
	}
	defer dec.Close()
	return dec.DecodeAll(raw, nil)
}
