package codexsdk

import "sync"

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
func identityFor(thread, window, ts string, sink TurnStateSink) Identity {
	if thread == "" {
		return Identity{TurnState: ts, Sink: sink}
	}
	session := Session{SessionID: "sess-" + thread, ThreadID: thread, WindowID: window}
	meta := CodexMeta{
		InstallationID: "inst-test",
		SessionID:      session.SessionID,
		ThreadID:       thread,
		WindowID:       window,
	}
	return Identity{Session: session, Meta: meta, TurnState: ts, Sink: sink}
}
