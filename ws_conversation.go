package codexsdk

import (
	"bytes"
	"context"
	"runtime"
	"sync/atomic"
	"time"

	"github.com/tidwall/gjson"
)

// wsConversationFIFO 是"已写出、等待 response.created"的帧身份队列容量。
const wsConversationFIFO = 16

// wsCreatedSpin 是 response.created 到达而 FIFO 为空、且有 Send 正在写帧时的
// 让出次数上限：写成功与入队之间只隔几条指令，上游不可能更早回帧，等待只发生在
// 这个极窄的窗口里。
const wsCreatedSpin = 1000

// conversationOption 是 WithConversation 的参数。
type conversationOption struct {
	book  *ConversationBook
	req   ConversationRequest
	first []byte
}

// WithConversation 只作用于 Dial（WS→WS）：身份全部由会话簿逐帧合成。first 与
// WithFirstFrame 的含义相同（随后第一次 Send 的帧，逐字节相同；Dial 用它算握手的
// x-codex-routing-hint 与 x-codex-turn-metadata，首次 Send 复用这次计算，不再
// Prepare）。req 提供 Key / InstallationID / Owner（Input 与 Now 由 SDK 逐帧计算，
// 传入值忽略）。
//
// 设置后 Dial 的 id 参数必须为零值，且不得同时设置 WithFirstFrame（构造期
// panic）；book 不能为 nil。逐帧规则：
//   - generate:false 帧：prewarm 形态，不开新轮（首帧时只取对话记录）；
//   - 不带 previous_response_id 的全量帧：会话簿 Prepare，开轮或续轮；
//   - 增量帧：输入以工具输出结尾 → 沿用连接当前轮次（没有则连接内临时轮次）；
//     否则在连接内开新轮（不读不写存储）。turn-state 取连接快照。
//
// Recv 按 response.created 的顺序把上游 turn-state 归给对应帧的轮次：先更新连接
// 快照（同一轮、同一 owner 首值生效），会话簿开的轮次再交给会话簿 Sink。
// 契约同 WithFirstFrame：first 在第一次 Send 前不得被修改。
func WithConversation(book *ConversationBook, req ConversationRequest, first []byte) Option {
	return func(o *options) { o.conversation = &conversationOption{book: book, req: req, first: first} }
}

// wsTurnSource 是连接当前轮次的来源。
type wsTurnSource uint8

const (
	turnSourceNone  wsTurnSource = iota // 还没有轮次（如首帧是预热）
	turnSourceBook                      // 会话簿 Prepare（ts 再交给会话簿 Sink）
	turnSourceLocal                     // 连接内开的轮次（只更新连接快照）
)

// wsConvState 是连接的会话快照（不可变，整体替换）。
type wsConvState struct {
	conv    *convEntry
	turn    Turn // ID / StartedAtMs / First（State 见 ts）
	source  wsTurnSource
	sink    TurnStateSink // 会话簿 Sink（source==turnSourceBook；nil = 丢弃）
	ts      string
	tsOwner [16]byte
	inject  uint64 // 上次插入的指令/工具项哈希
}

// wsFrameIdentity 是"本帧身份"：写帧成功后入 FIFO，response.created 时绑定。
type wsFrameIdentity struct {
	turnID string
	source wsTurnSource
	sink   TurnStateSink
}

// wsConversation 是 WithConversation 连接的会话状态。Send 与 Recv 并发安全：
// 快照用 atomic.Pointer 整体替换，FIFO 是 buffered channel；bound/boundID/open
// 只在 Recv 内使用（至多一个 goroutine 同时 Recv）。
type wsConversation struct {
	book  *ConversationBook
	key   ConversationKey
	inst  string
	owner [16]byte

	state     atomic.Pointer[wsConvState]
	fifo      chan *wsFrameIdentity
	untracked atomic.Bool  // FIFO 满过：整条连接不再捕获 ts
	inflight  atomic.Int32 // 正在写的帧数

	bound   *wsFrameIdentity
	boundID string
	open    bool
}

// identity 由快照组装请求身份（Sink 不经 Identity：ts 由 Recv 归属）。
func (s *wsConvState) identity(inst string) Identity {
	return Identity{
		InstallationID: inst,
		Session:        s.conv.session(),
		Window:         Window{Number: s.conv.rec.WindowN, ContextID: s.conv.rec.ContextWindowID},
		Turn:           Turn{ID: s.turn.ID, StartedAtMs: s.turn.StartedAtMs, State: s.ts, First: s.turn.First},
	}
}

// isZeroIdentity 报告 id 是否零值（不比较 Sink 的动态值，避免不可比较类型 panic）。
func isZeroIdentity(id Identity) bool {
	return id.InstallationID == "" && id.Session == (Session{}) && id.Window == (Window{}) &&
		id.Turn == (Turn{}) && id.Sink == nil
}

// prepareFrame 按 §5.8 为一帧（已 parse）决定连接的下一份快照。
func (cv *wsConversation) prepareFrame(ctx context.Context, p *parsedRequest, cur *wsConvState, now time.Time) *wsConvState {
	if bytes.Equal(p.vals[keyGenerate], litFalse) {
		return cur // 预热帧：不开新轮
	}
	in := analyzeInputValue(p.vals[keyInput])
	if !incrementalFrame(p) {
		// 全量帧：会话簿开轮/续轮。
		id, ce := cv.book.prepare(ctx, ConversationRequest{
			Key: cv.key, InstallationID: cv.inst, Owner: cv.owner, Input: in, Now: now,
		})
		if ce == nil { // 一次性身份
			ce = newConvEntry(ConversationRecord{
				Thread: id.Session.ThreadID, WindowN: id.Window.Number, ContextWindowID: id.Window.ContextID,
			}, now.UnixMilli())
		}
		ns := &wsConvState{
			conv:   ce,
			turn:   Turn{ID: id.Turn.ID, StartedAtMs: id.Turn.StartedAtMs, First: id.Turn.First},
			source: turnSourceBook,
			sink:   id.Sink,
			ts:     id.Turn.State,
			inject: cur.inject,
		}
		if ns.ts != "" {
			ns.tsOwner = cv.owner
		}
		return ns
	}
	// 增量帧：不读不写存储。
	if in.FollowUp() {
		if cur.turn.ID != "" {
			return cur
		}
		cv.book.stats.wsLocalTemporary.Add(1)
		ns := *cur
		ns.turn = Turn{ID: NewUUIDv7(), StartedAtMs: now.UnixMilli()}
		ns.source, ns.sink, ns.ts, ns.tsOwner = turnSourceLocal, nil, "", [16]byte{}
		return &ns
	}
	ns := *cur
	ns.turn = Turn{ID: NewUUIDv7(), StartedAtMs: now.UnixMilli(), First: true}
	ns.source, ns.sink, ns.ts, ns.tsOwner = turnSourceLocal, nil, "", [16]byte{}
	return &ns
}

// incrementalFrame 报告帧是否带非空 previous_response_id（增量帧）。
func incrementalFrame(p *parsedRequest) bool {
	s, ok := jsonStringValue(p.vals[keyPreviousResponseID])
	return ok && len(s) > 0
}

// dialConversation 在 Dial 时对首帧做一次计算：generate:false → 只取对话记录
// （prewarm 形态握手）；带 previous_response_id 的增量首帧 → 先取对话记录，再按
// 增量帧规则定轮次（turn 形态握手）；否则 Prepare（turn 形态握手）。返回握手身份
// 与首帧整形。
func dialConversation(ctx context.Context, opt *conversationOption, cfg shapeConfig) (*wsConversation, Identity, *firstFrameState, error) {
	now := time.Now()
	p, err := parseRequest(opt.first, surfaceWSNative, now)
	if err != nil {
		return nil, Identity{}, nil, err
	}
	cv := &wsConversation{
		book:  opt.book,
		key:   opt.req.Key,
		inst:  opt.req.InstallationID,
		owner: opt.req.Owner,
		fifo:  make(chan *wsFrameIdentity, wsConversationFIFO),
	}
	st := &wsConvState{}
	prewarm := bytes.Equal(p.vals[keyGenerate], litFalse)
	if prewarm || incrementalFrame(&p) {
		// 预热帧与增量首帧都没有全量输入可 Prepare：先补对话记录（缓存或 Open）。
		st.conv = opt.book.prepareConversation(ctx, ConversationRequest{
			Key: cv.key, InstallationID: cv.inst, Owner: cv.owner, Now: now,
		})
	}
	if !prewarm {
		st = cv.prepareFrame(ctx, &p, st, now)
	}
	cv.state.Store(st)
	id := st.identity(cv.inst)
	ff := &firstFrameState{frame: opt.first}
	ff.sh = shapeRequest(&p, id, now, cfg)
	return cv, id, ff, nil
}

// send 是 WithConversation 连接的 Send。
func (c *Client) sendConversation(ctx context.Context, frame []byte) error {
	cv := c.conv
	cur := cv.state.Load()
	next := cur
	var sh shapedRequest
	if p := c.first.Swap(nil); p != nil && bytes.Equal(p.frame, frame) {
		sh = p.sh // 首帧：复用 Dial 的计算，不再 Prepare
	} else {
		now := time.Now()
		p, err := parseRequest(frame, surfaceWSNative, now)
		if err != nil {
			return err
		}
		next = cv.prepareFrame(ctx, &p, cur, now)
		sh = shapeRequest(&p, next.identity(cv.inst), now, c.shape)
	}

	// 指令/工具插入去重（同静态身份）：全量帧照常插入并记下哈希；增量帧与连接
	// 上次相同则不插，不同则插入；连接此前已有插入记录时才计数。
	hash, record := sh.injectHash(), true
	if sh.clientPrevID != "" {
		switch {
		case hash == 0:
			record = false
		case hash == cur.inject:
			sh.inputPrefix, record = nil, false
		default:
			// 连接上的第一次插入不算“重新插入”（与静态身份一致）。
			if cur.inject != 0 {
				cv.book.stats.wsIncrementalInjects.Add(1)
				if c.shape.stats != nil {
					c.shape.stats.incrementalReinjects.Add(1)
				}
			}
		}
	}
	a := wsAssembly{prevID: sh.clientPrevID, generate: sh.clientGenerate}
	fid := &wsFrameIdentity{}
	if !sh.nativePrewarm {
		a.turnState = sh.turnState(next.identity(cv.inst))
		fid.turnID, fid.source, fid.sink = next.turn.ID, next.source, next.sink
	}
	wire, off := assembleWS(&sh, a)

	cv.inflight.Add(1)
	err := c.cn.writeStampedThen(ctx, wire, off, func() {
		cv.commit(next, hash, record)
		cv.push(fid)
	})
	cv.inflight.Add(-1)
	return err
}

// commit 在帧写出后发布下一份快照。同一轮次期间 Recv 捕获到的 ts 保留。
func (cv *wsConversation) commit(next *wsConvState, hash uint64, record bool) {
	for {
		old := cv.state.Load()
		if old == next && (!record || old.inject == hash) {
			return
		}
		ns := *next
		if ns.turn.ID == old.turn.ID && ns.ts == "" && old.ts != "" {
			ns.ts, ns.tsOwner = old.ts, old.tsOwner
		}
		if record {
			ns.inject = hash
		} else {
			ns.inject = old.inject
		}
		if cv.state.CompareAndSwap(old, &ns) {
			return
		}
	}
}

// push 把本帧身份入 FIFO；满则整条连接停止捕获 ts 并计数。
func (cv *wsConversation) push(f *wsFrameIdentity) {
	if cv.untracked.Load() {
		return
	}
	select {
	case cv.fifo <- f:
	default:
		if cv.untracked.CompareAndSwap(false, true) {
			cv.book.stats.wsUntracked.Add(1)
		}
	}
}

// pop 非阻塞取最早一份身份。spin 时，若 FIFO 为空而有 Send 正在写帧，短暂让出
// 等它入队（见 wsCreatedSpin）。
func (cv *wsConversation) pop(spin bool) *wsFrameIdentity {
	for i := 0; ; i++ {
		select {
		case f := <-cv.fifo:
			return f
		default:
		}
		if !spin || i >= wsCreatedSpin || cv.inflight.Load() == 0 {
			return nil
		}
		runtime.Gosched()
	}
}

// observe 是 WithConversation 连接 Recv 的逐帧观测（不改帧）。
func (cv *wsConversation) observe(raw []byte) {
	if cv.untracked.Load() {
		return
	}
	typ := frameEventType(raw)
	switch {
	case bytesAre(typ, responseEventCreated):
		cv.bound = cv.pop(true)
		cv.boundID = gjson.GetBytes(raw, "response.id").String()
		cv.open = true
	case bytesAre(typ, responseEventCompleted), bytesAre(typ, responseEventFailed),
		bytesAre(typ, responseEventIncomplete):
		cv.bound, cv.boundID, cv.open = nil, "", false
	case bytesAre(typ, responseEventError):
		if !cv.open {
			cv.pop(false) // 该 create 被上游拒绝，不会再有 created
		}
		cv.bound, cv.boundID, cv.open = nil, "", false
	case bytes.HasSuffix(typ, metadataEventSuffix):
		if cv.bound == nil || !bytes.Contains(raw, turnStateHeaderKey) {
			return
		}
		id, ts := metadataTurnState(raw)
		if ts == "" || id == "" || id != cv.boundID {
			return
		}
		cv.deliver(cv.bound, ts)
	}
}

// deliver 把 ts 交给绑定的帧身份：先 CAS 更新连接快照（快照当前轮次号等于该
// 身份的轮次号，且快照 TS 为空或 owner 不同）；会话簿开的轮次再交给会话簿 Sink
// （不依赖 CAS 是否成功：Sink 绑定的就是那一轮）。
func (cv *wsConversation) deliver(f *wsFrameIdentity, ts string) {
	if f.turnID != "" {
		for {
			s := cv.state.Load()
			if s.turn.ID != f.turnID {
				cv.book.stats.wsLateTS.Add(1)
				break
			}
			if s.ts != "" && s.tsOwner == cv.owner {
				break
			}
			ns := *s
			ns.ts, ns.tsOwner = ts, cv.owner
			if cv.state.CompareAndSwap(s, &ns) {
				break
			}
		}
	}
	if f.source == turnSourceBook && f.sink != nil {
		f.sink.SetTurnState(ts)
	}
}
