package codexsdk

import (
	"bytes"
	"context"
	"sync/atomic"
	"time"

	"github.com/tidwall/gjson"
)

// wsConversationFIFO 是"已登记、等待 response.created"的帧身份队列容量。
const wsConversationFIFO = 16

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
//     否则在连接内开新轮（不读不写存储）。turn-state 取连接快照；
//   - 首帧是增量帧时，Dial 先取对话记录（缓存或 Open），再按增量帧规则定轮次；
//   - 客户端预热帧拿到的 turn-state 存进连接快照：随后本连接新开的一轮若没有同
//     owner 的 turn-state，就带上它（只用一次；会话簿开的轮次同时记进会话簿）。
//
// 每帧的身份在写闸内、写帧之前登记（发布快照并压入 FIFO），写失败则撤销，因此
// FIFO 顺序等于帧在线上的顺序，上游再快的 response.created 也不会先于登记到达。
// Recv 按 response.created 的顺序把上游 turn-state 归给对应帧的轮次：先更新连接
// 快照（同一轮、同一 owner 首值生效），会话簿开的轮次再交给会话簿 Sink。
//
// 并发 Send 时各帧的轮次推进顺序不保证（快照在写闸外计算）；需要确定轮次顺序的
// 调用方应串行 Send。Dial 会先做一次会话簿查询（Load/Open/Advance 各 ≤30ms），
// 存储慢时会推迟拨号。
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
	conv    *convEntry // 快照时刻的对话条目：Flush 回填不追（下一次全量帧 Prepare 时更新）
	turn    Turn       // ID / StartedAtMs / First（State 见 ts）
	source  wsTurnSource
	sink    TurnStateSink // 会话簿 Sink（source==turnSourceBook；nil = 丢弃）
	ts      string
	tsOwner [16]byte
	inject  uint64 // 上次插入的指令/工具项哈希
	// prewarmTS 是本连接客户端预热帧（generate:false）拿到的 ts：随后本连接新开
	// 的一轮若没有同 owner 的 ts，就带上它（并清空，只用一次）。
	prewarmTS string
}

// wsFrameIdentity 是"本帧身份"：写闸内、写帧之前入 FIFO，response.created 时
// 绑定。写失败时置 withdrawn（帧没上线，不会有对应的 created），pop 跳过。
type wsFrameIdentity struct {
	turnID    string
	source    wsTurnSource
	sink      TurnStateSink
	prewarm   bool // 客户端预热帧：ts 存进连接快照的 prewarmTS
	withdrawn atomic.Bool
}

// wsConversation 是 WithConversation 连接的会话状态。Send 与 Recv 并发安全：
// 快照用 atomic.Pointer 整体替换，FIFO 是 buffered channel（只在写闸内入队）；
// bound/boundID/open 只在 Recv 内使用（至多一个 goroutine 同时 Recv）。
type wsConversation struct {
	book  *ConversationBook
	key   ConversationKey
	inst  string
	owner [16]byte

	state     atomic.Pointer[wsConvState]
	fifo      chan *wsFrameIdentity
	untracked atomic.Bool // FIFO 满过：整条连接不再捕获 ts

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
			conv:      ce,
			turn:      Turn{ID: id.Turn.ID, StartedAtMs: id.Turn.StartedAtMs, First: id.Turn.First},
			source:    turnSourceBook,
			sink:      id.Sink,
			ts:        id.Turn.State,
			inject:    cur.inject,
			prewarmTS: cur.prewarmTS,
		}
		if ns.ts != "" {
			ns.tsOwner = cv.owner
		} else if id.Turn.First {
			ns.takePrewarmTS(cv.owner)
		}
		return ns
	}
	// 增量帧：不读不写存储。
	if in.FollowUp() {
		if cur.turn.ID != "" {
			return cur
		}
		// 临时轮次 First=false：它不是一轮的第一个请求，而是查不到当前轮次的后续请求。
		cv.book.stats.wsLocalTemporary.Add(1)
		ns := *cur
		ns.turn = Turn{ID: NewUUIDv7(), StartedAtMs: now.UnixMilli()}
		ns.source, ns.sink, ns.ts, ns.tsOwner = turnSourceLocal, nil, "", [16]byte{}
		return &ns
	}
	ns := *cur
	ns.turn = Turn{ID: NewUUIDv7(), StartedAtMs: now.UnixMilli(), First: true}
	ns.source, ns.sink, ns.ts, ns.tsOwner = turnSourceLocal, nil, "", [16]byte{}
	ns.takePrewarmTS(cv.owner)
	return &ns
}

// takePrewarmTS：新开的一轮没有同 owner 的 ts 时带上本连接预热拿到的 ts（官方
// 客户端预热后的第一帧带预热 ts），并清空，只用一次。
func (s *wsConvState) takePrewarmTS(owner [16]byte) {
	if s.prewarmTS != "" {
		s.ts, s.tsOwner, s.prewarmTS = s.prewarmTS, owner, ""
	}
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

// sendConversation 是 WithConversation 连接的 Send。
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
	w := &wsSend{cv: cv, next: next, hash: sh.injectHash(), record: true}
	if sh.clientPrevID != "" {
		switch {
		case w.hash == 0:
			w.record = false
		case w.hash == cur.inject:
			sh.inputPrefix, w.record = nil, false
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
	if sh.nativePrewarm {
		w.fid.prewarm = true
	} else {
		a.turnState = sh.turnState(next.identity(cv.inst))
		w.fid.turnID, w.fid.source, w.fid.sink = next.turn.ID, next.source, next.sink
	}
	if next != cur && cur.prewarmTS != "" && next.prewarmTS == "" {
		w.took = cur.prewarmTS
	}
	wire, off := assembleWS(&sh, a)
	return c.cn.writeGated(ctx, wire, off, w)
}

// wsSend 是一次会话 Send 在写闸内的登记：写帧之前发布快照并把本帧身份入 FIFO，
// 写失败时撤销两者（见 conn.writeGated）。
type wsSend struct {
	cv        *wsConversation
	next      *wsConvState
	hash      uint64
	record    bool
	fid       wsFrameIdentity
	prev, pub *wsConvState // commit 前后的快照（pub==nil：快照未变）
	queued    bool
	took      string // 本帧用掉的预热 ts（"" = 没用）
}

func (w *wsSend) beforeWrite() {
	w.prev, w.pub = w.cv.commit(w.next, w.hash, w.record, w.took)
	w.queued = w.cv.push(&w.fid)
	// 本帧带上了预热 ts：会话簿开的轮次同样记下（与连接快照一致，跨副本的后续
	// 请求也带它）。须先于写帧：上游对本帧回的 ts 不得抢在它前面成为首值。
	if w.took != "" && w.next.source == turnSourceBook && w.next.sink != nil {
		w.next.sink.SetTurnState(w.took)
	}
}

func (w *wsSend) writeFailed() {
	if w.queued {
		w.fid.withdrawn.Store(true)
	}
	if w.pub != nil {
		w.cv.rollback(w.prev, w.pub)
	}
}

// commit 发布下一份快照（写闸内、写帧之前）。同一轮次期间 Recv 捕获到的 ts、
// 以及本帧未用掉的预热 ts 保留。返回发布前后的快照（快照未变时 pub 为 nil）。
func (cv *wsConversation) commit(next *wsConvState, hash uint64, record bool, took string) (prev, pub *wsConvState) {
	for {
		old := cv.state.Load()
		if old == next && (!record || old.inject == hash) {
			return old, nil
		}
		ns := *next
		if ns.turn.ID == old.turn.ID && ns.ts == "" && old.ts != "" {
			ns.ts, ns.tsOwner = old.ts, old.tsOwner
		}
		if old.prewarmTS != took { // Recv 期间到了（更新的）预热 ts：保留
			ns.prewarmTS = old.prewarmTS
		}
		if record {
			ns.inject = hash
		} else {
			ns.inject = old.inject
		}
		if cv.state.CompareAndSwap(old, &ns) {
			return old, &ns
		}
	}
}

// rollback 撤销写失败帧的快照发布（写闸内）：快照没被 Recv 改过则原样换回；
// 否则以发布前的快照为底，保留 Recv 期间捕获的同轮 ts 与预热 ts。
func (cv *wsConversation) rollback(prev, pub *wsConvState) {
	if cv.state.CompareAndSwap(pub, prev) {
		return
	}
	for {
		cur := cv.state.Load()
		r := *prev
		if cur.turn.ID == r.turn.ID && cur.ts != "" {
			r.ts, r.tsOwner = cur.ts, cur.tsOwner
		}
		if cur.prewarmTS != "" {
			r.prewarmTS = cur.prewarmTS
		}
		if cv.state.CompareAndSwap(cur, &r) {
			return
		}
	}
}

// push 把本帧身份入 FIFO（写闸内、写帧之前）；满则整条连接停止捕获 ts 并计数。
func (cv *wsConversation) push(f *wsFrameIdentity) bool {
	if cv.untracked.Load() {
		return false
	}
	select {
	case cv.fifo <- f:
		return true
	default:
		if cv.untracked.CompareAndSwap(false, true) {
			cv.book.stats.wsUntracked.Add(1)
		}
		return false
	}
}

// pop 非阻塞取最早一份未撤销的身份（FIFO 空返回 nil）。入队先于写帧，所以
// response.created 到达时它的身份一定已在队中，无需等待。
func (cv *wsConversation) pop() *wsFrameIdentity {
	for {
		select {
		case f := <-cv.fifo:
			if !f.withdrawn.Load() {
				return f
			}
		default:
			return nil
		}
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
		cv.bound = cv.pop()
		cv.boundID = gjson.GetBytes(raw, "response.id").String()
		cv.open = true
	case bytesAre(typ, responseEventCompleted), bytesAre(typ, responseEventFailed),
		bytesAre(typ, responseEventIncomplete):
		cv.bound, cv.boundID, cv.open = nil, "", false
	case bytesAre(typ, responseEventError):
		if !cv.open {
			cv.pop() // 该 create 被上游拒绝，不会再有 created
		}
		// error 一律结束当前绑定（该 response 若还有迟到的 metadata 则丢弃，不会写错 ts）。
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
	if f.prewarm {
		cv.setPrewarmTS(ts)
		return
	}
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

// setPrewarmTS 把客户端预热帧拿到的 ts 存进连接快照（最新一次预热为准）。
func (cv *wsConversation) setPrewarmTS(ts string) {
	for {
		s := cv.state.Load()
		if s.prewarmTS == ts {
			return
		}
		ns := *s
		ns.prewarmTS = ts
		if cv.state.CompareAndSwap(s, &ns) {
			return
		}
	}
}
