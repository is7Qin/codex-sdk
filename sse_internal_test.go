package codexsdk

import (
	"bytes"
	"context"
	"io"
	"testing"
	"time"

	"github.com/tidwall/gjson"
)

// 本文件是 sseView / frameEventType 的包内护栏（I-1）：
//   - TestSSERawSegmentIsFrameBacked 钉住"帧体零拷贝"——P2 的 0 allocs/op 不能
//     证明帧体没被复制（复用块缓冲也能 0 allocs），只有底层数组同一性可以；
//   - TestFrameEventTypeTopLevel 钉住"只认顶层、只认字符串"，补上 W14 只覆盖
//     "type 在首位"的缺口，并保证畸形帧不 panic。
//
// 帧流桩复用 bench_ws_test.go 的 benchStaticStream（同包、语义相同：按序吐帧，
// 吐完 io.EOF；直接返回帧切片、不复制——正是本护栏要观察的交付形状）。

// TestSSERawSegmentIsFrameBacked：视图吐 raw 段时，段与入站帧**同一底层数组**。
func TestSSERawSegmentIsFrameBacked(t *testing.T) {
	incomplete := `{"type":"response.incomplete","response":{"id":"resp_ws","status":"incomplete"}}`
	frames := []struct {
		name string
		raw  string
	}{
		{"无 type", `{"hello":"world"}`},
		{"type:error 透传", `{"type":"error","error":{"type":"invalid_request_error","message":"nope"}}`},
		{"created", wsCreatedEv},
		{"delta", wsDeltaEv},
		{"failed", wsFailedEv},
		{"incomplete", incomplete},
		{"completed", wsCompletedEv},
	}
	for _, tc := range frames {
		t.Run(tc.name, func(t *testing.T) {
			frame := []byte(tc.raw)
			stream := &benchStaticStream{frames: [][]byte{frame}}
			view := newSSEView(context.Background(), stream, wsStreamReadLimit)

			// 第一次读按 prefix 段长度取：正好把 prefix 吐完，当前段切到 raw。
			pfxLen := prefixNeed(frameEventType(frame))
			if _, err := view.Read(make([]byte, pfxLen)); err != nil {
				t.Fatalf("读 prefix: %v", err)
			}
			if view.seg != sseSegRaw {
				t.Fatalf("prefix 吐完后的当前段 = %d，想要 raw(%d)", view.seg, sseSegRaw)
			}
			got := view.segs[sseSegRaw]
			if len(got) != len(frame) {
				t.Fatalf("raw 段长度 = %d，帧长 = %d", len(got), len(frame))
			}
			if len(got) == 0 || &got[0] != &frame[0] {
				t.Fatalf("raw 段未与帧共享底层数组（发生了复制）")
			}

			// 再读满帧体：p 收到的字节逐字节等于帧（同一份内存被原样搬出）。
			out := make([]byte, len(frame))
			if _, err := view.Read(out); err != nil {
				t.Fatalf("读 raw: %v", err)
			}
			if !bytes.Equal(out, frame) {
				t.Fatalf("raw 段字节不符\n实际 = %q\n想要 = %q", out, frame)
			}
		})
	}
}

// TestFrameEventTypeTopLevel：只识别**顶层**、且值必须是字符串；畸形帧不 panic。
func TestFrameEventTypeTopLevel(t *testing.T) {
	cases := []struct {
		name string
		raw  string
		want string
		// malformed 的输入只断言"不 panic 且返回空"：gjson 对残缺 JSON 的容错
		// 程度不是本函数的契约，不做字符串对照。
		malformed bool
		// gjsonDiffers 标记"合法但故意与 gjson .String() 不同"的形态：
		// type 非字符串时 gjson 会转成 "123"，本函数只认字符串值。
		gjsonDiffers bool
	}{
		{name: "type 在首位", raw: wsCreatedEv, want: "response.created"},
		{name: "type 不在首位", raw: `{"id":"resp_1","object":"response","type":"response.output_text.delta","sequence_number":7}`, want: "response.output_text.delta"},
		{name: "type 在末位且前置嵌套对象", raw: `{"response":{"id":"resp_1","status":"in_progress"},"type":"response.created"}`, want: "response.created"},
		{name: "嵌套同名键不算", raw: `{"response":{"type":"nested"},"sequence_number":2}`, want: ""},
		{name: "值位置字符串不算键", raw: `{"x":"type","type":"response.created"}`, want: "response.created"},
		{name: "type 非字符串", raw: `{"type":123}`, want: "", gjsonDiffers: true},
		{name: "缺 type", raw: `{"hello":"world"}`, want: ""},
		{name: "数组根", raw: `[{"type":"response.created"}]`, want: ""},
		{name: "截断的 type 值", raw: `{"type":"response.cr`, want: "", malformed: true},
		{name: "未闭合引号", raw: `{"type":"x}`, want: "", malformed: true},
		{name: "只有键没有值", raw: `{"type":`, want: "", malformed: true},
		{name: "截断的嵌套对象", raw: `{"a":{"b":1}`, want: "", malformed: true},
		{name: "转义事件名（\u002e）", raw: `{"type":"response\u002ecompleted"}`, want: "response.completed"},
		{name: "转义事件名（\u005f）", raw: `{"type":"response.incomplete"}`, want: "response.incomplete"},
		{name: "空帧", raw: "", want: "", malformed: true},
		{name: "非 JSON", raw: `not json`, want: "", malformed: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := string(frameEventType([]byte(tc.raw))) // panic 即失败
			if got != tc.want {
				t.Fatalf("frameEventType(%q) = %q，想要 %q", tc.raw, got, tc.want)
			}
			if tc.malformed || tc.gjsonDiffers {
				return
			}
			if want := gjson.GetBytes([]byte(tc.raw), "type").String(); got != want {
				t.Fatalf("与 gjson 不一致：frameEventType(%q) = %q，gjson = %q", tc.raw, got, want)
			}
		})
	}
}

// blockingStream 是 frameStream 桩：吐完 frames 后**阻塞**在 block 上（不会 io.EOF），
// 用来证明"已经产出了字节的 Read 不会为了填满 p 而阻塞等下一帧"。
type blockingStream struct {
	frames [][]byte
	i      int
	block  chan struct{}
}

func (s *blockingStream) Next(ctx context.Context) ([]byte, error) {
	if s.i < len(s.frames) {
		f := s.frames[s.i]
		s.i++
		return f, nil
	}
	select {
	case <-s.block:
		return nil, io.EOF
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (s *blockingStream) Close() error { return nil }

// Release 实现 frameStream：阻塞桩背后没有连接，无可归还。
func (s *blockingStream) Release() bool { return false }

// TestSSEViewReadDoesNotBlockForNextFrame：一帧的三段吐完且 p 仍有余量时，Read
// 必须立刻返回已产出的字节，而不是转去 load() 等下一帧——否则 SSE 的逐事件时延
// 会变成"等下一帧到达"（模型思考期间就是秒级空白）。
func TestSSEViewReadDoesNotBlockForNextFrame(t *testing.T) {
	first := []byte(wsCreatedEv)
	stream := &blockingStream{frames: [][]byte{first}, block: make(chan struct{})}
	defer close(stream.block)
	view := newSSEView(context.Background(), stream, wsStreamReadLimit)
	want := prefixNeed(frameEventType(first)) + len(first) + len(sseSuffixFrame)

	type result struct {
		n   int
		err error
	}
	done := make(chan result, 1)
	go func() {
		n, err := view.Read(make([]byte, 4096))
		done <- result{n, err}
	}()
	select {
	case r := <-done:
		if r.err != nil {
			t.Fatalf("读第一帧出错: %v", r.err)
		}
		if r.n != want {
			t.Fatalf("首帧渲染长度 = %d，想要 %d", r.n, want)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Read 阻塞在下一帧上（应先交付已产出的字节）")
	}
}
