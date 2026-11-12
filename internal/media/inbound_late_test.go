package media

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// lateRecorderSink is a fakeAudioSocketSink that also counts the optional inbound reports.
type lateRecorderSink struct {
	fakeAudioSocketSink
	dropped atomic.Int64
	late    atomic.Int64
}

func (s *lateRecorderSink) InboundDropped() { s.dropped.Add(1) }
func (s *lateRecorderSink) InboundLate()    { s.late.Add(1) }

// lateHandler records which frames reached it, as a tick or as late.
type lateHandler struct {
	mu    sync.Mutex
	ticks []byte
	late  []byte
}

func (h *lateHandler) ProcessFrame(_ context.Context, _ string, pcm []byte) [][]byte {
	h.mu.Lock()
	defer h.mu.Unlock()

	h.ticks = append(h.ticks, pcm[0])

	return nil
}

func (h *lateHandler) HearLate(pcm []byte) {
	h.mu.Lock()
	defer h.mu.Unlock()

	h.late = append(h.late, pcm[0])
}

func lateFrameOf(b byte) *[]byte {
	f := GetFrame()
	*f = (*f)[:FrameSize]

	for i := range *f {
		(*f)[i] = b
	}

	return f
}

func newLateMedia(h Handler, s AudioSocketSink) *AudioSocketCallMedia {
	return &AudioSocketCallMedia{
		callID:   "c",
		sink:     s,
		handler:  h,
		recorder: NewWavRecorder("", "c"),
		inbound:  make(chan *[]byte, inboundQueue),
	}
}

// A clump of frames that waits for a tick: all of them are heard, in order, and the newest takes the tick.
func TestTakeInbound_ClumpIsHeardInOrderAndNewestTakesTheTick(t *testing.T) {
	h, s := &lateHandler{}, &lateRecorderSink{}
	c := newLateMedia(h, s)

	for i := byte(1); i <= 30; i++ {
		c.inbound <- lateFrameOf(i)
	}

	raw := c.takeInbound()
	require.NotNil(t, raw)
	assert.Equal(t, byte(30), (*raw)[0], "the newest frame takes the tick")

	require.Len(t, h.late, 29)

	for i, b := range h.late {
		assert.Equal(t, byte(i+1), b, "older frames are heard in the order they arrived")
	}

	assert.EqualValues(t, 29, s.late.Load())
	assert.Zero(t, s.dropped.Load())
	assert.Empty(t, c.inbound, "nothing is left waiting")
}

// One waiting frame is the plain case: no late hearing at all.
func TestTakeInbound_SingleFrameIsNotLate(t *testing.T) {
	h, s := &lateHandler{}, &lateRecorderSink{}
	c := newLateMedia(h, s)

	c.inbound <- lateFrameOf(7)

	raw := c.takeInbound()
	assert.Equal(t, byte(7), (*raw)[0])
	assert.Empty(t, h.late)
	assert.Zero(t, s.late.Load())
}

// Nothing waiting is silence, as before.
func TestTakeInbound_NothingWaitingIsSilence(t *testing.T) {
	c := newLateMedia(&lateHandler{}, &lateRecorderSink{})

	raw := c.takeInbound()
	assert.Equal(t, zeroFrame, *raw)
}

// A late frame is mixed into the oldest tick that was recorded without caller audio.
func TestMixRecorder_LateFrameFillsTheOldestSilentTick(t *testing.T) {
	inner := &lateHandler{}
	r := NewMixRecorder(inner)

	loud := make([]byte, FrameSize)
	for i := 0; i < FrameSize; i += 2 {
		loud[i] = 100 // sample 100 (LE)
	}

	r.ProcessFrame(context.Background(), "c", zeroFrame) // tick 0: the frame had not arrived
	r.ProcessFrame(context.Background(), "c", loud)      // tick 1: a real frame

	late := make([]byte, FrameSize)
	for i := 0; i < FrameSize; i += 2 {
		late[i] = 50
	}

	r.HearLate(late)

	wav := r.WAV()
	require.NotNil(t, wav)

	pcm := wav[wavHeaderSize:]
	require.Len(t, pcm, 2*FrameSize, "the recording keeps its length")
	assert.Equal(t, byte(50), pcm[0], "the late audio lands in the silent tick")
	assert.Equal(t, byte(100), pcm[FrameSize], "the real tick is untouched")
	assert.Equal(t, []byte{50}, inner.late[:1], "the wrapped handler heard it too")

	r.HearLate(late) // no silent tick left: heard, not recorded
	assert.Len(t, r.WAV()[wavHeaderSize:], 2*FrameSize)
}
