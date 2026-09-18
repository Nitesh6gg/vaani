package media

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeJitterSink counts each event so tests can assert exact call counts.
type fakeJitterSink struct {
	late, duplicate, silence, ssrcChange int
}

func (f *fakeJitterSink) Late()            { f.late++ }
func (f *fakeJitterSink) Duplicate()       { f.duplicate++ }
func (f *fakeJitterSink) SilenceInserted() { f.silence++ }
func (f *fakeJitterSink) SSRCChange()      { f.ssrcChange++ }

// markedFrame returns a pooled frame whose first byte is `marker`, so a released
// frame's origin can be identified in assertions.
func markedFrame(marker byte) *[]byte {
	f := GetFrame()
	(*f)[0] = marker

	return f
}

func isSilence(f *[]byte) bool {
	for _, b := range *f {
		if b != 0 {
			return false
		}
	}

	return true
}

func TestJitterBuffer_InOrder(t *testing.T) {
	sink := &fakeJitterSink{}
	jb := NewJitterBuffer(3, sink)

	jb.Push(100, 1, markedFrame(1))
	jb.Push(101, 1, markedFrame(2))
	jb.Push(102, 1, markedFrame(3))

	for i, want := range []byte{1, 2, 3} {
		out := jb.Release()
		assert.Equal(t, want, (*out)[0], "release %d", i)
		PutFrame(out)
	}

	assert.Zero(t, sink.late)
	assert.Zero(t, sink.duplicate)
	assert.Zero(t, sink.silence)
}

func TestJitterBuffer_Reorder(t *testing.T) {
	sink := &fakeJitterSink{}
	jb := NewJitterBuffer(3, sink)

	// Packets 100 and 101 arrive swapped.
	jb.Push(100, 1, markedFrame(1))
	jb.Push(102, 1, markedFrame(3))
	jb.Push(101, 1, markedFrame(2))

	for i, want := range []byte{1, 2, 3} {
		out := jb.Release()
		assert.Equal(t, want, (*out)[0], "release %d must still come out in sequence order", i)
		PutFrame(out)
	}

	assert.Zero(t, sink.late, "arriving early within the window is reordering, not lateness")
}

func TestJitterBuffer_Duplicate(t *testing.T) {
	sink := &fakeJitterSink{}
	jb := NewJitterBuffer(3, sink)

	jb.Push(100, 1, markedFrame(1))
	jb.Push(100, 1, markedFrame(99)) // duplicate seq

	out := jb.Release()
	assert.Equal(t, byte(1), (*out)[0], "the original frame must survive, not the duplicate")
	PutFrame(out)

	assert.Equal(t, 1, sink.duplicate)
}

func TestJitterBuffer_Late(t *testing.T) {
	sink := &fakeJitterSink{}
	jb := NewJitterBuffer(3, sink)

	jb.Push(100, 1, markedFrame(1))
	_ = jb.Release() // advances expected to 101

	jb.Push(100, 1, markedFrame(99)) // now behind the window: late

	assert.Equal(t, 1, sink.late)
}

func TestJitterBuffer_LossRunInsertsSilenceForEachMissingPacket(t *testing.T) {
	sink := &fakeJitterSink{}
	jb := NewJitterBuffer(3, sink)

	jb.Push(100, 1, markedFrame(1))
	// 101, 102, 103 never arrive.
	jb.Push(104, 1, markedFrame(4))

	out := jb.Release() // 100: real
	assert.Equal(t, byte(1), (*out)[0])
	PutFrame(out)

	for i := 0; i < 3; i++ {
		out := jb.Release() // 101, 102, 103: silence
		assert.True(t, isSilence(out), "missing packet %d must be concealed with silence", i)
		PutFrame(out)
	}

	assert.Equal(t, 3, sink.silence, "exactly one silence insertion per missing packet in the loss run")
}

func TestJitterBuffer_SSRCChangeResetsWindow(t *testing.T) {
	sink := &fakeJitterSink{}
	jb := NewJitterBuffer(3, sink)

	jb.Push(100, 1, markedFrame(1))
	jb.Push(101, 1, markedFrame(2))

	// A new SSRC arrives mid-call (e.g. Asterisk re-negotiated the media source).
	jb.Push(500, 2, markedFrame(9))

	out := jb.Release()
	assert.Equal(t, byte(9), (*out)[0], "after an SSRC change the window must restart at the new stream's sequence")
	PutFrame(out)

	assert.Equal(t, 1, sink.ssrcChange)
}

func TestJitterBuffer_ZeroAllocInSteadyState(t *testing.T) {
	sink := &fakeJitterSink{}
	jb := NewJitterBuffer(3, sink)

	// Warm the pool and the buffer's internal state before measuring.
	jb.Push(0, 1, markedFrame(0))
	PutFrame(jb.Release())

	seq := uint16(1)

	allocs := testing.AllocsPerRun(1000, func() {
		jb.Push(seq, 1, GetFrame())
		seq++
		PutFrame(jb.Release())
	})

	require.LessOrEqual(t, allocs, 0.0, "jitter buffer push+release must not allocate in steady state")
}
