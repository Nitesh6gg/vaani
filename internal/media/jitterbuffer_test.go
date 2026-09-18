package media

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeJitterSink counts each event so tests can assert exact call counts.
type fakeJitterSink struct {
	late, duplicate, silence, ssrcChange, reanchor int
}

func (f *fakeJitterSink) Late()            { f.late++ }
func (f *fakeJitterSink) Duplicate()       { f.duplicate++ }
func (f *fakeJitterSink) SilenceInserted() { f.silence++ }
func (f *fakeJitterSink) SSRCChange()      { f.ssrcChange++ }
func (f *fakeJitterSink) Reanchor()        { f.reanchor++ }

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

// primeWith pushes size consecutive real packets starting at startSeq, bringing
// the buffer from empty straight through priming without ever calling Release.
func primeWith(jb *JitterBuffer, startSeq uint16, size int) {
	for i := 0; i < size; i++ {
		jb.Push(startSeq+uint16(i), 1, markedFrame(byte(i)))
	}
}

func TestJitterBuffer_InOrder(t *testing.T) {
	sink := &fakeJitterSink{}
	jb := NewJitterBuffer(3, sink)

	primeWith(jb, 100, 3)

	for i, want := range []byte{0, 1, 2} {
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
	jb.Push(101, 1, markedFrame(2))
	jb.Push(102, 1, markedFrame(3)) // completes priming depth

	out := jb.Release()
	assert.Equal(t, byte(1), (*out)[0], "the original frame must survive, not the duplicate")
	PutFrame(out)

	assert.Equal(t, 1, sink.duplicate)
}

func TestJitterBuffer_Late(t *testing.T) {
	sink := &fakeJitterSink{}
	jb := NewJitterBuffer(3, sink)

	primeWith(jb, 100, 3)
	PutFrame(jb.Release()) // releases 100 (real), advancing expected to 101

	jb.Push(100, 1, markedFrame(99)) // now behind the window: late

	assert.Equal(t, 1, sink.late)
}

func TestJitterBuffer_LossRunInsertsSilenceForEachMissingPacket(t *testing.T) {
	sink := &fakeJitterSink{}
	jb := NewJitterBuffer(5, sink)

	primeWith(jb, 100, 5)

	for i := 0; i < 5; i++ {
		PutFrame(jb.Release()) // drain all 5 primed packets; expected now 105
	}

	// 105, 106, 107 never arrive.
	for i := 0; i < 3; i++ {
		out := jb.Release()
		assert.True(t, isSilence(out), "missing packet %d must be concealed with silence", i)
		PutFrame(out)
	}

	assert.Equal(t, 3, sink.silence, "exactly one silence insertion per missing packet in the loss run")
}

func TestJitterBuffer_SSRCChangeResetsWindow(t *testing.T) {
	sink := &fakeJitterSink{}
	jb := NewJitterBuffer(3, sink)

	primeWith(jb, 100, 3)

	// A new SSRC arrives mid-call (e.g. Asterisk re-negotiated the media source).
	jb.Push(500, 2, markedFrame(9))
	jb.Push(501, 2, markedFrame(10))
	jb.Push(502, 2, markedFrame(11)) // re-primed on the new stream

	out := jb.Release()
	assert.Equal(t, byte(9), (*out)[0], "after an SSRC change the window must restart at the new stream's sequence")
	PutFrame(out)

	assert.Equal(t, 1, sink.ssrcChange)
}

// TestJitterBuffer_PrimingWithholdsUntilDepthReached is the direct regression
// test for the reported production bug: the release ticker fires on a fixed
// schedule starting the instant the media plane starts, which is before any
// packet has ever arrived, and continues through partial buffer fill. Before this
// fix, every one of those early ticks silently advanced `expected`, permanently
// racing it ahead of the real stream by the time packets actually started
// flowing -- misclassifying every subsequent packet as late for the rest of the
// call (observed live: 850/899 = 94.5% late, audio_rms=0, total silence).
func TestJitterBuffer_PrimingWithholdsUntilDepthReached(t *testing.T) {
	sink := &fakeJitterSink{}
	jb := NewJitterBuffer(3, sink)

	// Release ticks fire before any packet has ever arrived.
	for i := 0; i < 5; i++ {
		out := jb.Release()
		assert.True(t, isSilence(out))
		PutFrame(out)
	}

	assert.Zero(t, sink.silence, "releases before the first packet must not count as concealment silence")

	jb.Push(1000, 1, markedFrame(1))

	// Still priming: only 1 of 3 needed packets buffered.
	out := jb.Release()
	assert.True(t, isSilence(out), "must still be silence mid-priming")
	PutFrame(out)
	assert.Zero(t, sink.silence, "mid-priming releases must not count as concealment silence either")

	jb.Push(1001, 1, markedFrame(2))
	jb.Push(1002, 1, markedFrame(3)) // depth now 3: priming complete

	out = jb.Release()
	assert.Equal(t, byte(1), (*out)[0], "priming complete: must release the first real buffered packet, in order")
	PutFrame(out)
}

// TestJitterBuffer_StarveFreezeThenReanchor proves the self-healing mechanism
// that priming alone can't provide: two independent, unsynchronized 20ms clocks
// (Vaani's release ticker and Asterisk's RTP pacer) drift relative to each other
// over a long call, so a fixed initial priming depth isn't enough on its own --
// the buffer must be able to notice sustained misses and resynchronize.
func TestJitterBuffer_StarveFreezeThenReanchor(t *testing.T) {
	sink := &fakeJitterSink{}
	jb := NewJitterBuffer(3, sink)

	primeWith(jb, 0, 3)

	for i := 0; i < 3; i++ {
		PutFrame(jb.Release()) // drain the primed packets; expected now 3
	}

	for i := 0; i < starveFreezeThreshold; i++ {
		PutFrame(jb.Release())
	}

	assert.Equal(t, starveFreezeThreshold, sink.silence)

	frozenExpected := jb.expected

	PutFrame(jb.Release()) // beyond the threshold: must not advance further
	assert.Equal(t, frozenExpected, jb.expected, "frozen buffer must stop advancing on further misses")

	jb.Push(500, 1, markedFrame(42)) // arrives at/after the frozen position
	assert.Equal(t, 1, sink.reanchor)

	out := jb.Release()
	assert.True(t, isSilence(out), "reanchoring re-enters priming; must not release real data yet")
	PutFrame(out)

	jb.Push(501, 1, markedFrame(43))
	jb.Push(502, 1, markedFrame(44)) // completes re-priming

	out = jb.Release()
	assert.Equal(t, byte(42), (*out)[0], "after re-priming, must release the packet that triggered the reanchor")
	PutFrame(out)
}

// TestJitterBuffer_AdversarialConstantPhaseLead simulates the exact field
// failure mode: the release clock persistently gets ahead of the arrival clock
// (modeled here as one extra release every 8 iterations, with no matching extra
// packet). Before this fix, ANY sustained rate mismatch -- however small --
// caused permanent, unbounded desync (every subsequent packet late forever).
// After the fix, the buffer's priming depth absorbs bounded mismatch and
// starve-freeze/reanchor caps the rest, so lateness stays bounded instead of
// approaching 100%.
func TestJitterBuffer_AdversarialConstantPhaseLead(t *testing.T) {
	sink := &fakeJitterSink{}
	jb := NewJitterBuffer(3, sink)

	const iterations = 5000

	seq := uint16(1000)

	for i := 0; i < iterations; i++ {
		PutFrame(jb.Release())
		jb.Push(seq, 1, markedFrame(1))
		seq++

		if i%8 == 0 {
			PutFrame(jb.Release()) // the release clock's persistent lead over arrival
		}
	}

	lateRate := float64(sink.late) / float64(iterations)
	assert.Less(t, lateRate, 0.20,
		"sustained release-leads-arrival phase pressure must stay bounded, not approach total lockout (got late_rate=%.3f)", lateRate)
}

// TestJitterBuffer_LongRunDriftDoesNotPermanentlyLockOut simulates ~10 minutes
// of 20ms ticks with the arrival clock running 200ppm slower than the release
// clock -- a realistic magnitude of crystal oscillator mismatch between two
// independent machines. The old implementation would desync permanently at the
// first missed tick and never recover; this asserts the late rate stays low
// throughout instead of climbing toward 100%.
func TestJitterBuffer_LongRunDriftDoesNotPermanentlyLockOut(t *testing.T) {
	sink := &fakeJitterSink{}
	jb := NewJitterBuffer(3, sink)

	const (
		ticks    = 30000 // ~10 minutes of 20ms ticks
		driftPPM = 200.0
	)

	driftFactor := 1.0 - driftPPM/1e6
	seq := uint16(1000)
	arrived := 0.0

	// Compare the second half's late rate against the first half's: if the
	// buffer were permanently locking out (the old bug), the back half would be
	// dramatically worse than the front half instead of similarly low.
	var lateFirstHalf, lateSecondHalf int

	for t := 0; t < ticks; t++ {
		PutFrame(jb.Release())

		arrived += driftFactor
		for arrived >= 1.0 {
			jb.Push(seq, 1, markedFrame(1))
			seq++
			arrived--
		}

		if t == ticks/2 {
			lateFirstHalf = sink.late
		}
	}

	lateSecondHalf = sink.late - lateFirstHalf

	firstRate := float64(lateFirstHalf) / float64(ticks/2)
	secondRate := float64(lateSecondHalf) / float64(ticks/2)

	assert.Less(t, firstRate, 0.05, "first half late rate must be low (got %.4f)", firstRate)
	assert.Less(t, secondRate, 0.05, "second half late rate must stay low too, not climb toward lockout (got %.4f)", secondRate)
}

func TestJitterBuffer_ZeroAllocInSteadyState(t *testing.T) {
	sink := &fakeJitterSink{}
	jb := NewJitterBuffer(3, sink)

	// Prime and fully drain so we measure the post-priming steady state, which
	// is what a real long call spends almost all its time in.
	seq := uint16(0)
	primeWith(jb, seq, 3)
	seq += 3

	for i := 0; i < 3; i++ {
		PutFrame(jb.Release())
	}

	allocs := testing.AllocsPerRun(1000, func() {
		jb.Push(seq, 1, GetFrame())
		seq++
		PutFrame(jb.Release())
	})

	require.LessOrEqual(t, allocs, 0.0, "jitter buffer push+release must not allocate in steady state")
}
