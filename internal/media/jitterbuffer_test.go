package media

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeJitterSink counts each event so tests can assert exact call counts.
type fakeJitterSink struct {
	late, duplicate, silence, ssrcChange, reanchor, outOfWindow int
}

func (f *fakeJitterSink) Late()            { f.late++ }
func (f *fakeJitterSink) Duplicate()       { f.duplicate++ }
func (f *fakeJitterSink) SilenceInserted() { f.silence++ }
func (f *fakeJitterSink) SSRCChange()      { f.ssrcChange++ }
func (f *fakeJitterSink) Reanchor()        { f.reanchor++ }
func (f *fakeJitterSink) OutOfWindow()     { f.outOfWindow++ }

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

// TestJitterBuffer_LossDuringPrimingRecovers is the regression test for the
// priming wedge: before the fix, one packet genuinely lost inside the priming
// window (102 below) could never be recovered -- `expected` doesn't advance
// while priming, so 103+ landed out-of-window forever and the buffer released
// nothing but silence for the rest of the call.
func TestJitterBuffer_LossDuringPrimingRecovers(t *testing.T) {
	sink := &fakeJitterSink{}
	jb := NewJitterBuffer(3, sink)

	jb.Push(100, 1, markedFrame(1))
	jb.Push(101, 1, markedFrame(2))
	// 102 is genuinely lost and will never arrive.

	out := jb.Release()
	assert.True(t, isSilence(out), "depth 2 of 3: still priming")
	PutFrame(out)

	// 103 lands outside the stuck window [100,103): the buffer must re-anchor
	// on it rather than drop it and wait forever.
	jb.Push(103, 1, markedFrame(4))
	assert.Equal(t, 1, sink.outOfWindow, "the out-of-window drop must be counted")

	jb.Push(104, 1, markedFrame(5))
	jb.Push(105, 1, markedFrame(6)) // depth 3 around the new anchor: primed

	for i, want := range []byte{4, 5, 6} {
		out := jb.Release()
		assert.Equal(t, want, (*out)[0], "release %d: real audio must flow again, in order", i)
		PutFrame(out)
	}

	assert.Zero(t, sink.late, "re-anchored packets are on-time, not late")
}

// TestJitterBuffer_MultipleLossesDuringPrimingConverge proves repeated holes in
// successive priming windows still converge: each out-of-window arrival
// re-anchors the buffer, and once any `size` consecutive packets arrive, real
// audio flows.
func TestJitterBuffer_MultipleLossesDuringPrimingConverge(t *testing.T) {
	sink := &fakeJitterSink{}
	jb := NewJitterBuffer(3, sink)

	jb.Push(100, 1, markedFrame(1))
	jb.Push(101, 1, markedFrame(2))
	// 102 and 103 lost; 104 re-anchors.
	jb.Push(104, 1, markedFrame(5))
	assert.Equal(t, 1, sink.outOfWindow)

	// 105 lost; 106 fits the window [104,107), keeping priming alive.
	jb.Push(106, 1, markedFrame(7))
	// 107 lost too; 108 re-anchors again.
	jb.Push(108, 1, markedFrame(9))
	assert.Equal(t, 2, sink.outOfWindow)

	jb.Push(109, 1, markedFrame(10))
	jb.Push(110, 1, markedFrame(11)) // depth 3: primed [108,109,110]

	for i, want := range []byte{9, 10, 11} {
		out := jb.Release()
		assert.Equal(t, want, (*out)[0], "release %d", i)
		PutFrame(out)
	}
}

// TestJitterBuffer_PrimingWaitIsBounded proves the waiting state gives up after
// maxPrimingTicks ticks: with the stream stalled mid-priming, the buffer must
// activate with a partial window (releasing what it has, advancing past the
// holes) instead of waiting forever, and normal operation must resume once
// packets return.
func TestJitterBuffer_PrimingWaitIsBounded(t *testing.T) {
	sink := &fakeJitterSink{}
	jb := NewJitterBuffer(3, sink)

	jb.Push(100, 1, markedFrame(1)) // anchors the window; the stream then goes dead

	// Strictly before the bound, priming still withholds silence.
	for i := 0; i < maxPrimingTicksFactor*3-1; i++ {
		out := jb.Release()
		assert.True(t, isSilence(out), "release %d before the priming bound must stay silent", i)
		PutFrame(out)
	}

	// At/after the bound, the buffered packet must come out.
	got := false

	for i := 0; i < maxPrimingTicksFactor*3+2 && !got; i++ {
		out := jb.Release()
		got = !isSilence(out)
		PutFrame(out)
	}

	require.True(t, got, "priming must give up waiting and release the buffered packet")

	// And once packets return, they flow normally through the active window.
	jb.Push(101, 1, markedFrame(2))

	out := jb.Release()
	assert.Equal(t, byte(2), (*out)[0])
	PutFrame(out)
}

// TestJitterBuffer_OutOfWindowCountedWhileActive pins the active-state behavior:
// a packet too far ahead of an active window is still dropped (the miss/freeze
// machinery owns recovery there), but the drop must now be counted.
func TestJitterBuffer_OutOfWindowCountedWhileActive(t *testing.T) {
	sink := &fakeJitterSink{}
	jb := NewJitterBuffer(3, sink)

	primeWith(jb, 100, 3)
	for i := 0; i < 3; i++ {
		PutFrame(jb.Release()) // expected now 103, state active
	}

	jb.Push(110, 1, markedFrame(9)) // diff 7 >= 3: out of window while active

	assert.Equal(t, 1, sink.outOfWindow)

	out := jb.Release()
	assert.True(t, isSilence(out), "the dropped packet's slot must be concealed with silence")
	PutFrame(out)
	assert.Equal(t, uint16(104), jb.expected, "the window must still advance past the drop")
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
