package media

import "sync"

// starveFreezeThreshold is how many consecutive missing releases it takes before
// the buffer stops trusting its own clock and freezes, waiting for a real packet
// to re-anchor on. Without this, sustained clock drift between Vaani's release
// ticker and Asterisk's RTP pacing clock (two independent, unsynchronized 20ms
// clocks) would eventually reproduce the same permanent-late lockout that
// priming alone only prevents at call start.
const starveFreezeThreshold = 3

// maxPrimingTicksFactor bounds the priming wait relative to the window size:
// priming normally completes within `size` arrivals (~size release ticks at
// Asterisk's 20ms pacing), so waiting 2×size ticks before force-activating
// gives comfortable reorder slack while still guaranteeing the buffer never
// waits on a full window indefinitely.
const maxPrimingTicksFactor = 2

// JitterSink receives jitter buffer observability events. Implemented by the
// metrics package at the call site, same pattern as Sink.
type JitterSink interface {
	Late()
	Duplicate()
	SilenceInserted()
	SSRCChange()
	Reanchor()
	OutOfWindow()
}

// jbState tracks where the buffer is relative to the release clock.
type jbState int

const (
	// jbWaiting: priming (or re-priming after a reanchor) -- waiting for at
	// least `size` real packets buffered before releasing anything but silence.
	// Release ticks in this state are a pure no-op: no metric, no advance --
	// the playout clock hasn't started yet.
	jbWaiting jbState = iota
	// jbActive: normal operation.
	jbActive
	// jbFrozen: starveFreezeThreshold consecutive misses happened; the buffer
	// no longer trusts `expected` enough to keep advancing it on faith, and
	// waits for a real packet at or after the frozen position to re-anchor.
	jbFrozen
)

// JitterBuffer reorders inbound RTP payloads over a fixed window (in packets),
// releasing exactly one frame per call in sequence order. It requires the window
// to fill (priming) before releasing real audio, and self-heals from sustained
// clock drift or loss bursts by freezing after repeated misses and re-anchoring
// on the next packet that arrives -- without this, two independent,
// unsynchronized 20ms clocks (Vaani's release ticker and Asterisk's RTP pacing)
// can permanently desync, misclassifying every subsequent packet as late.
//
// Two mechanisms guarantee priming can never wedge into permanent silence:
//
//   - While waiting, a packet landing outside the window re-anchors the buffer
//     on it. Since `expected` never advances while priming, a packet genuinely
//     lost inside the priming window can otherwise never be recovered -- every
//     newer packet stays out-of-window forever, silencing the call for life
//     (and re-arming in every re-prime window). Nothing real has been released
//     yet at that point, so dropping the partial window costs only silence.
//   - The priming wait itself is bounded (~2×window release ticks); beyond it
//     the buffer activates with a partial window and the miss/freeze machinery
//     owns recovery.
//
// It owns pooled frame buffers end-to-end: Push transfers ownership in (discarded
// frames are returned to the pool immediately), Release transfers ownership out
// (the caller must PutFrame the result). Safe for concurrent Push (reader
// goroutine) and Release (writer/pacer goroutine); internal state is
// mutex-protected.
type JitterBuffer struct {
	mu    sync.Mutex
	size  int
	slots []*[]byte
	have  []bool

	started         bool
	state           jbState
	misses          int
	primingTicks    int
	maxPrimingTicks int
	expected        uint16
	ssrc            uint32
	sink            JitterSink
}

// NewJitterBuffer creates a JitterBuffer with a window of size packets (1-10, i.e.
// 20-200ms at 20ms/packet). size also sets the priming depth.
func NewJitterBuffer(size int, sink JitterSink) *JitterBuffer {
	return &JitterBuffer{
		size:            size,
		slots:           make([]*[]byte, size),
		have:            make([]bool, size),
		maxPrimingTicks: maxPrimingTicksFactor * size,
		sink:            sink,
	}
}

// Push inserts an inbound packet, keyed by its RTP sequence number and SSRC.
// Ownership of frame transfers to the JitterBuffer: it is either stored for a
// future Release or immediately returned to the pool if discarded (late,
// duplicate, or outside the reorder window).
func (j *JitterBuffer) Push(seq uint16, ssrc uint32, frame *[]byte) {
	j.mu.Lock()
	defer j.mu.Unlock()

	switch {
	case !j.started:
		j.started = true
		j.expected = seq
		j.ssrc = ssrc
		j.state = jbWaiting
	case ssrc != j.ssrc:
		j.resetLocked(seq, ssrc)
		j.sink.SSRCChange()
	case j.state == jbFrozen:
		// Only accept packets at or after the frozen position; anything older
		// is genuinely stale relative to where we're waiting to resync.
		if int16(seq-j.expected) < 0 {
			j.sink.Late()
			PutFrame(frame)

			return
		}

		j.reanchorLocked(seq)
		j.sink.Reanchor()
	case j.state == jbWaiting && int16(seq-j.expected) >= int16(j.size):
		// Priming deadlock escape (see the type comment): `expected` never
		// advances while priming, so a packet genuinely lost inside the priming
		// window makes every newer packet land here forever. Re-anchor on the
		// newest arrival instead -- jbWaiting has released no real audio yet,
		// so clearing the partial window costs nothing but silence ticks.
		j.sink.OutOfWindow()
		j.reanchorLocked(seq)
	}

	// Signed circular distance from expected (RFC 1982 serial number arithmetic):
	// >=0 means seq is at or ahead of expected, <0 means it's behind (late).
	diff := int16(seq - j.expected)

	switch {
	case diff < 0:
		j.sink.Late()
		PutFrame(frame)
	case diff >= int16(j.size):
		// Too far ahead of the window to place (only reachable while active:
		// waiting re-anchors above and frozen re-anchors on anything at/after
		// the frozen position). The miss/freeze machinery owns recovery -- the
		// window advances over the gap and freezes if it stays empty.
		j.sink.OutOfWindow()
		PutFrame(frame)
	default:
		slot := int(seq) % j.size
		if j.have[slot] {
			j.sink.Duplicate()
			PutFrame(frame)

			return
		}

		j.slots[slot] = frame
		j.have[slot] = true
	}
}

// Release pops the frame for the next expected sequence number, advancing the
// window by one packet, and returns it. Before priming completes (buffer depth
// hasn't yet reached size), it returns silence without touching any state or
// metric -- the playout clock hasn't started -- but only up to maxPrimingTicks
// ticks, after which it activates with whatever depth it has so `expected`
// starts advancing past unrecoverable holes. Once active, a missing packet is
// concealed with silence and the window still advances, up to
// starveFreezeThreshold consecutive misses; beyond that the window freezes
// (stops advancing) until Push re-anchors it. The caller owns the returned frame
// and must PutFrame it.
func (j *JitterBuffer) Release() *[]byte {
	j.mu.Lock()
	defer j.mu.Unlock()

	if !j.started {
		return silentFrame()
	}

	if j.state == jbWaiting {
		if j.depthLocked() < j.size {
			j.primingTicks++
			if j.primingTicks < j.maxPrimingTicks {
				return silentFrame()
			}

			// Priming never completed within the bound: the stream stalled
			// mid-priming or loss is too heavy to ever assemble a full window.
			// Activate anyway -- the miss/freeze machinery below owns recovery
			// from here.
		}

		j.state = jbActive
		j.misses = 0
	}

	slot := int(j.expected) % j.size

	if j.have[slot] {
		out := j.slots[slot]
		j.slots[slot] = nil
		j.have[slot] = false
		j.expected++
		j.misses = 0

		return out
	}

	j.sink.SilenceInserted()

	if j.state == jbFrozen {
		return silentFrame() // waiting for Push to re-anchor; don't advance further
	}

	j.misses++
	j.expected++

	if j.misses >= starveFreezeThreshold {
		j.state = jbFrozen
	}

	return silentFrame()
}

// depthLocked counts populated slots, for the priming gate. Callers must hold j.mu.
func (j *JitterBuffer) depthLocked() int {
	n := 0

	for _, h := range j.have {
		if h {
			n++
		}
	}

	return n
}

// clearSlotsLocked drops every buffered frame back to the pool. Callers must
// hold j.mu.
func (j *JitterBuffer) clearSlotsLocked() {
	for i := range j.slots {
		if j.have[i] {
			PutFrame(j.slots[i])
			j.slots[i] = nil
			j.have[i] = false
		}
	}
}

// resetLocked drops every buffered frame and re-anchors the window on a new
// SSRC, re-entering priming. Callers must hold j.mu.
func (j *JitterBuffer) resetLocked(seq uint16, ssrc uint32) {
	j.clearSlotsLocked()
	j.expected = seq
	j.ssrc = ssrc
	j.state = jbWaiting
	j.misses = 0
	j.primingTicks = 0
}

// reanchorLocked drops every buffered frame and re-anchors the window on seq
// (same SSRC), re-entering priming. Callers must hold j.mu.
func (j *JitterBuffer) reanchorLocked(seq uint16) {
	j.clearSlotsLocked()
	j.expected = seq
	j.state = jbWaiting
	j.misses = 0
	j.primingTicks = 0
}

// silentFrame returns a pooled, zeroed FrameSize buffer.
func silentFrame() *[]byte {
	f := GetFrame()
	*f = (*f)[:FrameSize]

	for i := range *f {
		(*f)[i] = 0
	}

	return f
}
