package media

import "sync"

// JitterSink receives jitter buffer observability events. Implemented by the
// metrics package at the call site, same pattern as Sink.
type JitterSink interface {
	Late()
	Duplicate()
	SilenceInserted()
	SSRCChange()
}

// JitterBuffer reorders inbound RTP payloads over a fixed window (in packets) and
// releases exactly one frame per call, in sequence order, on each 20ms tick --
// concealing loss with silence rather than stalling or skipping. It owns pooled
// frame buffers end-to-end: Push transfers ownership in (discarded frames are
// returned to the pool immediately), Release transfers ownership out (the caller
// must PutFrame the result).
//
// Not safe for concurrent Push and Release from the caller's perspective in terms
// of ordering guarantees, but the internal state is mutex-protected since Push
// (reader goroutine) and Release (writer/pacer goroutine) run concurrently.
type JitterBuffer struct {
	mu    sync.Mutex
	size  int
	slots []*[]byte
	have  []bool

	started  bool
	expected uint16
	ssrc     uint32
	sink     JitterSink
}

// NewJitterBuffer creates a JitterBuffer with a window of size packets (1-10, i.e.
// 20-200ms at 20ms/packet).
func NewJitterBuffer(size int, sink JitterSink) *JitterBuffer {
	return &JitterBuffer{
		size:  size,
		slots: make([]*[]byte, size),
		have:  make([]bool, size),
		sink:  sink,
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
	case ssrc != j.ssrc:
		j.resetLocked(seq, ssrc)
		j.sink.SSRCChange()
	}

	// Signed circular distance from expected (RFC 1982 serial number arithmetic):
	// >=0 means seq is at or ahead of expected, <0 means it's behind (late).
	diff := int16(seq - j.expected)

	switch {
	case diff < 0:
		j.sink.Late()
		PutFrame(frame)
	case diff >= int16(j.size):
		// Too far ahead of the current window to place without corrupting ring
		// state; drop rather than force-advance mid-buffer.
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
// window by one packet. If that packet hasn't arrived, it returns a pooled,
// zeroed silence frame instead (loss concealment) and reports SilenceInserted.
// The caller owns the returned frame and must PutFrame it.
func (j *JitterBuffer) Release() *[]byte {
	j.mu.Lock()
	defer j.mu.Unlock()

	slot := int(j.expected) % j.size

	var out *[]byte

	if j.have[slot] {
		out = j.slots[slot]
		j.slots[slot] = nil
		j.have[slot] = false
	} else {
		out = GetFrame()
		*out = (*out)[:FrameSize]

		for i := range *out {
			(*out)[i] = 0
		}

		j.sink.SilenceInserted()
	}

	j.expected++

	return out
}

// resetLocked drops every buffered frame and re-anchors the window on a new SSRC.
// Callers must hold j.mu.
func (j *JitterBuffer) resetLocked(seq uint16, ssrc uint32) {
	for i := range j.slots {
		if j.have[i] {
			PutFrame(j.slots[i])
			j.slots[i] = nil
			j.have[i] = false
		}
	}

	j.expected = seq
	j.ssrc = ssrc
}
