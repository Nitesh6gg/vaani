package media

import (
	"bytes"
	"context"
	"encoding/binary"
	"math"
	"sync"
	"time"
)

// maxMixRecording caps a MixRecorder at Dograh's own in-memory recording
// limit (100MB, ~54 minutes of 16kHz mono); anything past it isn't kept.
const maxMixRecording = 100 << 20

// mixPrealloc is the buffer reserved up front, so a typical call never grows
// it on the 20ms tick: a minute of audio.
const mixPrealloc = int(time.Minute/FrameInterval) * FrameSize

// MixRecorder wraps a Handler and keeps the whole call as one mono track --
// the caller and the agent mixed, like Dograh's call recording (pipecat's
// AudioBufferProcessor, mono). Both directions meet in ProcessFrame, once per
// 20ms tick, so they stay in step without timestamps. Safe to read (WAV) from
// another goroutine while the call runs.
type MixRecorder struct {
	Handler

	mu        sync.Mutex
	pcm       []byte
	max       int  // maxMixRecording; smaller in tests
	truncated bool // the cap was hit: the rest of the call isn't recorded
	// silent holds the offsets of ticks recorded with no caller audio (the frame
	// had not arrived yet): a late frame is mixed into the oldest of them, so
	// the recording keeps its length and the audio lands where it belongs.
	silent []int
}

// maxSilentSlots bounds silent: a stall longer than this (2 s) loses the
// oldest slots, whose late audio is then not recorded.
const maxSilentSlots = 100

var zeroFrame = make([]byte, FrameSize)

// NewMixRecorder wraps h.
func NewMixRecorder(h Handler) *MixRecorder {
	return &MixRecorder{Handler: h, pcm: make([]byte, 0, mixPrealloc), max: maxMixRecording}
}

// ProcessFrame implements Handler: h's output is returned unchanged; the
// inbound frame plus what goes out this tick is recorded.
func (r *MixRecorder) ProcessFrame(ctx context.Context, callID string, pcm []byte) [][]byte {
	out := r.Handler.ProcessFrame(ctx, callID, pcm)

	r.mu.Lock()
	defer r.mu.Unlock()

	if len(r.pcm)+len(pcm) > r.max {
		r.truncated = true
		return out
	}

	at := len(r.pcm)
	r.pcm = append(r.pcm, pcm...)

	if bytes.Equal(pcm, zeroFrame) {
		if len(r.silent) == maxSilentSlots {
			r.silent = r.silent[1:]
		}

		r.silent = append(r.silent, at)
	}

	// A Handler sends at most one frame a tick in practice; any more are
	// mixed into the same slot rather than shifting the timeline.
	for _, f := range out {
		mixInto(r.pcm[at:], f)
	}

	return out
}

// HearLate implements LateHearer: a caller frame that missed its tick is
// passed on to the wrapped handler (if it can hear it) and mixed into the
// oldest tick that was recorded without caller audio.
func (r *MixRecorder) HearLate(pcm []byte) {
	if lh, ok := r.Handler.(LateHearer); ok {
		lh.HearLate(pcm)
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if len(r.silent) == 0 {
		return
	}

	at := r.silent[0]
	r.silent = r.silent[1:]

	mixInto(r.pcm[at:min(at+FrameSize, len(r.pcm))], pcm)
}

// mixInto adds src's PCM16 LE samples into dst's, clipping at int16's range.
func mixInto(dst, src []byte) {
	n := min(len(dst), len(src)) &^ 1

	for i := 0; i < n; i += 2 {
		s := int32(int16(binary.LittleEndian.Uint16(dst[i:]))) + int32(int16(binary.LittleEndian.Uint16(src[i:])))
		s = max(math.MinInt16, min(math.MaxInt16, s))
		binary.LittleEndian.PutUint16(dst[i:], uint16(int16(s)))
	}
}

// Truncated reports whether the size cap cut the recording short (the
// caller logs it: this runs on the 20ms tick, where nothing may log).
func (r *MixRecorder) Truncated() bool {
	r.mu.Lock()
	defer r.mu.Unlock()

	return r.truncated
}

// WAV returns the recording so far as a 16kHz mono PCM16 WAV file, or nil
// when nothing was recorded.
func (r *MixRecorder) WAV() []byte {
	r.mu.Lock()
	defer r.mu.Unlock()

	if len(r.pcm) == 0 {
		return nil
	}

	var b bytes.Buffer

	b.Grow(wavHeaderSize + len(r.pcm))
	_ = writeWavHeader(&b, len(r.pcm)) // a bytes.Buffer write can't fail
	b.Write(r.pcm)

	return b.Bytes()
}
