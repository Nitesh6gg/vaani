package agent

import "github.com/nitesh/vaani/internal/media"

// BargeInDetector decides, frame by frame, whether the caller has started
// talking over the agent's TTS playback. STT can't provide this signal: it
// isn't fed audio during SPEAKING (feeding it would transcribe the agent's own
// voice leaking back through the caller's mic), so barge-in detection has to
// run locally on every inbound frame regardless of state. Implementations must
// be safe to call once per 20ms frame from ProcessFrame's single goroutine --
// no blocking, no I/O.
type BargeInDetector interface {
	// Detect reports whether pcm (one 20ms LE PCM16 frame) counts as speech.
	Detect(pcm []byte) bool
	// Reset clears any internal history (call on leaving SPEAKING, so the next
	// SPEAKING turn starts with a clean slate rather than stale history).
	Reset()
}

// bargeInHistoryFrames is how many recent frames vote on whether the caller is
// speaking; bargeInVotesNeeded of them must say "speech" before EnergyDetector
// declares barge-in, so one loud click or a line-noise spike can't trigger it.
const (
	bargeInHistoryFrames = 5
	bargeInVotesNeeded   = 3
)

// EnergyDetector is the default BargeInDetector: a plain RMS-threshold gate.
// VAD_MODE=energy selects this; VAD_MODE=ten swaps in TEN VAD
// (internal/ai/agent/tenvad, Linux/cgo only) behind the same interface, with no
// change to the state machine that consumes it.
//
// ponytail: RMS-over-threshold is a naive heuristic with a real ceiling -- it
// can't distinguish speech from a loud cough or music on hold. Upgrade path is
// VAD_MODE=ten once it's benchmarked live; this is the correct default until
// then; it needs no native dependency and no live tuning to ship.
type EnergyDetector struct {
	floor   float64
	history [bargeInHistoryFrames]bool
	idx     int
}

// NewEnergyDetector creates a detector that treats a frame as speech once its
// RMS (see media.RMS) reaches rmsFloor.
func NewEnergyDetector(rmsFloor float64) *EnergyDetector {
	return &EnergyDetector{floor: rmsFloor}
}

// Detect votes across the last bargeInHistoryFrames frames seen so far this
// turn (unseen slots default to "not speech", their zero value) -- so 3
// genuine consecutive loud frames can trigger within the first 3 calls of a
// turn rather than waiting for a full 5-frame window to accumulate first.
// Speed matters more than that extra wait here: the acceptance target is
// stopping playback within 300ms of the caller actually starting to talk.
func (d *EnergyDetector) Detect(pcm []byte) bool {
	d.history[d.idx] = media.RMS(pcm) >= d.floor
	d.idx = (d.idx + 1) % len(d.history)

	votes := 0
	for _, speech := range d.history {
		if speech {
			votes++
		}
	}

	return votes >= bargeInVotesNeeded
}

func (d *EnergyDetector) Reset() {
	d.history = [bargeInHistoryFrames]bool{}
	d.idx = 0
}

// NoopBargeInDetector never reports speech: BARGE_IN_ENABLED=0 selects this
// instead of EnergyDetector, so the agent keeps talking over any inbound
// audio until its turn finishes -- useful for isolating whether a problem is
// in barge-in detection itself versus the rest of the pipeline.
type NoopBargeInDetector struct{}

func (NoopBargeInDetector) Detect([]byte) bool { return false }
func (NoopBargeInDetector) Reset()             {}
