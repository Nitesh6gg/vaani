package agent

import (
	"encoding/binary"
	"log/slog"
)

// tenVadHopSize is TEN VAD's analysis hop in samples: 256 = 16ms at 16kHz.
// It deliberately does not divide the 20ms media frame (320 samples): frames
// accumulate in TenVadDetector's carry buffer and are drained as whole hops,
// with the remainder carrying over to the next call.
const tenVadHopSize = 256

// HopSpeechFunc runs one hop of int16 samples (LE PCM16, 16kHz) through a
// speech/no-speech decision and reports the binary verdict. On Linux this is
// the native TEN VAD model (see internal/ai/agent/tenvad); tests inject a
// fake so the buffering and voting below are testable on any platform.
type HopSpeechFunc func(hop []int16) (bool, error)

// TenVadDetector is the VAD_MODE=ten BargeInDetector: a real neural VAD
// where EnergyDetector's RMS threshold hits its ceiling (speech vs cough,
// music-on-hold, line noise). It re-buffers the 20ms frames it receives into
// TEN VAD's 16ms hops and votes across the last bargeInHistoryFrames hops
// exactly like EnergyDetector votes across frames, so the consuming state
// machine (processSpeakingFrame) sees identical semantics from either
// detector.
//
// Safe for its intended caller only: ProcessFrame's single goroutine, once
// per 20ms frame -- no locking, no blocking, no I/O (the native process call
// is CPU-only and bounded).
type TenVadDetector struct {
	hop HopSpeechFunc

	// carry holds LE-decoded samples not yet a full hop; grown by 320 every
	// Detect call, drained 256 at a time.
	carry []int16

	// hopErrLogged: a persistent native failure would otherwise flood the
	// log at ~50 lines/sec/call; log the first error at Warn, the rest at
	// Debug (every failed hop still votes "not speech").
	hopErrLogged bool

	history [bargeInHistoryFrames]bool
	idx     int
}

// NewTenVadDetector creates a detector that feeds hops to hop (the native
// TEN VAD binding on Linux, a fake in tests) and votes on its per-hop
// verdicts.
func NewTenVadDetector(hop HopSpeechFunc) *TenVadDetector {
	return &TenVadDetector{hop: hop, carry: make([]int16, 0, tenVadHopSize+320)}
}

// Detect decodes one 20ms LE PCM16 frame (the BargeInDetector contract),
// drains every full hop it completes into the VAD, and reports whether the
// last bargeInHistoryFrames hops contain bargeInVotesNeeded speech verdicts.
// A hop that errors counts as "not speech" -- barge-in must never wedge on a
// detector problem.
func (d *TenVadDetector) Detect(pcm []byte) bool {
	for i := 0; i+1 < len(pcm); i += 2 {
		d.carry = append(d.carry, int16(binary.LittleEndian.Uint16(pcm[i:])))
	}

	for len(d.carry) >= tenVadHopSize {
		hop := d.carry[:tenVadHopSize]

		speech, err := d.hop(hop)
		if err != nil {
			if !d.hopErrLogged {
				d.hopErrLogged = true
				slog.Warn("ten vad process failed; hops count as silence until it recovers", "error", err)
			} else {
				slog.Debug("ten vad process failed", "error", err)
			}
		}

		d.history[d.idx] = speech && err == nil
		d.idx = (d.idx + 1) % len(d.history)

		d.carry = d.carry[tenVadHopSize:]
	}

	votes := 0
	for _, speech := range d.history {
		if speech {
			votes++
		}
	}

	return votes >= bargeInVotesNeeded
}

// Reset clears the carry buffer and vote history for a new SPEAKING turn.
// The native instance's internal model state persists across turns -- it
// carries no per-turn decision memory that would misfire here, and
// re-creating the instance per turn would put a native allocation on the
// barge-in path for no benefit.
func (d *TenVadDetector) Reset() {
	d.carry = d.carry[:0]
	d.history = [bargeInHistoryFrames]bool{}
	d.idx = 0
}
