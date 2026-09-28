package agent

import (
	"encoding/binary"
	"log/slog"
	"time"
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
	callID string
	hop    HopSpeechFunc

	// carry holds LE-decoded samples not yet a full hop; grown by 320 every
	// Detect call, drained 256 at a time.
	carry []int16

	// hopErrLogged: a persistent native failure would otherwise flood the
	// log at ~50 lines/sec/call; log the first error at Warn, the rest at
	// Debug (every failed hop still votes "not speech").
	hopErrLogged bool

	// speechActive/speechStartedAt track the vote window's state so Detect
	// can log SPEECH ONSET and OFFSET transitions -- "speech started" when
	// sustained caller speech first crosses the vote threshold, "speech
	// ended" (with how long it lasted) when it drops back. Per-hop verdicts
	// themselves are never logged: at ~60 hops/sec/call that's a log flood.
	// These are ProcessFrame-goroutine-owned, like everything else here.
	speechActive    bool
	speechStartedAt time.Time

	history [bargeInHistoryFrames]bool
	idx     int
}

// NewTenVadDetector creates a detector that feeds hops to hop (the native
// TEN VAD binding on Linux, a fake in tests) and votes on its per-hop
// verdicts, logging speech onset/offset transitions under callID.
func NewTenVadDetector(callID string, hop HopSpeechFunc) *TenVadDetector {
	return &TenVadDetector{
		callID: callID,
		hop:    hop,
		carry:  make([]int16, 0, tenVadHopSize+320),
	}
}

// Detect decodes one 20ms LE PCM16 frame (the BargeInDetector contract),
// drains every full hop it completes into the VAD, and reports whether the
// last bargeInHistoryFrames hops contain bargeInVotesNeeded speech verdicts.
// A hop that errors counts as "not speech" -- barge-in must never wedge on a
// detector problem. Logs "speech started"/"speech ended" on window
// transitions, nothing else.
func (d *TenVadDetector) Detect(pcm []byte) bool {
	for i := 0; i+1 < len(pcm); i += 2 {
		d.carry = append(d.carry, int16(binary.LittleEndian.Uint16(pcm[i:])))
	}

	result := false

	for len(d.carry) >= tenVadHopSize {
		hop := d.carry[:tenVadHopSize]

		speech, err := d.hop(hop)
		if err != nil {
			if !d.hopErrLogged {
				d.hopErrLogged = true
				slog.Warn("ten vad process failed; hops count as silence until it recovers", "call_id", d.callID, "error", err)
			} else {
				slog.Debug("ten vad process failed", "call_id", d.callID, "error", err)
			}
		}

		d.history[d.idx] = speech && err == nil
		d.idx = (d.idx + 1) % len(d.history)

		d.carry = d.carry[tenVadHopSize:]

		// The window verdict as of THIS hop drives the onset/offset logs --
		// a single Detect call can straddle a transition (it usually carries
		// two hops), and each transition must log exactly once.
		votes := 0
		for _, speech := range d.history {
			if speech {
				votes++
			}
		}

		result = votes >= bargeInVotesNeeded

		switch {
		case result && !d.speechActive:
			d.speechActive = true
			d.speechStartedAt = time.Now()
			slog.Info("speech started", "call_id", d.callID)
		case !result && d.speechActive:
			d.speechActive = false
			slog.Info("speech ended", "call_id", d.callID,
				"duration_ms", time.Since(d.speechStartedAt).Milliseconds())
		}
	}

	return result
}

// Reset clears the carry buffer and vote history for a new SPEAKING turn.
// Deliberately does NOT log "speech ended": Reset runs right after a barge-in
// fired, whose own log line already marks the onset; an offset here would be
// noise. The native instance's internal model state persists across turns --
// it carries no per-turn decision memory that would misfire here, and
// re-creating the instance per turn would put a native allocation on the
// barge-in path for no benefit.
func (d *TenVadDetector) Reset() {
	d.carry = d.carry[:0]
	d.history = [bargeInHistoryFrames]bool{}
	d.idx = 0
	d.speechActive = false
}
