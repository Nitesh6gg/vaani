package agent

import (
	"context"
	"encoding/binary"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/nitesh/vaani/internal/ai/tts"
)

// pcmFrame encodes int16 samples as one 20ms LE PCM16 frame, the shape
// ProcessFrame hands BargeInDetector.
func pcmFrame(samples []int16) []byte {
	buf := make([]byte, len(samples)*2)
	for i, s := range samples {
		binary.LittleEndian.PutUint16(buf[i*2:], uint16(s))
	}

	return buf
}

func loudFrame() []byte {
	samples := make([]int16, 320) // 20ms @ 16kHz
	for i := range samples {
		samples[i] = 10000
	}

	return pcmFrame(samples)
}

func quietFrame() []byte {
	samples := make([]int16, 320) // 20ms @ 16kHz
	for i := range samples {
		samples[i] = 5
	}

	return pcmFrame(samples)
}

// levelHop is a HopSpeechFunc fake with real-input-dependent verdicts: a hop
// counts as speech when its leading sample's absolute value reaches floor
// (loudFrame=10000, quietFrame=5). It records every hop so tests can assert
// exact hop sizes and sample values.
type levelHop struct {
	hops  [][]int16
	floor int16
	err   error
}

func newLevelHop() *levelHop { return &levelHop{floor: 1000} }

func (r *levelHop) detect(hop []int16) (bool, error) {
	cp := make([]int16, len(hop))
	copy(cp, hop)
	r.hops = append(r.hops, cp)

	if r.err != nil {
		return false, r.err
	}

	s := hop[0]
	if s < 0 {
		s = -s
	}

	return s >= r.floor, nil
}

func TestTenVadDetector_Rebuffers320SampleFramesInto256SampleHops(t *testing.T) {
	rh := newLevelHop()
	d := NewTenVadDetector("call1", rh.detect)

	// 4 frames x 320 samples = 1280 samples = exactly 5 hops of 256, with no
	// remainder left over at the end of the fourth frame.
	for i := 0; i < 4; i++ {
		d.Detect(loudFrame())
	}

	require.Len(t, rh.hops, 5, "1+1+1+2 hops as the 64-sample remainder accumulates")

	for i, hop := range rh.hops {
		assert.Len(t, hop, tenVadHopSize, "hop %d", i)
	}

	// And every sample must arrive in order, byte-order intact.
	assert.Equal(t, int16(10000), rh.hops[0][0])
	assert.Equal(t, int16(10000), rh.hops[4][tenVadHopSize-1])
}

func TestTenVadDetector_VotesAcrossHopsLikeEnergyDetector(t *testing.T) {
	rh := newLevelHop()
	d := NewTenVadDetector("call1", rh.detect)

	// Three consecutive speech hops are the earliest a 3-of-5 vote can fire.
	assert.False(t, d.Detect(loudFrame()), "one speech hop must not trigger")
	assert.False(t, d.Detect(loudFrame()), "two speech hops must not trigger")
	assert.True(t, d.Detect(loudFrame()), "three speech hops must trigger")

	// Silence drains the votes back out as the loud hops age out of the
	// 5-hop window. Note the hop straddling the loud->quiet boundary is
	// still 75% loud samples (192 loud + 64 quiet, from the carry buffer),
	// so it legitimately votes speech: it takes three quiet frames for the
	// window to fully clear.
	assert.True(t, d.Detect(quietFrame()))
	assert.True(t, d.Detect(quietFrame()))
	assert.False(t, d.Detect(quietFrame()))
}

func TestTenVadDetector_ResetClearsVotesAndCarry(t *testing.T) {
	rh := newLevelHop()
	d := NewTenVadDetector("call1", rh.detect)

	require.False(t, d.Detect(loudFrame()))
	require.False(t, d.Detect(loudFrame()))
	require.True(t, d.Detect(loudFrame()))
	require.True(t, d.Detect(loudFrame()))

	d.Reset()

	assert.False(t, d.Detect(loudFrame()), "after Reset a single speech hop must not trigger on stale votes")
	assert.False(t, d.Detect(loudFrame()))
	assert.True(t, d.Detect(loudFrame()), "and three fresh speech hops must be required again")

	// A partial hop left over before Reset must not leak into later hops:
	// after a clean Reset, 4 frames must produce exactly the 1+1+1+2 hop
	// pattern again, every hop a full 256 samples.
	d.Reset()
	rh.hops = nil

	for i := 0; i < 4; i++ {
		d.Detect(loudFrame())
	}

	require.Len(t, rh.hops, 5, "hop pattern restarts cleanly after Reset")

	for i, hop := range rh.hops {
		assert.Len(t, hop, tenVadHopSize, "hop %d", i)
	}
}

func TestTenVadDetector_HopErrorVotesNotSpeechAndDoesNotPanic(t *testing.T) {
	rh := &levelHop{floor: 1000, err: errors.New("native exploded")}
	d := NewTenVadDetector("call1", rh.detect)

	for i := 0; i < 6; i++ {
		assert.False(t, d.Detect(loudFrame()), "failed hops must count as silence, never trigger barge-in")
	}
}

func TestTenVadDetector_SpeechRunFiresMidSequence(t *testing.T) {
	// The real shape: caller mostly quiet, then starts talking over playback.
	hopVerdicts := []bool{false, false, false, true, true, true}
	i := 0

	d := NewTenVadDetector("call1", func([]int16) (bool, error) {
		v := hopVerdicts[i%len(hopVerdicts)]
		i++

		return v, nil
	})

	fired := false

	for call := 0; call < 6; call++ {
		if d.Detect(quietFrame()) {
			fired = true
		}
	}

	assert.True(t, fired, "a speech run of three hops must fire barge-in within the sequence")
}

// TestTenVadDetector_LogsSpeechStartAndEndTransitionsOnly pins the onset/
// offset logging contract: exactly one "speech started" when sustained
// speech first crosses the vote threshold, exactly one "speech ended" (with
// its duration) when it drops back, and NOTHING for stable state -- per-hop
// verdicts would flood the log at ~60 lines/sec/call.
func TestTenVadDetector_LogsSpeechStartAndEndTransitionsOnly(t *testing.T) {
	buf := captureSlog(t)()
	rh := newLevelHop()
	d := NewTenVadDetector("call1", rh.detect)

	// Sustained silence: no transitions, no logs.
	d.Detect(quietFrame())
	d.Detect(quietFrame())
	assert.Empty(t, buf.String(), "quiet frames below the vote threshold must not log anything")

	// Clear the vote window (Reset is silent) so the onset below starts from
	// a clean slate: three speech hops flip it, exactly one onset.
	d.Reset()
	d.Detect(loudFrame())
	d.Detect(loudFrame())
	assert.Empty(t, buf.String(), "one/two speech hops must not log anything")
	d.Detect(loudFrame())
	assert.Equal(t, 1, strings.Count(buf.String(), "speech started"))
	assert.NotContains(t, buf.String(), "speech ended")

	// Continued speech: the 192-sample carry from the onset frame makes this
	// frame emit TWO hops (window now all-speech) -- still exactly one onset.
	d.Detect(loudFrame())
	assert.Equal(t, 1, strings.Count(buf.String(), "speech started"))

	// Quiet until the vote window drops below threshold. Each quiet frame
	// emits one hop (carry stays under a hop), retiring one loud vote per
	// call: votes go 5 -> 4 -> 3 -> 2, so the offset lands on the THIRD
	// quiet call, with a duration.
	d.Detect(quietFrame())
	d.Detect(quietFrame())
	assert.NotContains(t, buf.String(), "speech ended")
	d.Detect(quietFrame())
	assert.Equal(t, 1, strings.Count(buf.String(), "speech ended"))
	assert.Contains(t, buf.String(), "duration_ms=")

	// Sustained silence again: nothing more.
	d.Detect(quietFrame())
	d.Detect(quietFrame())
	assert.Equal(t, 1, strings.Count(buf.String(), "speech ended"))
	assert.Equal(t, 1, strings.Count(buf.String(), "speech started"))
}

func TestTenVadDetector_ResetDoesNotLogSpeechEnded(t *testing.T) {
	buf := captureSlog(t)()
	rh := newLevelHop()
	d := NewTenVadDetector("call1", rh.detect)

	// Speech starts (barge-in would fire here in production).
	d.Detect(loudFrame())
	d.Detect(loudFrame())
	d.Detect(loudFrame())
	require.Equal(t, 1, strings.Count(buf.String(), "speech started"))

	// Reset runs right after the barge-in log, which already marks the
	// onset -- an offset here would be noise, so Reset must stay silent.
	d.Reset()
	assert.NotContains(t, buf.String(), "speech ended")

	// And the next turn must be able to log a fresh onset.
	d.Detect(loudFrame())
	d.Detect(loudFrame())
	d.Detect(loudFrame())
	assert.Equal(t, 2, strings.Count(buf.String(), "speech started"))
	assert.NotContains(t, buf.String(), "speech ended")
}

// TestHandler_ObserveOnlyNeverCuts pins the BARGE_IN_ENABLED=0 + VAD_MODE=ten
// semantics ("observe, don't cut"): the real detector still runs and its
// speech started transition still logs, but a sustained speech verdict never
// fires a barge-in -- the agent keeps playing its reply, the TTS connection
// stays alive, and no preroll is flushed to STT.
func TestHandler_ObserveOnlyNeverCuts(t *testing.T) {
	buf := captureSlog(t)()
	fSTT := newFakeSTT()
	fTTS := newFakeTTS()
	fLLM := &fakeLLM{tokens: []string{"a reply the caller talks over"}}

	rh := newLevelHop()

	h := NewHandler(context.Background(), "call1", Config{
		STT:                fSTT,
		NewTTS:             func() (tts.Client, error) { return fTTS, nil },
		LLM:                fLLM,
		BargeIn:            NewTenVadDetector("call1", rh.detect),
		BargeInObserveOnly: true,
		BargeInGuard:       0,
		PostCutSilence:     50 * time.Millisecond,
		Sink:               NoopSink{},
	})

	fSTT.sendFinal("hi")
	require.Eventually(t, func() bool { return fTTS.spokenCount() > 0 }, time.Second, time.Millisecond)

	const gen = 1

	fTTS.audio <- tts.Chunk{PCM: constFrame(loudAmplitude), Gen: gen}
	require.Eventually(t, func() bool { return h.State() == StateSpeaking }, time.Second, time.Millisecond)

	fedBefore := fSTT.fedCount()

	// Far more loud frames than a 3-of-5 vote needs: in cutting mode this
	// would fire barge-in several times over.
	for i := 0; i < 12; i++ {
		h.ProcessFrame(context.Background(), "call1", constFrame(loudAmplitude))
	}

	assert.Equal(t, StateSpeaking, h.State(), "observe-only must never leave Speaking via a barge-in")
	assert.False(t, fTTS.wasCancelled(), "observe-only must never cancel the TTS connection")
	assert.Equal(t, fedBefore, fSTT.fedCount(), "observe-only must not flush preroll to STT")
	assert.Equal(t, 1, strings.Count(buf.String(), "speech started"),
		"the detector's transition logging must survive in observe-only mode")
	assert.NotContains(t, buf.String(), "agent barge-in detected")
}
