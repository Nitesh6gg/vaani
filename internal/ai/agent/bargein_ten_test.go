package agent

import (
	"encoding/binary"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
	d := NewTenVadDetector(rh.detect)

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
	d := NewTenVadDetector(rh.detect)

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
	d := NewTenVadDetector(rh.detect)

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
	d := NewTenVadDetector(rh.detect)

	for i := 0; i < 6; i++ {
		assert.False(t, d.Detect(loudFrame()), "failed hops must count as silence, never trigger barge-in")
	}
}

func TestTenVadDetector_SpeechRunFiresMidSequence(t *testing.T) {
	// The real shape: caller mostly quiet, then starts talking over playback.
	hopVerdicts := []bool{false, false, false, true, true, true}
	i := 0

	d := NewTenVadDetector(func([]int16) (bool, error) {
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
