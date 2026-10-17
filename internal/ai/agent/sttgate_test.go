package agent

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/nitesh/vaani/internal/media"
)

// gateRig is a gate fed by a fake clock and recording what it sends.
type gateRig struct {
	g    *sttGate
	now  time.Time
	sent [][]byte
	err  error
}

func newGateRig(preroll, hangover time.Duration) *gateRig {
	r := &gateRig{now: time.Unix(1000, 0)}
	r.g = newSTTGate(STTGateConfig{Enabled: true, Preroll: preroll, Hangover: hangover},
		func(p []byte) error {
			r.sent = append(r.sent, append([]byte(nil), p...))

			return r.err
		}, func() time.Time { return r.now })

	return r
}

// tick advances the clock one frame and offers a frame whose bytes all equal id.
func (r *gateRig) tick(id byte, voiced bool) error {
	r.now = r.now.Add(gateFrameDur)

	f := make([]byte, media.FrameSize)
	for i := range f {
		f[i] = id
	}

	return r.g.frame(f, voiced)
}

func (r *gateRig) ids() []byte {
	var out []byte

	for _, p := range r.sent {
		if len(p) == media.FrameSize {
			out = append(out, p[0])
		}
	}

	return out
}

func TestSTTGate_SilenceSendsNothingBetweenKeepalives(t *testing.T) {
	r := newGateRig(200*time.Millisecond, 100*time.Millisecond)

	for i := 0; i < 500; i++ { // 10s of quiet
		require.NoError(t, r.tick(1, false))
	}

	assert.Empty(t, r.sent, "nothing goes out while quiet")
	assert.Zero(t, r.g.Sent())

	for i := 0; i < 500; i++ { // 20s in all: the keepalive is due
		require.NoError(t, r.tick(1, false))
	}

	require.Len(t, r.sent, 1, "one keepalive")
	assert.Equal(t, int(gateKeepaliveChunk/gateFrameDur)*media.FrameSize, len(r.sent[0]))
	assert.Equal(t, make([]byte, len(r.sent[0])), r.sent[0], "it is silence")
	assert.InDelta(t, 0.1, r.g.Sent(), 0.001)
}

func TestSTTGate_SpeechSendsPrerollThenLive(t *testing.T) {
	r := newGateRig(200*time.Millisecond, 100*time.Millisecond) // 10 frames of pre-roll

	for id := byte(1); id <= 30; id++ { // quiet frames 1..30
		require.NoError(t, r.tick(id, false))
	}

	require.Empty(t, r.sent)

	require.NoError(t, r.tick(31, true))
	assert.Equal(t, []byte{21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31}, r.ids(),
		"the 10 frames before the speech, oldest first, then the speech frame")

	require.NoError(t, r.tick(32, true))
	assert.Equal(t, byte(32), r.ids()[len(r.ids())-1], "then live frames")
}

func TestSTTGate_ShortBufferSendsWhatItHas(t *testing.T) {
	r := newGateRig(time.Second, 100*time.Millisecond)

	require.NoError(t, r.tick(1, false))
	require.NoError(t, r.tick(2, false))
	require.NoError(t, r.tick(3, true))

	assert.Equal(t, []byte{1, 2, 3}, r.ids())
}

func TestSTTGate_ClosesAfterHangoverAndRefillsPreroll(t *testing.T) {
	r := newGateRig(100*time.Millisecond, 60*time.Millisecond) // 5 frames, 3 frames

	require.NoError(t, r.tick(1, true))
	require.Equal(t, []byte{1}, r.ids())

	// Quiet: the hangover (60ms = 3 frames) keeps sending, then the gate closes.
	for id := byte(2); id <= 8; id++ {
		require.NoError(t, r.tick(id, false))
	}

	assert.Equal(t, []byte{1, 2, 3, 4}, r.ids(), "frames up to and just past the hangover")

	// Speech again: only frames since the gate closed are buffered, so nothing
	// that was already sent goes out twice.
	require.NoError(t, r.tick(9, true))
	assert.Equal(t, []byte{1, 2, 3, 4, 5, 6, 7, 8, 9}, r.ids()[:9], "frames 5..8 were held, now sent once")
	assert.Equal(t, 9, len(r.ids()))
}

func TestSTTGate_ContinuousSpeechNeverBuffersOrDuplicates(t *testing.T) {
	r := newGateRig(200*time.Millisecond, 100*time.Millisecond)

	for id := byte(1); id <= 50; id++ {
		require.NoError(t, r.tick(id, true))
	}

	require.Len(t, r.ids(), 50)

	for i, id := range r.ids() {
		assert.Equal(t, byte(i+1), id)
	}
}

func TestSTTGate_FeedErrorIsReturnedAndStateKeepsGoing(t *testing.T) {
	r := newGateRig(100*time.Millisecond, 60*time.Millisecond)
	r.err = errors.New("stt: not connected")

	assert.Error(t, r.tick(1, true))
	assert.Error(t, r.tick(2, true), "still open: each frame is offered")

	r.err = nil

	assert.NoError(t, r.tick(3, true))
}

func TestSTTGate_Voiced(t *testing.T) {
	now := time.Now()
	loud, quiet := make([]byte, media.FrameSize), make([]byte, media.FrameSize)

	for i := 0; i+1 < len(loud); i += 2 {
		loud[i], loud[i+1] = 0xE8, 0x03 // 1000
	}

	for _, tc := range []struct {
		name  string
		cfg   STTGateConfig
		last  time.Time
		frame []byte
		want  bool
	}{
		{"VAD heard speech a moment ago", STTGateConfig{}, now.Add(-50 * time.Millisecond), quiet, true},
		{"VAD speech too long ago", STTGateConfig{}, now.Add(-time.Second), quiet, false},
		{"VAD never heard any", STTGateConfig{}, time.Time{}, quiet, false},
		{"quiet frame, floor set", STTGateConfig{RMSFloor: 400}, time.Time{}, quiet, false},
		{"loud frame the VAD missed", STTGateConfig{RMSFloor: 400}, time.Time{}, loud, true},
		{"loud frame, floor off", STTGateConfig{}, time.Time{}, loud, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, tc.cfg.voiced(tc.last, now, tc.frame))
		})
	}
}
