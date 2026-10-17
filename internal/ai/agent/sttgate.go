package agent

import (
	"sync/atomic"
	"time"

	"github.com/nitesh/vaani/internal/media"
)

const (
	// gateFrameDur is one media frame (media.FrameSize bytes of 16kHz s16le).
	gateFrameDur = 20 * time.Millisecond
	// gateBytesPerSecond is 16kHz * 2 bytes.
	gateBytesPerSecond = 32000
	// gateKeepalive: while the gate is closed Sarvam gets one silent chunk this
	// often. Its docs close an idle socket after 60s (our own probe saw 70s
	// survive), and the legacy endpoint rejects a ping, so silent audio it is
	// (pipecat's keepalive does the same). Short enough that a socket that
	// died while nothing was being sent is noticed -- and redialed -- well
	// before the caller's next words need it.
	gateKeepalive = 20 * time.Second
	// gateKeepaliveChunk is the silence sent as that keepalive: 100ms, as
	// pipecat's (and what the 2026-10 probe used).
	gateKeepaliveChunk = 100 * time.Millisecond
	// gateVoicedWindow: the VAD heard speech this recently counts as speech now.
	// LastSpeechAt is the last 16ms hop that was speech, not a smoothed verdict.
	gateVoicedWindow = 100 * time.Millisecond
)

// STTGateConfig configures speech-only sending to the STT (STT_SPEECH_ONLY).
// See sttGate.
type STTGateConfig struct {
	Enabled bool
	// Preroll is how much audio from before speech was noticed goes out when
	// the gate opens, so a word's first syllable isn't lost to the VAD's lag
	// (pipecat's SegmentedSTTService keeps 1s for the same reason).
	Preroll time.Duration
	// Hangover is how long the gate stays open after the last speech: it has
	// to outlast STTFlushAfter plus the STT's reply to the flush.
	Hangover time.Duration
	// RMSFloor, if above 0, also opens the gate for a frame at least this
	// loud (media.RMS), a net under a VAD that misses a quiet word. 0 = off.
	RMSFloor float64
}

// sttGate decides which of the caller's frames are worth sending to the STT.
// Sarvam bills the audio it is sent (verified 2026-10-13: 100s connection,
// 10s sent, 10s billed), and Vaani used to send every frame of the call
// (CLAUDE.md invariant 7, kept so a word begun over the agent isn't clipped).
// The gate keeps that promise differently: every frame goes into a ring
// buffer first, and when speech is noticed the buffered Preroll goes out
// ahead of the live frames, so the first syllable still arrives.
//
// Closed: frames are kept (the newest Preroll's worth), nothing is sent, but
// a short silent chunk goes out every gateKeepalive so the connection lives.
// Open: every frame is sent until Hangover after the last speech.
//
// One goroutine (ProcessFrame's) calls frame. sent may be read from any.
type sttGate struct {
	feed     func([]byte) error
	now      func() time.Time
	hangover time.Duration

	// ring holds the last len(ring) frames while closed: slot count..head.
	ring        [][]byte
	head, count int

	open       bool
	lastVoiced time.Time
	lastSent   time.Time
	silence    []byte

	sent atomic.Int64 // bytes handed to feed
}

// newSTTGate builds a gate that sends through feed. now is time.Now outside
// tests.
func newSTTGate(cfg STTGateConfig, feed func([]byte) error, now func() time.Time) *sttGate {
	frames := max(1, int(cfg.Preroll/gateFrameDur))

	g := &sttGate{
		feed:     feed,
		now:      now,
		hangover: cfg.Hangover,
		ring:     make([][]byte, frames),
		silence:  make([]byte, int(gateKeepaliveChunk/gateFrameDur)*media.FrameSize),
		lastSent: now(),
	}

	for i := range g.ring {
		g.ring[i] = make([]byte, 0, media.FrameSize)
	}

	return g
}

// frame takes one 20ms frame and whether it counts as speech, and sends what
// the STT should get: nothing, the frame, or the buffered pre-roll and the
// frame. Errors are feed's (the frame or pre-roll frames it refused).
func (g *sttGate) frame(pcm []byte, voiced bool) error {
	now := g.now()

	if voiced {
		g.lastVoiced = now
	}

	if g.open && now.Sub(g.lastVoiced) > g.hangover {
		g.open = false
	}

	if g.open {
		return g.send(pcm, now)
	}

	if voiced {
		g.open = true

		err := g.sendPreroll(now)
		if e := g.send(pcm, now); err == nil {
			err = e
		}

		return err
	}

	g.push(pcm)

	if now.Sub(g.lastSent) >= gateKeepalive {
		return g.send(g.silence, now)
	}

	return nil
}

// Sent is the audio handed to the STT so far, in seconds.
func (g *sttGate) Sent() float64 { return float64(g.sent.Load()) / gateBytesPerSecond }

func (g *sttGate) send(pcm []byte, now time.Time) error {
	g.sent.Add(int64(len(pcm)))
	g.lastSent = now

	return g.feed(pcm)
}

// push keeps pcm as the newest frame, dropping the oldest when full.
func (g *sttGate) push(pcm []byte) {
	g.ring[g.head] = append(g.ring[g.head][:0], pcm...)
	g.head = (g.head + 1) % len(g.ring)
	g.count = min(g.count+1, len(g.ring))
}

// sendPreroll sends the buffered frames oldest first and empties the buffer.
func (g *sttGate) sendPreroll(now time.Time) error {
	var first error

	for i := 0; i < g.count; i++ {
		idx := (g.head - g.count + i + len(g.ring)) % len(g.ring)
		if err := g.send(g.ring[idx], now); err != nil && first == nil {
			first = err
		}
	}

	g.count = 0

	return first
}

// voiced reports whether a frame counts as speech for the gate: the VAD heard
// speech a moment ago, or the frame is loud enough on its own.
func (c STTGateConfig) voiced(lastSpeech, now time.Time, pcm []byte) bool {
	if !lastSpeech.IsZero() && now.Sub(lastSpeech) < gateVoicedWindow {
		return true
	}

	return c.RMSFloor > 0 && media.RMS(pcm) >= c.RMSFloor
}
