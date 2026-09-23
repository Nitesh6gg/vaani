package media

import (
	"context"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeAudioSocketSink is an AudioSocketSink for tests: counters are atomic since
// the reader, release, and write goroutines all call into it concurrently.
type fakeAudioSocketSink struct {
	packetIn    atomic.Int64
	packetOut   atomic.Int64
	silenceSent atomic.Int64
	malformed   atomic.Int64
}

func (f *fakeAudioSocketSink) PacketIn(int)       { f.packetIn.Add(1) }
func (f *fakeAudioSocketSink) PacketOut()         { f.packetOut.Add(1) }
func (f *fakeAudioSocketSink) SilenceSent()       { f.silenceSent.Add(1) }
func (f *fakeAudioSocketSink) SendError()         {}
func (f *fakeAudioSocketSink) Malformed()         { f.malformed.Add(1) }
func (f *fakeAudioSocketSink) QueueDropped()      {}
func (f *fakeAudioSocketSink) WatchdogTimeout()   {}
func (f *fakeAudioSocketSink) PacerDrift(float64) {}
func (f *fakeAudioSocketSink) AudioLevel(float64) {}

func TestAudioSocketCallMedia_NearSilent_TooFewTicksReturnsFalse(t *testing.T) {
	cm := &AudioSocketCallMedia{rmsTicks: nearSilenceMinTicks - 1}

	_, ok := cm.NearSilent()
	assert.False(t, ok)
}

func TestAudioSocketCallMedia_NearSilent_BelowThresholdReturnsTrue(t *testing.T) {
	cm := &AudioSocketCallMedia{rmsTicks: nearSilenceMinTicks, rmsSum: 10 * float64(nearSilenceMinTicks)}

	avg, ok := cm.NearSilent()
	assert.True(t, ok)
	assert.InDelta(t, 10.0, avg, 0.001)
}

func TestAudioSocketCallMedia_NearSilent_AboveThresholdReturnsFalse(t *testing.T) {
	cm := &AudioSocketCallMedia{rmsTicks: nearSilenceMinTicks, rmsSum: 5000 * float64(nearSilenceMinTicks)}

	_, ok := cm.NearSilent()
	assert.False(t, ok)
}

// TestAudioSocketCallMedia_DebugAudioPublishesToTap mirrors
// TestCallMedia_DebugAudioPublishesToTap for the AudioSocket transport: same
// registry, same contract, different wire.
func TestAudioSocketCallMedia_DebugAudioPublishesToTap(t *testing.T) {
	const callID = "debug-audio-audiosocket-call"

	asteriskSide, vaaniSide := net.Pipe()
	defer func() { _ = asteriskSide.Close() }()

	cm := NewAudioSocketCallMedia(callID, vaaniSide, &fakeAudioSocketSink{}, AudioSocketConfig{DebugAudio: true})

	tap := LookupAudioTap(callID)
	require.NotNil(t, tap, "DebugAudio:true must register a tap under the call ID")

	tapCh, unsubscribe := tap.Subscribe()
	defer unsubscribe()

	ctx, cancel := context.WithCancel(context.Background())
	runDone := make(chan struct{})

	go func() {
		cm.Run(ctx)
		close(runDone)
	}()

	payload := make([]byte, FrameSize)
	for i := range payload {
		payload[i] = byte(i)
	}

	writeErrs := make(chan error, 1)

	go func() {
		writeErrs <- WriteAudioSocketFrame(asteriskSide, AudioSocketKindSlin16, payload)
	}()
	require.NoError(t, <-writeErrs)

	select {
	case frame := <-tapCh:
		assert.Equal(t, FrameSize, len(frame))
	case <-time.After(2 * time.Second):
		t.Fatal("tap subscriber never received a published frame")
	}

	cancel()

	select {
	case <-runDone:
	case <-time.After(2 * time.Second):
		t.Fatal("AudioSocketCallMedia.Run did not return promptly after context cancellation")
	}

	assert.Nil(t, LookupAudioTap(callID), "tap must be unregistered once the call's media plane stops")
}

// TestAudioSocketCallMedia_EndToEndLoopback proves the full AudioSocket pipeline
// -- reader -> inbound queue -> release -> LoopbackHandler -> writer -- actually
// echoes real audio over the wire protocol, using net.Pipe as a stand-in for the
// TCP connection Asterisk would provide (no real socket needed: net.Pipe gives an
// in-memory, fully synchronous net.Conn pair).
func TestAudioSocketCallMedia_EndToEndLoopback(t *testing.T) {
	asteriskSide, vaaniSide := net.Pipe()
	defer func() { _ = asteriskSide.Close() }()

	sink := &fakeAudioSocketSink{}
	cm := NewAudioSocketCallMedia("test-call", vaaniSide, sink, AudioSocketConfig{})

	ctx, cancel := context.WithCancel(context.Background())
	runDone := make(chan struct{})

	go func() {
		cm.Run(ctx)
		close(runDone)
	}()

	payload := make([]byte, FrameSize)
	for i := range payload {
		payload[i] = byte(i)
	}

	// Feed a few frames from the "Asterisk" side so the release loop has
	// something real to echo.
	writeErrs := make(chan error, 1)

	go func() {
		for i := 0; i < 6; i++ {
			if err := WriteAudioSocketFrame(asteriskSide, AudioSocketKindSlin16, payload); err != nil {
				writeErrs <- err
				return
			}

			time.Sleep(FrameInterval)
		}

		writeErrs <- nil
	}()

	require.NoError(t, <-writeErrs)

	sawRealAudio := false

	for i := 0; i < 20; i++ {
		_ = asteriskSide.SetReadDeadline(time.Now().Add(2 * time.Second))

		frame, err := ReadAudioSocketFrame(asteriskSide)
		if err != nil {
			break
		}

		if frame.Kind != AudioSocketKindSlin16 {
			continue
		}

		if len(frame.Payload) == FrameSize && frame.Payload[0] == payload[0] && frame.Payload[1] == payload[1] {
			sawRealAudio = true
			PutFrame(&frame.Payload)

			break
		}

		PutFrame(&frame.Payload)
	}

	assert.True(t, sawRealAudio, "the echoed stream must contain the original payload, not just silence")
	assert.Greater(t, sink.packetIn.Load(), int64(0))
	assert.Greater(t, sink.packetOut.Load(), int64(0))

	cancel()

	select {
	case <-runDone:
	case <-time.After(2 * time.Second):
		t.Fatal("AudioSocketCallMedia.Run did not return promptly after context cancellation")
	}
}

// TestAudioSocketCallMedia_SilentHandlerAlwaysTransmits proves AudioSocket has
// the same "never starve the outbound stream" guarantee as the RTP pipeline: a
// Handler producing nothing still gets a silence frame sent every tick.
func TestAudioSocketCallMedia_SilentHandlerAlwaysTransmits(t *testing.T) {
	asteriskSide, vaaniSide := net.Pipe()
	defer func() { _ = asteriskSide.Close() }()

	sink := &fakeAudioSocketSink{}
	cm := NewAudioSocketCallMedia("silent-call", vaaniSide, sink, AudioSocketConfig{
		Handler: SilentHandler{},
	})

	ctx, cancel := context.WithCancel(context.Background())
	runDone := make(chan struct{})

	go func() {
		cm.Run(ctx)
		close(runDone)
	}()

	const wantFrames = 5

	received := 0
	allSilence := true

	for received < wantFrames {
		_ = asteriskSide.SetReadDeadline(time.Now().Add(3 * time.Second))

		frame, err := ReadAudioSocketFrame(asteriskSide)
		require.NoError(t, err, "pacer must never stall: a silence frame is expected every tick")

		if frame.Kind != AudioSocketKindSlin16 {
			continue
		}

		received++

		for _, b := range frame.Payload {
			if b != 0 {
				allSilence = false
			}
		}

		PutFrame(&frame.Payload)
	}

	assert.True(t, allSilence, "every transmitted frame with an empty handler must be zeroed silence")

	// net.Pipe is fully synchronous: WriteAudioSocketFrame unblocks the instant
	// the reader above finishes reading it, which races the writer's own
	// subsequent sink.SilenceSent() bookkeeping call against this goroutine
	// continuing on. Poll rather than assert immediately.
	require.Eventually(t, func() bool {
		return sink.silenceSent.Load() >= int64(wantFrames)
	}, time.Second, 5*time.Millisecond, "expected at least %d silence frames counted", wantFrames)

	cancel()

	select {
	case <-runDone:
	case <-time.After(2 * time.Second):
		t.Fatal("AudioSocketCallMedia.Run did not return promptly after context cancellation")
	}
}

// TestAudioSocketCallMedia_MalformedFrameSizeCounted proves an audio frame with
// the wrong payload size is counted as malformed and skipped, not fed to the
// Handler or a crash.
func TestAudioSocketCallMedia_MalformedFrameSizeCounted(t *testing.T) {
	asteriskSide, vaaniSide := net.Pipe()
	defer func() { _ = asteriskSide.Close() }()

	sink := &fakeAudioSocketSink{}
	cm := NewAudioSocketCallMedia("bad-frame-call", vaaniSide, sink, AudioSocketConfig{})

	ctx, cancel := context.WithCancel(context.Background())
	runDone := make(chan struct{})

	go func() {
		cm.Run(ctx)
		close(runDone)
	}()

	require.NoError(t, WriteAudioSocketFrame(asteriskSide, AudioSocketKindSlin16, make([]byte, 100)))

	require.Eventually(t, func() bool {
		return sink.malformed.Load() == 1
	}, 2*time.Second, 10*time.Millisecond)

	cancel()

	select {
	case <-runDone:
	case <-time.After(2 * time.Second):
		t.Fatal("AudioSocketCallMedia.Run did not return promptly after context cancellation")
	}
}
