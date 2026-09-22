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

// fakeCallSink is a Sink for integration tests: counters are atomic since the
// reader, release, and write goroutines all call into it concurrently.
type fakeCallSink struct {
	packetOut   atomic.Int64
	silenceSent atomic.Int64
	sendError   atomic.Int64
}

func (f *fakeCallSink) PacketIn(int)       {}
func (f *fakeCallSink) PacketOut()         { f.packetOut.Add(1) }
func (f *fakeCallSink) SeqGap()            {}
func (f *fakeCallSink) Malformed()         {}
func (f *fakeCallSink) SendError()         { f.sendError.Add(1) }
func (f *fakeCallSink) Late()              {}
func (f *fakeCallSink) Duplicate()         {}
func (f *fakeCallSink) SilenceInserted()   {}
func (f *fakeCallSink) SSRCChange()        {}
func (f *fakeCallSink) Reanchor()          {}
func (f *fakeCallSink) SilenceSent()       { f.silenceSent.Add(1) }
func (f *fakeCallSink) WatchdogTimeout()   {}
func (f *fakeCallSink) PacerDrift(float64) {}
func (f *fakeCallSink) AudioLevel(float64) {}

// TestCallMedia_EndToEndLoopback proves the full pipeline -- reader -> jitter
// buffer -> release/normalize -> LoopbackHandler -> writer -> UDP -- actually
// echoes real audio, not just that each stage passes its unit tests in isolation.
func TestCallMedia_EndToEndLoopback(t *testing.T) {
	serverConn, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	require.NoError(t, err)

	clientConn, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	require.NoError(t, err)
	defer func() { _ = clientConn.Close() }()

	cm := NewCallMedia("test-call", serverConn, &fakeCallSink{}, Config{
		JitterBufferPackets: 3,
		FromWire:            LittleEndian,
	})

	ctx, cancel := context.WithCancel(context.Background())
	runDone := make(chan struct{})

	go func() {
		cm.Run(ctx)
		close(runDone)
	}()

	serverAddr := serverConn.LocalAddr().(*net.UDPAddr)
	sender := NewSender()

	payload := make([]byte, FrameSize)
	for i := range payload {
		payload[i] = byte(i)
	}

	// Send a few packets so the jitter buffer window fills past its 3-packet size
	// and starts releasing real (non-silence) audio.
	for seq := 0; seq < 6; seq++ {
		pkt := sender.Build(payload)

		out, err := pkt.Marshal()
		require.NoError(t, err)

		_, err = clientConn.WriteToUDP(out, serverAddr)
		require.NoError(t, err)

		time.Sleep(FrameInterval)
	}

	require.NoError(t, clientConn.SetReadDeadline(time.Now().Add(2*time.Second)))

	sawRealAudio := false
	buf := make([]byte, 1500)

	for i := 0; i < 20; i++ {
		n, _, err := clientConn.ReadFromUDP(buf)
		if err != nil {
			break
		}

		pkt, err := ParseRTP(buf[:n])
		if err != nil {
			continue
		}

		if len(pkt.Payload) == FrameSize && pkt.Payload[0] == payload[0] && pkt.Payload[1] == payload[1] {
			sawRealAudio = true
			break
		}
	}

	assert.True(t, sawRealAudio, "the echoed stream must contain the original payload, not just silence")

	cancel()

	select {
	case <-runDone:
	case <-time.After(2 * time.Second):
		t.Fatal("CallMedia.Run did not return promptly after context cancellation")
	}
}

// TestCallMedia_DebugAudioPublishesToTap proves the DEBUG_AUDIO wiring end to
// end: with Config.DebugAudio set, a live tap is registered under the call ID,
// real inbound audio is published to it as the call runs, and it's unregistered
// on teardown so a stale tap can't outlive its call.
func TestCallMedia_DebugAudioPublishesToTap(t *testing.T) {
	const callID = "debug-audio-call"

	serverConn, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	require.NoError(t, err)

	clientConn, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	require.NoError(t, err)
	defer func() { _ = clientConn.Close() }()

	cm := NewCallMedia(callID, serverConn, &fakeCallSink{}, Config{
		JitterBufferPackets: 3,
		FromWire:            LittleEndian,
		DebugAudio:          true,
	})

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

	serverAddr := serverConn.LocalAddr().(*net.UDPAddr)
	sender := NewSender()

	payload := make([]byte, FrameSize)
	for i := range payload {
		payload[i] = byte(i)
	}

	for seq := 0; seq < 6; seq++ {
		pkt := sender.Build(payload)

		out, err := pkt.Marshal()
		require.NoError(t, err)

		_, err = clientConn.WriteToUDP(out, serverAddr)
		require.NoError(t, err)

		time.Sleep(FrameInterval)
	}

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
		t.Fatal("CallMedia.Run did not return promptly after context cancellation")
	}

	assert.Nil(t, LookupAudioTap(callID), "tap must be unregistered once the call's media plane stops")
}

func TestCallMedia_NearSilent_TooFewTicksReturnsFalse(t *testing.T) {
	cm := &CallMedia{rmsTicks: nearSilenceMinTicks - 1}

	_, ok := cm.NearSilent()
	assert.False(t, ok, "too short a call to judge, regardless of level")
}

func TestCallMedia_NearSilent_BelowThresholdReturnsTrue(t *testing.T) {
	cm := &CallMedia{rmsTicks: nearSilenceMinTicks, rmsSum: 10 * float64(nearSilenceMinTicks)}

	avg, ok := cm.NearSilent()
	assert.True(t, ok)
	assert.InDelta(t, 10.0, avg, 0.001)
}

func TestCallMedia_NearSilent_AboveThresholdReturnsFalse(t *testing.T) {
	cm := &CallMedia{rmsTicks: nearSilenceMinTicks, rmsSum: 5000 * float64(nearSilenceMinTicks)}

	_, ok := cm.NearSilent()
	assert.False(t, ok)
}

// TestCallMedia_SilentHandlerAlwaysTransmitsSilence exercises TEST_SILENT_HANDLER:
// with a Handler that returns zero frames on every tick, RTP must never starve on
// an active call -- the write tick still transmits a zeroed silence packet every
// 20ms once the remote is locked.
func TestCallMedia_SilentHandlerAlwaysTransmitsSilence(t *testing.T) {
	serverConn, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	require.NoError(t, err)

	clientConn, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	require.NoError(t, err)
	defer func() { _ = clientConn.Close() }()

	sink := &fakeCallSink{}
	cm := NewCallMedia("silent-call", serverConn, sink, Config{
		JitterBufferPackets: 3,
		FromWire:            LittleEndian,
		Handler:             SilentHandler{},
	})

	ctx, cancel := context.WithCancel(context.Background())
	runDone := make(chan struct{})

	go func() {
		cm.Run(ctx)
		close(runDone)
	}()

	serverAddr := serverConn.LocalAddr().(*net.UDPAddr)
	sender := NewSender()

	// One real inbound packet locks the remote; the handler never produces
	// anything afterward, so every subsequent transmission must be the write
	// tick's silence fallback.
	pkt := sender.Build(make([]byte, FrameSize))

	out, err := pkt.Marshal()
	require.NoError(t, err)

	_, err = clientConn.WriteToUDP(out, serverAddr)
	require.NoError(t, err)

	require.NoError(t, clientConn.SetReadDeadline(time.Now().Add(3*time.Second)))

	const wantPackets = 10

	received := 0
	allSilence := true
	buf := make([]byte, 1500)
	start := time.Now()

	for received < wantPackets {
		n, _, err := clientConn.ReadFromUDP(buf)
		require.NoError(t, err, "pacer must never stall: a silence packet is expected every tick")

		p, err := ParseRTP(buf[:n])
		if err != nil {
			continue
		}

		if len(p.Payload) != FrameSize {
			continue
		}

		received++

		for _, b := range p.Payload {
			if b != 0 {
				allSilence = false
			}
		}
	}

	elapsed := time.Since(start)

	assert.True(t, allSilence, "every transmitted frame with an empty handler must be zeroed silence")
	assert.InDelta(t, float64(wantPackets)*float64(FrameInterval), float64(elapsed), float64(FrameInterval)*5,
		"pacer must not stall: %d ticks should take roughly %v, took %v", wantPackets, wantPackets*FrameInterval, elapsed)
	assert.GreaterOrEqual(t, sink.silenceSent.Load(), int64(wantPackets),
		"each empty-queue tick transmission must be counted")

	cancel()

	select {
	case <-runDone:
	case <-time.After(2 * time.Second):
		t.Fatal("CallMedia.Run did not return promptly after context cancellation")
	}
}

// TestWriteTickPath_SilenceAddsNoExtraAllocationVsRealFrame proves the
// silence-fallback path doesn't allocate any more than the real-frame path
// already does. It deliberately doesn't claim zero allocations outright:
// rtp.Packet.Marshal always allocates its output buffer internally (pion/rtp's
// own implementation, not something this fix touches) -- the honest, correctly
// scoped claim is that adding the silence fallback introduced no *additional*
// allocation over that existing baseline.
func TestWriteTickPath_SilenceAddsNoExtraAllocationVsRealFrame(t *testing.T) {
	realSender := NewSender()
	realPayload := make([]byte, FrameSize)

	realAllocs := testing.AllocsPerRun(1000, func() {
		frame := GetFrame()
		*frame = (*frame)[:0]
		*frame = append(*frame, realPayload...)

		pkt := realSender.Build(*frame)

		out, err := pkt.Marshal()
		if err != nil {
			t.Fatal(err)
		}

		_ = out

		PutFrame(frame)
	})

	silenceSender := NewSender()

	silenceAllocs := testing.AllocsPerRun(1000, func() {
		frame := silentFrame()

		pkt := silenceSender.Build(*frame)

		out, err := pkt.Marshal()
		if err != nil {
			t.Fatal(err)
		}

		_ = out

		PutFrame(frame)
	})

	assert.Equal(t, realAllocs, silenceAllocs,
		"the silence-fallback write path must not allocate any more than the real-frame write path already does")
}
