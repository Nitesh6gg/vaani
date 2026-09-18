package media

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeCallSink is a no-op Sink for integration tests that don't care about
// individual metric calls, just that the pipeline runs without crashing.
type fakeCallSink struct{}

func (fakeCallSink) PacketIn(int)       {}
func (fakeCallSink) PacketOut()         {}
func (fakeCallSink) SeqGap()            {}
func (fakeCallSink) Malformed()         {}
func (fakeCallSink) SendError()         {}
func (fakeCallSink) Late()              {}
func (fakeCallSink) Duplicate()         {}
func (fakeCallSink) SilenceInserted()   {}
func (fakeCallSink) SSRCChange()        {}
func (fakeCallSink) WatchdogTimeout()   {}
func (fakeCallSink) PacerDrift(float64) {}
func (fakeCallSink) AudioLevel(float64) {}

// TestCallMedia_EndToEndLoopback proves the full pipeline -- reader -> jitter
// buffer -> release/normalize -> LoopbackHandler -> writer -> UDP -- actually
// echoes real audio, not just that each stage passes its unit tests in isolation.
func TestCallMedia_EndToEndLoopback(t *testing.T) {
	serverConn, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	require.NoError(t, err)

	clientConn, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	require.NoError(t, err)
	defer func() { _ = clientConn.Close() }()

	cm := NewCallMedia("test-call", serverConn, fakeCallSink{}, Config{
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
