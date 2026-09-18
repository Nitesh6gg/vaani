package media

import (
	"net"
	"testing"
	"time"

	"github.com/pion/rtp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseRTP_RoundTrip(t *testing.T) {
	sender := NewSender()
	payload := make([]byte, FrameSize)
	for i := range payload {
		payload[i] = byte(i)
	}

	pkt := sender.Build(payload)

	buf, err := pkt.Marshal()
	require.NoError(t, err)

	got, err := ParseRTP(buf)
	require.NoError(t, err)

	assert.Equal(t, uint8(2), got.Version)
	assert.True(t, got.Marker, "first packet must set the marker bit")
	assert.Equal(t, pkt.SequenceNumber, got.SequenceNumber)
	assert.Equal(t, pkt.Timestamp, got.Timestamp)
	assert.Equal(t, pkt.SSRC, got.SSRC)
	assert.Equal(t, payload, got.Payload)
}

func TestParseRTP_SequenceAndTimestampAdvance(t *testing.T) {
	sender := NewSender()
	payload := make([]byte, FrameSize)

	first := sender.Build(payload)
	second := sender.Build(payload)

	assert.Equal(t, first.SequenceNumber+1, second.SequenceNumber)
	assert.Equal(t, first.Timestamp+uint32(SamplesPerFrame), second.Timestamp)
	assert.True(t, first.Marker)
	assert.False(t, second.Marker, "marker must only be set on the first packet")
}

func TestParseRTP_RejectsWrongVersion(t *testing.T) {
	pkt := &rtp.Packet{
		Header:  rtp.Header{Version: 1, SequenceNumber: 1, Timestamp: 1, SSRC: 1},
		Payload: []byte{0x00, 0x01},
	}

	buf, err := pkt.Marshal()
	require.NoError(t, err)

	_, err = ParseRTP(buf)
	assert.ErrorIs(t, err, ErrMalformed)
}

func TestParseRTP_RejectsTruncatedPacket(t *testing.T) {
	_, err := ParseRTP([]byte{0x80, 0x00}) // far too short for a valid RTP header
	assert.ErrorIs(t, err, ErrMalformed)
}

func TestIsRTCP(t *testing.T) {
	assert.False(t, IsRTCP([]byte{0x80, 0x00})) // PT 0
	assert.True(t, IsRTCP([]byte{0x80, 200}))   // RTCP SR
	assert.True(t, IsRTCP([]byte{0x80, 204}))   // RTCP APP
	assert.False(t, IsRTCP([]byte{0x80, 118}))  // typical slin16 dynamic PT
	assert.False(t, IsRTCP([]byte{0x80}))       // too short to tell
}

func TestSeqTracker(t *testing.T) {
	var tr SeqTracker

	assert.False(t, tr.Observe(100), "first observation is never a gap")
	assert.False(t, tr.Observe(101), "sequential increment is not a gap")
	assert.True(t, tr.Observe(105), "jump ahead is a gap")
	assert.True(t, tr.Observe(103), "reorder/duplicate is also flagged")

	// wraparound: 65535 -> 0 is sequential, not a gap
	tr = SeqTracker{}
	tr.Observe(65535)
	assert.False(t, tr.Observe(0), "sequence wraparound must not be flagged as a gap")
}

// TestEndpoint_NoSendBeforeLock is the Phase 1 carryover invariant: zero outbound
// packets until a valid inbound packet locks the remote address -- no fallback
// destination, ever.
func TestEndpoint_NoSendBeforeLock(t *testing.T) {
	local, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	require.NoError(t, err)
	defer func() { _ = local.Close() }()

	ep := NewEndpoint(local)

	sent, err := ep.WriteTo([]byte("hello"))
	require.NoError(t, err)
	assert.False(t, sent, "WriteTo must not send anything before a remote is locked")
}

// TestEndpoint_LockRemoteThenSend confirms the full contract: LockRemote reports
// true exactly once (the first caller), and WriteTo only reports sent=true once a
// remote is known, with a real packet actually observed on the wire.
func TestEndpoint_LockRemoteThenSend(t *testing.T) {
	server, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	require.NoError(t, err)
	defer func() { _ = server.Close() }()

	client, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	require.NoError(t, err)
	defer func() { _ = client.Close() }()

	ep := NewEndpoint(server)

	clientAddr := client.LocalAddr().(*net.UDPAddr)

	assert.True(t, ep.LockRemote(clientAddr), "first LockRemote call must report locked=true")
	assert.False(t, ep.LockRemote(&net.UDPAddr{IP: net.IPv4(9, 9, 9, 9), Port: 1}),
		"a second LockRemote call must be a no-op and report false")
	assert.Equal(t, clientAddr.String(), ep.Remote().String(), "remote must stay the first address, not the second")

	sent, err := ep.WriteTo([]byte("payload"))
	require.NoError(t, err)
	assert.True(t, sent, "WriteTo must report sent=true once a remote is locked")

	buf := make([]byte, 64)
	_ = client.SetReadDeadline(time.Now().Add(2 * time.Second))

	n, _, err := client.ReadFromUDP(buf)
	require.NoError(t, err, "the packet must actually arrive at the locked remote")
	assert.Equal(t, "payload", string(buf[:n]))
}
