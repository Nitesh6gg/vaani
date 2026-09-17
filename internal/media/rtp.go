package media

import (
	"errors"
	"fmt"
	"math/rand"
	"net"
	"sync"
	"sync/atomic"
	"syscall"

	"github.com/pion/rtp"
)

const (
	// SampleRate is the fixed audio sample rate used end-to-end: slin16.
	SampleRate = 16000
	// SamplesPerFrame is the RTP timestamp increment per 20ms frame at SampleRate: 320.
	SamplesPerFrame = SampleRate * 20 / 1000

	// DefaultPayloadType is used until the actual inbound payload type is learned
	// from Asterisk's first packet (commonly 118 for slin16, but never assumed).
	DefaultPayloadType = 118

	rtcpPTLow  = 200
	rtcpPTHigh = 204
)

// ErrMalformed marks an inbound buffer that failed RTP validation.
var ErrMalformed = errors.New("media: malformed rtp packet")

// IsRTCP reports whether buf looks like an RTCP packet. Unlike RTP's second byte
// (marker bit + 7-bit payload type), RTCP's second byte is the packet type in full
// (200-204), so it's checked unmasked.
func IsRTCP(buf []byte) bool {
	if len(buf) < 2 {
		return false
	}

	pt := buf[1]

	return pt >= rtcpPTLow && pt <= rtcpPTHigh
}

// ParseRTP validates and unmarshals an inbound RTP packet. Only RTP version 2 is
// accepted.
func ParseRTP(buf []byte) (*rtp.Packet, error) {
	pkt := &rtp.Packet{}
	if err := pkt.Unmarshal(buf); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrMalformed, err)
	}

	if pkt.Version != 2 {
		return nil, fmt.Errorf("%w: version %d", ErrMalformed, pkt.Version)
	}

	return pkt, nil
}

// SeqTracker counts discontinuities in an inbound RTP sequence number stream.
type SeqTracker struct {
	have bool
	last uint16
}

// Observe records the next inbound sequence number and reports whether it was a
// gap (anything other than exactly last+1, including reorders and duplicates).
func (t *SeqTracker) Observe(seq uint16) (gap bool) {
	if !t.have {
		t.have = true
		t.last = seq
		return false
	}

	expected := t.last + 1
	t.last = seq

	return seq != expected
}

// Sender builds successive outbound RTP packets for one call: a random SSRC and
// initial sequence/timestamp, sequence +1 and timestamp +SamplesPerFrame each call,
// marker bit set on the first packet only. The payload type defaults to
// DefaultPayloadType and can be corrected once via SetPayloadType when the inbound
// payload type is learned, so replies echo whatever type Asterisk used.
type Sender struct {
	ssrc  uint32
	seq   uint16
	ts    uint32
	pt    atomic.Uint32
	first atomic.Bool
}

// NewSender creates a Sender with randomized SSRC/sequence/timestamp.
func NewSender() *Sender {
	s := &Sender{
		ssrc: rand.Uint32(),
		seq:  uint16(rand.Intn(1 << 16)),
		//nolint:gosec // initial RTP timestamp offset has no security relevance
		ts: rand.Uint32(),
	}
	s.pt.Store(DefaultPayloadType)
	s.first.Store(true)

	return s
}

// SetPayloadType corrects the outbound payload type to match what was learned from
// the first inbound packet.
func (s *Sender) SetPayloadType(pt uint8) {
	s.pt.Store(uint32(pt))
}

// Build constructs the next outbound RTP packet carrying payload, advancing
// sequence and timestamp state.
func (s *Sender) Build(payload []byte) *rtp.Packet {
	marker := s.first.CompareAndSwap(true, false)

	pkt := &rtp.Packet{
		Header: rtp.Header{
			Version:        2,
			Marker:         marker,
			PayloadType:    uint8(s.pt.Load()),
			SequenceNumber: s.seq,
			Timestamp:      s.ts,
			SSRC:           s.ssrc,
		},
		Payload: payload,
	}

	s.seq++
	s.ts += uint32(SamplesPerFrame)

	return pkt
}

// Endpoint owns one UDP socket for a single call. The outbound destination is
// locked to the source address of the first valid inbound packet; outbound writes
// before that are dropped rather than sent nowhere.
type Endpoint struct {
	conn *net.UDPConn

	mu     sync.RWMutex
	remote *net.UDPAddr
}

// NewEndpoint wraps conn, sizing its socket buffers for RTP traffic.
func NewEndpoint(conn *net.UDPConn) *Endpoint {
	_ = conn.SetReadBuffer(1 << 20)
	_ = conn.SetWriteBuffer(1 << 20)

	return &Endpoint{conn: conn}
}

// ReadFrom reads one packet into buf.
func (e *Endpoint) ReadFrom(buf []byte) (int, *net.UDPAddr, error) {
	return e.conn.ReadFromUDP(buf)
}

// LockRemote sets the outbound destination the first time it's called; later calls
// are no-ops, per the "lock to first inbound packet" invariant.
func (e *Endpoint) LockRemote(addr *net.UDPAddr) {
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.remote == nil {
		e.remote = addr
	}
}

// Remote returns the locked outbound destination, or nil if none is known yet.
func (e *Endpoint) Remote() *net.UDPAddr {
	e.mu.RLock()
	defer e.mu.RUnlock()

	return e.remote
}

// WriteTo sends b to the locked remote address. It is a silent no-op if no remote
// is known yet, and swallows ICMP-induced connection-refused errors from a closed
// or unreachable peer rather than propagating them as call-ending failures.
func (e *Endpoint) WriteTo(b []byte) error {
	remote := e.Remote()
	if remote == nil {
		return nil
	}

	_, err := e.conn.WriteToUDP(b, remote)
	if err != nil && errors.Is(err, syscall.ECONNREFUSED) {
		return nil
	}

	return err
}

// Close closes the underlying UDP socket.
func (e *Endpoint) Close() error {
	return e.conn.Close()
}
