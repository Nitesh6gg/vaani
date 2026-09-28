package media

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

// AudioSocket kind bytes (Asterisk's res_audiosocket wire protocol):
//
//	+------------------+-----------------------+--------------------------+
//	| Type (1 Byte)    | Payload Length (2B)    | Payload (Variable)       |
//	+------------------+-----------------------+--------------------------+
//	| 0x12 for slin16  | big-endian uint16      | Raw PCM16 (Little-Endian)|
//	+------------------+-----------------------+--------------------------+
//
// Unlike RTP, this is TCP: no sequence numbers, no SSRC, no packet loss or
// reordering to conceal -- delivery order and completeness are the transport's
// job, not ours. The audio payload's byte order is little-endian by definition
// of the protocol itself, not a per-deployment question the way RTP L16's is.
//
// The header and payload MUST go out as a single write, not two: Asterisk's
// res_audiosocket reads the 3-byte header, then reads the payload separately
// with only a 5ms poll timeout on that second read (ast_wait_for_input(svc,
// 5) in ast_audiosocket_receive_frame_with_hangup) -- if the payload's bytes
// aren't already sitting in its socket buffer by then, it logs "Poll timed
// out while waiting for data" and hangs up the channel. Two separate Write
// calls let Go's (Nagle-disabled) TCP socket put the header and payload in
// different packets; on localhost or same-subnet testing the gap between them
// is microseconds and this never shows up, but observed live across a routed,
// cross-subnet path, ordinary network delay past 5ms between the two packets
// was enough to drop calls mid-conversation, roundly at random-looking
// moments. See WriteAudioSocketFrame/writeAudioSocketFrameInto.
const (
	AudioSocketKindHangup = 0x00
	AudioSocketKindUUID   = 0x01
	AudioSocketKindDTMF   = 0x03
	AudioSocketKindSlin16 = 0x12
	AudioSocketKindError  = 0xff
)

const (
	audioSocketHeaderSize = 3 // 1 byte kind + 2 byte big-endian length
	audioSocketMaxPayload = 65535
)

// ErrAudioSocketFrameTooLarge marks a WriteAudioSocketFrame payload that can't
// fit in the protocol's 16-bit length field.
var ErrAudioSocketFrameTooLarge = errors.New("media: audiosocket frame payload too large")

// AudioSocketFrame is one length-prefixed AudioSocket protocol frame.
type AudioSocketFrame struct {
	Kind byte
	// Payload is a pooled FrameSize buffer (see GetFrame/PutFrame) when Kind is
	// AudioSocketKindSlin16 and the length matches FrameSize -- the caller must
	// PutFrame it in that case. For every other kind (control frames: UUID,
	// DTMF, hangup, error -- all tiny, and only ever a handful per call) it's an
	// ordinary allocation.
	Payload []byte
}

// ReadAudioSocketFrame reads exactly one frame from r, blocking until the full
// frame (header + payload) has arrived.
func ReadAudioSocketFrame(r io.Reader) (AudioSocketFrame, error) {
	var header [audioSocketHeaderSize]byte

	if _, err := io.ReadFull(r, header[:]); err != nil {
		return AudioSocketFrame{}, err
	}

	kind := header[0]
	length := binary.BigEndian.Uint16(header[1:3])

	var payload []byte

	if kind == AudioSocketKindSlin16 && int(length) == FrameSize {
		buf := GetFrame()
		*buf = (*buf)[:FrameSize]
		payload = *buf
	} else if length > 0 {
		payload = make([]byte, length)
	}

	if length > 0 {
		if _, err := io.ReadFull(r, payload); err != nil {
			return AudioSocketFrame{}, err
		}
	}

	return AudioSocketFrame{Kind: kind, Payload: payload}, nil
}

// WriteAudioSocketFrame writes one length-prefixed AudioSocket protocol frame
// to w in a single Write call (see the package doc comment for why this must
// not be two separate writes). Allocates a fresh combined buffer each call --
// fine for control frames (hangup/UUID/DTMF: a handful per call) and tests,
// but the hot 20ms audio path (AudioSocketCallMedia.writeLoop) uses
// writeAudioSocketFrameInto with a reused buffer instead, per invariant #3.
func WriteAudioSocketFrame(w io.Writer, kind byte, payload []byte) error {
	if len(payload) > audioSocketMaxPayload {
		return fmt.Errorf("%w: %d bytes", ErrAudioSocketFrameTooLarge, len(payload))
	}

	buf := make([]byte, audioSocketHeaderSize+len(payload))

	return writeAudioSocketFrameInto(w, buf, kind, payload)
}

// writeAudioSocketFrameInto builds one length-prefixed AudioSocket frame into
// buf (which must have length/capacity >= audioSocketHeaderSize+len(payload))
// and writes it to w in a single Write call. buf is caller-owned so a hot
// loop can reuse the same backing array across calls instead of allocating.
func writeAudioSocketFrameInto(w io.Writer, buf []byte, kind byte, payload []byte) error {
	if len(payload) > audioSocketMaxPayload {
		return fmt.Errorf("%w: %d bytes", ErrAudioSocketFrameTooLarge, len(payload))
	}

	buf = buf[:audioSocketHeaderSize+len(payload)]
	buf[0] = kind
	binary.BigEndian.PutUint16(buf[1:3], uint16(len(payload)))
	copy(buf[audioSocketHeaderSize:], payload)

	_, err := w.Write(buf)

	return err
}
