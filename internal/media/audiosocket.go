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

// WriteAudioSocketFrame writes one length-prefixed AudioSocket protocol frame to w.
func WriteAudioSocketFrame(w io.Writer, kind byte, payload []byte) error {
	if len(payload) > audioSocketMaxPayload {
		return fmt.Errorf("%w: %d bytes", ErrAudioSocketFrameTooLarge, len(payload))
	}

	var header [audioSocketHeaderSize]byte

	header[0] = kind
	binary.BigEndian.PutUint16(header[1:3], uint16(len(payload)))

	if _, err := w.Write(header[:]); err != nil {
		return err
	}

	if len(payload) > 0 {
		if _, err := w.Write(payload); err != nil {
			return err
		}
	}

	return nil
}
