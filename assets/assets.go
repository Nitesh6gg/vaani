// Package assets embeds the call-control sound files and serves them as
// 16kHz LE PCM16, the pipeline's native format. Embedding (rather than
// reading from a path at runtime) means they work no matter which directory
// the server is started from.
package assets

import (
	_ "embed"
	"encoding/binary"
	"errors"
	"fmt"
	"sync"
)

// transferHoldRing loops to the caller while a call transfer rings; beep
// plays once the transfer destination answers, just before the bridge swap.
// Both match the self-hosted Dograh's api/assets files.
var (
	//go:embed transfer_hold_ring_8000.wav
	transferHoldRingWAV []byte

	//go:embed beep.wav
	beepWAV []byte
)

var (
	holdOnce sync.Once
	holdPCM  []byte
	holdErr  error

	beepOnce sync.Once
	beepPCM  []byte
	beepErr  error
)

// TransferHoldRing returns the transfer hold/ring loop as 16kHz LE PCM16.
func TransferHoldRing() ([]byte, error) {
	holdOnce.Do(func() { holdPCM, holdErr = decode(transferHoldRingWAV) })
	return holdPCM, holdErr
}

// Beep returns the transfer-connected beep as 16kHz LE PCM16.
func Beep() ([]byte, error) {
	beepOnce.Do(func() { beepPCM, beepErr = decode(beepWAV) })
	return beepPCM, beepErr
}

// decode reads a mono PCM16 WAV at 8kHz or 16kHz and returns 16kHz LE PCM16.
func decode(wav []byte) ([]byte, error) {
	rate, pcm, err := parseWAV(wav)
	if err != nil {
		return nil, err
	}

	switch rate {
	case 16000:
		return pcm, nil
	case 8000:
		return upsample2x(pcm), nil
	default:
		return nil, fmt.Errorf("assets: unsupported sample rate %d (want 8000 or 16000)", rate)
	}
}

// parseWAV walks the RIFF chunks (files may carry LIST/metadata chunks before
// "data") and returns the sample rate and raw sample bytes of a mono PCM16 WAV.
func parseWAV(b []byte) (rate int, pcm []byte, err error) {
	if len(b) < 12 || string(b[0:4]) != "RIFF" || string(b[8:12]) != "WAVE" {
		return 0, nil, errors.New("assets: not a RIFF/WAVE file")
	}

	var fmtSeen bool

	for off := 12; off+8 <= len(b); {
		id := string(b[off : off+4])
		size := int(binary.LittleEndian.Uint32(b[off+4 : off+8]))
		body := off + 8

		if body+size > len(b) {
			size = len(b) - body // tolerate a truncated final chunk
		}

		switch id {
		case "fmt ":
			if size < 16 {
				return 0, nil, errors.New("assets: short fmt chunk")
			}

			format := binary.LittleEndian.Uint16(b[body:])
			channels := binary.LittleEndian.Uint16(b[body+2:])
			rate = int(binary.LittleEndian.Uint32(b[body+4:]))
			bits := binary.LittleEndian.Uint16(b[body+14:])

			if format != 1 || channels != 1 || bits != 16 {
				return 0, nil, fmt.Errorf("assets: want mono 16-bit PCM, got format=%d channels=%d bits=%d",
					format, channels, bits)
			}

			fmtSeen = true
		case "data":
			if !fmtSeen {
				return 0, nil, errors.New("assets: data chunk before fmt chunk")
			}

			return rate, b[body : body+size&^1], nil
		}

		off = body + size + size&1 // chunks are word-aligned
	}

	return 0, nil, errors.New("assets: no data chunk")
}

// upsample2x doubles the sample rate by linear interpolation. Plenty for a
// ring tone and a beep; a real resampling filter would be overkill.
func upsample2x(pcm []byte) []byte {
	n := len(pcm) / 2
	out := make([]byte, 0, n*4)

	for i := 0; i < n; i++ {
		cur := int16(binary.LittleEndian.Uint16(pcm[2*i:]))
		next := cur

		if i+1 < n {
			next = int16(binary.LittleEndian.Uint16(pcm[2*i+2:]))
		}

		mid := int16((int32(cur) + int32(next)) / 2)
		out = binary.LittleEndian.AppendUint16(out, uint16(cur))
		out = binary.LittleEndian.AppendUint16(out, uint16(mid))
	}

	return out
}
