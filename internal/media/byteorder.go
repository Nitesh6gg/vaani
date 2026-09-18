package media

import (
	"fmt"
	"strings"
)

// Endianness identifies the wire byte order of 16-bit PCM samples.
type Endianness int

const (
	// LittleEndian is the Handler contract's only accepted byte order.
	LittleEndian Endianness = iota
	// BigEndian is RFC 3551's nominal default for RTP L16, but Asterisk's actual
	// externalMedia wire format must be verified empirically -- see
	// cmd/endianness-check and docs/AUDIO_PIPELINE.md.
	BigEndian
)

// ParseEndianness parses the AUDIO_L16_ENDIANNESS env var ("le" or "be",
// case-insensitive; empty defaults to "le"). It never silently guesses on an
// unrecognized value -- that's a startup-time configuration error.
func ParseEndianness(s string) (Endianness, error) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "le", "":
		return LittleEndian, nil
	case "be":
		return BigEndian, nil
	default:
		return LittleEndian, fmt.Errorf("media: invalid AUDIO_L16_ENDIANNESS %q (want \"le\" or \"be\")", s)
	}
}

// NormalizeToLE rewrites pcm in place into little-endian PCM16, given that it
// arrived on the wire in byte order `from`. Zero cost when from is already
// LittleEndian -- the Handler contract downstream is LE-only, so this is the one
// place that decision gets made. A trailing odd byte (malformed/truncated input)
// is left untouched rather than panicking.
func NormalizeToLE(pcm []byte, from Endianness) {
	if from == LittleEndian {
		return
	}

	n := len(pcm) - (len(pcm) % 2)
	for i := 0; i < n; i += 2 {
		pcm[i], pcm[i+1] = pcm[i+1], pcm[i]
	}
}
