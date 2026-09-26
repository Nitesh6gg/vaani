package media

import (
	"fmt"
	"log/slog"
	"os"
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

// WarnIfUnset logs a startup warning if AUDIO_L16_ENDIANNESS was never
// explicitly configured (real environment or .env, both of which config.Load
// resolves into the real process environment before this runs) -- the "le"
// default is an assumption, not a verified fact, until cmd/endianness-check has
// actually run against the live stack. Call this once at startup, after
// config.Load.
func WarnIfUnset() {
	if v, ok := os.LookupEnv("AUDIO_L16_ENDIANNESS"); !ok || v == "" {
		slog.Warn("endianness unverified -- set AUDIO_L16_ENDIANNESS after running cmd/endianness-check")
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

// FromLE rewrites pcm in place from the Handler contract's little-endian byte
// order into `to`, for audio headed back out onto the wire -- the exact inverse
// of NormalizeToLE. Zero cost when to is already LittleEndian (AudioSocket's
// payload is LE by protocol definition, so only the RTP path ever sets
// BigEndian here). A trailing odd byte is left untouched rather than panicking.
func FromLE(pcm []byte, to Endianness) {
	if to == LittleEndian {
		return
	}

	n := len(pcm) - (len(pcm) % 2)
	for i := 0; i < n; i += 2 {
		pcm[i], pcm[i+1] = pcm[i+1], pcm[i]
	}
}
