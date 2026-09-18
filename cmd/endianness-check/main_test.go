package main

import (
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestAnalyze_DetectsLittleEndian feeds analyze() a tone actually encoded as
// little-endian PCM16 and checks it's recognized as such -- this is the same
// heuristic the live probe uses, just without a real Asterisk round-trip.
func TestAnalyze_DetectsLittleEndian(t *testing.T) {
	raw := encodeTone(t, binary.LittleEndian)

	v := analyze(raw)

	assert.True(t, v.LittleEndian(), "expected %s to be detected as little-endian", v)
	assert.Less(t, v.LittleEndianScore, v.BigEndianScore)
}

// TestAnalyze_DetectsBigEndian is the mirror case: a tone actually encoded
// big-endian must be detected as such, not assumed away.
func TestAnalyze_DetectsBigEndian(t *testing.T) {
	raw := encodeTone(t, binary.BigEndian)

	v := analyze(raw)

	assert.False(t, v.LittleEndian(), "expected %s to be detected as big-endian", v)
	assert.Less(t, v.BigEndianScore, v.LittleEndianScore)
}

// TestVerdict_EnvValue confirms EnvValue's output is ready to paste as-is into
// AUDIO_L16_ENDIANNESS -- it must agree with LittleEndian() in both directions.
func TestVerdict_EnvValue(t *testing.T) {
	le := analyze(encodeTone(t, binary.LittleEndian))
	assert.Equal(t, "le", le.EnvValue())

	be := analyze(encodeTone(t, binary.BigEndian))
	assert.Equal(t, "be", be.EnvValue())
}

func encodeTone(t *testing.T, order binary.ByteOrder) []byte {
	t.Helper()

	le := generateTone(toneFreqHz, toneDuration, 16000) // always generated as LE by this helper

	if order == binary.LittleEndian {
		return le
	}

	// Re-encode the same samples in the opposite byte order.
	out := make([]byte, len(le))
	for i := 0; i+1 < len(le); i += 2 {
		sample := binary.LittleEndian.Uint16(le[i:])
		order.PutUint16(out[i:], sample)
	}

	return out
}
