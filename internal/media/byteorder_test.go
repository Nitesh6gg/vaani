package media

import (
	"encoding/binary"
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func sineLE(freqHz float64, n, sampleRate int) []byte {
	buf := make([]byte, n*2)

	for i := 0; i < n; i++ {
		t := float64(i) / float64(sampleRate)
		sample := int16(math.Sin(2*math.Pi*freqHz*t) * 0.8 * math.MaxInt16)
		binary.LittleEndian.PutUint16(buf[i*2:], uint16(sample))
	}

	return buf
}

func TestParseEndianness(t *testing.T) {
	cases := []struct {
		in   string
		want Endianness
	}{
		{"le", LittleEndian},
		{"LE", LittleEndian},
		{" le ", LittleEndian},
		{"", LittleEndian},
		{"be", BigEndian},
		{"BE", BigEndian},
	}
	for _, tc := range cases {
		got, err := ParseEndianness(tc.in)
		require.NoError(t, err, "input %q", tc.in)
		assert.Equal(t, tc.want, got, "input %q", tc.in)
	}

	_, err := ParseEndianness("garbage")
	assert.Error(t, err)
}

func TestNormalizeToLE_LittleEndianIsNoOp(t *testing.T) {
	original := sineLE(440, 320, SampleRate)
	pcm := append([]byte(nil), original...)

	NormalizeToLE(pcm, LittleEndian)

	assert.Equal(t, original, pcm)
}

func TestNormalizeToLE_SwapsBigEndian(t *testing.T) {
	le := sineLE(440, 320, SampleRate)

	// Build the "wire" version: same samples, re-encoded big-endian.
	be := make([]byte, len(le))
	for i := 0; i+1 < len(le); i += 2 {
		sample := binary.LittleEndian.Uint16(le[i:])
		binary.BigEndian.PutUint16(be[i:], sample)
	}

	NormalizeToLE(be, BigEndian)

	assert.Equal(t, le, be, "normalizing a BE-encoded sine wave must reproduce the original LE bytes")
}

func TestNormalizeToLE_OddTrailingByteLeftAlone(t *testing.T) {
	pcm := []byte{0x01, 0x02, 0x03}

	assert.NotPanics(t, func() { NormalizeToLE(pcm, BigEndian) })
	assert.Equal(t, byte(0x03), pcm[2], "trailing unpaired byte must be left untouched")
}
