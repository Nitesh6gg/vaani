package media

import (
	"bytes"
	"encoding/binary"
	"log/slog"
	"math"
	"os"
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

func TestFromLE(t *testing.T) {
	le := sineLE(440, 320, SampleRate)

	cases := []struct {
		name string
		to   Endianness
		want []byte // expected result of FromLE(leCopy, to)
	}{
		{
			name: "little endian is a no-op",
			to:   LittleEndian,
			want: le,
		},
		{
			name: "big endian swaps every sample pair",
			to:   BigEndian,
			want: func() []byte {
				be := make([]byte, len(le))
				for i := 0; i+1 < len(le); i += 2 {
					sample := binary.LittleEndian.Uint16(le[i:])
					binary.BigEndian.PutUint16(be[i:], sample)
				}
				return be
			}(),
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			pcm := append([]byte(nil), le...)
			FromLE(pcm, tc.to)
			assert.Equal(t, tc.want, pcm)

			// And the exact inverse: what NormalizeToLE converts in,
			// FromLE must convert back out.
			roundTrip := append([]byte(nil), tc.want...)
			NormalizeToLE(roundTrip, tc.to)
			assert.Equal(t, le, roundTrip, "FromLE must invert NormalizeToLE")
		})
	}
}

func TestFromLE_OddTrailingByteLeftAlone(t *testing.T) {
	pcm := []byte{0x01, 0x02, 0x03}

	assert.NotPanics(t, func() { FromLE(pcm, BigEndian) })
	assert.Equal(t, byte(0x03), pcm[2], "trailing unpaired byte must be left untouched")
}

func TestWarnIfUnset_LogsWhenEnvVarAbsent(t *testing.T) {
	restoreLogger := captureSlog(t)

	orig, existed := os.LookupEnv("AUDIO_L16_ENDIANNESS")
	require.NoError(t, os.Unsetenv("AUDIO_L16_ENDIANNESS"))

	t.Cleanup(func() {
		if existed {
			_ = os.Setenv("AUDIO_L16_ENDIANNESS", orig)
		}
	})

	buf := restoreLogger()
	WarnIfUnset()

	assert.Contains(t, buf.String(), "endianness unverified")
}

func TestWarnIfUnset_SilentWhenEnvVarSet(t *testing.T) {
	restoreLogger := captureSlog(t)

	t.Setenv("AUDIO_L16_ENDIANNESS", "le")

	buf := restoreLogger()
	WarnIfUnset()

	assert.Empty(t, buf.String(), "must not warn once AUDIO_L16_ENDIANNESS is explicitly set")
}

// captureSlog swaps the default slog logger for one writing to a buffer, restores
// it on test cleanup, and returns a function that hands back that buffer (called
// after any env setup so log output from unrelated setup doesn't pollute it).
func captureSlog(t *testing.T) func() *bytes.Buffer {
	t.Helper()

	buf := &bytes.Buffer{}
	original := slog.Default()

	slog.SetDefault(slog.New(slog.NewTextHandler(buf, nil)))
	t.Cleanup(func() { slog.SetDefault(original) })

	return func() *bytes.Buffer { return buf }
}
