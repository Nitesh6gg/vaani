package media

import (
	"context"
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fixedHandler returns its frames on every call.
type fixedHandler [][]byte

func (f fixedHandler) ProcessFrame(context.Context, string, []byte) [][]byte { return f }

func frameOf(v int16) []byte {
	b := make([]byte, FrameSize)
	for i := 0; i < FrameSize; i += 2 {
		binary.LittleEndian.PutUint16(b[i:], uint16(v))
	}

	return b
}

func TestMixRecorder(t *testing.T) {
	cases := []struct {
		name    string
		in      int16
		out     [][]byte
		want    int16
		wantOut int
	}{
		{"caller only", 1000, nil, 1000, 0},
		{"both, summed", 1000, [][]byte{frameOf(-300)}, 700, 1},
		{"clipped high", 30000, [][]byte{frameOf(10000)}, 32767, 1},
		{"clipped low", -30000, [][]byte{frameOf(-10000)}, -32768, 1},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := NewMixRecorder(fixedHandler(tc.out))

			out := r.ProcessFrame(context.Background(), "c", frameOf(tc.in))
			assert.Len(t, out, tc.wantOut, "the wrapped handler's output passes through")

			wav := r.WAV()
			require.Len(t, wav, wavHeaderSize+FrameSize)
			assert.Equal(t, "RIFF", string(wav[:4]))
			assert.Equal(t, uint32(FrameSize), binary.LittleEndian.Uint32(wav[40:44]))
			assert.Equal(t, tc.want, int16(binary.LittleEndian.Uint16(wav[wavHeaderSize:])))
		})
	}

	assert.Nil(t, NewMixRecorder(fixedHandler(nil)).WAV(), "nothing recorded: no file")
}
