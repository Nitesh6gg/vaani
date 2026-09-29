package assets

import (
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEmbeddedAssetsDecodeTo16kHz(t *testing.T) {
	for name, load := range map[string]func() ([]byte, error){
		"hold": TransferHoldRing,
		"beep": Beep,
	} {
		pcm, err := load()
		require.NoError(t, err, name)
		assert.NotEmpty(t, pcm, name)
		assert.Zero(t, len(pcm)%2, "%s: whole samples only", name)
	}
}

func TestUpsample2xInterpolates(t *testing.T) {
	in := make([]byte, 0, 4)
	in = binary.LittleEndian.AppendUint16(in, uint16(0))
	in = binary.LittleEndian.AppendUint16(in, uint16(100))

	out := upsample2x(in)

	var got []int16
	for i := 0; i+1 < len(out); i += 2 {
		got = append(got, int16(binary.LittleEndian.Uint16(out[i:])))
	}

	assert.Equal(t, []int16{0, 50, 100, 100}, got)
}

func TestParseWAVSkipsMetadataChunks(t *testing.T) {
	// fmt, then a LIST chunk, then data -- the layout of the hold-ring file.
	wav := []byte("RIFF\x00\x00\x00\x00WAVE")
	wav = append(wav, "fmt "...)
	wav = binary.LittleEndian.AppendUint32(wav, 16)
	wav = binary.LittleEndian.AppendUint16(wav, 1)     // PCM
	wav = binary.LittleEndian.AppendUint16(wav, 1)     // mono
	wav = binary.LittleEndian.AppendUint32(wav, 8000)  // rate
	wav = binary.LittleEndian.AppendUint32(wav, 16000) // byte rate
	wav = binary.LittleEndian.AppendUint16(wav, 2)     // block align
	wav = binary.LittleEndian.AppendUint16(wav, 16)    // bits
	wav = append(wav, "LIST"...)
	wav = binary.LittleEndian.AppendUint32(wav, 3) // odd size: padded to 4
	wav = append(wav, 'a', 'b', 'c', 0)
	wav = append(wav, "data"...)
	wav = binary.LittleEndian.AppendUint32(wav, 4)
	wav = append(wav, 1, 2, 3, 4)

	rate, pcm, err := parseWAV(wav)
	require.NoError(t, err)
	assert.Equal(t, 8000, rate)
	assert.Equal(t, []byte{1, 2, 3, 4}, pcm)
}
