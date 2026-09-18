package media

import (
	"encoding/binary"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWavRecorder_DisabledWhenDirEmpty(t *testing.T) {
	r := NewWavRecorder("", "call1")

	assert.NotPanics(t, func() {
		r.WriteIn(make([]byte, FrameSize))
		r.WriteOut(make([]byte, FrameSize))
		r.Close()
	})
}

func TestWavRecorder_WritesValidHeaderAndSize(t *testing.T) {
	dir := t.TempDir()
	r := NewWavRecorder(dir, "call1")

	frame := make([]byte, FrameSize)
	for i := 0; i < 50; i++ { // 50 frames = 1s @ 20ms
		r.WriteIn(frame)
		r.WriteOut(frame)
	}

	r.Close()

	inPath := filepath.Join(dir, "call1-in.wav")
	outPath := filepath.Join(dir, "call1-out.wav")

	assertValidWav(t, inPath, 50*FrameSize)
	assertValidWav(t, outPath, 50*FrameSize)
}

func assertValidWav(t *testing.T, path string, wantDataBytes int) {
	t.Helper()

	data, err := os.ReadFile(path)
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(data), wavHeaderSize)

	assert.Equal(t, "RIFF", string(data[0:4]))
	assert.Equal(t, "WAVE", string(data[8:12]))
	assert.Equal(t, "fmt ", string(data[12:16]))
	assert.Equal(t, uint16(1), binary.LittleEndian.Uint16(data[20:22]), "must be PCM format")
	assert.Equal(t, uint16(1), binary.LittleEndian.Uint16(data[22:24]), "must be mono")
	assert.Equal(t, uint32(SampleRate), binary.LittleEndian.Uint32(data[24:28]))
	assert.Equal(t, uint16(16), binary.LittleEndian.Uint16(data[34:36]), "must be 16-bit samples")
	assert.Equal(t, "data", string(data[36:40]))
	assert.Equal(t, uint32(wantDataBytes), binary.LittleEndian.Uint32(data[40:44]))
	assert.Len(t, data, wavHeaderSize+wantDataBytes)

	if _, err := exec.LookPath("ffprobe"); err != nil {
		t.Skip("ffprobe not available; header fields already verified byte-for-byte")
	}

	out, err := exec.Command("ffprobe", "-v", "error", "-show_entries",
		"stream=sample_rate,channels,bits_per_sample", "-of", "default=noprint_wrappers=1", path).CombinedOutput()
	require.NoError(t, err, "ffprobe output: %s", out)
	assert.Contains(t, string(out), "sample_rate=16000")
	assert.Contains(t, string(out), "channels=1")
}
