package media

import (
	"encoding/binary"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
)

const (
	wavBitsPerSample = 16
	wavNumChannels   = 1
	wavHeaderSize    = 44
)

// WavRecorder writes two 16kHz mono s16le WAV files for one call -- inbound and
// outbound, post-normalization (exactly what a Handler, and eventually Phase 3
// STT, sees) -- for debugging. A recorder that fails to open, or errors mid-call,
// silently disables itself rather than affecting the call: recording is a
// diagnostic aid, never a call-ending dependency.
type WavRecorder struct {
	in  *wavFile
	out *wavFile
}

// NewWavRecorder creates the two WAV files for callID under dir. An empty dir
// disables recording entirely; WriteIn/WriteOut/Close are then all no-ops.
func NewWavRecorder(dir, callID string) *WavRecorder {
	if dir == "" {
		return &WavRecorder{}
	}

	if err := os.MkdirAll(dir, 0o755); err != nil {
		slog.Warn("wav recording disabled: could not create RECORD_DIR", "dir", dir, "error", err)
		return &WavRecorder{}
	}

	return &WavRecorder{
		in:  newWavFile(filepath.Join(dir, fmt.Sprintf("%s-in.wav", callID))),
		out: newWavFile(filepath.Join(dir, fmt.Sprintf("%s-out.wav", callID))),
	}
}

// WriteIn appends inbound (caller -> Vaani) PCM16 LE samples.
func (r *WavRecorder) WriteIn(pcm []byte) { r.in.write(pcm) }

// WriteOut appends outbound (Vaani -> caller) PCM16 LE samples.
func (r *WavRecorder) WriteOut(pcm []byte) { r.out.write(pcm) }

// Close finalizes both WAV headers with the real data size. Safe on a disabled or
// partially-failed recorder.
func (r *WavRecorder) Close() {
	r.in.close()
	r.out.close()
}

// wavFile is one streaming-write WAV file: a placeholder header is written up
// front, and patched with the real data size on close (since total duration isn't
// known until the call ends). A nil *wavFile (or one whose open failed) is safe to
// call every method on -- it just does nothing.
type wavFile struct {
	f         *os.File
	dataBytes int
}

func newWavFile(path string) *wavFile {
	f, err := os.Create(path) //nolint:gosec // debug recording path is operator-configured, not user input
	if err != nil {
		slog.Warn("wav recording disabled: could not create file", "path", path, "error", err)
		return &wavFile{}
	}

	if err := writeWavHeader(f, 0); err != nil {
		slog.Warn("wav recording disabled: could not write header", "path", path, "error", err)
		_ = f.Close()

		return &wavFile{}
	}

	return &wavFile{f: f}
}

func (w *wavFile) write(pcm []byte) {
	if w == nil || w.f == nil {
		return
	}

	n, err := w.f.Write(pcm)
	if err != nil {
		slog.Warn("wav write error; disabling further recording for this file", "error", err)
		_ = w.f.Close()
		w.f = nil

		return
	}

	w.dataBytes += n
}

func (w *wavFile) close() {
	if w == nil || w.f == nil {
		return
	}

	defer func() { _ = w.f.Close() }()

	if _, err := w.f.Seek(0, 0); err != nil {
		slog.Warn("wav finalize seek error", "error", err)
		return
	}

	if err := writeWavHeader(w.f, w.dataBytes); err != nil {
		slog.Warn("wav finalize header error", "error", err)
	}
}

func writeWavHeader(f *os.File, dataBytes int) error {
	byteRate := SampleRate * wavNumChannels * wavBitsPerSample / 8
	blockAlign := wavNumChannels * wavBitsPerSample / 8
	riffSize := wavHeaderSize - 8 + dataBytes

	buf := make([]byte, wavHeaderSize)
	copy(buf[0:4], "RIFF")
	binary.LittleEndian.PutUint32(buf[4:8], uint32(riffSize))
	copy(buf[8:12], "WAVE")
	copy(buf[12:16], "fmt ")
	binary.LittleEndian.PutUint32(buf[16:20], 16) // fmt chunk size
	binary.LittleEndian.PutUint16(buf[20:22], 1)  // PCM
	binary.LittleEndian.PutUint16(buf[22:24], uint16(wavNumChannels))
	binary.LittleEndian.PutUint32(buf[24:28], uint32(SampleRate))
	binary.LittleEndian.PutUint32(buf[28:32], uint32(byteRate))
	binary.LittleEndian.PutUint16(buf[32:34], uint16(blockAlign))
	binary.LittleEndian.PutUint16(buf[34:36], uint16(wavBitsPerSample))
	copy(buf[36:40], "data")
	binary.LittleEndian.PutUint32(buf[40:44], uint32(dataBytes))

	_, err := f.Write(buf)

	return err
}
