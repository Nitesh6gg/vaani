// Package tenvad wraps TEN Framework's TEN VAD native library
// (third_party/ten-vad, Apache 2.0) behind agent.BargeInDetector -- the
// VAD_MODE=ten upgrade over the RMS EnergyDetector. TEN VAD is a real
// neural voice-activity model: unlike an RMS threshold it can tell speech
// from a cough, music-on-hold, or line noise, which is exactly the ceiling
// EnergyDetector's doc comment admits to.
//
// This file is cgo and Linux-only (the vendored .so is Linux x86-64; that is
// Vaani's only production target). Everywhere else a stub returns an error
// and the call wiring falls back to the energy detector, so non-Linux
// builds -- including this repo's Windows dev box -- never compile cgo and
// never need the native library.
//
// The wrapper deliberately stays a thin, dumb binding: all buffering and
// voting policy lives in agent.TenVadDetector (pure Go, unit-tested on any
// platform) so the logic is testable without the native library.
//
// The vendored library lives at the repo root, four levels up from this
// package's directory. ${SRCDIR} is substituted at build time with an
// absolute path, so the rpath keeps working for both `go run` and binaries
// built from the same checkout. Everything in the comment block directly
// above `import "C"` below is the cgo preamble, which cgo strips of its
// comment markers and feeds to the C compiler as source -- prose must live
// up here in the Go doc comment, never in the preamble.
package tenvad

import (
	"fmt"
	"runtime"

	"github.com/nitesh/vaani/internal/ai/agent"
)

// #cgo CFLAGS: -I${SRCDIR}/../../../../third_party/ten-vad/include
// #cgo LDFLAGS: -L${SRCDIR}/../../../../third_party/ten-vad/lib/Linux/x64 -lten_vad -Wl,-rpath,${SRCDIR}/../../../../third_party/ten-vad/lib/Linux/x64
// #include <ten_vad.h>
import "C"

// HopSize is TEN VAD's analysis hop in samples: 256 = 16ms at 16kHz (the
// value the official Go example uses; agent.TenVadDetector re-buffers the
// 20ms media frames into hops of this size).
const HopSize = 256

// Vad wraps one native ten_vad instance. Not safe for concurrent use -- the
// detector drives it from ProcessFrame's single goroutine, matching the
// BargeInDetector contract.
type Vad struct {
	handle  C.ten_vad_handle_t
	hopSize int
}

// New creates a native VAD instance for the given hop size (samples between
// analysis frames) and speech threshold in [0,1].
func New(hopSize int, threshold float32) (*Vad, error) {
	if hopSize <= 0 {
		return nil, fmt.Errorf("tenvad: hop size must be positive, got %d", hopSize)
	}

	var handle C.ten_vad_handle_t

	res := C.ten_vad_create(&handle, C.size_t(hopSize), C.float(threshold))
	if res != 0 || handle == nil {
		return nil, fmt.Errorf("tenvad: ten_vad_create failed (res=%d)", int(res))
	}

	v := &Vad{handle: handle, hopSize: hopSize}
	runtime.SetFinalizer(v, func(v *Vad) { _ = v.Close() })

	return v, nil
}

// HopSize returns the hop size this instance was created with.
func (v *Vad) HopSize() int { return v.hopSize }

// Process runs one hop of audio through the model. frame must be exactly
// HopSize int16 samples (16kHz LE PCM16, the pipeline's native format).
// Returns the raw speech probability and the native binary decision
// (probability >= threshold).
func (v *Vad) Process(frame []int16) (float32, bool, error) {
	if len(frame) != v.hopSize {
		return 0, false, fmt.Errorf("tenvad: frame length %d != hop size %d", len(frame), v.hopSize)
	}

	var (
		prob C.float
		flag C.int
	)

	res := C.ten_vad_process(v.handle, (*C.short)(&frame[0]), C.size_t(len(frame)), &prob, &flag)
	if res != 0 {
		return 0, false, fmt.Errorf("tenvad: ten_vad_process failed (res=%d)", int(res))
	}

	return float32(prob), int(flag) == 1, nil
}

// Close destroys the native instance. Safe to call more than once; the GC
// finalizer registered at creation covers a leaked instance, but explicit
// close is preferred.
func (v *Vad) Close() error {
	if v.handle == nil {
		return nil
	}

	C.ten_vad_destroy(&v.handle)
	v.handle = nil
	runtime.SetFinalizer(v, nil)

	return nil
}

// Version returns the native library's version string.
func Version() string {
	return C.GoString(C.ten_vad_get_version())
}

// NewBargeInDetector builds the agent.BargeInDetector wired to a fresh
// native TEN VAD instance. The instance is per-call (one detector per call,
// created in newAgentHandler) so calls never share VAD state; the GC
// finalizer reclaims it at teardown.
func NewBargeInDetector(callID string, threshold float64) (agent.BargeInDetector, error) {
	vad, err := New(HopSize, float32(threshold))
	if err != nil {
		return nil, err
	}

	hop := func(samples []int16) (bool, error) {
		_, speech, err := vad.Process(samples)
		return speech, err
	}

	return agent.NewTenVadDetector(callID, hop), nil
}
