//go:build !linux || !cgo

// Package tenvad's non-Linux (or cgo-disabled) stub: TEN VAD's vendored
// library is Linux x86-64 only. The call wiring treats this error as "fall
// back to the energy detector, loudly" -- see newAgentHandler -- so
// development builds on other platforms keep working without the native
// library.
package tenvad

import (
	"errors"

	"github.com/nitesh/vaani/internal/ai/agent"
)

// NewBargeInDetector always fails off-Linux: VAD_MODE=ten cannot work
// without the native library.
func NewBargeInDetector(callID string, threshold float64) (agent.BargeInDetector, error) {
	return nil, errors.New("tenvad: TEN VAD needs a Linux cgo build (VAD_MODE=ten falls back to the energy detector on this platform)")
}

// Version returns "" off-Linux: there is no native library to query. The
// wiring logs it unconditionally, so the symbol must exist on all platforms.
func Version() string {
	return ""
}
