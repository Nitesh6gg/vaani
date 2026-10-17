package agent

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func gateHandler(t *testing.T, vad BargeInDetector, gate STTGateConfig) (*Handler, *fakeSTT) {
	t.Helper()

	fSTT := newFakeSTT()
	cfg := testConfig(fSTT, newFakeTTS(), &fakeLLM{}, time.Hour, 0)
	cfg.BargeIn = vad
	cfg.STTGate = gate

	return NewHandler(context.Background(), "call1", cfg), fSTT
}

func tickFrame(h *Handler) { h.ProcessFrame(context.Background(), "call1", constFrame(quietAmplitude)) }

// With the gate on, quiet frames never reach the STT; when the VAD hears
// speech the buffered pre-roll and the frame do; after the hangover it shuts.
func TestHandler_STTGate_SendsOnlySpeech(t *testing.T) {
	vad := &fakeClockDetector{}
	h, fSTT := gateHandler(t, vad, STTGateConfig{Enabled: true, Preroll: 100 * time.Millisecond, Hangover: 60 * time.Millisecond})

	for i := 0; i < 20; i++ {
		tickFrame(h)
	}

	assert.Zero(t, fSTT.fedCount(), "quiet: nothing sent")

	vad.set(time.Now())
	tickFrame(h)
	assert.Equal(t, 6, fSTT.fedCount(), "5 frames of pre-roll and the speech frame")

	tickFrame(h)
	assert.Equal(t, 7, fSTT.fedCount(), "live while open")

	time.Sleep(gateVoicedWindow + 30*time.Millisecond) // past the VAD's window and the hangover

	tickFrame(h)
	assert.Equal(t, 7, fSTT.fedCount(), "closed again")
	assert.InDelta(t, 7*0.02, h.gate.Sent(), 0.001)
}

// The default: every frame goes out, as before the gate existed.
func TestHandler_STTGate_OffSendsEverything(t *testing.T) {
	h, fSTT := gateHandler(t, &fakeClockDetector{}, STTGateConfig{})

	for i := 0; i < 10; i++ {
		tickFrame(h)
	}

	assert.Equal(t, 10, fSTT.fedCount())
	assert.Nil(t, h.gate)
}

// A VAD that can't say when it last heard speech (the energy detector) can't
// drive the gate: every frame goes out, never fewer than before.
func TestHandler_STTGate_WithoutAClockVADSendsEverything(t *testing.T) {
	h, fSTT := gateHandler(t, NewEnergyDetector(floor), STTGateConfig{Enabled: true, Preroll: time.Second, Hangover: time.Second})

	for i := 0; i < 10; i++ {
		tickFrame(h)
	}

	assert.Equal(t, 10, fSTT.fedCount())
	assert.Nil(t, h.gate)
}
