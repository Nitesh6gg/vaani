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
	seconds, gated := h.STTAudio()
	assert.InDelta(t, 7*0.02, seconds, 0.001, "what the gate sent, not what the call carried")
	assert.True(t, gated)

	log := &CallLog{}
	log.STTAudio(seconds, gated)
	assert.True(t, log.Summary().STTGated)
	assert.InDelta(t, seconds, log.Summary().STTAudioSeconds, 1e-9)
}

// Each time the gate opens its log line says why: the VAD, or the loud-frame
// floor (with the level that tripped it).
func TestHandler_STTGate_LogsWhyItOpened(t *testing.T) {
	getLog := captureSlog(t)

	vad := &fakeClockDetector{}
	h, _ := gateHandler(t, vad, STTGateConfig{Enabled: true, Preroll: 100 * time.Millisecond, Hangover: 60 * time.Millisecond, RMSFloor: 400})

	for i := 0; i < 20; i++ {
		tickFrame(h) // quiet: stays shut, says nothing
	}

	assert.NotContains(t, getLog().String(), "stt.gate_open")

	vad.set(time.Now())
	tickFrame(h)
	assert.Contains(t, getLog().String(), "event=stt.gate_open reason=vad preroll_ms=100")
	assert.NotContains(t, getLog().String(), "rms=", "the level is only logged when the floor opened it")

	time.Sleep(gateVoicedWindow + 30*time.Millisecond) // the VAD's window and the hangover pass: closed again
	vad.set(time.Now().Add(-time.Hour))

	for i := 0; i < 10; i++ {
		tickFrame(h) // refills the pre-roll
	}

	h.ProcessFrame(context.Background(), "call1", constFrame(2000)) // loud, and the VAD heard nothing
	assert.Contains(t, getLog().String(), "reason=rms preroll_ms=100 rms=2000")
}

// The default: every frame goes out, as before the gate existed.
func TestHandler_STTGate_OffSendsEverything(t *testing.T) {
	getLog := captureSlog(t)

	h, fSTT := gateHandler(t, &fakeClockDetector{}, STTGateConfig{})

	for i := 0; i < 10; i++ {
		tickFrame(h)
	}

	assert.Equal(t, 10, fSTT.fedCount())
	assert.NotContains(t, getLog().String(), "stt.gate_open")
	assert.Nil(t, h.gate)

	seconds, gated := h.STTAudio()
	assert.InDelta(t, 0.2, seconds, 1e-9, "10 frames of 20ms, counted without the gate")
	assert.False(t, gated)
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
