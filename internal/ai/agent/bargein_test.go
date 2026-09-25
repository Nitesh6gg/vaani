package agent

import (
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/assert"
)

// constFrame builds a 640-byte LE PCM16 frame of constant amplitude, so its
// RMS equals amplitude exactly (a fixed signal has zero variance).
func constFrame(amplitude int16) []byte {
	buf := make([]byte, 640)
	for i := 0; i < len(buf); i += 2 {
		binary.LittleEndian.PutUint16(buf[i:], uint16(amplitude))
	}

	return buf
}

const (
	loudAmplitude  = 1000
	quietAmplitude = 10
	floor          = 500
)

func TestEnergyDetector_SilenceNeverTriggers(t *testing.T) {
	d := NewEnergyDetector(floor)

	for i := 0; i < 20; i++ {
		assert.False(t, d.Detect(constFrame(quietAmplitude)))
	}
}

func TestEnergyDetector_SustainedLoudTriggers(t *testing.T) {
	d := NewEnergyDetector(floor)

	var triggered bool
	for i := 0; i < bargeInHistoryFrames; i++ {
		if d.Detect(constFrame(loudAmplitude)) {
			triggered = true
		}
	}

	assert.True(t, triggered, "5 consecutive loud frames must trigger (5 >= 3 votes needed)")
}

func TestEnergyDetector_SingleLoudSpikeDoesNotTrigger(t *testing.T) {
	d := NewEnergyDetector(floor)

	// Fill history with quiet, one loud spike, more quiet -- never 3 of 5 loud.
	assert.False(t, d.Detect(constFrame(quietAmplitude)))
	assert.False(t, d.Detect(constFrame(quietAmplitude)))
	assert.False(t, d.Detect(constFrame(loudAmplitude)))
	assert.False(t, d.Detect(constFrame(quietAmplitude)))
	assert.False(t, d.Detect(constFrame(quietAmplitude)))
}

func TestEnergyDetector_ThreeConsecutiveLoudTriggersImmediately(t *testing.T) {
	d := NewEnergyDetector(floor)

	// Unseen history slots default to "not speech" (zero value), so 3 genuine
	// consecutive loud frames trigger within the first 3 calls of a turn --
	// no need to wait for a full 5-frame window first. Speed matters here.
	assert.False(t, d.Detect(constFrame(loudAmplitude)))
	assert.False(t, d.Detect(constFrame(loudAmplitude)))
	assert.True(t, d.Detect(constFrame(loudAmplitude)), "3rd consecutive loud frame must trigger (3 >= 3 votes)")
}

func TestEnergyDetector_ResetClearsHistory(t *testing.T) {
	d := NewEnergyDetector(floor)

	for i := 0; i < bargeInHistoryFrames; i++ {
		d.Detect(constFrame(loudAmplitude))
	}

	d.Reset()

	assert.False(t, d.Detect(constFrame(loudAmplitude)), "one frame right after Reset must not trigger")
}
