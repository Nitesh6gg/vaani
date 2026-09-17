package media

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestPacer_StopEndsRun(t *testing.T) {
	p := NewPacer(time.Millisecond)

	ticks := 0
	done := make(chan struct{})

	go func() {
		p.Run(func(_ time.Time) { ticks++ })
		close(done)
	}()

	time.Sleep(20 * time.Millisecond)
	p.Stop()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Run did not return after Stop")
	}

	assert.Greater(t, ticks, 0, "pacer should have ticked at least once before stopping")
}

// TestPacer_DriftOverOneMinute is the acceptance-criteria drift test: 60 real
// seconds of 20ms ticks, each within +/-2ms of its nominal deadline. It's skipped
// under -short since it necessarily takes a full minute of wall-clock time.
func TestPacer_DriftOverOneMinute(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping 60s pacer drift test in -short mode")
	}

	const (
		duration = 60 * time.Second
		interval = FrameInterval
		maxDrift = 2 * time.Millisecond
	)

	p := NewPacer(interval)

	start := time.Now()
	tickN := 0
	maxObservedDrift := time.Duration(0)

	done := make(chan struct{})

	go func() {
		p.Run(func(now time.Time) {
			tickN++
			nominal := start.Add(time.Duration(tickN) * interval)

			drift := now.Sub(nominal)
			if drift < 0 {
				drift = -drift
			}

			if drift > maxObservedDrift {
				maxObservedDrift = drift
			}
		})
		close(done)
	}()

	time.Sleep(duration)
	p.Stop()
	<-done

	assert.LessOrEqual(t, maxObservedDrift, maxDrift,
		"pacer drifted more than %v from nominal deadline over %v", maxDrift, duration)
	assert.InDelta(t, int(duration/interval), tickN, 5,
		"expected roughly one tick per %v over %v", interval, duration)
}
