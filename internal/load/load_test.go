package load

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func feed(g *Guard, n int, use float64) {
	for i := 0; i < n; i++ {
		g.observe(use)
	}
}

func TestGuard_OverloadedAtTheThresholdAverage(t *testing.T) {
	g := New(90, 80)

	feed(g, 4, 1.0)
	assert.False(t, g.Overloaded(), "not before a full 5-second window")

	g.observe(1.0)
	assert.True(t, g.Overloaded())
}

func TestGuard_ASpikeIsNotOverload(t *testing.T) {
	g := New(90, 80)

	feed(g, 10, 0.5)
	g.observe(1.0) // one busy second in a 5-second average of ~0.6

	assert.False(t, g.Overloaded())
}

func TestGuard_StaysOverloadedUntilCalmFor10Seconds(t *testing.T) {
	g := New(90, 80)
	feed(g, 5, 0.95)
	require.True(t, g.Overloaded())

	// Under the threshold but not under the resume level: still overloaded.
	feed(g, 20, 0.85)
	assert.True(t, g.Overloaded(), "between resume and threshold it stays refusing")

	// The average is under 80% from the first calm second on.
	feed(g, 9, 0.5)
	assert.True(t, g.Overloaded(), "9 calm seconds are not enough")

	g.observe(0.5)
	assert.False(t, g.Overloaded())
}

func TestGuard_LoadComingBackRestartsTheCalmCount(t *testing.T) {
	g := New(90, 80)
	feed(g, 5, 0.95)
	feed(g, 8, 0.5) // calm, 7 seconds counted
	require.True(t, g.Overloaded())

	feed(g, 5, 0.9) // busy again: the count starts over

	feed(g, 10, 0.5)
	assert.True(t, g.Overloaded(), "10 seconds after the window drained are not yet 10 calm ones")

	g.observe(0.5)
	assert.False(t, g.Overloaded())
}

func TestGuard_OffIsNilAndNeverOverloaded(t *testing.T) {
	g := New(0, 0)
	assert.Nil(t, g)
	assert.False(t, g.Overloaded())
	g.Run(context.Background()) // returns at once
}

func TestParseStat(t *testing.T) {
	busy, total, err := parseStat("cpu  100 10 50 800 20 5 15 0 0 0\n")
	require.NoError(t, err)
	assert.EqualValues(t, 180, busy, "user+nice+system+irq+softirq+steal; idle and iowait are not busy")
	assert.EqualValues(t, 1000, total)

	_, _, err = parseStat("cpu0 1 2 3 4 5\n")
	assert.Error(t, err)

	_, _, err = parseStat("cpu  1 2 x 4 5\n")
	assert.Error(t, err)
}

func TestReadProcStat_RealMachine(t *testing.T) {
	busy, total, err := readProcStat()
	if err != nil {
		t.Skip("no /proc/stat here")
	}

	assert.Positive(t, total)
	assert.LessOrEqual(t, busy, total)
}

// Run turns the machine's counters into samples once a second.
func TestGuard_RunSamplesTheCounters(t *testing.T) {
	g := New(90, 80)

	var busy, total uint64

	g.read = func() (uint64, uint64, error) {
		busy += 98 // 98% busy every "second"
		total += 100

		return busy, total, nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go g.Run(ctx)

	// The sampler ticks in real time (once a second), so this takes ~6s.
	require.Eventually(t, g.Overloaded, 10*time.Second, 100*time.Millisecond)
}
