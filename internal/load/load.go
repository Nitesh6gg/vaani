// Package load tells whether this machine is too busy to take on another call.
//
// Past a CPU level the media threads of the calls already up start missing
// their 20ms ticks, which the callers hear as jitter. A Guard measures the
// whole machine's CPU (Asterisk shares it, and a starved Asterisk is as bad as
// a starved Vaani) and, like LiveKit's agent workers, only stops *new* work
// while it is over the limit: calls in progress are never touched.
package load

import (
	"bufio"
	"context"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/nitesh/vaani/internal/metrics"
)

const (
	interval   = time.Second
	window     = 5  // samples averaged: the load is the last 5 seconds' CPU
	calmSample = 10 // samples below the resume level before new work is taken again
)

// Guard says whether the machine is overloaded. A nil Guard never is.
type Guard struct {
	threshold, resume float64 // CPU fraction: overloaded at/above the first, calm again below the second
	read              func() (busy, total uint64, err error)

	over atomic.Bool

	// Touched only by observe, from Run's goroutine.
	samples []float64
	calm    int
}

// New returns a Guard that turns overloaded at thresholdPercent CPU (a 5-second
// average) and calm again after 10 seconds below resumePercent. thresholdPercent
// 0 returns nil: the guard is off.
func New(thresholdPercent, resumePercent int) *Guard {
	if thresholdPercent <= 0 {
		return nil
	}

	return &Guard{threshold: float64(thresholdPercent) / 100, resume: float64(resumePercent) / 100, read: readProcStat}
}

// Overloaded says whether new calls should be refused right now.
func (g *Guard) Overloaded() bool {
	return g != nil && g.over.Load()
}

// Run samples the machine's CPU every second until ctx is cancelled.
func (g *Guard) Run(ctx context.Context) {
	if g == nil {
		return
	}

	prevBusy, prevTotal, err := g.read()
	if err != nil {
		slog.Warn("cpu load guard disabled: cannot read the machine's CPU", "event", "load.unavailable", "error", err)
		return
	}

	slog.Info("cpu load guard on", "event", "load.enabled", "refuse_new_calls_at_percent", int(g.threshold*100),
		"accept_again_below_percent", int(g.resume*100))

	t := time.NewTicker(interval)
	defer t.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}

		busy, total, err := g.read()
		if err != nil || total <= prevTotal {
			continue
		}

		g.observe(float64(busy-prevBusy) / float64(total-prevTotal))
		prevBusy, prevTotal = busy, total
	}
}

// observe takes one second's CPU use (0..1). Overloaded starts when the average
// of the last 5 samples reaches the threshold; it ends only after 10 consecutive
// samples whose average is below the resume level, so it can't flap.
func (g *Guard) observe(use float64) {
	g.samples = append(g.samples, use)
	if len(g.samples) > window {
		g.samples = g.samples[1:]
	}

	avg := 0.0
	for _, s := range g.samples {
		avg += s
	}

	avg /= float64(len(g.samples))
	metrics.CPULoadRatio.Set(avg)

	switch {
	case !g.over.Load():
		if len(g.samples) == window && avg >= g.threshold {
			g.over.Store(true)
			g.calm = 0
			metrics.Overloaded.Set(1)
			slog.Warn("cpu over the limit: refusing new calls", "event", "load.overloaded", "cpu_percent", int(avg*100),
				"limit_percent", int(g.threshold*100))
		}
	case avg < g.resume:
		if g.calm++; g.calm >= calmSample {
			g.over.Store(false)
			metrics.Overloaded.Set(0)
			slog.Info("cpu back under the limit: accepting calls again", "event", "load.recovered", "cpu_percent", int(avg*100))
		}
	default:
		g.calm = 0
	}
}

// readProcStat is the machine's cumulative CPU time from /proc/stat: busy
// (everything but idle and iowait) and total, in clock ticks.
func readProcStat() (busy, total uint64, err error) {
	f, err := os.Open("/proc/stat")
	if err != nil {
		return 0, 0, err
	}
	defer f.Close()

	line, err := bufio.NewReader(f).ReadString('\n')
	if err != nil {
		return 0, 0, err
	}

	return parseStat(line)
}

// parseStat parses the aggregate "cpu  user nice system idle iowait irq
// softirq steal ..." line. Guest time is already inside user and nice.
func parseStat(line string) (busy, total uint64, err error) {
	f := strings.Fields(line)
	if len(f) < 5 || f[0] != "cpu" {
		return 0, 0, fmt.Errorf("unexpected /proc/stat line %q", strings.TrimSpace(line))
	}

	var v [8]uint64

	for i := 0; i < len(v) && i+1 < len(f); i++ {
		if v[i], err = strconv.ParseUint(f[i+1], 10, 64); err != nil {
			return 0, 0, fmt.Errorf("/proc/stat: %w", err)
		}
	}

	// user nice system idle iowait irq softirq steal
	idle := v[3] + v[4]
	for _, x := range v {
		total += x
	}

	return total - idle, total, nil
}
