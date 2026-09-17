package media

import (
	"sync"
	"time"
)

// FrameInterval is the fixed pacing interval for outbound RTP: one slin16 frame
// every 20ms.
const FrameInterval = 20 * time.Millisecond

// Pacer emits one tick per FrameInterval via time.NewTicker. Never use time.Sleep
// for pacing: it drifts and, after any stall, delivers a burst of catch-up sleeps.
// A ticker's channel is fed the actual fire time on every tick (never an
// accumulated theoretical deadline), so a stall costs exactly the missed ticks —
// no burst follows.
type Pacer struct {
	interval time.Duration
	stopCh   chan struct{}
	stopOnce sync.Once
}

// NewPacer creates a Pacer with the given tick interval.
func NewPacer(interval time.Duration) *Pacer {
	return &Pacer{interval: interval, stopCh: make(chan struct{})}
}

// Run invokes fn once per tick, passing the tick's fire time, until Stop is called.
// It blocks the calling goroutine — run it in its own goroutine.
func (p *Pacer) Run(fn func(now time.Time)) {
	ticker := time.NewTicker(p.interval)
	defer ticker.Stop()

	for {
		select {
		case now := <-ticker.C:
			fn(now)
		case <-p.stopCh:
			return
		}
	}
}

// Stop terminates Run. Safe to call more than once.
func (p *Pacer) Stop() {
	p.stopOnce.Do(func() { close(p.stopCh) })
}
