package media

import (
	"context"
	"log/slog"
	"sync/atomic"
	"time"
)

// WatchdogTimeout is how long a call may go without a valid inbound RTP packet
// before the watchdog warns. It re-warns every interval for as long as silence
// continues, rather than spamming continuously or warning only once.
const WatchdogTimeout = 5 * time.Second

// WatchdogSink receives the watchdog's observability event.
type WatchdogSink interface {
	WatchdogTimeout()
}

// Watchdog detects a call whose inbound RTP has gone quiet. It never affects call
// teardown -- a quiet loopback call keeps running -- it only logs and counts, so
// an operator (or, later, Phase 3's session logic) can see it happening.
type Watchdog struct {
	callID  string
	timeout time.Duration
	sink    WatchdogSink
	lastPkt atomic.Int64 // UnixNano of the last valid inbound packet
}

// NewWatchdog creates a Watchdog for callID with the given silence timeout
// (production callers should pass WatchdogTimeout; tests may pass a shorter
// duration), starting its clock now.
func NewWatchdog(callID string, timeout time.Duration, sink WatchdogSink) *Watchdog {
	w := &Watchdog{callID: callID, timeout: timeout, sink: sink}
	w.Touch()

	return w
}

// Touch records that a valid inbound packet just arrived, resetting the silence
// clock. Call this from the reader on every valid (non-malformed) inbound packet.
func (w *Watchdog) Touch() {
	w.lastPkt.Store(time.Now().UnixNano())
}

// Run polls once per timeout interval until ctx is cancelled, warning (and
// incrementing the counter) each time inbound audio has been silent for at least
// that long.
func (w *Watchdog) Run(ctx context.Context) {
	ticker := time.NewTicker(w.timeout)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			last := time.Unix(0, w.lastPkt.Load())
			if since := time.Since(last); since >= w.timeout {
				w.sink.WatchdogTimeout()
				slog.Warn("media watchdog: no inbound rtp", "call_id", w.callID, "silent_for", since.Round(time.Second))
			}
		}
	}
}
