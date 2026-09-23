package media

import (
	"context"
	"log/slog"
	"sync"
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

// Watchdog detects a call whose inbound RTP has gone quiet. Warning is pure
// observation -- a quiet call keeps running -- but sustained silence past
// deadTimeout additionally closes the channel Dead() returns, so a caller
// (internal/ari.Manager) can proactively hang the call up instead of leaving a
// media-dead call running indefinitely. This is deliberately a second, distinct
// layer from Asterisk's own rtptimeout (deploy/asterisk/rtp.conf): rtptimeout
// only watches the caller's own SIP-side RTP, so it can't catch a call whose
// caller-side RTP is perfectly healthy but whose audio never reaches this
// externalMedia leg because of an Asterisk-internal bridging/mixing problem --
// exactly the failure class behind the jitter-buffer production incident (see
// docs/AUDIO_PIPELINE.md). Only watching this leg directly can catch that.
type Watchdog struct {
	callID      string
	timeout     time.Duration
	deadTimeout time.Duration // 0 disables dead-call detection
	sink        WatchdogSink
	lastPkt     atomic.Int64 // UnixNano of the last valid inbound packet

	deadCh   chan struct{}
	deadOnce sync.Once
}

// NewWatchdog creates a Watchdog for callID with the given silence timeout
// (production callers should pass WatchdogTimeout; tests may pass a shorter
// duration) and dead-call threshold (0 disables Dead() entirely), starting its
// clock now.
func NewWatchdog(callID string, timeout, deadTimeout time.Duration, sink WatchdogSink) *Watchdog {
	w := &Watchdog{
		callID:      callID,
		timeout:     timeout,
		deadTimeout: deadTimeout,
		sink:        sink,
		deadCh:      make(chan struct{}),
	}
	w.Touch()

	return w
}

// Touch records that a valid inbound packet just arrived, resetting the silence
// clock. Call this from the reader on every valid (non-malformed) inbound packet.
func (w *Watchdog) Touch() {
	w.lastPkt.Store(time.Now().UnixNano())
}

// Dead returns a channel that's closed once inbound media has been silent for
// at least deadTimeout. Never closes if deadTimeout is 0. Safe to select on
// even after Run's ctx is cancelled -- it just never closes in that case.
func (w *Watchdog) Dead() <-chan struct{} {
	return w.deadCh
}

// Run polls once per timeout interval until ctx is cancelled, warning (and
// incrementing the counter) each time inbound audio has been silent for at
// least that long, and closing Dead()'s channel (once) if silence reaches
// deadTimeout.
func (w *Watchdog) Run(ctx context.Context) {
	ticker := time.NewTicker(w.timeout)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			last := time.Unix(0, w.lastPkt.Load())
			since := time.Since(last)

			if since >= w.timeout {
				w.sink.WatchdogTimeout()
				slog.Warn("media watchdog: no inbound rtp", "call_id", w.callID, "silent_for", since.Round(time.Second))
			}

			if w.deadTimeout > 0 && since >= w.deadTimeout {
				w.deadOnce.Do(func() {
					slog.Warn("media watchdog: declaring call dead, no inbound audio past threshold",
						"call_id", w.callID, "silent_for", since.Round(time.Second), "dead_timeout", w.deadTimeout)
					close(w.deadCh)
				})
			}
		}
	}
}
