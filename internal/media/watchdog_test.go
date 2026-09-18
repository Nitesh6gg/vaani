package media

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

type fakeWatchdogSink struct {
	count atomic.Int32
}

func (f *fakeWatchdogSink) WatchdogTimeout() { f.count.Add(1) }

func TestWatchdog_WarnsAfterSilence(t *testing.T) {
	sink := &fakeWatchdogSink{}
	w := NewWatchdog("call1", 30*time.Millisecond, sink)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go w.Run(ctx)

	time.Sleep(120 * time.Millisecond) // ~4 timeout windows of pure silence

	cancel()
	time.Sleep(10 * time.Millisecond) // let Run's goroutine observe cancellation

	got := sink.count.Load()
	assert.GreaterOrEqual(t, got, int32(2), "must re-warn roughly once per timeout interval, not just once")
}

func TestWatchdog_TouchResetsSilenceClock(t *testing.T) {
	sink := &fakeWatchdogSink{}
	w := NewWatchdog("call1", 30*time.Millisecond, sink)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go w.Run(ctx)

	stop := time.After(150 * time.Millisecond)

loop:
	for {
		select {
		case <-stop:
			break loop
		case <-time.After(10 * time.Millisecond):
			w.Touch() // continuous "audio" arriving faster than the timeout
		}
	}

	cancel()
	time.Sleep(10 * time.Millisecond)

	assert.Zero(t, sink.count.Load(), "must never warn while packets keep arriving inside the timeout window")
}
