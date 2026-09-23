// Command loadgen exercises the ARI control plane (answer, externalMedia stage,
// bridge, teardown) at scale without needing real SIP devices or audio: it
// originates N Local channels against a dialplan extension that immediately
// re-enters the same Stasis app, so each origination drives Manager through a
// full call lifecycle. This proves the control plane holds up under concurrency
// and port-allocator pressure; it carries no real media, so it says nothing about
// audio quality -- that's what the SIPp-based load test in deploy/sipp is for.
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/CyCoreSystems/ari/v5"

	ariclient "github.com/nitesh/vaani/internal/ari"
	"github.com/nitesh/vaani/internal/config"
)

func main() {
	if err := run(); err != nil {
		slog.Error("loadgen failed", "error", err)
		os.Exit(1)
	}
}

func run() error {
	n := flag.Int("n", 25, "number of calls to originate")
	rampPerSec := flag.Float64("ramp", 5, "calls originated per second")
	hold := flag.Duration("hold", 5*time.Second, "how long to hold each call bridged before hanging it up")
	endpoint := flag.String("endpoint", "Local/2001@test", "ARI Endpoint to originate (tech/resource)")
	flag.Parse()

	cfg, err := config.Load()
	if err != nil {
		return err
	}

	if *rampPerSec <= 0 {
		return fmt.Errorf("-ramp must be positive (calls per second), got %v", *rampPerSec)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	cl, err := ariclient.Connect(ctx, cfg)
	if err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	defer cl.Close()

	slog.Info("loadgen starting", "n", *n, "ramp_per_sec", *rampPerSec, "hold", *hold, "endpoint", *endpoint)

	var (
		originated atomic.Int64
		failed     atomic.Int64
		wg         sync.WaitGroup
	)

	interval := time.Duration(float64(time.Second) / *rampPerSec)
	ticker := time.NewTicker(interval)

	defer ticker.Stop()

	start := time.Now()

	for i := 0; i < *n; i++ {
		select {
		case <-ctx.Done():
			slog.Warn("interrupted before all calls originated", "originated", originated.Load(), "target", *n)

			wg.Wait()

			return ctx.Err()
		case <-ticker.C:
		}

		wg.Add(1)

		go func(idx int) {
			defer wg.Done()

			if err := originateAndHold(ctx, cl, cfg.AriApp, *endpoint, *hold); err != nil {
				failed.Add(1)
				slog.Warn("call failed", "index", idx, "error", err)

				return
			}

			originated.Add(1)
		}(i)
	}

	wg.Wait()

	elapsed := time.Since(start)
	slog.Info("loadgen summary",
		"requested", *n,
		"succeeded", originated.Load(),
		"failed", failed.Load(),
		"elapsed", elapsed.Round(time.Millisecond),
	)

	return nil
}

// originateAndHold originates one channel, waits either for hold to elapse or the
// channel to end on its own (e.g. Manager failing to bridge it), then hangs it up.
func originateAndHold(ctx context.Context, cl ari.Client, app, endpoint string, hold time.Duration) error {
	sub := cl.Bus().Subscribe(nil, "StasisEnd")
	defer sub.Cancel()

	handle, err := cl.Channel().Originate(nil, ari.OriginateRequest{
		Endpoint: endpoint,
		App:      app,
		Timeout:  10,
	})
	if err != nil {
		return fmt.Errorf("originate: %w", err)
	}

	channelID := handle.Key().ID

	timer := time.NewTimer(hold)
	defer timer.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timer.C:
			_ = cl.Channel().Hangup(ari.NewKey(ari.ChannelKey, channelID), "")
			return nil
		case evt, ok := <-sub.Events():
			if !ok {
				return nil
			}

			if end, ok := evt.(*ari.StasisEnd); ok && end.Channel.ID == channelID {
				return nil // ended on its own before the hold elapsed
			}
		}
	}
}
