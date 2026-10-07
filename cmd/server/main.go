// Command server is the Vaani monolith entry point: ARI client + RTP media plane.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"
	// The IANA time-zone database, embedded (~450 KB): Dograh's
	// {{current_time_<Zone>}} template variables need time.LoadLocation, which
	// otherwise fails -- and the variable silently renders empty -- on a host
	// or slim container image without /usr/share/zoneinfo.
	_ "time/tzdata"

	"github.com/CyCoreSystems/ari/v5"

	vaaniari "github.com/nitesh/vaani/internal/ari"
	"github.com/nitesh/vaani/internal/config"
	"github.com/nitesh/vaani/internal/dograh"
	"github.com/nitesh/vaani/internal/freeswitch"
	"github.com/nitesh/vaani/internal/media"
	"github.com/nitesh/vaani/internal/metrics"
)

// shutdownDrainDeadline bounds how long main waits for Manager.Run to finish
// draining calls after SIGTERM. Teardown's ARI REST calls have no deadline of
// their own (the native client's HTTP client is shared), so an Asterisk that
// stopped answering would otherwise hang process exit indefinitely.
const shutdownDrainDeadline = 10 * time.Second

func main() {
	config.ConfigureLogging()

	if err := run(); err != nil {
		slog.Error("fatal", "error", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := config.Load()
	if err != nil {
		return err
	}
	config.Startup(cfg)

	media.WarnIfUnset()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if cfg.Telephony == "freeswitch" {
		return runFreeSWITCH(ctx, cfg)
	}

	// clPtr is nil until ari.Connect below succeeds, and Serve's /healthz route
	// needs to report "not connected" honestly during that startup/retry window
	// rather than either blocking or panicking -- hence a pointer, checked for
	// nil, instead of just capturing cl (which doesn't exist yet) in the closure.
	var clPtr atomic.Pointer[ari.Client]

	ariConnected := func() bool {
		p := clPtr.Load()
		return p != nil && (*p).Connected()
	}

	go func() {
		if err := metrics.Serve(ctx, cfg.MetricsAddr, cfg.DebugAudio, ariConnected); err != nil {
			slog.Error("metrics server error", "error", err)
		}
	}()

	slog.Info("connecting to ARI", "url", config.SafeURL(cfg.AriURL), "app", cfg.AriApp)

	cl, err := vaaniari.Connect(ctx, cfg)
	if err != nil {
		return err
	}
	defer cl.Close()

	clPtr.Store(&cl)

	slog.Info("connected to ARI", "event", "ari.connected")

	// Dograh's database holds the workflow the agent runs. Failing to reach it
	// is fatal at startup rather than failing every call.
	var store *dograh.Store

	if cfg.DograhDBURL != "" {
		pingCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		store, err = dograh.Open(pingCtx, cfg.DograhDBURL)
		cancel()

		if err != nil {
			return err
		}
		defer store.Close()

		slog.Info("connected to dograh database", "workflow_id", cfg.DograhWorkflowID, "event", "db.connected")
	}

	// Dograh's MinIO, for call recordings and transcripts; nil = no uploads.
	var storage *dograh.Storage
	if cfg.MinioEndpoint != "" {
		storage = &dograh.Storage{Endpoint: cfg.MinioEndpoint, AccessKey: cfg.MinioAccessKey,
			SecretKey: cfg.MinioSecretKey, Bucket: cfg.MinioBucket, Secure: cfg.MinioSecure}
	}

	ports := media.NewPortAllocator(cfg.MediaPortBase, cfg.MediaPortCount)
	mgr := vaaniari.NewManager(cl, cfg, ports, store, storage)

	slog.Info("vaani running", "metrics_addr", cfg.MetricsAddr, "media_ip", cfg.MediaIP, "app_mode", cfg.AppMode,
		"event", "service.started")

	runDone := make(chan struct{})

	go func() {
		defer close(runDone)
		mgr.Run(ctx) // blocks until ctx is cancelled (SIGTERM/SIGINT), then drains calls
	}()

	select {
	case <-runDone:
		if ctx.Err() == nil {
			// Run gave up without a shutdown signal: the event bus closed for
			// good (the native client reconnects the WebSocket itself, so this
			// shouldn't happen short of client.Close()).
			return fmt.Errorf("ari event bus closed unexpectedly")
		}

		slog.Info("shutdown complete")

		return nil
	case <-ctx.Done():
		// SIGTERM/SIGINT: Run is now draining calls; bound that drain.
		select {
		case <-runDone:
			slog.Info("shutdown complete")
		case <-time.After(shutdownDrainDeadline):
			slog.Warn("graceful shutdown drain exceeded deadline; exiting",
				"deadline", shutdownDrainDeadline)
		}

		return nil
	}
}

// runFreeSWITCH serves TELEPHONY=freeswitch (docs/FREESWITCH.md): calls arrive
// as mod_earshot WebSockets, control goes over the Event Socket.
func runFreeSWITCH(ctx context.Context, cfg config.Config) error {
	mgr := freeswitch.New(cfg)

	go func() {
		if err := metrics.Serve(ctx, cfg.MetricsAddr, cfg.DebugAudio, mgr.Connected); err != nil {
			slog.Error("metrics server error", "error", err)
		}
	}()

	slog.Info("vaani running", "metrics_addr", cfg.MetricsAddr, "telephony", cfg.Telephony, "app_mode", cfg.AppMode,
		"event", "service.started")

	if err := mgr.Run(ctx); err != nil { // blocks until SIGTERM/SIGINT, then drains calls
		return err
	}

	slog.Info("shutdown complete")

	return nil
}
