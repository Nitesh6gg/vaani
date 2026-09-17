// Command server is the Vaani monolith entry point: ARI client + RTP media plane.
package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/nitesh/vaani/internal/ari"
	"github.com/nitesh/vaani/internal/config"
	"github.com/nitesh/vaani/internal/media"
	"github.com/nitesh/vaani/internal/metrics"
)

func main() {
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

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	go func() {
		if err := metrics.Serve(ctx, cfg.MetricsAddr); err != nil {
			slog.Error("metrics server error", "error", err)
		}
	}()

	slog.Info("connecting to ARI", "url", cfg.AriURL, "app", cfg.AriApp)

	cl, err := ari.Connect(ctx, cfg)
	if err != nil {
		return err
	}
	defer cl.Close()

	slog.Info("connected to ARI")

	ports := media.NewPortAllocator(cfg.MediaPortBase, cfg.MediaPortCount)
	mgr := ari.NewManager(cl, cfg, ports)

	slog.Info("vaani running", "metrics_addr", cfg.MetricsAddr, "media_ip", cfg.MediaIP)
	mgr.Run(ctx) // blocks until ctx is cancelled (SIGTERM/SIGINT), then drains calls

	slog.Info("shutdown complete")

	return nil
}
