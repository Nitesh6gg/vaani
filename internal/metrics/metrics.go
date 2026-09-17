// Package metrics registers and serves Vaani's Prometheus metrics on
// METRICS_ADDR (default :9091) at /metrics.
package metrics

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	CallsActive = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "vaani_calls_active",
		Help: "Number of calls currently bridged to the media plane.",
	})

	CallDurationSeconds = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "vaani_call_duration_seconds",
		Help:    "Duration of completed calls, from bridge to teardown.",
		Buckets: prometheus.ExponentialBuckets(1, 2, 12), // 1s .. ~68m
	})

	RTPPacketsIn = promauto.NewCounter(prometheus.CounterOpts{
		Name: "vaani_rtp_packets_in_total",
		Help: "Total valid inbound RTP packets received.",
	})

	RTPPacketsOut = promauto.NewCounter(prometheus.CounterOpts{
		Name: "vaani_rtp_packets_out_total",
		Help: "Total outbound RTP packets sent.",
	})

	RTPBytesIn = promauto.NewCounter(prometheus.CounterOpts{
		Name: "vaani_rtp_bytes_in_total",
		Help: "Total inbound RTP payload bytes received.",
	})

	RTPSeqGaps = promauto.NewCounter(prometheus.CounterOpts{
		Name: "vaani_rtp_seq_gaps_total",
		Help: "Total inbound RTP sequence discontinuities detected.",
	})

	RTPMalformed = promauto.NewCounter(prometheus.CounterOpts{
		Name: "vaani_rtp_malformed_total",
		Help: "Total inbound packets that failed RTP validation.",
	})

	MediaPortsInUse = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "vaani_media_ports_in_use",
		Help: "Number of UDP media ports currently allocated.",
	})

	ARIReconnects = promauto.NewCounter(prometheus.CounterOpts{
		Name: "vaani_ari_reconnects_total",
		Help: "Total number of times the ARI WebSocket had to reconnect.",
	})
)

// Sink adapts the package-level RTP counters to the media.Sink interface, keeping
// internal/media free of a prometheus dependency.
type Sink struct{}

func (Sink) PacketIn(bytes int) {
	RTPPacketsIn.Inc()
	RTPBytesIn.Add(float64(bytes))
}

func (Sink) PacketOut() { RTPPacketsOut.Inc() }
func (Sink) SeqGap()    { RTPSeqGaps.Inc() }
func (Sink) Malformed() { RTPMalformed.Inc() }

// Serve runs the /metrics HTTP server on addr until ctx is cancelled, then shuts it
// down gracefully.
func Serve(ctx context.Context, addr string) error {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())

	srv := &http.Server{
		Addr:              addr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	errCh := make(chan error, 1)

	go func() {
		errCh <- srv.ListenAndServe()
	}()

	select {
	case err := <-errCh:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}

		return err
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		if err := srv.Shutdown(shutdownCtx); err != nil {
			slog.Warn("metrics server shutdown error", "error", err)
		}

		return nil
	}
}
