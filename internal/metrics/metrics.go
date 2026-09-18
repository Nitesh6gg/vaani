// Package metrics registers and serves Vaani's Prometheus metrics on
// METRICS_ADDR (default :9091) at /metrics.
package metrics

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"net/http/pprof"
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

	RTPSendErrors = promauto.NewCounter(prometheus.CounterOpts{
		Name: "vaani_rtp_send_errors_total",
		Help: "Total outbound RTP writes that failed (excludes swallowed ICMP unreachable).",
	})

	RTPLate = promauto.NewCounter(prometheus.CounterOpts{
		Name: "vaani_rtp_late_total",
		Help: "Total inbound RTP packets discarded by the jitter buffer as arriving too late.",
	})

	RTPDuplicates = promauto.NewCounter(prometheus.CounterOpts{
		Name: "vaani_rtp_duplicates_total",
		Help: "Total inbound RTP packets discarded by the jitter buffer as duplicates.",
	})

	RTPSilenceInserted = promauto.NewCounter(prometheus.CounterOpts{
		Name: "vaani_rtp_silence_inserted_total",
		Help: "Total frames where the jitter buffer released concealment silence instead of real audio.",
	})

	RTPSSRCChanges = promauto.NewCounter(prometheus.CounterOpts{
		Name: "vaani_rtp_ssrc_changes_total",
		Help: "Total mid-call SSRC changes observed on the inbound RTP stream.",
	})

	MediaWatchdogTimeouts = promauto.NewCounter(prometheus.CounterOpts{
		Name: "vaani_media_watchdog_timeouts_total",
		Help: "Total times the media watchdog logged a call with no inbound packets for 5s.",
	})

	PacerDriftMs = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "vaani_pacer_drift_ms",
		Help:    "Absolute difference between the writer loop's actual tick interval and the nominal 20ms.",
		Buckets: []float64{0.5, 1, 2, 3, 5, 8, 13, 21},
	})

	AudioRMS = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "vaani_audio_rms",
		Help: "RMS level of the last released inbound audio frame (not VAD; proves audio is flowing).",
	})
)

// Sink adapts the package-level RTP counters to the media.Sink interface, keeping
// internal/media free of a prometheus dependency.
type Sink struct{}

func (Sink) PacketIn(bytes int) {
	RTPPacketsIn.Inc()
	RTPBytesIn.Add(float64(bytes))
}

func (Sink) PacketOut()             { RTPPacketsOut.Inc() }
func (Sink) SeqGap()                { RTPSeqGaps.Inc() }
func (Sink) Malformed()             { RTPMalformed.Inc() }
func (Sink) SendError()             { RTPSendErrors.Inc() }
func (Sink) Late()                  { RTPLate.Inc() }
func (Sink) Duplicate()             { RTPDuplicates.Inc() }
func (Sink) SilenceInserted()       { RTPSilenceInserted.Inc() }
func (Sink) SSRCChange()            { RTPSSRCChanges.Inc() }
func (Sink) WatchdogTimeout()       { MediaWatchdogTimeouts.Inc() }
func (Sink) PacerDrift(ms float64)  { PacerDriftMs.Observe(ms) }
func (Sink) AudioLevel(rms float64) { AudioRMS.Set(rms) }

// Serve runs the /metrics HTTP server on addr until ctx is cancelled, then shuts it
// down gracefully.
func Serve(ctx context.Context, addr string) error {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)

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
