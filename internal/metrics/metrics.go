// Package metrics registers and serves Vaani's Prometheus metrics on
// METRICS_ADDR (default :9091) at /metrics.
package metrics

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"net/http/pprof"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	dto "github.com/prometheus/client_model/go"

	"github.com/nitesh/vaani/internal/media"
)

var (
	CallsActive = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "vaani_calls_active",
		Help: "Number of calls currently bridged to the media plane.",
	})

	CallDurationSeconds = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "vaani_call_duration_seconds",
		Help:    "Duration of completed calls, from channel staging (answer) to teardown.",
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

	RTPPayloadSizeMismatch = promauto.NewCounter(prometheus.CounterOpts{
		Name: "vaani_rtp_payload_size_mismatch_total",
		Help: "Total inbound RTP packets whose payload was not the expected 640-byte (20ms) slin16 frame; sustained growth means Asterisk's packetization does not match this pipeline.",
	})

	MediaQueueDropped = promauto.NewCounter(prometheus.CounterOpts{
		Name: "vaani_media_queue_dropped_total",
		Help: "Total handler output frames dropped because the outbound queue was full (writer not keeping up).",
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

	JitterBufferReanchors = promauto.NewCounter(prometheus.CounterOpts{
		Name: "vaani_jb_reanchors_total",
		Help: "Total times the jitter buffer froze after sustained misses and re-anchored on a later packet.",
	})

	RTPOutOfWindow = promauto.NewCounter(prometheus.CounterOpts{
		Name: "vaani_rtp_out_of_window_total",
		Help: "Total inbound RTP packets dropped for landing outside the jitter buffer's window; sustained growth means packet loss the window could not absorb.",
	})

	RTPSilenceSent = promauto.NewCounter(prometheus.CounterOpts{
		Name: "vaani_rtp_silence_sent_total",
		Help: "Total outbound RTP packets that were zeroed silence because the handler's outbound queue was empty on that tick.",
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

	AudioSocketFramesIn = promauto.NewCounter(prometheus.CounterOpts{
		Name: "vaani_audiosocket_frames_in_total",
		Help: "Total valid inbound AudioSocket audio frames received.",
	})

	AudioSocketFramesOut = promauto.NewCounter(prometheus.CounterOpts{
		Name: "vaani_audiosocket_frames_out_total",
		Help: "Total outbound AudioSocket audio frames sent.",
	})

	AudioSocketBytesIn = promauto.NewCounter(prometheus.CounterOpts{
		Name: "vaani_audiosocket_bytes_in_total",
		Help: "Total inbound AudioSocket audio payload bytes received.",
	})

	AudioSocketSilenceSent = promauto.NewCounter(prometheus.CounterOpts{
		Name: "vaani_audiosocket_silence_sent_total",
		Help: "Total outbound AudioSocket frames that were zeroed silence because the handler's outbound queue was empty on that tick.",
	})

	AudioSocketSendErrors = promauto.NewCounter(prometheus.CounterOpts{
		Name: "vaani_audiosocket_send_errors_total",
		Help: "Total outbound AudioSocket writes that failed.",
	})

	AudioSocketMalformed = promauto.NewCounter(prometheus.CounterOpts{
		Name: "vaani_audiosocket_malformed_total",
		Help: "Total inbound AudioSocket audio frames with an unexpected size.",
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
func (Sink) PayloadSizeMismatch()   { RTPPayloadSizeMismatch.Inc() }
func (Sink) QueueDropped()          { MediaQueueDropped.Inc() }
func (Sink) SendError()             { RTPSendErrors.Inc() }
func (Sink) Late()                  { RTPLate.Inc() }
func (Sink) Duplicate()             { RTPDuplicates.Inc() }
func (Sink) SilenceInserted()       { RTPSilenceInserted.Inc() }
func (Sink) SSRCChange()            { RTPSSRCChanges.Inc() }
func (Sink) Reanchor()              { JitterBufferReanchors.Inc() }
func (Sink) OutOfWindow()           { RTPOutOfWindow.Inc() }
func (Sink) SilenceSent()           { RTPSilenceSent.Inc() }
func (Sink) WatchdogTimeout()       { MediaWatchdogTimeouts.Inc() }
func (Sink) PacerDrift(ms float64)  { PacerDriftMs.Observe(ms) }
func (Sink) AudioLevel(rms float64) { AudioRMS.Set(rms) }

// AudioSocketSink adapts the package-level AudioSocket counters to
// media.AudioSocketSink. Distinct counters from Sink's RTP ones -- AudioSocket
// carries no RTP packets at all, so vaani_rtp_* would be misleading here -- but
// it shares the transport-neutral watchdog/drift/RMS series with Sink, since
// those describe "is audio flowing on schedule," not anything RTP-specific.
type AudioSocketSink struct{}

func (AudioSocketSink) PacketIn(bytes int) {
	AudioSocketFramesIn.Inc()
	AudioSocketBytesIn.Add(float64(bytes))
}

func (AudioSocketSink) PacketOut()             { AudioSocketFramesOut.Inc() }
func (AudioSocketSink) SilenceSent()           { AudioSocketSilenceSent.Inc() }
func (AudioSocketSink) SendError()             { AudioSocketSendErrors.Inc() }
func (AudioSocketSink) Malformed()             { AudioSocketMalformed.Inc() }
func (AudioSocketSink) QueueDropped()          { MediaQueueDropped.Inc() }
func (AudioSocketSink) WatchdogTimeout()       { MediaWatchdogTimeouts.Inc() }
func (AudioSocketSink) PacerDrift(ms float64)  { PacerDriftMs.Observe(ms) }
func (AudioSocketSink) AudioLevel(rms float64) { AudioRMS.Set(rms) }

// debugAudioHandler streams a live call's inbound audio (raw s16le PCM,
// post-normalization -- the same bytes written to the WAV recorder) to an HTTP
// client in real time, e.g.:
//
//	curl -sN http://host:9091/debug/audio/<callID> | aplay -f S16_LE -r 16000 -c1 -
//
// Only reachable at all when DEBUG_AUDIO=1 (see Serve). 404s if callID has no
// active media plane; blocks, flushing each frame as it's published, until the
// call ends or the client disconnects.
func debugAudioHandler(w http.ResponseWriter, r *http.Request) {
	callID := r.PathValue("callID")

	tap := media.LookupAudioTap(callID)
	if tap == nil {
		http.Error(w, "no active call with that ID", http.StatusNotFound)
		return
	}

	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}

	frames, unsubscribe := tap.Subscribe()
	defer unsubscribe()

	w.Header().Set("Content-Type", "audio/l16;rate=16000;channels=1")
	w.WriteHeader(http.StatusOK)
	flusher.Flush()

	for {
		select {
		case <-r.Context().Done():
			return
		case frame, open := <-frames:
			if !open {
				return // call ended; tap was unregistered and closed the channel
			}

			if _, err := w.Write(frame); err != nil {
				return
			}

			flusher.Flush()
		}
	}
}

// healthResponse is /healthz's JSON body.
type healthResponse struct {
	Status        string  `json:"status"`
	AriConnected  bool    `json:"ari_connected"`
	ActiveCalls   int     `json:"active_calls"`
	UptimeSeconds float64 `json:"uptime_seconds"`
}

// activeCalls reads CallsActive's current value directly, rather than adding a
// second, separately-maintained counter -- the gauge is already authoritative
// (incremented/decremented right alongside every completeBridge/teardown).
func activeCalls() int {
	var m dto.Metric

	if err := CallsActive.Write(&m); err != nil {
		return 0
	}

	return int(m.GetGauge().GetValue())
}

// healthHandler reports process health at /healthz: 200 when Asterisk's ARI
// WebSocket is connected, 503 otherwise -- e.g. for a container orchestrator's
// readiness probe. ariConnected is a func, not a bool, because the ARI client
// doesn't exist yet when Serve starts (see Serve's doc comment) and because
// Connected() itself already reflects live reconnect state -- no separate flag
// to keep in sync.
func healthHandler(ariConnected func() bool, startedAt time.Time) http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		connected := ariConnected()

		resp := healthResponse{
			Status:        "ok",
			AriConnected:  connected,
			ActiveCalls:   activeCalls(),
			UptimeSeconds: time.Since(startedAt).Seconds(),
		}

		status := http.StatusOK
		if !connected {
			status = http.StatusServiceUnavailable
			resp.Status = "ari disconnected"
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_ = json.NewEncoder(w).Encode(resp)
	}
}

// Serve runs the /metrics HTTP server on addr until ctx is cancelled, then shuts it
// down gracefully. debugAudio mirrors config.Config.DebugAudio: only when true is
// the /debug/audio/{callID} live tap mounted at all, so it 404s outright rather
// than just being undocumented when the operator hasn't opted in. ariConnected
// backs /healthz -- see healthHandler.
func Serve(ctx context.Context, addr string, debugAudio bool, ariConnected func() bool) error {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
	mux.HandleFunc("/healthz", healthHandler(ariConnected, time.Now()))

	if debugAudio {
		mux.HandleFunc("/debug/audio/{callID}", debugAudioHandler)
	}

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
