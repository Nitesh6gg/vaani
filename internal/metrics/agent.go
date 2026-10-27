package metrics

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/nitesh/vaani/internal/ai/agent"
)

var (
	AgentBargeInTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "vaani_agent_bargein_total",
		Help: "Total confirmed interruptions: a transcript confirmed a paused reply, and all five cuts executed.",
	})

	AgentInterruptionPausesTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "vaani_agent_interruption_pauses_total",
		Help: "Total times a possible caller interruption paused the agent's reply (confirmed or not).",
	})

	AgentFalseInterruptionsTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "vaani_agent_false_interruptions_total",
		Help: "Total paused replies that resumed because no transcript confirmed the interruption (noise, echo).",
	})

	RecordingTruncated = promauto.NewCounter(prometheus.CounterOpts{
		Name: "vaani_recording_truncated_total",
		Help: "Calls whose recording hit its size cap (100MB, ~54 minutes) and is missing the rest of the call.",
	})

	STTAudioSecondsTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "vaani_stt_audio_seconds_total",
		Help: "Seconds of caller audio sent to the STT -- what Sarvam bills. The call's length, or with STT_SPEECH_ONLY only the speech (plus pre-roll, hangover and keepalives). Added when a call ends.",
	})

	AgentTurnsTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "vaani_agent_turns_total",
		Help: "Total agent turns started: one per accepted caller transcript, plus the LLM opening a call and caller-silence prompts.",
	})

	AgentErrorsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "vaani_agent_errors_total",
		Help: "Total agent-pipeline errors by stage (stt, llm, tts_open, tts_speak, tts_cancel, tts_outbound_full).",
	}, []string{"stage"})

	LLMRetriesTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "vaani_llm_retries_total",
		Help: "LLM requests that needed retries, by how they ended (recovered, exhausted).",
	}, []string{"result"})

	LLMFallbackTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "vaani_llm_fallback_total",
		Help: "Turns the LLM failed that Vaani covered by speaking a fixed line: apology (asks the caller to repeat) or hangup (goodbye, call ended).",
	}, []string{"kind"})

	// ttfbBuckets: 50ms..~25.6s, covering a fast TTS first-chunk through a
	// slow LLM first-token on a cold connection.
	ttfbBuckets = prometheus.ExponentialBuckets(50, 2, 10)

	LLMTTFTMs = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "vaani_llm_ttft_ms",
		Help:    "Time from transcript accepted to the first LLM token, in milliseconds.",
		Buckets: ttfbBuckets,
	})

	TTSTTFAMs = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "vaani_tts_ttfa_ms",
		Help:    "Time from transcript accepted to the first TTS audio chunk, in milliseconds.",
		Buckets: ttfbBuckets,
	})

	STTEndpointMs = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "vaani_stt_endpoint_ms",
		Help:    "Time from Sarvam's END_SPEECH to its final transcript, in milliseconds.",
		Buckets: ttfbBuckets,
	})

	ToolCallsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "vaani_tool_calls_total",
		Help: "Total tool runs (runTool's t.Run branch) by tool name and outcome (ok, error).",
	}, []string{"tool", "outcome"})

	ToolCallDurationMs = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "vaani_tool_call_duration_ms",
		Help:    "Tool run duration by tool name, in milliseconds.",
		Buckets: prometheus.ExponentialBuckets(10, 2, 12), // 10ms..~20.5s
	}, []string{"tool"})
)

func init() {
	// Created at 0 so panels show 0 rather than "No data", and the first
	// increment of a series is a visible change.
	for _, stage := range []string{"stt", "llm", "tts_open", "tts_speak", "tts_cancel", "tts_outbound_full", "tts_closed", "tts_drain_timeout"} {
		AgentErrorsTotal.WithLabelValues(stage)
	}

	for _, r := range []string{"recovered", "exhausted"} {
		LLMRetriesTotal.WithLabelValues(r)
	}

	for _, k := range []string{"apology", "hangup"} {
		LLMFallbackTotal.WithLabelValues(k)
	}
}

// LLMRetry is llm.Client.OnRetry.
func LLMRetry(result string) { LLMRetriesTotal.WithLabelValues(result).Inc() }

// AgentSink adapts the package-level agent counters to agent.Sink, keeping
// internal/ai/agent free of a prometheus dependency (same pattern as Sink and
// AudioSocketSink for the media package).
type AgentSink struct{}

func (AgentSink) BargeIn()            { AgentBargeInTotal.Inc() }
func (AgentSink) InterruptionPaused() { AgentInterruptionPausesTotal.Inc() }
func (AgentSink) FalseInterruption()  { AgentFalseInterruptionsTotal.Inc() }
func (AgentSink) TurnStarted()        { AgentTurnsTotal.Inc() }
func (AgentSink) Error(stage string)  { AgentErrorsTotal.WithLabelValues(stage).Inc() }
func (AgentSink) LLMFallback(kind string) { LLMFallbackTotal.WithLabelValues(kind).Inc() }

func (AgentSink) TTFB(service string, d time.Duration) {
	ms := float64(d.Milliseconds())

	switch service {
	case "llm":
		LLMTTFTMs.Observe(ms)
	case "tts":
		TTSTTFAMs.Observe(ms)
	case "stt":
		STTEndpointMs.Observe(ms)
	}
}

func (AgentSink) ToolCall(tool string, ok bool, d time.Duration) {
	outcome := "ok"
	if !ok {
		outcome = "error"
	}

	ToolCallsTotal.WithLabelValues(tool, outcome).Inc()
	ToolCallDurationMs.WithLabelValues(tool).Observe(float64(d.Milliseconds()))
}

var _ agent.Sink = AgentSink{}
