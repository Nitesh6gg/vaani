package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/nitesh/vaani/internal/ai/agent"
)

var (
	AgentBargeInTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "vaani_agent_bargein_total",
		Help: "Total times the caller interrupted the agent's TTS playback (all five cuts executed).",
	})

	AgentPrerollFlushedFramesTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "vaani_agent_preroll_flushed_frames_total",
		Help: "Total pre-roll frames flushed to STT on barge-in (the SPEAKING-state audio STT never saw live).",
	})

	AgentTurnsTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "vaani_agent_turns_total",
		Help: "Total agent turns started (one per accepted final transcript).",
	})

	AgentErrorsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "vaani_agent_errors_total",
		Help: "Total agent-pipeline errors by stage (stt, llm, tts_open, tts_speak, tts_cancel, tts_outbound_full).",
	}, []string{"stage"})
)

// AgentSink adapts the package-level agent counters to agent.Sink, keeping
// internal/ai/agent free of a prometheus dependency (same pattern as Sink and
// AudioSocketSink for the media package).
type AgentSink struct{}

func (AgentSink) BargeIn()     { AgentBargeInTotal.Inc() }
func (AgentSink) TurnStarted() { AgentTurnsTotal.Inc() }
func (AgentSink) PrerollFlushed(frames int) {
	AgentPrerollFlushedFramesTotal.Add(float64(frames))
}
func (AgentSink) Error(stage string) { AgentErrorsTotal.WithLabelValues(stage).Inc() }

var _ agent.Sink = AgentSink{}
