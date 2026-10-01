package metrics

import (
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

	AgentTurnsTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "vaani_agent_turns_total",
		Help: "Total agent turns started: one per accepted caller transcript, plus the LLM opening a call and caller-silence prompts.",
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

func (AgentSink) BargeIn()            { AgentBargeInTotal.Inc() }
func (AgentSink) InterruptionPaused() { AgentInterruptionPausesTotal.Inc() }
func (AgentSink) FalseInterruption()  { AgentFalseInterruptionsTotal.Inc() }
func (AgentSink) TurnStarted()        { AgentTurnsTotal.Inc() }
func (AgentSink) Error(stage string)  { AgentErrorsTotal.WithLabelValues(stage).Inc() }

var _ agent.Sink = AgentSink{}
