package dograh

import (
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/nitesh/vaani/internal/ai/agent"
)

// transcriptEntry is one line of a QA transcript: Dograh's conversation
// structure (qa/conversation.py's build_conversation_structure).
type transcriptEntry struct {
	TimeFromStart float64 // seconds since the call's first event, rounded to 2 decimals
	Speaker       string  // "user", "assistant", "tool_call"
	Text          string
}

// buildConversation is Dograh's build_conversation_structure: only final
// user transcripts, agent replies, and started tool calls become lines,
// timed from the call's very first event's own timestamp (not its payload
// timestamp -- see agent.EventTime for why those can differ).
func buildConversation(events []agent.LogEvent) []transcriptEntry {
	if len(events) == 0 {
		return nil
	}

	start, err := time.Parse(time.RFC3339Nano, events[0].Timestamp)
	if err != nil {
		start = time.Time{}
	}

	var out []transcriptEntry

	for _, e := range events {
		var speaker, text string

		switch {
		case e.Type == "rtf-bot-text":
			speaker, text = "assistant", strVal(e.Payload["text"])
		case e.Type == "rtf-user-transcription" && e.Payload["final"] == true:
			speaker, text = "user", strVal(e.Payload["text"])
		case e.Type == "rtf-function-call-start":
			speaker = "tool_call"

			text = strVal(e.Payload["function_name"])
			if text == "" {
				text = "unknown"
			}
		default:
			continue
		}

		t := agent.EventTime(e)
		if t.IsZero() {
			t = start
		}

		out = append(out, transcriptEntry{
			TimeFromStart: math.Round(t.Sub(start).Seconds()*100) / 100,
			Speaker:       speaker,
			Text:          text,
		})
	}

	return out
}

// formatTranscript is Dograh's format_transcript: "[12.3s] speaker: text"
// lines, a tool call as "[12.3s] [tool_call]: name".
func formatTranscript(entries []transcriptEntry) string {
	lines := make([]string, len(entries))

	for i, e := range entries {
		if e.Speaker == "tool_call" {
			lines[i] = fmt.Sprintf("[%.1fs] [tool_call]: %s", e.TimeFromStart, e.Text)
		} else {
			lines[i] = fmt.Sprintf("[%.1fs] %s: %s", e.TimeFromStart, e.Speaker, e.Text)
		}
	}

	return strings.Join(lines, "\n")
}

func strVal(v any) string {
	s, _ := v.(string)
	return s
}

// nodeSplit is one node's slice of a call's events, in the order the call
// visited nodes.
type nodeSplit struct {
	NodeID   string
	NodeName string
	Events   []agent.LogEvent
}

// splitEventsByNode is Dograh's split_events_by_node: events grouped by
// node_id, first-occurrence order, keeping only nodes with conversational
// content (a bot reply or a user transcript -- final or not, as Dograh
// checks only the event type here, not payload.final). ok is false if any
// event lacks a node_id -- the caller falls back to one whole-call review
// (shouldn't happen for Vaani calls: every event is tagged from the first,
// see agent.CallLog.NodeEntered).
func splitEventsByNode(events []agent.LogEvent) (splits []nodeSplit, ok bool) {
	index := map[string]int{}

	for _, e := range events {
		if e.NodeID == "" {
			return nil, false
		}

		i, seen := index[e.NodeID]
		if !seen {
			i = len(splits)
			index[e.NodeID] = i

			splits = append(splits, nodeSplit{NodeID: e.NodeID, NodeName: e.NodeName})
		}

		splits[i].Events = append(splits[i].Events, e)
	}

	out := splits[:0]

	for _, sp := range splits {
		if hasConversation(sp.Events) {
			out = append(out, sp)
		}
	}

	return out, true
}

func hasConversation(events []agent.LogEvent) bool {
	for _, e := range events {
		if e.Type == "rtf-bot-text" || e.Type == "rtf-user-transcription" {
			return true
		}
	}

	return false
}

// qaMetrics is Dograh's compute_call_metrics result, serialized verbatim
// into a QA prompt's {{metrics}} placeholder.
type qaMetrics struct {
	CallDurationSeconds *float64 `json:"call_duration_seconds"`
	NumTurns            int      `json:"num_turns"`
	AvgLatencySeconds   *float64 `json:"avg_latency_seconds"`
	AvgTTFBSeconds      *float64 `json:"avg_ttfb_seconds"`
	MaxLatencySeconds   *float64 `json:"max_latency_seconds"`
}

// computeMetrics is Dograh's compute_call_metrics (qa/metrics.py):
// pre-computed numbers alongside the transcript, from rtf-latency-measured
// and rtf-ttfb-metric events (see CallLog.ReplyLatency/TTFBMeasured) plus
// the distinct turns seen. callDuration is nil for a per-node call (Dograh
// passes none there either); the whole-call fallback passes the call's
// total duration.
func computeMetrics(events []agent.LogEvent, callDuration *float64) qaMetrics {
	var latencies, ttfbs []float64

	turns := map[int]bool{}

	for _, e := range events {
		switch e.Type {
		case "rtf-latency-measured":
			if v, ok := e.Payload["latency_seconds"].(float64); ok {
				latencies = append(latencies, v)
			}
		case "rtf-ttfb-metric":
			if v, ok := e.Payload["ttfb_seconds"].(float64); ok {
				ttfbs = append(ttfbs, v)
			}
		}

		if e.Type == "rtf-user-transcription" || e.Type == "rtf-bot-text" {
			turns[e.Turn] = true
		}
	}

	return qaMetrics{
		CallDurationSeconds: callDuration,
		NumTurns:            len(turns),
		AvgLatencySeconds:   avgRounded(latencies),
		AvgTTFBSeconds:      avgRounded(ttfbs),
		MaxLatencySeconds:   maxRounded(latencies),
	}
}

func round2(v float64) float64 { return math.Round(v*100) / 100 }

func avgRounded(vals []float64) *float64 {
	if len(vals) == 0 {
		return nil
	}

	var sum float64
	for _, v := range vals {
		sum += v
	}

	r := round2(sum / float64(len(vals)))

	return &r
}

func maxRounded(vals []float64) *float64 {
	if len(vals) == 0 {
		return nil
	}

	m := vals[0]
	for _, v := range vals[1:] {
		if v > m {
			m = v
		}
	}

	r := round2(m)

	return &r
}
