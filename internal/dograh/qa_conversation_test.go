package dograh

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/nitesh/vaani/internal/ai/agent"
)

func ev(typ, nodeID, nodeName, timestamp string, turn int, payload map[string]any) agent.LogEvent {
	return agent.LogEvent{Type: typ, NodeID: nodeID, NodeName: nodeName, Timestamp: timestamp, Turn: turn, Payload: payload}
}

func TestBuildConversationAndFormatTranscript(t *testing.T) {
	events := []agent.LogEvent{
		ev("rtf-node-transition", "n1", "Start", "2026-10-09T10:00:00.000000Z", 0, nil),
		ev("rtf-bot-text", "n1", "Start", "2026-10-09T10:00:01.500000Z", 0,
			map[string]any{"text": "नमस्ते", "timestamp": "2026-10-09T10:00:01.000Z"}),
		ev("rtf-user-transcription", "n1", "Start", "2026-10-09T10:00:05.000000Z", 1,
			map[string]any{"text": "जी", "final": true, "timestamp": "2026-10-09T10:00:04.500Z"}),
		ev("rtf-user-transcription", "n1", "Start", "2026-10-09T10:00:06.000000Z", 1,
			map[string]any{"text": "interim, not final", "final": false, "timestamp": "2026-10-09T10:00:05.900Z"}),
		ev("rtf-function-call-start", "n1", "Start", "2026-10-09T10:00:07.000000Z", 1,
			map[string]any{"function_name": "move_to_main"}),
	}

	convo := buildConversation(events)
	require.Len(t, convo, 3, "the interim (non-final) transcript is skipped")

	assert.Equal(t, transcriptEntry{TimeFromStart: 1, Speaker: "assistant", Text: "नमस्ते"}, convo[0],
		"timed by the payload timestamp, from the FIRST event's own (not payload) timestamp")
	assert.Equal(t, transcriptEntry{TimeFromStart: 4.5, Speaker: "user", Text: "जी"}, convo[1])
	assert.Equal(t, transcriptEntry{TimeFromStart: 7, Speaker: "tool_call", Text: "move_to_main"}, convo[2],
		"a tool call has no payload timestamp -> its own (event) timestamp")

	assert.Equal(t, "[1.0s] assistant: नमस्ते\n[4.5s] user: जी\n[7.0s] [tool_call]: move_to_main",
		formatTranscript(convo))
}

func TestSplitEventsByNode(t *testing.T) {
	events := []agent.LogEvent{
		ev("rtf-node-transition", "n1", "Start", "t", 0, nil),
		ev("rtf-bot-text", "n1", "Start", "t", 0, map[string]any{"text": "hi"}),
		ev("rtf-user-transcription", "n1", "Start", "t", 1, map[string]any{"text": "bye", "final": true}),
		ev("rtf-node-transition", "n2", "Main", "t", 1, nil), // transition-only: no conversation of its own
		ev("rtf-node-transition", "n3", "End", "t", 1, nil),
		ev("rtf-bot-text", "n3", "End", "t", 1, map[string]any{"text": "bye now"}),
	}

	splits, ok := splitEventsByNode(events)
	require.True(t, ok)
	require.Len(t, splits, 2, "n2 has no conversational content and is dropped")
	assert.Equal(t, "n1", splits[0].NodeID)
	assert.Equal(t, "End", splits[1].NodeName)
	assert.Len(t, splits[0].Events, 3)
	assert.Len(t, splits[1].Events, 2)

	// Any event missing a node_id -> fall back to one whole-call review.
	_, ok = splitEventsByNode(append(events, agent.LogEvent{Type: "rtf-bot-text"}))
	assert.False(t, ok)

	splits, ok = splitEventsByNode(nil)
	assert.True(t, ok)
	assert.Empty(t, splits)
}

func TestComputeMetrics(t *testing.T) {
	events := []agent.LogEvent{
		ev("rtf-bot-text", "n1", "Start", "t", 0, map[string]any{"text": "hi"}),
		ev("rtf-user-transcription", "n1", "Start", "t", 1, map[string]any{"text": "ok", "final": true}),
		ev("rtf-latency-measured", "n1", "Start", "t", 1, map[string]any{"latency_seconds": 0.9}),
		ev("rtf-latency-measured", "n1", "Start", "t", 1, map[string]any{"latency_seconds": 1.1}),
		ev("rtf-ttfb-metric", "n1", "Start", "t", 1, map[string]any{"ttfb_seconds": 0.3, "service": "llm"}),
		ev("rtf-bot-text", "n1", "Start", "t", 1, map[string]any{"text": "bye"}),
	}

	m := computeMetrics(events, nil)
	assert.Equal(t, 2, m.NumTurns, "distinct turn values among user/bot events")
	require.NotNil(t, m.AvgLatencySeconds)
	assert.InDelta(t, 1.0, *m.AvgLatencySeconds, 0.001)
	require.NotNil(t, m.MaxLatencySeconds)
	assert.InDelta(t, 1.1, *m.MaxLatencySeconds, 0.001)
	require.NotNil(t, m.AvgTTFBSeconds)
	assert.InDelta(t, 0.3, *m.AvgTTFBSeconds, 0.001)
	assert.Nil(t, m.CallDurationSeconds, "nil for a per-node call, as Dograh passes none there either")

	d := 42.0
	m = computeMetrics(nil, &d)
	assert.Equal(t, &d, m.CallDurationSeconds)
	assert.Zero(t, m.NumTurns)
	assert.Nil(t, m.AvgLatencySeconds)
	assert.Nil(t, m.AvgTTFBSeconds)
	assert.Nil(t, m.MaxLatencySeconds)
}
