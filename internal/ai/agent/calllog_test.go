package agent

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCallLogMatchesDograhEvents(t *testing.T) {
	start := &Node{ID: "1", Name: "Start", AllowInterrupt: true}
	end := &Node{ID: "2", Name: "End Call", End: true}

	var l CallLog

	// A few ms apart, so the events' own order is also their time order
	// (Summary sorts by time, as Dograh does).
	step := func() { time.Sleep(2 * time.Millisecond) }

	l.NodeEntered(start)
	step()
	l.AgentSaid("नमस्ते", time.Time{})
	step()
	l.UserSaid("हाँ", time.Time{})
	step()
	l.FunctionStarted("move_to_end", "call_1")
	step()
	l.NodeEntered(end)
	step()
	l.FunctionEnded("move_to_end", "call_1", `{"status":"done"}`)
	l.Ending(EndReasonEndNode)
	l.Ending(EndReasonUserHangup) // too late: the first reason stands
	step()
	l.NodeEntered(start) // revisited: listed once

	s := l.Summary()
	assert.Equal(t, EndReasonEndNode, s.EndReason)
	assert.True(t, s.UserSpoke)
	assert.Equal(t, []string{"Start", "End Call"}, s.NodesVisited)

	types := make([]string, len(s.Events))
	for i, e := range s.Events {
		types[i] = e.Type
	}

	assert.Equal(t, []string{"rtf-node-transition", "rtf-bot-text", "rtf-user-transcription",
		"rtf-function-call-start", "rtf-node-transition", "rtf-function-call-end", "rtf-node-transition"}, types)

	first := s.Events[0]
	assert.Equal(t, 0, first.Turn)
	assert.Equal(t, "1", first.NodeID, "a transition is tagged with the node it enters")
	assert.Nil(t, first.Payload["previous_node_id"])
	assert.Equal(t, true, first.Payload["allow_interrupt"])

	user := s.Events[2]
	assert.Equal(t, 1, user.Turn, "each caller turn starts a new turn")
	assert.Equal(t, true, user.Payload["final"])
	assert.Equal(t, "हाँ", user.Payload["text"])
	require.IsType(t, "", user.Payload["timestamp"])
	assert.Regexp(t, `^\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d\.\d{3}\+00:00$`, user.Payload["timestamp"])
	assert.Regexp(t, `^\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d\.\d{6}\+00:00$`, user.Timestamp)

	toEnd := s.Events[4]
	assert.Equal(t, "Start", toEnd.Payload["previous_node_name"])
	assert.Equal(t, "End Call", toEnd.NodeName)
	assert.Equal(t, `{"status":"done"}`, s.Events[5].Payload["result"])

	var nilLog *CallLog
	nilLog.UserSaid("x", time.Time{}) // a call without a log records nothing, safely
	nilLog.BargeIn()
	nilLog.Error("llm")
}

// TestCallLogCountsForTheSummary: as a Sink (fanned out next to metrics), a
// CallLog counts what call.summary reports.
func TestCallLogCountsForTheSummary(t *testing.T) {
	var l CallLog

	sink := Sinks(NoopSink{}, &l)

	sink.TurnStarted()
	sink.TurnStarted()
	sink.InterruptionPaused()
	sink.InterruptionPaused()
	sink.FalseInterruption()
	sink.BargeIn()
	sink.Error("llm")
	sink.Error("llm")
	sink.Error("tts_open")
	l.UserSaid("हाँ", time.Time{})
	l.ReplyLatency(850 * time.Millisecond)

	s := l.Summary()
	assert.Equal(t, 1, s.UserTurns)
	assert.Equal(t, 2, s.AgentReplies)
	assert.Equal(t, 2, s.Pauses)
	assert.Equal(t, 1, s.FalseInterruptions)
	assert.Equal(t, 1, s.BargeIns)
	assert.Equal(t, map[string]int{"llm": 2, "tts_open": 1}, s.Errors)
	assert.Equal(t, []int64{850}, s.ReplyLatenciesMS)
}

// TestCallLogTimesTurnsByTheirStart: as in Dograh (run 484, 2026-10-02), a
// transcript line carries when its turn started -- the caller's first word,
// the reply's start -- while the event itself is stamped when it was logged,
// and the stored events are ordered by turn start.
func TestCallLogTimesTurnsByTheirStart(t *testing.T) {
	var l CallLog

	now := time.Now()
	replyStarted := now.Add(-10 * time.Second) // a 10 s reply, cut by the caller
	callerStarted := now.Add(-3 * time.Second)

	l.UserSaid("बीच में बोला", callerStarted) // logged first, at the cut...
	l.AgentSaid("लंबा जवाब", replyStarted)    // ...the cut reply right after

	s := l.Summary()
	require.Len(t, s.Events, 2)

	agent, user := s.Events[0], s.Events[1]
	assert.Equal(t, "rtf-bot-text", agent.Type, "the reply started first, so it's shown first")
	assert.Equal(t, replyStarted.UTC().Format(payloadTimeFormat), agent.Payload["timestamp"])
	assert.Equal(t, callerStarted.UTC().Format(payloadTimeFormat), user.Payload["timestamp"])

	logged, err := time.Parse(time.RFC3339Nano, user.Timestamp)
	require.NoError(t, err)
	assert.WithinDuration(t, now, logged, time.Second, "the event itself is stamped when logged")
}
