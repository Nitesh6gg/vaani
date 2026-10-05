package agent

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCallLogMatchesDograhEvents(t *testing.T) {
	start := &Node{ID: "1", Name: "Start", AllowInterrupt: true}
	end := &Node{ID: "2", Name: "End Call", End: true}

	var l CallLog

	l.NodeEntered(start)
	l.AgentSaid("नमस्ते")
	l.UserSaid("हाँ")
	l.FunctionStarted("move_to_end", "call_1")
	l.NodeEntered(end)
	l.FunctionEnded("move_to_end", "call_1", `{"status":"done"}`)
	l.Ending(EndReasonEndNode)
	l.Ending(EndReasonUserHangup) // too late: the first reason stands
	l.NodeEntered(start)          // revisited: listed once

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
	nilLog.UserSaid("x") // a call without a log records nothing, safely
}
