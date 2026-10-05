package agent

import (
	"sync"
	"time"
)

// Dograh's end-of-call reasons (pipecat EndTaskReason) Vaani can produce:
// what ended the call, saved as the run's call_disposition. The first one
// reported wins, as in Dograh's end_call_with_reason.
const (
	EndReasonEndNode      = "user_qualified"                  // the End node's closing line played
	EndReasonEndCallTool  = "end_call_tool"                   // the LLM called end_call
	EndReasonTransfer     = "transfer_call"                   // handed over to a transfer destination
	EndReasonCallerSilent = "user_idle_max_duration_exceeded" // caller silent twice
	EndReasonMaxDuration  = "call_duration_exceeded"          // max_call_duration reached
	EndReasonUserHangup   = "user_hangup"                     // none of the above: the caller hung up
)

// Dograh's realtime feedback event types (pipecat RealtimeFeedbackType), as
// its run page reads them from workflow_runs.logs.
const (
	eventUserTranscription = "rtf-user-transcription"
	eventBotText           = "rtf-bot-text"
	eventNodeTransition    = "rtf-node-transition"
	eventFunctionCallStart = "rtf-function-call-start"
	eventFunctionCallEnd   = "rtf-function-call-end"
)

// LogEvent is one entry of Dograh's realtime_feedback_events, shaped as its
// InMemoryLogsBuffer stores them (api/services/pipecat/in_memory_buffers.py).
type LogEvent struct {
	Type      string         `json:"type"`
	Payload   map[string]any `json:"payload"`
	Timestamp string         `json:"timestamp"`
	Turn      int            `json:"turn"`
	NodeID    string         `json:"node_id,omitempty"`
	NodeName  string         `json:"node_name,omitempty"`
}

// CallLog records a call the way Dograh's call history shows it: the
// conversation, node transitions and function calls as events, the nodes
// visited, and why the call ended. Safe for concurrent use; a nil *CallLog
// records nothing.
type CallLog struct {
	mu        sync.Mutex
	events    []LogEvent
	turn      int // Dograh's turn counter: +1 on each caller turn
	node      *Node
	visited   []string
	reason    string
	userSpoke bool
}

// CallSummary is everything a CallLog recorded.
type CallSummary struct {
	Events       []LogEvent
	NodesVisited []string
	EndReason    string // "" until something ended the call: then it's a caller hangup
	UserSpoke    bool   // the caller said anything that became a turn
}

// Timestamp formats as Python's isoformat() on a UTC datetime, which is
// what Dograh stores: the event's own with microseconds, a transcript's
// (pipecat's time_now_iso8601) with milliseconds.
const (
	eventTimeFormat   = "2006-01-02T15:04:05.000000-07:00"
	payloadTimeFormat = "2006-01-02T15:04:05.000-07:00"
)

// add appends an event tagged with the current turn and node. mu held.
func (l *CallLog) add(typ string, payload map[string]any) {
	e := LogEvent{Type: typ, Payload: payload, Timestamp: time.Now().UTC().Format(eventTimeFormat), Turn: l.turn}
	if l.node != nil {
		e.NodeID, e.NodeName = l.node.ID, l.node.Name
	}

	l.events = append(l.events, e)
}

// NodeEntered: the conversation moved to n (or started there).
func (l *CallLog) NodeEntered(n *Node) {
	if l == nil {
		return
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	payload := map[string]any{"node_id": n.ID, "node_name": n.Name,
		"previous_node_id": nil, "previous_node_name": nil, "allow_interrupt": n.AllowInterrupt}
	if l.node != nil {
		payload["previous_node_id"], payload["previous_node_name"] = l.node.ID, l.node.Name
	}

	l.node = n
	l.add(eventNodeTransition, payload)

	for _, v := range l.visited {
		if v == n.Name {
			return
		}
	}

	l.visited = append(l.visited, n.Name)
}

// UserSaid: a caller transcript became a turn ([User]).
func (l *CallLog) UserSaid(text string) {
	if l == nil {
		return
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	l.turn++
	l.userSpoke = l.userSpoke || text != ""
	l.add(eventUserTranscription, map[string]any{"text": text, "final": true,
		"timestamp": time.Now().UTC().Format(payloadTimeFormat)})
}

// AgentSaid: what the caller heard of one reply ([Agent]).
func (l *CallLog) AgentSaid(text string) {
	if l == nil {
		return
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	l.add(eventBotText, map[string]any{"text": text, "timestamp": time.Now().UTC().Format(payloadTimeFormat)})
}

// FunctionStarted / FunctionEnded: the LLM called a tool or took an edge
// (Dograh registers both as functions, so both are logged).
func (l *CallLog) FunctionStarted(name, callID string) {
	if l == nil {
		return
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	l.add(eventFunctionCallStart, map[string]any{"function_name": name, "tool_call_id": callID})
}

func (l *CallLog) FunctionEnded(name, callID, result string) {
	if l == nil {
		return
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	var r any // Dograh: None for an empty result
	if result != "" {
		r = result
	}

	l.add(eventFunctionCallEnd, map[string]any{"function_name": name, "tool_call_id": callID, "result": r})
}

// Ending records why the call is ending; only the first reason counts.
func (l *CallLog) Ending(reason string) {
	if l == nil {
		return
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	if l.reason == "" {
		l.reason = reason
	}
}

// Summary returns a copy of everything recorded so far.
func (l *CallLog) Summary() CallSummary {
	l.mu.Lock()
	defer l.mu.Unlock()

	return CallSummary{
		Events:       append([]LogEvent(nil), l.events...),
		NodesVisited: append([]string(nil), l.visited...),
		EndReason:    l.reason,
		UserSpoke:    l.userSpoke,
	}
}
