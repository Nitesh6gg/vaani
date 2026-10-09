package agent

import (
	"sort"
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
	extracted map[string]any // nil until an extraction has returned
	keys      []string       // extracted's keys, in the order they first came

	// For the call.summary log line (CallLog is also a Sink).
	replies, bargeIns, pauses, falseInterruptions int
	acksHeld, acksDelivered                       int
	errors                                        map[string]int
	replyLatencies                                []int64
}

// CallSummary is everything a CallLog recorded.
type CallSummary struct {
	Events       []LogEvent
	NodesVisited []string
	EndReason    string // "" until something ended the call: then it's a caller hangup
	UserSpoke    bool   // the caller said anything that became a turn
	// Extracted is every variable extraction's result merged, later ones
	// winning (nil: none returned); ExtractedKeys its keys in arrival order.
	Extracted     map[string]any
	ExtractedKeys []string

	// UserTurns counts the caller's accepted transcripts and AgentReplies
	// the agent's turns; BargeIns confirmed interruptions, Pauses possible
	// ones, FalseInterruptions pauses that resumed; Errors failures by stage
	// (Sink.Error). ReplyLatenciesMS: per reply, caller's last sound (or the
	// STT's end of speech) to the reply's first audio.
	UserTurns, AgentReplies, BargeIns, Pauses, FalseInterruptions int
	// AcksHeld counts acknowledgements said over a paused reply (Config.
	// AckFilter); AcksDelivered those that turned out to be the caller's
	// answer (said during the reply's last sentence).
	AcksHeld, AcksDelivered int
	Errors                  map[string]int
	ReplyLatenciesMS        []int64
}

// Sink: a CallLog counts what the handler reports, for the call's summary.
func (l *CallLog) BargeIn()            { l.count(func() { l.bargeIns++ }) }
func (l *CallLog) InterruptionPaused() { l.count(func() { l.pauses++ }) }
func (l *CallLog) FalseInterruption()  { l.count(func() { l.falseInterruptions++ }) }
func (l *CallLog) TurnStarted()        { l.count(func() { l.replies++ }) }

// AckHeld / AckDelivered: see CallSummary.AcksHeld.
func (l *CallLog) AckHeld()      { l.count(func() { l.acksHeld++ }) }
func (l *CallLog) AckDelivered() { l.count(func() { l.acksDelivered++ }) }
func (l *CallLog) Error(stage string) {
	l.count(func() {
		if l.errors == nil {
			l.errors = map[string]int{}
		}

		l.errors[stage]++
	})
}

// ReplyLatency records one reply's wait as the caller experienced it.
func (l *CallLog) ReplyLatency(d time.Duration) {
	l.count(func() { l.replyLatencies = append(l.replyLatencies, d.Milliseconds()) })
}

func (l *CallLog) count(f func()) {
	if l == nil {
		return
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	f()
}

// Sinks fans each Sink event out to all of sinks (e.g. metrics and the
// call's CallLog).
func Sinks(sinks ...Sink) Sink { return multiSink(sinks) }

type multiSink []Sink

func (m multiSink) BargeIn() {
	for _, s := range m {
		s.BargeIn()
	}
}

func (m multiSink) InterruptionPaused() {
	for _, s := range m {
		s.InterruptionPaused()
	}
}

func (m multiSink) FalseInterruption() {
	for _, s := range m {
		s.FalseInterruption()
	}
}

func (m multiSink) TurnStarted() {
	for _, s := range m {
		s.TurnStarted()
	}
}

func (m multiSink) Error(stage string) {
	for _, s := range m {
		s.Error(stage)
	}
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

// UserSaid: a caller transcript became a turn ([User]). started is when the
// caller began saying it -- the time Dograh's transcript shows (pipecat's
// UserTurnStoppedMessage.timestamp, "when the user turn started"); the
// event's own timestamp stays when it was logged. Zero means now.
func (l *CallLog) UserSaid(text string, started time.Time) {
	if l == nil {
		return
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	l.turn++
	l.userSpoke = l.userSpoke || text != ""
	l.add(eventUserTranscription, map[string]any{"text": text, "final": true, "timestamp": turnTime(started)})
}

// AgentSaid: what the caller heard of one reply ([Agent]). started is when
// the reply began (pipecat's AssistantTurnStoppedMessage.timestamp: the LLM
// response's start), as Dograh's transcript shows it. Zero means now.
func (l *CallLog) AgentSaid(text string, started time.Time) {
	if l == nil {
		return
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	l.add(eventBotText, map[string]any{"text": text, "timestamp": turnTime(started)})
}

// turnTime formats a turn's start for its payload (now if unknown).
func turnTime(started time.Time) string {
	if started.IsZero() {
		started = time.Now()
	}

	return started.UTC().Format(payloadTimeFormat)
}

// sortKey is Dograh's _event_sort_key: the payload's (turn) timestamp, else
// the event's own. Compared as times at the millisecond -- the turn stamps'
// precision -- rather than as strings as Python does: within one millisecond
// Python's string order puts the millisecond stamps first, this keeps the
// order events were logged in.
func sortKey(e LogEvent) time.Time {
	ts, _ := e.Payload["timestamp"].(string)
	if ts == "" {
		ts = e.Timestamp
	}

	t, _ := time.Parse(time.RFC3339Nano, ts)

	return t.Truncate(time.Millisecond)
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

// Extracted merges one variable extraction's result (keys in the order the
// LLM wrote them), as Dograh updates gathered_context with it.
func (l *CallLog) Extracted(vars map[string]any, keys []string) {
	if l == nil {
		return
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	if l.extracted == nil {
		l.extracted = map[string]any{}
	}

	for _, k := range keys {
		if _, seen := l.extracted[k]; !seen {
			l.keys = append(l.keys, k)
		}

		l.extracted[k] = vars[k]
	}
}

// Summary returns a copy of everything recorded so far.
func (l *CallLog) Summary() CallSummary {
	l.mu.Lock()
	defer l.mu.Unlock()

	s := CallSummary{
		Events:        sortedEvents(l.events),
		NodesVisited:  append([]string(nil), l.visited...),
		EndReason:     l.reason,
		UserSpoke:     l.userSpoke,
		ExtractedKeys: append([]string(nil), l.keys...),

		UserTurns: l.turn, AgentReplies: l.replies, BargeIns: l.bargeIns, Pauses: l.pauses,
		FalseInterruptions: l.falseInterruptions,
		AcksHeld:           l.acksHeld,
		AcksDelivered:      l.acksDelivered,
		ReplyLatenciesMS:   append([]int64(nil), l.replyLatencies...),
	}

	if l.errors != nil {
		s.Errors = make(map[string]int, len(l.errors))
		for k, v := range l.errors {
			s.Errors[k] = v
		}
	}

	if l.extracted != nil {
		s.Extracted = make(map[string]any, len(l.extracted))
		for k, v := range l.extracted {
			s.Extracted[k] = v
		}
	}

	return s
}

// sortedEvents is Dograh's _sorted_events (what it stores, and what its
// transcript is built from): a stable sort by sortKey, so a turn sits where
// it started -- an interrupted reply before the caller who cut in -- and
// events sharing a time keep the order they were logged in.
func sortedEvents(events []LogEvent) []LogEvent {
	out := append([]LogEvent(nil), events...)
	sort.SliceStable(out, func(i, j int) bool { return sortKey(out[i]).Before(sortKey(out[j])) })

	return out
}
