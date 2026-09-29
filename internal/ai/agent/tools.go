package agent

import (
	"context"
	"encoding/json"
	"time"

	"github.com/nitesh/vaani/internal/ai/llm"
)

// maxToolRounds caps LLM -> tool -> LLM round trips within one turn, so a
// model stuck calling tools can't keep the caller waiting indefinitely.
const maxToolRounds = 5

// toolTimeout bounds one function tool's execution. Dograh uses the same 10s
// default for its function calls. Transfers have their own, longer timeout.
const toolTimeout = 10 * time.Second

// defaultTransferTimeout is how long a transfer rings before giving up when
// the tool doesn't set one -- Dograh's default.
const defaultTransferTimeout = 30 * time.Second

// ToolKind says what a tool does when the LLM calls it.
type ToolKind int

const (
	// ToolFunction runs Tool.Run and sends its result back to the LLM.
	ToolFunction ToolKind = iota
	// ToolEndCall ends the call once everything said so far has played; the
	// LLM is not asked again.
	ToolEndCall
	// ToolTransfer dials Tool.Destination and, if it answers, hands the
	// caller over to it (Config.Transfer). On success the agent leaves the
	// call; on failure the LLM is told why and carries on.
	ToolTransfer
)

// Tool is one function the agent's LLM may call during a turn.
type Tool struct {
	Def  llm.FunctionDef
	Kind ToolKind
	// Message is spoken before the tool acts -- Dograh's "customMessage"
	// (e.g. "please stay on the line" before a transfer). Empty means none.
	Message string

	// Run executes a ToolFunction with the LLM's JSON arguments and returns
	// the result, JSON text sent back to the LLM. An error becomes a
	// {"status":"error"} result -- the LLM hears about it rather than the
	// turn failing.
	Run func(ctx context.Context, args json.RawMessage) (string, error)

	// Destination is the Asterisk dial string for a ToolTransfer (e.g.
	// "SIP/7317185224@server225"); Timeout is how long it may ring (zero
	// means defaultTransferTimeout).
	Destination string
	Timeout     time.Duration
}

// toolErrorResult is the JSON result sent to the LLM when a tool fails.
func toolErrorResult(err error) string {
	b, _ := json.Marshal(map[string]string{"status": "error", "error": err.Error()})
	return string(b)
}
