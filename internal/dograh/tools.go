package dograh

import (
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/nitesh/vaani/internal/ai/agent"
	"github.com/nitesh/vaani/internal/ai/llm"
)

// Tool categories as stored in Dograh's tools.category column.
const (
	CategoryEndCall      = "end_call"
	CategoryTransferCall = "transfer_call"
	CategoryHTTPAPI      = "http_api"
)

// toolConfig is the "config" object inside a tool's definition JSON. Field
// names are Dograh's (api/services/workflow/pipecat_engine_custom_tools.py).
type toolConfig struct {
	// Spoken before the tool acts: messageType "custom" + customMessage.
	// ("audio" recordings aren't supported yet.)
	MessageType   string `json:"messageType"`
	CustomMessage string `json:"customMessage"`

	// end_call: when true, the LLM must give a reason, which is logged.
	EndCallReason            bool   `json:"endCallReason"`
	EndCallReasonDescription string `json:"endCallReasonDescription"`

	// transfer_call
	Destination string   `json:"destination"`
	Timeout     *float64 `json:"timeout"` // seconds; Dograh's default is 30
}

// errUnsupported marks a tool category Vaani can't run yet (e.g. http_api
// before custom tools are implemented): skipped with a warning, not fatal.
var errUnsupported = errors.New("tool category not supported yet")

// buildTool turns one tools row into an agent tool.
func buildTool(r ToolRow) (agent.Tool, error) {
	var def struct {
		Config toolConfig `json:"config"`
	}

	if len(r.Definition) > 0 {
		if err := json.Unmarshal(r.Definition, &def); err != nil {
			return agent.Tool{}, fmt.Errorf("tool %q (%s): bad definition: %w", r.Name, r.UUID, err)
		}
	}

	cfg := def.Config
	t := agent.Tool{
		Def: llm.FunctionDef{
			Name:        FunctionName(r.Name),
			Description: r.Description,
		},
	}

	if t.Def.Description == "" {
		t.Def.Description = "Execute " + r.Name + " tool"
	}

	if cfg.MessageType == "custom" {
		t.Message = cfg.CustomMessage
	}

	switch r.Category {
	case CategoryEndCall:
		t.Kind = agent.ToolEndCall

		if cfg.EndCallReason {
			desc := cfg.EndCallReasonDescription
			if desc == "" {
				desc = "The reason for ending the call (e.g., 'voicemail_detected', 'issue_resolved', 'customer_requested')"
			}

			params, _ := json.Marshal(map[string]any{
				"type":       "object",
				"properties": map[string]any{"reason": map[string]string{"type": "string", "description": desc}},
				"required":   []string{"reason"},
			})
			t.Def.Parameters = params
		}
	case CategoryTransferCall:
		if strings.TrimSpace(cfg.Destination) == "" {
			return agent.Tool{}, fmt.Errorf("transfer tool %q (%s) has no destination", r.Name, r.UUID)
		}

		t.Kind = agent.ToolTransfer
		t.Destination = cfg.Destination

		if cfg.Timeout != nil && *cfg.Timeout > 0 {
			t.Timeout = time.Duration(*cfg.Timeout * float64(time.Second))
		}
	default:
		return agent.Tool{}, fmt.Errorf("tool %q (category %s): %w", r.Name, r.Category, errUnsupported)
	}

	return t, nil
}

var (
	nonFuncChar = regexp.MustCompile(`[^a-z0-9_]`)
	underscores = regexp.MustCompile(`_+`)
)

// FunctionName is the LLM function name for a Dograh tool: Dograh's own rule
// (custom_tool.py) -- lowercase, anything but [a-z0-9_] becomes "_",
// repeated underscores collapsed, trimmed. "Transfer Call to Support Team"
// -> "transfer_call_to_support_team".
func FunctionName(toolName string) string {
	s := nonFuncChar.ReplaceAllString(strings.ToLower(toolName), "_")
	return strings.Trim(underscores.ReplaceAllString(s, "_"), "_")
}
