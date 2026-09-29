package dograh

import (
	"encoding/json"
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

// Unsupported is a tool that was found but can't run yet (e.g. http_api
// before custom tools are implemented), reported so the caller can log it.
type Unsupported struct {
	Name     string
	Category string
}

// BuildTools turns tool rows into agent tools. Categories Vaani doesn't
// support yet come back in unsupported instead of failing the call.
func BuildTools(rows []ToolRow) (tools []agent.Tool, unsupported []Unsupported, err error) {
	for _, r := range rows {
		var def struct {
			Config toolConfig `json:"config"`
		}

		if len(r.Definition) > 0 {
			if err := json.Unmarshal(r.Definition, &def); err != nil {
				return nil, nil, fmt.Errorf("dograh: tool %q (%s): bad definition: %w", r.Name, r.UUID, err)
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
				return nil, nil, fmt.Errorf("dograh: transfer tool %q (%s) has no destination", r.Name, r.UUID)
			}

			t.Kind = agent.ToolTransfer
			t.Destination = cfg.Destination

			if cfg.Timeout != nil && *cfg.Timeout > 0 {
				t.Timeout = time.Duration(*cfg.Timeout * float64(time.Second))
			}
		default:
			unsupported = append(unsupported, Unsupported{Name: r.Name, Category: r.Category})
			continue
		}

		tools = append(tools, t)
	}

	return tools, unsupported, nil
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
