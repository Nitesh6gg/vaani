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

	// http_api (custom_tool.py). Its spoken message is customMessage unless
	// customMessageType is "audio" -- not messageType, which it doesn't use.
	CustomMessageType string            `json:"customMessageType"`
	Method            string            `json:"method"`
	URL               string            `json:"url"`
	Headers           map[string]string `json:"headers"`
	CredentialUUID    string            `json:"credential_uuid"`
	TimeoutMS         *float64          `json:"timeout_ms"`
	Parameters        []toolParam       `json:"parameters"`
}

// errUnsupported marks a tool category Vaani can't run yet: skipped with a
// warning, not fatal.
var errUnsupported = errors.New("tool category not supported yet")

// buildTool turns one tools row into an agent tool. warning is a problem that
// doesn't stop the tool from being used (logged, like Dograh does).
func buildTool(r ToolRow) (t agent.Tool, warning string, err error) {
	var def struct {
		Config toolConfig `json:"config"`
	}

	if len(r.Definition) > 0 {
		if err := json.Unmarshal(r.Definition, &def); err != nil {
			return agent.Tool{}, "", fmt.Errorf("tool %q (%s): bad definition: %w", r.Name, r.UUID, err)
		}
	}

	cfg := def.Config
	t = agent.Tool{
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
			return agent.Tool{}, "", fmt.Errorf("transfer tool %q (%s) has no destination", r.Name, r.UUID)
		}

		t.Kind = agent.ToolTransfer
		t.Destination = cfg.Destination

		if cfg.Timeout != nil && *cfg.Timeout > 0 {
			t.Timeout = time.Duration(*cfg.Timeout * float64(time.Second))
		}
	case CategoryHTTPAPI:
		return buildHTTPTool(r, cfg, t)
	default:
		return agent.Tool{}, "", fmt.Errorf("tool %q (category %s): %w", r.Name, r.Category, errUnsupported)
	}

	return t, "", nil
}

// buildHTTPTool finishes an http_api tool. A credential that's missing,
// inactive or unreadable is a warning, not an error: Dograh then sends the
// request without it.
func buildHTTPTool(r ToolRow, cfg toolConfig, t agent.Tool) (agent.Tool, string, error) {
	if strings.TrimSpace(cfg.URL) == "" {
		return agent.Tool{}, "", fmt.Errorf("http tool %q (%s) has no url", r.Name, r.UUID)
	}

	h := httpTool{
		method:  strings.ToUpper(cfg.Method),
		url:     cfg.URL,
		headers: map[string]string{},
		timeout: defaultHTTPToolTimeout,
	}

	if h.method == "" {
		h.method = "POST"
	}

	if cfg.TimeoutMS != nil && *cfg.TimeoutMS > 0 {
		h.timeout = time.Duration(*cfg.TimeoutMS * float64(time.Millisecond))
	}

	for k, v := range cfg.Headers {
		h.headers[k] = v
	}

	var warning string

	if cfg.CredentialUUID != "" {
		if r.Credential == nil {
			warning = fmt.Sprintf("http tool %q (%s): credential %s not found or not active; calling without it", r.Name, r.UUID, cfg.CredentialUUID)
		} else if name, value, ok, err := authHeader(*r.Credential); err != nil {
			warning = fmt.Sprintf("http tool %q (%s): credential %s: %v; calling without it", r.Name, r.UUID, cfg.CredentialUUID, err)
		} else if ok {
			h.headers[name] = value
		}
	}

	t.Message = cfg.CustomMessage
	if cfg.CustomMessageType == "audio" { // recordings aren't supported yet
		t.Message = ""
	}

	t.Kind = agent.ToolFunction
	t.Def.Parameters = httpToolSchema(cfg.Parameters)
	t.Run = h.run

	return t, warning, nil
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
