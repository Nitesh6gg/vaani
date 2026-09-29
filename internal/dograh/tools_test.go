package dograh

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/nitesh/vaani/internal/ai/agent"
)

func TestFunctionNameMatchesDograh(t *testing.T) {
	cases := map[string]string{
		"Transfer Call to Support Team": "transfer_call_to_support_team",
		"web search":                    "web_search",
		"  End -- Call!! ":              "end_call",
		"Check_PIN code":                "check_pin_code",
	}
	for in, want := range cases {
		assert.Equal(t, want, FunctionName(in), in)
	}
}

// TestBuildToolsFromRealRows uses the definitions of the live deployment's
// tools (as pasted from its database), so a Dograh schema drift shows up here.
func TestBuildToolsFromRealRows(t *testing.T) {
	rows := []ToolRow{
		{
			UUID: "3eeefd31", Name: "Transfer Call to Support Team", Category: "transfer_call",
			Description: "Transfer the caller to another phone number when requested or user is already existing customer.",
			Definition:  json.RawMessage(`{"schema_version": 1, "type": "transfer_call", "config": {"destination": "SIP/7317185224@server225", "messageType": "custom", "customMessage": "कृपया लाइन पर बने रहिए"}}`),
		},
		{
			UUID: "e1", Name: "End Call", Category: "end_call", Description: "End the call",
			Definition: json.RawMessage(`{"schema_version": 1, "type": "end_call", "config": {"messageType": "custom", "customMessage": "Dhanyawad!", "endCallReason": true}}`),
		},
		{
			UUID: "2d431b07", Name: "web search", Category: "http_api",
			Definition: json.RawMessage(`{"schema_version": 1, "type": "http_api", "config": {"method": "POST", "url": "https://api.tavily.com/search"}}`),
		},
	}

	tools, unsupported, err := BuildTools(rows)
	require.NoError(t, err)
	require.Len(t, tools, 2)

	transfer := tools[0]
	assert.Equal(t, agent.ToolTransfer, transfer.Kind)
	assert.Equal(t, "transfer_call_to_support_team", transfer.Def.Name)
	assert.Equal(t, "SIP/7317185224@server225", transfer.Destination)
	assert.Equal(t, "कृपया लाइन पर बने रहिए", transfer.Message)
	assert.Zero(t, transfer.Timeout, "no timeout configured -> agent default")

	end := tools[1]
	assert.Equal(t, agent.ToolEndCall, end.Kind)
	assert.Equal(t, "Dhanyawad!", end.Message)
	assert.JSONEq(t, `{"type":"object","properties":{"reason":{"type":"string","description":"The reason for ending the call (e.g., 'voicemail_detected', 'issue_resolved', 'customer_requested')"}},"required":["reason"]}`,
		string(end.Def.Parameters))

	assert.Equal(t, []Unsupported{{Name: "web search", Category: "http_api"}}, unsupported)
}

func TestBuildToolsTransferTimeoutAndMissingDestination(t *testing.T) {
	tools, _, err := BuildTools([]ToolRow{{
		Name: "t", Category: "transfer_call",
		Definition: json.RawMessage(`{"config": {"destination": "SIP/1@x", "timeout": 45}}`),
	}})
	require.NoError(t, err)
	assert.Equal(t, 45*time.Second, tools[0].Timeout)

	_, _, err = BuildTools([]ToolRow{{Name: "t", Category: "transfer_call", Definition: json.RawMessage(`{"config": {}}`)}})
	assert.Error(t, err, "a transfer without a destination must fail loudly")
}
