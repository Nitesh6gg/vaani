package dograh

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/nitesh/vaani/internal/ai/agent"
)

// riyaWorkflow is the shape of the live "Survey & Feedback - Riya" workflow
// (prompts shortened), plus a transfer tool on the main node.
const riyaWorkflow = `{"nodes": [
	{"id": "1", "type": "startCall", "data": {"name": "Start Call", "prompt": "Greet. Caller {{caller_number}}.",
		"is_start": true, "add_global_prompt": true, "greeting_type": "text", "allow_interrupt": false}},
	{"id": "2", "type": "agentNode", "data": {"name": "Main Agenda and Questions", "prompt": "Ask the questions.",
		"add_global_prompt": true, "tool_uuids": ["tr-1", "gone-2"]}},
	{"id": "0", "type": "globalNode", "data": {"name": "Global Node", "prompt": "You are Riya."}},
	{"id": "4", "type": "endCall", "data": {"name": "End Call", "prompt": "Say goodbye.", "add_global_prompt": false, "is_end": true}}
], "edges": [
	{"source": "1", "target": "2", "data": {"label": "Move to Main Agenda", "condition": "When they engage."}},
	{"source": "1", "target": "4", "data": {"label": "End call", "condition": "Wrong number."}},
	{"source": "2", "target": "4", "data": {"label": "End call", "condition": "When done."}}
]}`

func TestBuildWorkflowRiya(t *testing.T) {
	wf, err := parseWorkflow([]byte(riyaWorkflow))
	require.NoError(t, err)
	assert.Equal(t, []string{"tr-1", "gone-2"}, wf.toolUUIDs())

	rows := []ToolRow{{UUID: "tr-1", Name: "Transfer to Support", Category: "transfer_call",
		Definition: json.RawMessage(`{"config": {"destination": "PJSIP/100@pbx"}}`)}}

	start, warnings, err := buildWorkflow(wf, map[string]any{"caller_number": "08448805135"}, rows, time.Now())
	require.NoError(t, err)

	assert.Equal(t, "Start Call", start.Name)
	assert.Equal(t, "You are Riya.\n\nGreet. Caller 08448805135.", start.Prompt, "global prompt + node prompt, variables filled")
	assert.Empty(t, start.Greeting, "no greeting -> the LLM opens the call")
	require.Len(t, start.Edges, 2)
	assert.Equal(t, "move_to_main_agenda", start.Edges[0].Def.Name)
	assert.Equal(t, "When they engage.", start.Edges[0].Def.Description)
	assert.Equal(t, "end_call", start.Edges[1].Def.Name)

	main := start.Edges[0].To
	assert.Equal(t, "Main Agenda and Questions", main.Name)
	require.Len(t, main.Tools, 1)
	assert.Equal(t, agent.ToolTransfer, main.Tools[0].Kind)

	end := main.Edges[0].To
	assert.True(t, end.End)
	assert.Equal(t, "Say goodbye.", end.Prompt, "add_global_prompt=false leaves the global prompt out")
	assert.Same(t, end, start.Edges[1].To, "both End call edges lead to the same node")

	assert.Equal(t, []string{`node "Main Agenda and Questions": tool gone-2 not found or not active`}, warnings)
}

func TestBuildWorkflowGreetingAndErrors(t *testing.T) {
	wf, err := parseWorkflow([]byte(`{"nodes": [{"id": "1", "type": "startCall",
		"data": {"name": "S", "is_start": true, "greeting": "Namaste {{name | ji}}"}}], "edges": []}`))
	require.NoError(t, err)

	start, _, err := buildWorkflow(wf, map[string]any{}, nil, time.Now())
	require.NoError(t, err)
	assert.Equal(t, "Namaste ji", start.Greeting)

	noStart, _ := parseWorkflow([]byte(`{"nodes": [{"id": "1", "type": "agentNode", "data": {}}]}`))
	_, _, err = buildWorkflow(noStart, nil, nil, time.Now())
	assert.Error(t, err)

	badEdge, _ := parseWorkflow([]byte(`{"nodes": [{"id": "1", "data": {"is_start": true}}],
		"edges": [{"source": "1", "target": "9", "data": {"label": "x"}}]}`))
	_, _, err = buildWorkflow(badEdge, nil, nil, time.Now())
	assert.Error(t, err)
}

func TestCallLimits(t *testing.T) {
	cases := []struct {
		name      string
		json      string
		idle, max time.Duration
		warn      bool
	}{
		{"never saved", "", 10 * time.Second, 300 * time.Second, false},
		{"empty object", "{}", 10 * time.Second, 300 * time.Second, false},
		{"set in dograh", `{"max_call_duration": 600, "max_user_idle_timeout": 7.5, "smart_turn_stop_secs": 2}`, 7500 * time.Millisecond, 600 * time.Second, false},
		{"zero disables", `{"max_call_duration": 0, "max_user_idle_timeout": 0}`, 0, 0, false},
		{"garbage", `{"max_call_duration": "ten"}`, 10 * time.Second, 300 * time.Second, true},
	}

	for _, c := range cases {
		idle, maxDuration, warn := callLimits(c.json)
		assert.Equal(t, c.idle, idle, c.name)
		assert.Equal(t, c.max, maxDuration, c.name)
		assert.Equal(t, c.warn, warn != "", c.name)
	}
}

func TestRenderTemplateMatchesDograh(t *testing.T) {
	now := time.Date(2026, 9, 29, 10, 30, 0, 0, time.UTC) // a Tuesday
	vars := map[string]any{
		"name":     "Asha",
		"empty":    "",
		"customer": map[string]any{"city": "Lucknow", "age": float64(30), "vip": true},
	}

	cases := map[string]string{
		"Hi {{name}}":                         "Hi Asha",
		"Hi {{ name }}":                       "Hi Asha",
		"{{customer.city}}, {{customer.age}}": "Lucknow, 30",
		"{{customer.vip}}":                    "True",
		"[{{missing}}]":                       "[]",
		"{{empty | friend}}":                  "friend",
		"{{missing | fallback:Sir}}":          "Sir",
		"{{caller_name | fallback}}":          "Caller_Name",
		"{{current_time}}":                    "2026-09-29 10:30:00 UTC",
		"{{current_time_Asia/Kolkata}}":       "2026-09-29 16:00:00 IST",
		"{{current_weekday}}":                 "Tuesday",
		// A bare current_weekday inherits the zone of a current_time_<Zone>.
		"{{current_time_Asia/Kolkata}} {{current_weekday}}": "2026-09-29 16:00:00 IST Tuesday",
		`line1\nline2`: "line1\nline2",
	}

	for in, want := range cases {
		assert.Equal(t, want, renderTemplate(in, vars, now), in)
	}
}
