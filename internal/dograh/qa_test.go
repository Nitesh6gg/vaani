package dograh

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/nitesh/vaani/internal/ai/agent"
)

// qaWorkflow is a workflow with one QA node (its default settings except
// sample_rate) plus the script nodes a node-summary pass would describe.
const qaWorkflow = `{"nodes": [
	{"id": "s1", "type": "startCall", "data": {"name": "Start", "prompt": "Greet warmly."}},
	{"id": "a1", "type": "agentNode", "data": {"name": "Main", "prompt": "Ask the survey questions.", "tool_uuids": ["t1"]}},
	{"id": "q1", "type": "qa", "data": {"name": "Compliance Check",
		"qa_system_prompt": "Review. Before: {{previous_conversation_summary}}. Node: {{node_summary}}. Transcript: {{transcript}}",
		"qa_min_call_duration": 10, "qa_sample_rate": 100}}
], "edges": [
	{"source": "a1", "target": "s1", "data": {"label": "Done", "condition": "when finished"}}
], "node_summaries": {
	"s1": {"summary": "cached start summary", "trace_url": "https://x"},
	"a1": "legacy plain-string summary"
}}`

func TestParseQANodes(t *testing.T) {
	wf, err := parseWorkflow([]byte(qaWorkflow))
	require.NoError(t, err)

	nodes := parseQANodes(wf)
	require.Len(t, nodes, 1)

	n := nodes[0]
	assert.Equal(t, "q1", n.ID)
	assert.Equal(t, "Compliance Check", n.Name)
	assert.True(t, n.Enabled, "qa_enabled absent -> Dograh's default true")
	assert.Equal(t, "Review. Before: {{previous_conversation_summary}}. Node: {{node_summary}}. Transcript: {{transcript}}", n.SystemPrompt)
	assert.Equal(t, 10*time.Second, n.MinCallDuration)
	assert.False(t, n.VoicemailCalls, "default false")
	assert.Equal(t, 100, n.SampleRate)
	assert.True(t, n.UseWorkflowLLM, "qa_use_workflow_llm absent -> default true")

	// Defaults when the editor never saved anything (a bare "qa" node).
	bare, err := parseWorkflow([]byte(`{"nodes": [{"id": "q2", "type": "qa", "data": {}}]}`))
	require.NoError(t, err)
	n = parseQANodes(bare)[0]
	assert.Equal(t, "QA Analysis", n.Name)
	assert.Equal(t, 15*time.Second, n.MinCallDuration)
	assert.Equal(t, 100, n.SampleRate)

	// A non-"qa" node is never picked up.
	assert.Empty(t, parseQANodes(workflowJSON{Nodes: []struct {
		ID   string   `json:"id"`
		Type string   `json:"type"`
		Data nodeData `json:"data"`
	}{{ID: "x", Type: "agentNode"}}}))
}

func TestExistingNodeSummaries(t *testing.T) {
	wf, err := parseWorkflow([]byte(qaWorkflow))
	require.NoError(t, err)

	summaries := existingNodeSummaries(wf)
	assert.Equal(t, "cached start summary", summaries["s1"], "current {summary, trace_url} shape")
	assert.Equal(t, "legacy plain-string summary", summaries["a1"], "legacy plain-string shape")

	_, cached := summaries["nope"]
	assert.False(t, cached)
}

func TestBuildNodeScripts(t *testing.T) {
	wf, err := parseWorkflow([]byte(qaWorkflow))
	require.NoError(t, err)

	tools := []ToolRow{{UUID: "t1", Name: "Lookup Order", Description: "finds an order by id"}}

	scripts := buildNodeScripts(wf, tools)
	require.Contains(t, scripts, "a1")
	require.Contains(t, scripts, "s1")
	assert.NotContains(t, scripts, "q1", "a QA node is never described as a script")

	a1 := scripts["a1"]
	assert.Equal(t, "Main", a1.Name)
	assert.Equal(t, "Node name: Main\nAgent prompt:\nAsk the survey questions.\n"+
		"Available tools:\n- Lookup Order: finds an order by id\n- Done: when finished", a1.Info)
}

func TestShouldSkipQA(t *testing.T) {
	n := QANode{MinCallDuration: 15 * time.Second, SampleRate: 100}

	assert.Contains(t, shouldSkipQA(n, 5*time.Second, ""), "below minimum")
	assert.Empty(t, shouldSkipQA(n, 20*time.Second, ""))

	n.VoicemailCalls = false
	assert.Contains(t, shouldSkipQA(n, 20*time.Second, "voicemail_detected"), "voicemail")

	n.VoicemailCalls = true
	assert.Empty(t, shouldSkipQA(n, 20*time.Second, "voicemail_detected"), "voicemail calls explicitly allowed")

	// Sampling is a dice roll (Dograh: random.randint(1,100) > sample_rate);
	// not deterministic, so assert the two ends behave as expected over many
	// tries rather than a single outcome.
	n = QANode{MinCallDuration: 0, SampleRate: 1}

	skipped := 0

	for range 300 {
		if shouldSkipQA(n, time.Minute, "") != "" {
			skipped++
		}
	}

	assert.Greater(t, skipped, 250, "1%% sample rate should skip almost every call")

	n.SampleRate = 100
	for range 300 {
		assert.Empty(t, shouldSkipQA(n, time.Minute, ""), "100%% sample rate never skips on sampling")
	}
}

func TestQAProviderBaseURL(t *testing.T) {
	for _, p := range []string{"openai", "openrouter", "anthropic"} {
		url, ok := qaProviderBaseURL(p)
		assert.True(t, ok, p)
		assert.NotEmpty(t, url, p)
	}

	_, ok := qaProviderBaseURL("azure")
	assert.False(t, ok, "azure needs its own auth shape, not supported yet")

	_, ok = qaProviderBaseURL("something-unknown")
	assert.False(t, ok)
}

func TestResolveQALLM(t *testing.T) {
	owner := LLMConfig{Provider: "bifrost", BaseURL: "http://x/v1", APIKey: "owner-key", Model: "owner-model"}

	// Default: the owner's LLM, unchanged.
	cfg, ok := resolveQALLM(QANode{UseWorkflowLLM: true}, owner, true)
	require.True(t, ok)
	assert.Equal(t, owner, cfg)

	// The owner's LLM with this QA node's own model substituted in.
	cfg, ok = resolveQALLM(QANode{UseWorkflowLLM: true, Model: "gpt-5"}, owner, true)
	require.True(t, ok)
	assert.Equal(t, "gpt-5", cfg.Model)
	assert.Equal(t, owner.APIKey, cfg.APIKey)

	// "default" is a no-op, not a literal model name.
	cfg, ok = resolveQALLM(QANode{UseWorkflowLLM: true, Model: "default"}, owner, true)
	require.True(t, ok)
	assert.Equal(t, owner.Model, cfg.Model)

	// No owner LLM configured at all.
	_, ok = resolveQALLM(QANode{UseWorkflowLLM: true}, LLMConfig{}, false)
	assert.False(t, ok)

	// The QA node's own LLM.
	cfg, ok = resolveQALLM(QANode{UseWorkflowLLM: false, Provider: "openai", APIKey: "k", Model: "gpt-4o"}, owner, true)
	require.True(t, ok)
	assert.Equal(t, LLMConfig{Provider: "openai", BaseURL: "https://api.openai.com/v1", APIKey: "k", Model: "gpt-4o"}, cfg)

	// Azure (unsupported) or a missing key/model: unusable.
	_, ok = resolveQALLM(QANode{UseWorkflowLLM: false, Provider: "azure", APIKey: "k", Model: "m"}, owner, true)
	assert.False(t, ok)
	_, ok = resolveQALLM(QANode{UseWorkflowLLM: false, Provider: "openai", Model: "gpt-4o"}, owner, true)
	assert.False(t, ok, "no api key")
}

func TestParseQAReply(t *testing.T) {
	r := parseQAReply("Main", `{"tags":[{"tag":"USER_FRUSTRATED","reason":"raised voice"}],`+
		`"summary":"went fine","call_quality_score":7,"overall_sentiment":"neutral"}`)
	assert.Equal(t, "Main", r.NodeName)
	assert.Equal(t, []qaTag{{Tag: "USER_FRUSTRATED", Reason: "raised voice"}}, r.Tags)
	assert.Equal(t, "went fine", r.Summary)
	require.NotNil(t, r.Score)
	assert.Equal(t, 7.0, *r.Score)
	assert.Equal(t, "neutral", r.OverallSentiment)

	// Unparseable: everything zero-valued, never an error -- a bad verdict
	// isn't a failed call.
	r = parseQAReply("Main", "not json at all")
	assert.Equal(t, []qaTag{}, r.Tags, "never nil, so it marshals as [] not null")
	assert.Empty(t, r.Summary)
	assert.Nil(t, r.Score)
}

// qaLLMServer answers every /chat/completions call with one fixed
// non-streaming... actually streaming SSE reply (llm.Client always speaks
// SSE), the given text, regardless of what was asked.
func qaLLMServer(t *testing.T, textFor func(systemPrompt string) string) *httptest.Server {
	t.Helper()

	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Messages []struct {
				Role, Content string
			} `json:"messages"`
		}
		require.NoError(t, json.NewDecoder(r.Body).Decode(&body))

		system := ""
		if len(body.Messages) > 0 {
			system = body.Messages[0].Content
		}

		w.Header().Set("Content-Type", "text/event-stream")

		text := textFor(system)
		b, _ := json.Marshal(map[string]any{"choices": []map[string]any{{"delta": map[string]any{"content": text}}}})
		_, _ = w.Write([]byte("data: " + string(b) + "\n\n"))
		_, _ = w.Write([]byte("data: [DONE]\n\n"))
	}))
}

// TestRunQANodePerNode is an end-to-end run of one QA node's review over a
// two-node call, against a fake LLM: node-summary generation for the
// missing node, the previous-conversation summary, and the per-node review
// -- Dograh's run_per_node_qa_analysis.
func TestRunQANodePerNode(t *testing.T) {
	var systemPrompts []string

	srv := qaLLMServer(t, func(system string) string {
		systemPrompts = append(systemPrompts, system)

		switch system {
		case nodeSummarySystemPrompt:
			return "This node starts the call."
		case conversationSummarySystemPrompt:
			return "The caller said hello."
		default:
			return `{"tags":[{"tag":"DEAD_AIR","reason":"long pause"}],"summary":"ok","call_quality_score":8,"overall_sentiment":"positive"}`
		}
	})
	defer srv.Close()

	wfJSON, err := parseWorkflow([]byte(qaWorkflow))
	require.NoError(t, err)

	wf := &Workflow{DefinitionID: 1, QA: QAConfig{
		Nodes:         parseQANodes(wfJSON),
		NodeScripts:   buildNodeScripts(wfJSON, nil),
		NodeSummaries: map[string]string{"a1": "cached, not regenerated"}, // s1 is missing -> generated
		OwnerLLM:      LLMConfig{Provider: "openai", BaseURL: srv.URL, APIKey: "k", Model: "gpt-test"},
		ownerLLMOK:    true,
	}}

	sum := agent.CallSummary{Events: []agent.LogEvent{
		ev("rtf-node-transition", "s1", "Start", "2026-10-09T10:00:00.000000Z", 0, nil),
		ev("rtf-bot-text", "s1", "Start", "2026-10-09T10:00:01.000000Z", 0, map[string]any{"text": "Hello!"}),
		ev("rtf-node-transition", "a1", "Main", "2026-10-09T10:00:05.000000Z", 1, nil),
		ev("rtf-user-transcription", "a1", "Main", "2026-10-09T10:00:06.000000Z", 1,
			map[string]any{"text": "I'm fine", "final": true}),
		ev("rtf-bot-text", "a1", "Main", "2026-10-09T10:00:07.000000Z", 1, map[string]any{"text": "Great."}),
	}}

	var s Store

	res, err := s.runQANode(t.Context(), wf.QA.Nodes[0], wf, sum, time.Minute, map[string]string{"a1": "cached, not regenerated"})
	require.NoError(t, err)

	assert.Equal(t, "gpt-test", res.Model)
	require.Len(t, res.NodeResults, 2)

	main := res.NodeResults["a1"]
	assert.Equal(t, []qaTag{{Tag: "DEAD_AIR", Reason: "long pause"}}, main.Tags)
	assert.Equal(t, 8.0, *main.Score)
	assert.Equal(t, "positive", main.OverallSentiment)

	// The review for "a1" (the second node) got a conversation summary of
	// "s1" and "s1"'s own generated node summary -- proof prior-node and
	// node-script context both flow through the rendered prompt.
	assert.Contains(t, systemPrompts, conversationSummarySystemPrompt, "the prior node's conversation was summarized")

	var sawSubstitution bool

	for _, p := range systemPrompts {
		if p != nodeSummarySystemPrompt && p != conversationSummarySystemPrompt {
			if assert.Contains(t, p, "Transcript: [") {
				sawSubstitution = true
			}

			if strings.Contains(p, "I'm fine") { // a1's own review
				assert.Contains(t, p, "Before: The caller said hello.")
			}
		}
	}

	assert.True(t, sawSubstitution)
}

func TestRunQANodeNoSystemPrompt(t *testing.T) {
	var s Store

	_, err := s.runQANode(t.Context(), QANode{}, &Workflow{}, agent.CallSummary{}, 0, nil)
	assert.ErrorContains(t, err, "no system prompt")
}

func TestRunQANodeNoLLM(t *testing.T) {
	var s Store

	_, err := s.runQANode(t.Context(), QANode{SystemPrompt: "x"}, &Workflow{}, agent.CallSummary{}, 0, nil)
	assert.ErrorContains(t, err, "no LLM API key")

	_, err = s.runQANode(t.Context(), QANode{SystemPrompt: "x", UseWorkflowLLM: false, Provider: "azure", APIKey: "k", Model: "m"},
		&Workflow{}, agent.CallSummary{}, 0, nil)
	assert.ErrorContains(t, err, "azure")
}
