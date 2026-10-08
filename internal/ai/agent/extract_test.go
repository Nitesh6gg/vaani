package agent

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/nitesh/vaani/internal/ai/llm"
	"github.com/nitesh/vaani/internal/ai/tts"
)

func strPtr(s string) *string { return &s }

func TestExtractionMessagesMatchDograh(t *testing.T) {
	x := &Extraction{Prompt: "Survey for Delhi.", Vars: []ExtractionVar{
		{Name: "party", Type: "string", Prompt: strPtr("which party they support")},
		{Name: "age", Type: "number"},
	}}
	history := []llm.Message{
		{Role: "assistant", Content: "नमस्ते"},
		{Role: "user", Content: "AAP"},
	}

	msgs := extractionMessages(x, history)
	require.Len(t, msgs, 2)
	assert.Equal(t, "system", msgs[0].Role)
	assert.Equal(t, "You are an assistant tasked with extracting structured data from the conversation. "+
		"Return ONLY a valid JSON object with the requested variables as top-level keys. Do not wrap the JSON in markdown."+
		"\n\nSurvey for Delhi.", msgs[0].Content)
	assert.Equal(t, "user", msgs[1].Role)
	assert.Equal(t, "\n\nVariables to extract:\n"+
		"- party (VariableType.string): which party they support\n"+
		"- age (VariableType.number): None"+
		"\n\nConversation history:\n"+
		"assistant: नमस्ते\nuser: AAP", msgs[1].Content)

	assert.NotContains(t, extractionMessages(&Extraction{}, nil)[0].Content, "\n\n", "no extraction prompt: the base prompt alone")
}

func TestConversationTextFormatsToolResponses(t *testing.T) {
	calls := []llm.ToolCall{
		{ID: "a", Function: llm.FunctionCall{Name: "web_search"}},
		{ID: "b", Function: llm.FunctionCall{Name: "move_on"}},
		{ID: "c", Function: llm.FunctionCall{Name: "lookup"}},
	}
	history := []llm.Message{
		{Role: "user", Content: "मौसम?"},
		{Role: "assistant", ToolCalls: calls}, // no text: not a line
		{Role: "tool", ToolCallID: "a", Content: `{"status":"success","status_code":200,"data":{"temp":"30°"}}`},
		{Role: "tool", ToolCallID: "b", Content: `{"status":"done"}`},
		{Role: "tool", ToolCallID: "c", Content: `{"status":"failed","reason":"busy"}`},
		{Role: "tool", ToolCallID: "zz", Content: "plain text"},
		{Role: "tool", ToolCallID: "c", Content: `{"data":"` + strings.Repeat("x", 2100) + `"}`},
	}

	lines := strings.Split(conversationText(history), "\n")
	assert.Equal(t, []string{
		"user: मौसम?",
		"[Tool Response: web_search]", `{"temp":"30°"}`,
		"[Tool Response: lookup]", `{"reason":"busy"}`,
		"[Tool Response: unknown]", "plain text",
		"[Tool Response: lookup]", `"` + strings.Repeat("x", 1999) + "...(truncated)",
	}, lines)
}

func TestParseLLMJSONMatchesDograh(t *testing.T) {
	cases := map[string]string{
		`{"party": "AAP"}`:                         `{"party": "AAP"}`,
		"```json\n{\"party\": \"AAP\"}\n```":       `{"party": "AAP"}`,
		"Here you go: {\"a\": {\"b\": 1}} thanks.": `{"a": {"b": 1}}`,
		"list: [1, 2]":                             `[1, 2]`,
		"   ":                                      `{}`,
		"no json here":                             `{"raw": "no json here"}`,
	}
	for in, want := range cases {
		assert.JSONEq(t, want, string(parseLLMJSON(in)), in)
	}
}

// extractingLLM answers extraction requests with reply and everything else
// from conv.
type extractingLLM struct {
	conv  *fakeLLM
	reply func(node string) string

	mu   sync.Mutex
	seen []string // the user message of each extraction request
}

func (f *extractingLLM) Stream(ctx context.Context, msgs []llm.Message, tools []llm.Tool, onToken func(string)) ([]llm.ToolCall, error) {
	if len(msgs) == 2 && strings.HasPrefix(msgs[0].Content, extractionSystemPrompt) {
		f.mu.Lock()
		f.seen = append(f.seen, msgs[1].Content)
		f.mu.Unlock()
		onToken(f.reply(msgs[0].Content))

		return nil, nil
	}

	return f.conv.Stream(ctx, msgs, tools, onToken)
}

// extractions returns the extraction requests' user messages, under the lock.
func (f *extractingLLM) extractions() []string {
	f.mu.Lock()
	defer f.mu.Unlock()

	return append([]string(nil), f.seen...)
}

// TestHandler_ExtractsOnLeavingANodeAndAtTheEnd: Dograh's two moments --
// leaving a node with extraction on, and the end of the call for the node
// it ended at -- both land in the call log, later values winning.
func TestHandler_ExtractsOnLeavingANodeAndAtTheEnd(t *testing.T) {
	fSTT := newFakeSTT()
	fTTS := newFakeTTS()

	end := &Node{ID: "e", Name: "End", End: true,
		Extraction: &Extraction{Prompt: "END", Vars: []ExtractionVar{{Name: "mood", Type: "string"}}}}
	start := &Node{ID: "s", Name: "Start",
		Extraction: &Extraction{Prompt: "START", Vars: []ExtractionVar{{Name: "party", Type: "string"}}},
		Edges:      []Edge{{Def: llm.FunctionDef{Name: "finish"}, To: end}}}

	call := llm.ToolCall{ID: "t1", Type: "function", Function: llm.FunctionCall{Name: "finish", Arguments: "{}"}}
	fLLM := &extractingLLM{
		conv: &fakeLLM{rounds: []fakeRound{{toolCalls: []llm.ToolCall{call}}, {tokens: []string{"Bye."}}}},
		reply: func(system string) string {
			if strings.HasSuffix(system, "START") {
				return `{"party": "AAP", "tag_party": "aap_voter"}`
			}

			return "```json\n{\"mood\": \"happy\", \"party\": \"BJP\"}\n```"
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cfg := testConfig(fSTT, fTTS, fLLM, 0, 0)
	cfg.Start = start
	cfg.Log = &CallLog{}
	h := NewHandler(ctx, "call1", cfg)

	fSTT.sendFinal("मैं AAP को वोट दूंगा")
	require.Eventually(t, func() bool { return fTTS.spokenCount() == 1 }, time.Second, time.Millisecond)

	cancel()
	<-h.Done()
	h.FinishExtraction(context.Background())

	s := cfg.Log.Summary()
	assert.Equal(t, map[string]any{"party": "BJP", "tag_party": "aap_voter", "mood": "happy"}, s.Extracted,
		"the end-of-call extraction runs last and wins")
	assert.Equal(t, []string{"party", "tag_party", "mood"}, s.ExtractedKeys)

	require.Len(t, fLLM.extractions(), 2)
	assert.Contains(t, fLLM.extractions()[0], "user: मैं AAP को वोट दूंगा", "the start node's extraction sees the caller's answer")
}

// TestHandler_StartExtractionAfterFinishIsRefused: a straggling extraction
// call arriving after FinishExtraction has already taken its snapshot must
// not silently run unobserved -- it's refused instead.
func TestHandler_StartExtractionAfterFinishIsRefused(t *testing.T) {
	fLLM := &extractingLLM{conv: &fakeLLM{}, reply: func(string) string { return `{"late": "value"}` }}
	cfg := testConfig(newFakeSTT(), newFakeTTS(), fLLM, 0, 0)
	cfg.Log = &CallLog{}

	ctx, cancel := context.WithCancel(context.Background())
	h := NewHandler(ctx, "call1", cfg)
	cancel()
	<-h.Done()

	h.FinishExtraction(context.Background())

	node := &Node{Name: "Late", Extraction: &Extraction{Vars: []ExtractionVar{{Name: "late", Type: "string"}}}}
	h.startExtraction(node, nil)

	assert.Nil(t, cfg.Log.Summary().Extracted, "a post-finish extraction never runs")
	assert.Empty(t, fLLM.extractions(), "the LLM must never be asked")
}

// TestHandler_FinishExtractionSkippedAtShutdown: when Vaani is stopping, the
// end-of-call extraction must not hold up the run's completion.
func TestHandler_FinishExtractionSkippedAtShutdown(t *testing.T) {
	fLLM := &extractingLLM{conv: &fakeLLM{}, reply: func(string) string { return `{"a": 1}` }}
	cfg := testConfig(newFakeSTT(), newFakeTTS(), fLLM, 0, 0)
	cfg.Start = &Node{Name: "S", Extraction: &Extraction{Vars: []ExtractionVar{{Name: "a", Type: "number"}}}}
	cfg.Log = &CallLog{}

	ctx, cancel := context.WithCancel(context.Background())
	h := NewHandler(ctx, "call1", cfg)
	cancel()
	<-h.Done()

	stopping, stop := context.WithCancel(context.Background())
	stop()
	h.FinishExtraction(stopping)

	assert.Empty(t, fLLM.extractions(), "no LLM call while shutting down")
}

func TestHandler_NoExtractionWithoutSettings(t *testing.T) {
	fSTT := newFakeSTT()
	fLLM := &fakeLLM{}
	cfg := testConfig(fSTT, newFakeTTS(), fLLM, 0, 0)
	cfg.Log = &CallLog{}

	ctx, cancel := context.WithCancel(context.Background())
	h := NewHandler(ctx, "call1", cfg)

	cancel()
	<-h.Done()
	h.FinishExtraction(context.Background())

	assert.Nil(t, cfg.Log.Summary().Extracted)
	assert.Zero(t, fLLM.streamCalls())
}

var _ tts.Client = (*fakeTTS)(nil)

// The End Call request of call 2026-10-08 (run 511), which Sarvam rejected
// with 400 "Tool messages found but no tools provided".
func TestPlainHistoryHasNoToolMessages(t *testing.T) {
	call := func(id, name string) llm.Message {
		return llm.Message{Role: "assistant", ToolCalls: []llm.ToolCall{{ID: id, Type: "function",
			Function: llm.FunctionCall{Name: name, Arguments: "{}"}}}}
	}
	done := func(id string) llm.Message {
		return llm.Message{Role: "tool", ToolCallID: id, Content: `{"status":"done"}`}
	}

	in := []llm.Message{
		{Role: "system", Content: "End the call politely."},
		{Role: "assistant", Content: "नमस्ते, आपका नाम?"},
		{Role: "user", Content: "मेरा नाम नितेश है।"},
		call("t1", "move_to_main_agenda"), done("t1"),
		{Role: "assistant", Content: "सबसे बड़ा मुद्दा क्या है?"},
		{Role: "user", Content: "महंगाई।"},
		{Role: "assistant", Content: "एक सेकंड, देखती हूँ।", ToolCalls: []llm.ToolCall{{ID: "t2", Type: "function",
			Function: llm.FunctionCall{Name: "lookup_order", Arguments: "{}"}}}},
		{Role: "tool", ToolCallID: "t2", Content: `{"status":"success","data":{"order":"A12"}}`},
		{Role: "assistant", Content: "आपका ऑर्डर मिल गया।"},
		{Role: "user", Content: "नो मैम, थैंक यू।"},
		call("t3", "end_call"), done("t3"),
	}

	out := plainHistory(in)

	for _, m := range out {
		assert.NotEqual(t, "tool", m.Role)
		assert.Empty(t, m.ToolCalls)
		assert.Empty(t, m.ToolCallID)
	}
	for i := 1; i < len(out); i++ {
		assert.False(t, out[i].Role == "assistant" && out[i-1].Role == "assistant", "adjacent assistant messages at %d", i)
	}

	assert.Equal(t, []llm.Message{
		{Role: "system", Content: "End the call politely."},
		{Role: "assistant", Content: "नमस्ते, आपका नाम?"},
		{Role: "user", Content: "मेरा नाम नितेश है।"},
		{Role: "assistant", Content: "सबसे बड़ा मुद्दा क्या है?"},
		{Role: "user", Content: "महंगाई।"},
		{Role: "assistant", Content: "एक सेकंड, देखती हूँ।\n\n[Tool Response: lookup_order]\n{\"order\":\"A12\"}\n\nआपका ऑर्डर मिल गया।"},
		{Role: "user", Content: "नो मैम, थैंक यू।"},
	}, out)

	assert.Len(t, in, 13, "the history itself is never changed")
	assert.Len(t, in[3].ToolCalls, 1)
}
