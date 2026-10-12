package dograh

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/nitesh/vaani/internal/ai/agent"
	"github.com/nitesh/vaani/internal/ai/llm"
)

func TestInferProvider(t *testing.T) {
	for _, tc := range []struct{ model, service, want string }{
		{"gemini/gemini-3.1-flash-lite", "llm", "google"},
		{"sarvam/sarvam-105b-conversations", "llm", "sarvam"},
		{"bulbul:v3", "tts", "sarvam"},
		{"saaras:v4", "stt", "sarvam"},
		{"openai/gpt-oss-120b", "llm", "groq"}, // before the openai check, as in Dograh
		{"gpt-4o", "llm", "openai"},
		{"", "llm", "unknown"},
		{"mystery", "llm", "openai"},
		{"mystery", "stt", "deepgram"},
	} {
		assert.Equal(t, tc.want, inferProvider(tc.model, tc.service), tc.model)
	}
}

func TestLookupPriceGatewayPrefix(t *testing.T) {
	p, ok := lookupPrice(llmPrices, "sarvam", "sarvam/sarvam-105b-conversations")
	require.True(t, ok)
	assert.Equal(t, llmPrices["sarvam"]["sarvam-105b-conversations"], p, "prefix stripped, not the provider default")

	p, ok = lookupPrice(llmPrices, "sarvam", "sarvam-v2/gemma4")
	require.True(t, ok)
	assert.Equal(t, llmPrices["sarvam"]["gemma4"], p)

	p, ok = lookupPrice(llmPrices, "sarvam", "sarvam-mystery")
	require.True(t, ok)
	assert.Equal(t, llmPrices["sarvam"]["default"], p)

	_, ok = lookupPrice(llmPrices, "azure", "gpt-5") // no azure default, no default provider: unpriced
	assert.False(t, ok)
}

// TestCalculateCost: a workflow-19-shaped call, priced independently of the
// tables' own arithmetic. Sarvam 105B Conversations is Rs15 input, Rs5
// cached input, Rs60 output per 1M tokens: 10k prompt tokens of which 4k
// cached, 1k completion = (6000*15 + 4000*5 + 1000*60) / 1M rupees.
func TestCalculateCost(t *testing.T) {
	c := calculateCost(usageInfo{
		LLM: map[string]tokenUsage{
			"LLM|||sarvam/sarvam-105b-conversations":    {PromptTokens: 10000, CompletionTokens: 1000, CacheReadInputTokens: 4000},
			"QAAnalysis|||gemini/gemini-3.1-flash-lite": {PromptTokens: 5000, CompletionTokens: 500},
		},
		TTS: map[string]int{"SarvamTTSService|||bulbul:v3": 1000},
		STT: map[string]float64{"SarvamSTTService|||saaras:v4": 60},
	})

	sarvam := (6000.0*15 + 4000*5 + 1000*60) / 1e6 / 96.5
	gemini := 5000*0.25/1e6 + 500*1.50/1e6
	tts := 1000 * 2.7 / 1000 / 96.5 // Rs2.7 per 1K characters
	stt := 60 * 30 / 96.5 / 3600    // Rs30 per hour

	assert.InDelta(t, sarvam+gemini, c.LLM, 1e-12)
	assert.InDelta(t, tts, c.TTS, 1e-12)
	assert.InDelta(t, stt, c.STT, 1e-12)
	assert.InDelta(t, sarvam+gemini+tts+stt, c.Total, 1e-12)
}

// TestSarvamTTSPrices: Sarvam's rate card, per 1K characters -- v3 Rs2.7,
// v4 Flash Rs1.7 -- converted at Rs96.5/USD.
func TestSarvamTTSPrices(t *testing.T) {
	for model, rupeesPer1K := range map[string]float64{"bulbul:v3": 2.7, "bulbul:v4-flash": 1.7} {
		c := calculateCost(usageInfo{TTS: map[string]int{usageKeyTTS + model: 1000}})
		assert.InDelta(t, rupeesPer1K/96.5, c.TTS, 1e-12, model)
	}

	assert.Equal(t, "sarvam", inferProvider("bulbul:v4-flash", "tts"))
}

func TestBuildUsageInfo(t *testing.T) {
	wf := &Workflow{Services: Services{STT: STTConfig{Model: "saaras:v4"}, TTS: TTSConfig{Model: "bulbul:v3"}}}
	sum := agent.CallSummary{
		LLMUsage: map[string]llm.Usage{"LLM|||m": {PromptTokens: 100, CompletionTokens: 10, TotalTokens: 110, CachedTokens: 40}},
		TTSChars: 250,
	}
	qa := map[string]llm.Usage{"QAAnalysis|||q": {PromptTokens: 50, CompletionTokens: 5, TotalTokens: 55}}

	b, err := json.Marshal(buildUsageInfo(wf, sum, qa, 79600*time.Millisecond))
	require.NoError(t, err)

	assert.JSONEq(t, `{
		"llm": {
			"LLM|||m": {"prompt_tokens": 100, "completion_tokens": 10, "total_tokens": 110, "cache_read_input_tokens": 40, "cache_creation_input_tokens": null},
			"QAAnalysis|||q": {"prompt_tokens": 50, "completion_tokens": 5, "total_tokens": 55, "cache_read_input_tokens": 0, "cache_creation_input_tokens": null}
		},
		"tts": {"SarvamTTSService|||bulbul:v3": 250},
		"stt": {"SarvamSTTService|||saaras:v4": 80},
		"call_duration_seconds": 80
	}`, string(b))

	// Nothing spoken: no TTS entry, as Dograh's defaultdict leaves none.
	b, _ = json.Marshal(buildUsageInfo(wf, agent.CallSummary{}, nil, time.Second))
	assert.JSONEq(t, `{"llm": {}, "tts": {}, "stt": {"SarvamSTTService|||saaras:v4": 1}, "call_duration_seconds": 1}`, string(b))
}
