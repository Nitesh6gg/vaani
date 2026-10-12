package dograh

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// userConfig is the shape of the live deployment's user_configurations row
// (user 3), keys replaced.
const userConfig = `{
	"llm": {"provider": "bifrost", "api_key": "user-llm-key", "model": "gemini/gemini-3.1-flash-lite", "base_url": "http://192.168.26.130:8080/v1"},
	"stt": {"provider": "sarvam", "api_key": "sarvam-key", "model": "saaras:v4", "language": "unknown", "segmented": false},
	"tts": {"provider": "sarvam", "api_key": "sarvam-key", "model": "bulbul:v3", "voice": "shubh", "language": "hi-IN"},
	"is_realtime": false
}`

// riyaOverrides is workflow 19's model_overrides.
const riyaOverrides = `{"max_call_duration": 300, "model_overrides": {
	"llm": {"provider": "bifrost", "api_key": "wf-llm-key", "model": "sarvam-v2/gemma4", "base_url": "http://192.168.26.130:8080/v1"},
	"tts": {"provider": "sarvam", "api_key": "wf-tts-key", "model": "bulbul:v3", "voice": "simran", "language": "hi-IN"}
}}`

func TestResolveServicesRiya(t *testing.T) {
	s, warnings, err := resolveServices(userConfig, riyaOverrides)
	require.NoError(t, err)
	assert.Empty(t, warnings)

	assert.Equal(t, LLMConfig{Provider: "bifrost", BaseURL: "http://192.168.26.130:8080/v1", APIKey: "wf-llm-key", Model: "sarvam-v2/gemma4"}, s.LLM,
		"the workflow's LLM override wins")
	assert.Equal(t, STTConfig{APIKey: "sarvam-key", Model: "saaras:v4", Language: "unknown"}, s.STT,
		"no STT override: the user's config")
	assert.Equal(t, TTSConfig{APIKey: "wf-tts-key", Model: "bulbul:v3", Voice: "simran", Language: "hi-IN"}, s.TTS)
}

func TestResolveServicesMergeRules(t *testing.T) {
	// Same provider: only the given fields change (Dograh's model_copy).
	s, _, err := resolveServices(userConfig, `{"model_overrides": {"tts": {"voice": "anushka"}}}`)
	require.NoError(t, err)
	assert.Equal(t, TTSConfig{APIKey: "sarvam-key", Model: "bulbul:v3", Voice: "anushka", Language: "hi-IN"}, s.TTS)

	// Provider change: the override replaces the section; google needs no
	// stored base_url.
	s, _, err = resolveServices(userConfig, `{"model_overrides": {"llm": {"provider": "google", "api_key": "g", "model": "gemini-3.1-flash-lite"}}}`)
	require.NoError(t, err)
	assert.Equal(t, LLMConfig{Provider: "google", BaseURL: "https://generativelanguage.googleapis.com/v1beta/openai", APIKey: "g", Model: "gemini-3.1-flash-lite"}, s.LLM)

	// A key list: one of them.
	s, _, err = resolveServices(userConfig, `{"model_overrides": {"llm": {"api_key": ["k1", "k2"]}}}`)
	require.NoError(t, err)
	assert.Contains(t, []string{"k1", "k2"}, s.LLM.APIKey)
	assert.Equal(t, "gemini/gemini-3.1-flash-lite", s.LLM.Model, "same provider keeps the user's other fields")
}

func TestResolveServicesUnsupported(t *testing.T) {
	cases := map[string]string{
		"deepgram stt": `{"model_overrides": {"stt": {"provider": "deepgram", "api_key": "d", "model": "nova-3"}}}`,
		"azure llm":    `{"model_overrides": {"llm": {"provider": "azure", "api_key": "a", "model": "gpt"}}}`,
		"realtime":     `{"model_overrides": {"is_realtime": true}}`,
	}

	for name, overrides := range cases {
		_, _, err := resolveServices(userConfig, overrides)
		assert.Error(t, err, name)
	}

	_, _, err := resolveServices("", "")
	assert.ErrorContains(t, err, "no model configuration")

	_, warnings, err := resolveServices(userConfig, `{"model_overrides": {"stt": {"segmented": true}}}`)
	require.NoError(t, err)
	assert.Len(t, warnings, 1)
}
