package dograh

import (
	"encoding/json"
	"fmt"
	"math/rand/v2"
)

// Services are the LLM, STT and TTS one call uses, as configured in Dograh.
type Services struct {
	LLM LLMConfig
	STT STTConfig
	TTS TTSConfig
}

// LLMConfig is an OpenAI-compatible chat-completions endpoint.
type LLMConfig struct {
	Provider, BaseURL, APIKey, Model string
}

// STTConfig is Sarvam streaming speech-to-text.
type STTConfig struct {
	APIKey, Model, Language string
}

// TTSConfig is Sarvam streaming text-to-speech.
type TTSConfig struct {
	APIKey, Model, Voice, Language string
}

// llmBaseURLs are the OpenAI-compatible endpoints of LLM providers whose
// Dograh config carries no base_url; the others (bifrost, openrouter,
// sarvam, speaches) store theirs, and llmDefaultBaseURLs are Dograh's
// field defaults for those, used only when it's missing
// (api/services/configuration/registry.py).
var (
	llmBaseURLs = map[string]string{
		"openai": "https://api.openai.com/v1",
		"groq":   "https://api.groq.com/openai/v1",
		// Dograh talks to Gemini through pipecat's native Google client;
		// Vaani uses Google's OpenAI-compatible endpoint with the same key.
		"google": "https://generativelanguage.googleapis.com/v1beta/openai",
	}
	llmDefaultBaseURLs = map[string]string{
		"bifrost":    "http://192.168.26.130:8080/v1",
		"openrouter": "https://openrouter.ai/api/v1",
		"sarvam":     "https://api.sarvam.ai/v1",
		"speaches":   "http://localhost:11434/v1",
	}
)

// section is one service's config object (provider, api_key, model, ...).
type section map[string]any

// resolveServices builds a call's services the way Dograh does
// (run_pipeline.py + configuration/resolve.py): the workflow owner's
// user_configurations, with the workflow's model_overrides merged on top --
// per service, a different provider replaces the section, the same provider
// only changes the fields given. Anything Vaani can't run is an error:
// STT/TTS must be Sarvam, the LLM an OpenAI-compatible provider.
func resolveServices(userConfigJSON, workflowConfigJSON string) (Services, []string, error) {
	var user map[string]json.RawMessage
	if userConfigJSON == "" {
		return Services{}, nil, fmt.Errorf("the workflow's owner has no model configuration in Dograh")
	}

	if err := json.Unmarshal([]byte(userConfigJSON), &user); err != nil {
		return Services{}, nil, fmt.Errorf("unreadable user configuration: %w", err)
	}

	var wfCfg struct {
		Overrides map[string]json.RawMessage `json:"model_overrides"`
	}

	if workflowConfigJSON != "" {
		if err := json.Unmarshal([]byte(workflowConfigJSON), &wfCfg); err != nil {
			return Services{}, nil, fmt.Errorf("unreadable workflow_configurations: %w", err)
		}
	}

	realtime := false
	_ = json.Unmarshal(user["is_realtime"], &realtime)
	_ = json.Unmarshal(wfCfg.Overrides["is_realtime"], &realtime)

	if realtime {
		return Services{}, nil, fmt.Errorf("realtime (speech-to-speech) mode isn't supported")
	}

	effective := func(name string) (section, error) {
		var base, over section
		_ = json.Unmarshal(user[name], &base)

		if raw, ok := wfCfg.Overrides[name]; ok {
			if err := json.Unmarshal(raw, &over); err != nil {
				return nil, fmt.Errorf("unreadable %s override: %w", name, err)
			}
		}

		switch {
		case over == nil:
			// no override
		case base == nil || (over["provider"] != nil && over["provider"] != base["provider"]):
			base = over
		default:
			merged := section{}
			for k, v := range base {
				merged[k] = v
			}

			for k, v := range over {
				merged[k] = v
			}

			base = merged
		}

		if base == nil {
			return nil, fmt.Errorf("no %s configured in Dograh", name)
		}

		return base, nil
	}

	var (
		s        Services
		warnings []string
	)

	llmSec, err := effective("llm")
	if err != nil {
		return s, nil, err
	}

	s.LLM = LLMConfig{Provider: llmSec.str("provider"), APIKey: llmSec.apiKey(), Model: llmSec.str("model")}

	s.LLM.BaseURL = llmSec.str("base_url")
	if url, ok := llmBaseURLs[s.LLM.Provider]; ok {
		s.LLM.BaseURL = url
	} else if s.LLM.BaseURL == "" {
		s.LLM.BaseURL = llmDefaultBaseURLs[s.LLM.Provider]
	}

	if s.LLM.BaseURL == "" {
		return s, nil, fmt.Errorf("llm provider %q isn't supported (Vaani needs an OpenAI-compatible one: openai, google, groq, openrouter, sarvam, bifrost, speaches)", s.LLM.Provider)
	}

	if s.LLM.Model == "" {
		return s, nil, fmt.Errorf("llm has no model configured")
	}

	sttSec, err := effective("stt")
	if err != nil {
		return s, nil, err
	}

	if p := sttSec.str("provider"); p != "sarvam" {
		return s, nil, fmt.Errorf("stt provider %q isn't supported (Vaani supports sarvam)", p)
	}

	// Dograh's Sarvam defaults, for fields a stored config lacks.
	s.STT = STTConfig{APIKey: sttSec.apiKey(), Model: sttSec.strOr("model", "saaras:v3"), Language: sttSec.strOr("language", "hi-IN")}

	if seg, _ := sttSec["segmented"].(bool); seg {
		warnings = append(warnings, "sarvam stt 'segmented' mode isn't supported; streaming instead")
	}

	ttsSec, err := effective("tts")
	if err != nil {
		return s, nil, err
	}

	if p := ttsSec.str("provider"); p != "sarvam" {
		return s, nil, fmt.Errorf("tts provider %q isn't supported (Vaani supports sarvam)", p)
	}

	s.TTS = TTSConfig{
		APIKey:   ttsSec.apiKey(),
		Model:    ttsSec.strOr("model", "bulbul:v2"),
		Voice:    ttsSec.strOr("voice", "suhani"),
		Language: ttsSec.strOr("language", "hi-IN"),
	}

	if s.STT.APIKey == "" || s.TTS.APIKey == "" {
		return s, nil, fmt.Errorf("sarvam api_key missing in Dograh's configuration")
	}

	return s, warnings, nil
}

// resolveOwnerLLM is Dograh's resolve_user_llm_config (api/services/
// workflow/qa/llm_config.py): the workflow owner's LLM user-configuration,
// deliberately WITHOUT the workflow's model_overrides merged in -- unlike
// resolveServices's "llm" section, which IS the conversation's effective
// LLM. QA's qa_use_workflow_llm default reads this. "openai"/"gpt-4.1" are
// Dograh's own fallbacks for an owner with no llm configuration at all; ok
// is false when the resolved provider/model/key still can't make a call
// (QA then reports "no_api_key", as Dograh does).
func resolveOwnerLLM(userConfigJSON string) (cfg LLMConfig, ok bool) {
	var user map[string]json.RawMessage
	_ = json.Unmarshal([]byte(userConfigJSON), &user)

	var sec section
	_ = json.Unmarshal(user["llm"], &sec)

	cfg = LLMConfig{Provider: sec.strOr("provider", "openai"), Model: sec.strOr("model", "gpt-4.1"), APIKey: sec.apiKey()}

	if url, ok := llmBaseURLs[cfg.Provider]; ok {
		cfg.BaseURL = url
	} else if cfg.BaseURL = sec.str("base_url"); cfg.BaseURL == "" {
		cfg.BaseURL = llmDefaultBaseURLs[cfg.Provider]
	}

	return cfg, cfg.BaseURL != "" && cfg.APIKey != ""
}

func (s section) str(key string) string {
	v, _ := s[key].(string)
	return v
}

func (s section) strOr(key, def string) string {
	if v := s.str(key); v != "" {
		return v
	}

	return def
}

// apiKey is the section's api_key; when it's a list, one picked at random
// per call, as Dograh does to spread load across keys.
func (s section) apiKey() string {
	switch v := s["api_key"].(type) {
	case string:
		return v
	case []any:
		if len(v) == 0 {
			return ""
		}

		k, _ := v[rand.IntN(len(v))].(string)

		return k
	}

	return ""
}
