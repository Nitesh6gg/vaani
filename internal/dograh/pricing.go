package dograh

import (
	"math"
	"strings"
)

// Keys of Dograh's usage_info ("<processor>|||<model>"); its cost job prices
// by the model part only. QAAnalysis is Dograh's own name for QA tokens
// (run_integrations.py's _update_usage_info_with_qa_tokens).
const (
	UsageKeyLLM = "LLM|||"
	usageKeyQA  = "QAAnalysis|||"
	usageKeyTTS = "SarvamTTSService|||"
	usageKeySTT = "SarvamSTTService|||"
)

// inrPerUSD is Dograh's conversion rate for Sarvam's INR prices.
const inrPerUSD = 96.5

// tokenPrice is Dograh's TokenPricingModel: USD per token, cached input
// billed at (1 - cacheReadDiscount) of the input price.
type tokenPrice struct {
	prompt, completion, cacheReadDiscount float64
}

func perM(prompt, completion float64) tokenPrice {
	return tokenPrice{prompt: prompt / 1e6, completion: completion / 1e6, cacheReadDiscount: 0.5}
}

// sarvamINR is a Sarvam rate card entry, INR per 1M tokens.
func sarvamINR(input, cached, output float64) tokenPrice {
	return tokenPrice{
		prompt:            input / inrPerUSD / 1e6,
		completion:        output / inrPerUSD / 1e6,
		cacheReadDiscount: 1 - cached/input,
	}
}

// llmPrices is Dograh's LLM_PRICING (api/services/pricing/llm.py), except
// Sarvam's entries, which follow Sarvam's own rate card (Oct 2026: 105B and
// 105B Conversations Rs15/5/60 per 1M input/cached/output, Gemma 4 31B
// Rs35/10/90, GLM 5.3 Rs126/20/390).
var llmPrices = map[string]map[string]tokenPrice{
	"openai": {
		"gpt-3.5-turbo":                tokenPrice{0.0015 / 1000, 0.002 / 1000, 0.5},
		"gpt-4":                        tokenPrice{0.03 / 1000, 0.06 / 1000, 0.5},
		"gpt-4.1":                      perM(2.00, 8.00),
		"gpt-4.1-mini":                 perM(0.40, 1.60),
		"gpt-4.1-nano":                 perM(0.10, 0.40),
		"gpt-4.5-preview":              perM(75.00, 150.00),
		"gpt-4o":                       perM(2.50, 10.00),
		"gpt-4o-audio-preview":         perM(2.50, 10.00),
		"gpt-4o-realtime-preview":      perM(5.00, 20.00),
		"gpt-4o-mini":                  perM(0.15, 0.60),
		"gpt-4o-mini-audio-preview":    perM(0.15, 0.60),
		"gpt-4o-mini-realtime-preview": perM(0.60, 2.40),
		"gpt-4o-search-preview":        perM(2.50, 10.00),
		"gpt-4o-mini-search-preview":   perM(0.15, 0.60),
		"o1":                           perM(15.00, 60.00),
		"o1-pro":                       perM(150.00, 600.00),
		"o1-mini":                      perM(1.10, 4.40),
		"o3":                           perM(10.00, 40.00),
		"o3-mini":                      perM(1.10, 4.40),
		"o4-mini":                      perM(1.10, 4.40),
		"computer-use-preview":         perM(3.00, 12.00),
		"gpt-image-1":                  perM(5.00, 0),
		"codex-mini-latest":            perM(1.50, 6.00),
		"gpt-4o-transcribe":            perM(2.50, 10.00),
		"gpt-4o-mini-transcribe":       perM(1.25, 5.00),
		"gpt-4o-mini-tts":              perM(0.60, 0),
	},
	"google": {
		"gemini-3.5-flash":      perM(1.50, 9.00),
		"gemini-3.1-pro":        perM(2.00, 12.00),
		"gemini-3.1-flash-lite": perM(0.25, 1.50),
		"gemini-2.5-flash":      perM(0.30, 2.50),
		"gemini-2.5-flash-lite": perM(0.10, 0.40),
		"default":               perM(0.25, 1.50),
	},
	"groq": {
		"llama-3.3-70b-versatile":       perM(0.59, 0.79),
		"llama-3.1-8b-instant":          perM(0.05, 0.08),
		"openai/gpt-oss-120b":           perM(0.15, 0.60),
		"openai/gpt-oss-20b":            perM(0.075, 0.30),
		"deepseek-r1-distill-llama-70b": perM(0.59, 0.79),
		"default":                       perM(0.59, 0.79),
	},
	"sarvam": {
		"sarvam-105b":               sarvamINR(15, 5, 60),
		"sarvam-105b-conversations": sarvamINR(15, 5, 60),
		"gemma4":                    sarvamINR(35, 10, 90),
		"glm5.3":                    sarvamINR(126, 20, 390),
		"default":                   sarvamINR(15, 5, 60),
	},
	"azure": {
		"gpt-4.1-mini": perM(0.44, 8.80),
	},
}

// ttsPrices is Dograh's TTS_PRICING: USD per character.
var ttsPrices = map[string]map[string]float64{
	"openai": {"gpt-4o-mini-tts": 0.6 / 1e6, "default": 0.6 / 1e6},
	// Sarvam's rate card (Oct 2026): Bulbul v3 Rs2.7 and v4 Flash Rs1.7 per
	// 1K characters, whichever API is used. Dograh's table still has v3 at
	// Rs3 per 1K and no v4 Flash, which would then be charged the v3 rate.
	"sarvam": {"bulbul:v3": 2.7 / 1000 / inrPerUSD, "bulbul:v4-flash": 1.7 / 1000 / inrPerUSD,
		"bulbul:v2": 15 / inrPerUSD / 10_000, "default": 2.7 / 1000 / inrPerUSD},
	"deepgram":   {"aura-2": 0.030 / 1000, "aura-1": 0.015 / 1000, "default": 0.030 / 1000},
	"elevenlabs": {"default": 0.0256 / 1000},
	"default":    {"default": 0.030 / 1000},
}

// sttPrices is Dograh's STT_PRICING: USD per second.
var sttPrices = map[string]map[string]float64{
	"deepgram": {"nova-3-general": 0.0048 / 60, "nova-2": 0.0058 / 60, "default": 0.0048 / 60},
	"sarvam": {
		"saaras:v3": 30 / inrPerUSD / 3600, "saaras:v4": 30 / inrPerUSD / 3600,
		"saarika:v2.5": 30 / inrPerUSD / 3600, "saaras:v2.5": 30 / inrPerUSD / 3600,
		"default": 30 / inrPerUSD / 3600,
	},
	"openai":  {"gpt-4o-transcribe": 0.015 / 60, "default": 0.015 / 60},
	"default": {"default": 0.0077 / 60},
}

// firstProvider is Dograh's fallback provider per service: the first key of
// its registry dict.
var firstProvider = map[string]string{"llm": "openai", "tts": "openai", "stt": "deepgram"}

// inferProvider is cost_calculator.py's _infer_provider_from_model.
func inferProvider(model, service string) string {
	m := strings.ToLower(model)

	has := func(words ...string) bool {
		for _, w := range words {
			if strings.Contains(m, w) {
				return true
			}
		}

		return false
	}

	switch {
	case m == "":
		return "unknown"
	case has("gemini"):
		return "google"
	case has("saaras", "saarika", "bulbul", "sarvam"):
		return "sarvam"
	case has("groq", "gpt-oss", "llama"):
		return "groq"
	case has("gpt", "whisper", "openai"):
		return "openai"
	case has("eleven"):
		return "elevenlabs"
	case has("deepgram", "nova", "phonecall", "general"):
		return "deepgram"
	default:
		return firstProvider[service]
	}
}

// lookupPrice is cost_calculator.py's get_pricing_model: the provider's
// model, else its "default", else the "default" provider's. One addition:
// a model not found as written is retried without a gateway prefix
// ("sarvam/sarvam-105b-conversations" -> "sarvam-105b-conversations").
// Dograh misses those and falls back to the provider default, which for
// Sarvam's conversation model was the wrong rate.
func lookupPrice[P any](table map[string]map[string]P, provider, model string) (P, bool) {
	try := func(prov, mod string) (P, bool) {
		p, ok := table[prov][mod]
		return p, ok
	}

	if p, ok := try(provider, model); ok {
		return p, true
	}

	if i := strings.Index(model, "/"); i >= 0 {
		if p, ok := try(provider, model[i+1:]); ok {
			return p, true
		}
	}

	if p, ok := try(provider, "default"); ok {
		return p, true
	}

	if p, ok := try("default", model); ok {
		return p, true
	}

	return try("default", "default")
}

// modelOf is a usage key's model part (cost_calculator.py's _parse_key).
func modelOf(key string) string {
	if _, m, ok := strings.Cut(key, "|||"); ok {
		return m
	}

	return "unknown"
}

// tokenUsage is one usage_info "llm" entry, as Dograh stores it.
type tokenUsage struct {
	PromptTokens             int  `json:"prompt_tokens"`
	CompletionTokens         int  `json:"completion_tokens"`
	TotalTokens              int  `json:"total_tokens"`
	CacheReadInputTokens     int  `json:"cache_read_input_tokens"`
	CacheCreationInputTokens *int `json:"cache_creation_input_tokens"`
}

// usageInfo is a run's usage_info: Dograh's
// get_all_usage_metrics_serialized shape.
type usageInfo struct {
	LLM                 map[string]tokenUsage `json:"llm"`
	TTS                 map[string]int        `json:"tts"`
	STT                 map[string]float64    `json:"stt"`
	CallDurationSeconds int                   `json:"call_duration_seconds"`
}

// costBreakdown is cost_calculator.py's calculate_total_cost result.
type costBreakdown struct {
	LLM   float64 `json:"llm_cost"`
	TTS   float64 `json:"tts_cost"`
	STT   float64 `json:"stt_cost"`
	Total float64 `json:"total"`
}

// calculateCost prices u as Dograh's calculate_total_cost does.
func calculateCost(u usageInfo) costBreakdown {
	var c costBreakdown

	for key, t := range u.LLM {
		model := modelOf(key)

		p, ok := lookupPrice(llmPrices, inferProvider(model, "llm"), model)
		if !ok {
			continue
		}

		cost := float64(t.PromptTokens)*p.prompt + float64(t.CompletionTokens)*p.completion -
			float64(t.CacheReadInputTokens)*p.prompt*p.cacheReadDiscount
		c.LLM += math.Max(cost, 0)
	}

	for key, chars := range u.TTS {
		model := modelOf(key)
		if p, ok := lookupPrice(ttsPrices, inferProvider(model, "tts"), model); ok {
			c.TTS += float64(chars) * p
		}
	}

	for key, secs := range u.STT {
		model := modelOf(key)
		if p, ok := lookupPrice(sttPrices, inferProvider(model, "stt"), model); ok {
			c.STT += secs * p
		}
	}

	c.Total = c.LLM + c.TTS + c.STT

	return c
}
