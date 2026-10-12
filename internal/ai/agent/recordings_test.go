package agent

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/nitesh/vaani/internal/ai/llm"
)

// TestClipRouter: Dograh's RecordingRouterProcessor, token by token.
func TestClipRouter(t *testing.T) {
	for _, tc := range []struct {
		name   string
		tokens []string
		speak  string
		id     string
	}{
		{"tts marker stripped", []string{"▸ Hello", " there."}, "Hello there.", ""},
		{"recording id ends at a space", []string{"●", " offer_01", " Our offer is…"}, "", "offer_01"},
		{"recording id at the end", []string{"● offer_01"}, "", "offer_01"},
		{"no marker: spoken as-is", []string{"Hello ", "there."}, "Hello there.", ""},
		{"second marker drops the rest", []string{"▸ Yes. ● rec_01 x"}, "Yes. ", ""},
		{"text before the marker dropped", []string{"\n", "▸", "Hi"}, "Hi", ""},
		{"recording then tts marker", []string{"● rec_01", "▸ more"}, "", "rec_01"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var (
				r          clipRouter
				speak, ids string
			)

			for _, tok := range tc.tokens {
				s, id := r.feed(tok)
				speak += s
				ids += id
			}

			s, id := r.flush()
			speak += s
			ids += id

			assert.Equal(t, tc.speak, speak)
			assert.Equal(t, tc.id, ids)
		})
	}
}

// TestClipRouterSpeakRest: when the recording turns out not to exist, what
// the LLM wrote after the id becomes ordinary speech, however the tokens
// split.
func TestClipRouterSpeakRest(t *testing.T) {
	for _, tc := range []struct {
		name   string
		tokens []string
		want   string
	}{
		{"split across tokens", []string{"● miss", "ing_01", " Hi", " there."}, "Hi there."},
		{"all in one token", []string{"● missing_01 Hi there."}, "Hi there."},
		{"id and a space only", []string{"● missing_01 "}, ""},
		{"second marker still ends it", []string{"● missing_01 Hi ● other"}, "Hi "},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var (
				r     clipRouter
				spoke string
			)

			for _, tok := range tc.tokens {
				s, id := r.feed(tok)
				spoke += s

				if id != "" {
					assert.Equal(t, "missing_01", id)

					spoke += r.speakRest()
				}
			}

			s, id := r.flush()
			assert.Empty(t, id)

			assert.Equal(t, tc.want, spoke+s)
		})
	}
}

func (f *fakeTTS) callList() []string {
	f.mu.Lock()
	defer f.mu.Unlock()

	return append([]string(nil), f.calls...)
}

func TestHandler_RecordedGreeting(t *testing.T) {
	fSTT := newFakeSTT()
	fTTS := newFakeTTS()

	cfg := testConfig(fSTT, fTTS, &fakeLLM{}, 0, 0)
	cfg.Start = &Node{GreetingClip: &Clip{ID: "hello", PCM: []byte{1, 2}, Transcript: "Namaste!"}}
	NewHandler(context.Background(), "call1", cfg)

	require.Eventually(t, func() bool { return len(fTTS.callList()) > 0 }, time.Second, time.Millisecond)
	assert.Equal(t, []string{"play:Namaste!"}, fTTS.callList(), "played, never synthesized")
	require.Eventually(t, func() bool { return len(fTTS.endedGens()) == 1 }, time.Second, time.Millisecond)
}

// TestHandler_RecordedTransitionAndReply: an audio transition plays its
// clip, and at a node with recordings the LLM's "● <id>" reply plays that
// recording -- recorded with marker, id and transcript -- instead of TTS.
func TestHandler_RecordedTransitionAndReply(t *testing.T) {
	fSTT := newFakeSTT()
	fTTS := newFakeTTS()

	offer := &Clip{ID: "offer_01", PCM: []byte{3}, Transcript: "Our offer."}
	main := &Node{Name: "Main", Clips: map[string]*Clip{"offer_01": offer}}
	start := &Node{Name: "Start", Edges: []Edge{
		{Def: llm.FunctionDef{Name: "go"}, SpeechClip: &Clip{ID: "moving", PCM: []byte{2}, Transcript: "Ek second."}, To: main},
	}}

	fLLM := &fakeLLM{rounds: []fakeRound{
		{toolCalls: []llm.ToolCall{{ID: "e", Type: "function", Function: llm.FunctionCall{Name: "go", Arguments: "{}"}}}},
		{tokens: []string{"● ", "offer_01 ", "Our offer."}},
	}}

	cfg := testConfig(fSTT, fTTS, fLLM, 0, 0)
	cfg.Start = start
	NewHandler(context.Background(), "call1", cfg)

	fSTT.sendFinal("haan")
	require.Eventually(t, func() bool { return len(fTTS.callList()) == 2 }, time.Second, time.Millisecond)
	assert.Equal(t, []string{"play:Ek second. ", "play:● offer_01 Our offer. "}, fTTS.callList())
}

// TestHandler_UnknownRecordingSpeaksItsText: the LLM asked for a recording
// that doesn't exist (it invented the id); the caller hears the text it
// wrote after the id, not silence. With nothing written after the id there
// is nothing to say.
func TestHandler_UnknownRecordingSpeaksItsText(t *testing.T) {
	for _, tc := range []struct {
		name   string
		tokens []string
		want   []string
	}{
		{"with its text", []string{"● ", "missing_01 ", "धन्यवाद, आपका दिन शुभ हो."}, []string{"speak:धन्यवाद, आपका दिन शुभ हो."}},
		{"id only", []string{"● missing_01"}, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fSTT := newFakeSTT()
			fTTS := newFakeTTS()

			cfg := testConfig(fSTT, fTTS, &fakeLLM{tokens: tc.tokens}, 0, 0)
			cfg.Start = &Node{AllowInterrupt: true, Clips: map[string]*Clip{}}
			NewHandler(context.Background(), "call1", cfg)

			fSTT.sendFinal("haan")
			require.Eventually(t, func() bool { return len(fTTS.endedGens()) == 1 }, time.Second, time.Millisecond)
			assert.Equal(t, tc.want, fTTS.callList())
		})
	}
}

func TestHandler_RecordedToolMessage(t *testing.T) {
	fSTT := newFakeSTT()
	fTTS := newFakeTTS()

	start := &Node{Name: "Start", Tools: []Tool{{
		Def:         llm.FunctionDef{Name: "end_call"},
		Kind:        ToolEndCall,
		MessageClip: &Clip{ID: "bye", PCM: []byte{4}, Transcript: "Dhanyawad!"},
	}}}
	fLLM := &fakeLLM{rounds: []fakeRound{
		{toolCalls: []llm.ToolCall{{ID: "t", Type: "function", Function: llm.FunctionCall{Name: "end_call", Arguments: "{}"}}}},
	}}

	cfg := testConfig(fSTT, fTTS, fLLM, 0, 0)
	cfg.Start = start
	NewHandler(context.Background(), "call1", cfg)

	fSTT.sendFinal("bas")
	require.Eventually(t, func() bool { return len(fTTS.callList()) == 1 }, time.Second, time.Millisecond)
	assert.Equal(t, []string{"play:Dhanyawad! "}, fTTS.callList())
}

// TestHandler_RecordedClipCostsNoTTSCharacters: only synthesized text is
// billed.
func TestHandler_RecordedClipCostsNoTTSCharacters(t *testing.T) {
	fSTT := newFakeSTT()
	fTTS := newFakeTTS()

	cfg := testConfig(fSTT, fTTS, &fakeLLM{}, 0, 0)
	cfg.Start = &Node{GreetingClip: &Clip{ID: "hello", PCM: []byte{1}, Transcript: "Namaste!"}}
	cfg.Log = &CallLog{}
	NewHandler(context.Background(), "call1", cfg)

	require.Eventually(t, func() bool { return len(fTTS.callList()) == 1 }, time.Second, time.Millisecond)
	assert.Zero(t, cfg.Log.Summary().TTSChars)

}
