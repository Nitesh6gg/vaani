package agent

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nitesh/vaani/internal/ai/tts"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLLMFailurePolicy_Line(t *testing.T) {
	cases := []struct {
		name       string
		p          LLMFailurePolicy
		n          int
		wantKind   string
		wantHangup bool
		wantText   string // substring
	}{
		{"first failure apologises (hindi)", LLMFailurePolicy{MaxFailedTurns: 3, Language: "hi-IN"}, 1, "apology", false, "दोबारा"},
		{"first failure apologises (english)", LLMFailurePolicy{MaxFailedTurns: 3, Language: "en-IN"}, 1, "apology", false, "say it again"},
		{"override text", LLMFailurePolicy{Apology: "Please repeat", Language: "hi-IN"}, 2, "apology", false, "Please repeat"},
		{"third in a row hangs up", LLMFailurePolicy{MaxFailedTurns: 3, Language: "hi-IN"}, 3, "hangup", true, "धन्यवाद"},
		{"hangup in english", LLMFailurePolicy{MaxFailedTurns: 2}, 2, "hangup", true, "call you back"},
		{"max 0 never hangs up", LLMFailurePolicy{}, 99, "apology", false, ""},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			text, kind, hangup := tc.p.line(tc.n)
			assert.Equal(t, tc.wantKind, kind)
			assert.Equal(t, tc.wantHangup, hangup)
			assert.Contains(t, text, tc.wantText)
			assert.NotEmpty(t, text)
		})
	}
}

// TestHandler_LLMFailureSpeaksApologyThenHangsUp: a turn the LLM fails with
// nothing said gets a spoken apology (not silence); the MaxFailedTurns-th in
// a row says goodbye and hangs up, with end reason llm_unavailable.
func TestHandler_LLMFailureSpeaksApologyThenHangsUp(t *testing.T) {
	fSTT := newFakeSTT()
	fTTS := newFakeTTS()
	fLLM := &fakeLLM{err: errors.New("llm: non-2xx response: 429")}

	var hangups atomic.Int32

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cfg := testConfig(fSTT, fTTS, fLLM, 0, 0)
	cfg.Hangup = func() { hangups.Add(1) }
	cfg.Log = &CallLog{}
	cfg.LLMFailures = LLMFailurePolicy{MaxFailedTurns: 2, Apology: "please repeat"}
	h := NewHandler(ctx, "call1", cfg)

	// Turn 1: apology, back to listening, call still up.
	fSTT.sendFinal("hello")
	require.Eventually(t, func() bool { return fTTS.spokenCount() == 1 }, time.Second, time.Millisecond)
	assert.Contains(t, fTTS.spokenList()[0], "please repeat")

	fTTS.audio <- tts.Chunk{PCM: constFrame(quietAmplitude), Gen: 1, Req: 1, Text: "please repeat"}
	require.Eventually(t, func() bool { return h.State() == StateSpeaking }, time.Second, time.Millisecond)
	fTTS.done <- 1
	drainUntilListening(t, h)
	assert.Zero(t, hangups.Load())

	// Turn 2: goodbye, then hang up.
	fSTT.sendFinal("hello again")
	require.Eventually(t, func() bool { return fTTS.spokenCount() == 2 }, time.Second, time.Millisecond)
	assert.Contains(t, fTTS.spokenList()[1], "technical problem")

	fTTS.audio <- tts.Chunk{PCM: constFrame(quietAmplitude), Gen: 2, Req: 2, Text: "bye"}
	require.Eventually(t, func() bool { return h.State() == StateSpeaking }, time.Second, time.Millisecond)
	fTTS.done <- 2
	drainUntilListening(t, h)
	require.Eventually(t, func() bool { return hangups.Load() == 1 }, time.Second, time.Millisecond)

	cancel()
	<-h.Done()
	assert.Equal(t, EndReasonLLMDown, cfg.Log.Summary().EndReason)
}
