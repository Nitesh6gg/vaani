package agent

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/nitesh/vaani/internal/ai/llm"
	"github.com/nitesh/vaani/internal/ai/stt"
	"github.com/nitesh/vaani/internal/ai/tts"
)

// fakeSTT records every fed frame and lets a test push transcript results.
type fakeSTT struct {
	mu      sync.Mutex
	fed     [][]byte
	results chan stt.Result
	closed  bool
}

func newFakeSTT() *fakeSTT { return &fakeSTT{results: make(chan stt.Result, 16)} }

func (f *fakeSTT) Feed(pcm []byte) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	cp := make([]byte, len(pcm))
	copy(cp, pcm)
	f.fed = append(f.fed, cp)

	return nil
}

func (f *fakeSTT) Results() <-chan stt.Result { return f.results }

func (f *fakeSTT) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()

	if !f.closed {
		close(f.results)
		f.closed = true
	}

	return nil
}

func (f *fakeSTT) fedCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()

	return len(f.fed)
}

func (f *fakeSTT) sendFinal(text string) { f.results <- stt.Result{Text: text, Final: true} }

// fakeTTS records Speak/Cancel calls and lets a test control audio/done
// output. It mirrors the real client's call-scoped, reused-across-turns
// design: Audio and Done only close on Cancel/Close (connection torn down for
// good), never between turns -- a test signals one turn's completion by
// sending its generation on the done channel, not by closing anything.
type fakeTTS struct {
	mu         sync.Mutex
	spoken     []string
	spokenGens []uint64
	ended      []uint64
	cancelled  bool
	audio      chan tts.Chunk
	done       chan uint64
	closed     bool
}

func newFakeTTS() *fakeTTS {
	return &fakeTTS{audio: make(chan tts.Chunk, 16), done: make(chan uint64, 16)}
}

func (f *fakeTTS) Speak(text string, gen uint64) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.spoken = append(f.spoken, text)
	f.spokenGens = append(f.spokenGens, gen)

	return nil
}

func (f *fakeTTS) EndGeneration(gen uint64) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.ended = append(f.ended, gen)
}

func (f *fakeTTS) Audio() <-chan tts.Chunk { return f.audio }
func (f *fakeTTS) Done() <-chan uint64     { return f.done }

// Cancel matches the real client's immediate-close semantics (barge-in cut
// #2): it closes Audio()/Done() right away, discarding anything in flight.
func (f *fakeTTS) Cancel() error {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.cancelled = true

	if !f.closed {
		close(f.audio)
		close(f.done)
		f.closed = true
	}

	return nil
}

// Close matches the real client's call-teardown semantics: unlike Cancel it's
// only ever invoked once, from run()'s ctx.Done() path, so an immediate close
// here is safe.
func (f *fakeTTS) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()

	if !f.closed {
		close(f.audio)
		close(f.done)
		f.closed = true
	}

	return nil
}

func (f *fakeTTS) wasCancelled() bool {
	f.mu.Lock()
	defer f.mu.Unlock()

	return f.cancelled
}

func (f *fakeTTS) spokenCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()

	return len(f.spoken)
}

// fakeLLM streams a fixed token list, respecting context cancellation.
type fakeLLM struct {
	tokens []string
	err    error
}

func (f *fakeLLM) Stream(ctx context.Context, _ []llm.Message, onToken func(string)) error {
	for _, tok := range f.tokens {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		onToken(tok)
	}

	return f.err
}

func testConfig(sttClient stt.Client, ttsClient tts.Client, llmClient llmStreamer, guard, postCut time.Duration) Config {
	return Config{
		STT:            sttClient,
		NewTTS:         func() (tts.Client, error) { return ttsClient, nil },
		LLM:            llmClient,
		BargeIn:        NewEnergyDetector(floor),
		BargeInGuard:   guard,
		PostCutSilence: postCut,
		Sink:           NoopSink{},
	}
}

func TestHandler_GreetingIsSpokenWithoutWaitingForCaller(t *testing.T) {
	fSTT := newFakeSTT()
	fTTS := newFakeTTS()

	h := NewHandler(context.Background(), "call1", Config{
		STT:      fSTT,
		NewTTS:   func() (tts.Client, error) { return fTTS, nil },
		LLM:      &fakeLLM{}, // must never be called for the greeting
		Greeting: "Hi, this side Shubh.",
		BargeIn:  NewEnergyDetector(floor),
		Sink:     NoopSink{},
	})

	require.Eventually(t, func() bool { return fTTS.spokenCount() > 0 }, time.Second, time.Millisecond)
	assert.Equal(t, []string{"Hi, this side Shubh."}, fTTS.spoken)

	const gen = 1

	fTTS.audio <- tts.Chunk{PCM: constFrame(loudAmplitude), Gen: gen}
	require.Eventually(t, func() bool { return h.State() == StateSpeaking }, time.Second, time.Millisecond)

	fTTS.done <- gen
	drainUntilListening(t, h)

	assert.Equal(t, []llm.Message{{Role: "assistant", Content: "Hi, this side Shubh."}}, h.history,
		"the greeting must be recorded so the LLM doesn't redundantly re-greet")
}

// drainUntilListening keeps calling ProcessFrame (as CallMedia's releaseLoop
// would, once per 20ms tick) until the Handler reports Listening. Needed
// after pushing a fake Done signal: the turn doesn't actually end until
// ProcessFrame drains the last of outbound and observes delivery is also
// done -- see handleTTSDone/processSpeakingFrame's comments.
func drainUntilListening(t *testing.T, h *Handler) {
	t.Helper()

	require.Eventually(t, func() bool {
		h.ProcessFrame(context.Background(), "call1", constFrame(quietAmplitude))
		return h.State() == StateListening
	}, time.Second, time.Millisecond)
}

func TestHandler_NoGreetingConfiguredStaysSilentUntilCallerSpeaks(t *testing.T) {
	fSTT := newFakeSTT()
	h := NewHandler(context.Background(), "call1", testConfig(fSTT, newFakeTTS(), &fakeLLM{}, 0, 0))

	time.Sleep(20 * time.Millisecond) // let NewHandler's goroutines settle

	assert.Equal(t, StateListening, h.State())
	assert.Empty(t, h.history)
}

func TestHandler_ListeningFeedsSTTAndReturnsNoAudio(t *testing.T) {
	fSTT := newFakeSTT()
	h := NewHandler(context.Background(), "call1", testConfig(fSTT, newFakeTTS(), &fakeLLM{}, 0, 0))

	out := h.ProcessFrame(context.Background(), "call1", constFrame(quietAmplitude))

	assert.Nil(t, out)
	assert.Equal(t, 1, fSTT.fedCount())
	assert.Equal(t, StateListening, h.State())
}

// TestHandler_PlaysFullReplyEvenAfterTTSDoneFires is a regression test for the
// bug where a reply was cut off almost immediately: handleTTSDone fires once
// Sarvam has finished SENDING a generation's audio, which happens far faster
// than real time, long before ProcessFrame (draining at exactly one 20ms
// frame per tick) has had a chance to play it all. The old code moved
// straight to Listening right there, which stopped ProcessFrame from
// draining outbound at all -- most of every reply was simply never played,
// left to leak out as stale fragments of later turns instead.
func TestHandler_PlaysFullReplyEvenAfterTTSDoneFires(t *testing.T) {
	fSTT := newFakeSTT()
	fTTS := newFakeTTS()
	fLLM := &fakeLLM{tokens: []string{"hi"}}

	h := NewHandler(context.Background(), "call1", testConfig(fSTT, fTTS, fLLM, 0, 0))

	fSTT.sendFinal("hi")
	require.Eventually(t, func() bool { return fTTS.spokenCount() > 0 }, time.Second, time.Millisecond)

	const gen = 1
	const frameCount = 5

	for i := 0; i < frameCount; i++ {
		fTTS.audio <- tts.Chunk{PCM: constFrame(loudAmplitude), Gen: gen}
	}

	require.Eventually(t, func() bool { return h.State() == StateSpeaking }, time.Second, time.Millisecond)

	// Sarvam finished sending before the caller has heard any of it -- this
	// must not end the turn while outbound still holds unplayed frames.
	fTTS.done <- gen
	time.Sleep(20 * time.Millisecond) // let run() process the Done event
	assert.Equal(t, StateSpeaking, h.State(), "must not drop to Listening while outbound still has queued frames")

	played := 0

	for played < frameCount {
		out := h.ProcessFrame(context.Background(), "call1", constFrame(quietAmplitude))
		if len(out) == 1 {
			played++
		}
	}

	assert.Equal(t, frameCount, played, "every queued frame must be played, not dropped")
	assert.Equal(t, StateSpeaking, h.State(), "state only flips once an empty tick is observed after the queue drains")

	drainUntilListening(t, h)
}

func TestHandler_FullTurnEndToEnd(t *testing.T) {
	fSTT := newFakeSTT()
	fTTS := newFakeTTS()
	fLLM := &fakeLLM{tokens: []string{"Hello", " there."}}

	h := NewHandler(context.Background(), "call1", testConfig(fSTT, fTTS, fLLM, 0, 0))

	fSTT.sendFinal("hi there")

	require.Eventually(t, func() bool { return fTTS.spokenCount() > 0 }, time.Second, time.Millisecond)

	const gen = 1 // this is the handler's first-ever turn

	// State only flips to Speaking once real audio arrives, not merely once
	// Speak() sends text -- see sendToTTS/handleTTSAudio's comments.
	fTTS.audio <- tts.Chunk{PCM: constFrame(loudAmplitude), Gen: gen}

	require.Eventually(t, func() bool { return h.State() == StateSpeaking }, time.Second, time.Millisecond)

	var gotFrame []byte

	require.Eventually(t, func() bool {
		out := h.ProcessFrame(context.Background(), "call1", constFrame(quietAmplitude))
		if len(out) == 1 {
			gotFrame = out[0]
			return true
		}

		return false
	}, time.Second, time.Millisecond)

	assert.Len(t, gotFrame, 640)

	fTTS.done <- gen
	drainUntilListening(t, h)
}

// TestHandler_AudioBeforeDoneEvenWhenBothArriveTogether is a regression test
// for a race in readTTS: the TTS client always pushes a generation's audio
// before its Done signal, but a plain `select` between the two channels
// doesn't preserve that ordering once both are already buffered -- it can
// pick Done first, which flips state straight to Listening before
// handleTTSAudio's first chunk for that generation ever gets a chance to
// enter Speaking (its CompareAndSwap then fails silently and never retries).
// Symptom in production: TTS audio was buffered but never drained, with
// zero errors -- it only became visible once enough turns' worth of
// never-drained audio finally overflowed the outbound buffer.
func TestHandler_AudioBeforeDoneEvenWhenBothArriveTogether(t *testing.T) {
	fSTT := newFakeSTT()
	fTTS := newFakeTTS()
	fLLM := &fakeLLM{tokens: []string{"hi"}}

	h := NewHandler(context.Background(), "call1", testConfig(fSTT, fTTS, fLLM, 0, 0))

	fSTT.sendFinal("hi")
	require.Eventually(t, func() bool { return fTTS.spokenCount() > 0 }, time.Second, time.Millisecond)

	const gen = 1

	// A burst, not a single chunk: readTTS needs a backlog still sitting in
	// audioCh at the moment doneCh also becomes ready for the race to have any
	// window to occur at all -- a single chunk tends to already be drained by
	// the time Done is pushed, since readTTS's consumption is fast relative to
	// two sequential sends from this goroutine.
	for i := 0; i < 20; i++ {
		fTTS.audio <- tts.Chunk{PCM: constFrame(loudAmplitude), Gen: gen}
	}
	fTTS.done <- gen

	require.Eventually(t, func() bool { return h.State() == StateSpeaking }, time.Second, time.Millisecond)
	assert.NotZero(t, h.speakingSince.Load(),
		"Speaking must have been entered from the audio chunk even though Done arrived in the same batch")

	drainUntilListening(t, h)
}

func TestHandler_BargeInExecutesCutsAndFlushesPreroll(t *testing.T) {
	fSTT := newFakeSTT()
	fTTS := newFakeTTS()
	fLLM := &fakeLLM{tokens: []string{"a long response the caller interrupts"}}

	h := NewHandler(context.Background(), "call1", testConfig(fSTT, fTTS, fLLM, 0, 50*time.Millisecond))

	fSTT.sendFinal("hi")
	require.Eventually(t, func() bool { return fTTS.spokenCount() > 0 }, time.Second, time.Millisecond)

	fTTS.audio <- tts.Chunk{PCM: constFrame(quietAmplitude), Gen: 1}
	require.Eventually(t, func() bool { return h.State() == StateSpeaking }, time.Second, time.Millisecond)

	// Quiet SPEAKING frames first, so preroll has content besides the trigger.
	h.ProcessFrame(context.Background(), "call1", constFrame(quietAmplitude))
	h.ProcessFrame(context.Background(), "call1", constFrame(quietAmplitude))

	fedBefore := fSTT.fedCount()

	h.ProcessFrame(context.Background(), "call1", constFrame(loudAmplitude))
	h.ProcessFrame(context.Background(), "call1", constFrame(loudAmplitude))
	out := h.ProcessFrame(context.Background(), "call1", constFrame(loudAmplitude))

	assert.Nil(t, out, "the triggering tick returns no agent audio")
	assert.Equal(t, StateTranscribing, h.State(), "must flip state immediately, not wait for the event loop")

	require.Eventually(t, fTTS.wasCancelled, time.Second, time.Millisecond, "cut #2: TTS must be cancelled")
	assert.Greater(t, fSTT.fedCount(), fedBefore, "preroll frames must be flushed to STT")
}

func TestHandler_BargeInRespectsGuardWindow(t *testing.T) {
	fSTT := newFakeSTT()
	fTTS := newFakeTTS()
	fLLM := &fakeLLM{tokens: []string{"hi"}}

	h := NewHandler(context.Background(), "call1", testConfig(fSTT, fTTS, fLLM, time.Hour, 0))

	fSTT.sendFinal("hi")
	require.Eventually(t, func() bool { return fTTS.spokenCount() > 0 }, time.Second, time.Millisecond)

	fTTS.audio <- tts.Chunk{PCM: constFrame(quietAmplitude), Gen: 1}
	require.Eventually(t, func() bool { return h.State() == StateSpeaking }, time.Second, time.Millisecond)

	for i := 0; i < 10; i++ {
		h.ProcessFrame(context.Background(), "call1", constFrame(loudAmplitude))
	}

	assert.Equal(t, StateSpeaking, h.State(), "loud frames inside the guard window must not trigger barge-in")
	assert.False(t, fTTS.wasCancelled())
}

func TestHandler_PostCutSilenceGateDropsTooSoonFinal(t *testing.T) {
	fSTT := newFakeSTT()
	h := NewHandler(context.Background(), "call1", testConfig(fSTT, newFakeTTS(), &fakeLLM{}, 0, time.Hour))

	h.silenceUntil.Store(time.Now().Add(time.Hour).UnixNano()) // simulate a cut that just happened

	fSTT.sendFinal("too soon")
	time.Sleep(50 * time.Millisecond) // let readSTT/run process the event

	assert.Equal(t, StateListening, h.State(), "final inside the post-cut gate must be dropped, not start a turn")
}

func TestHandler_ProcessFrameNeverBlocks(t *testing.T) {
	fSTT := newFakeSTT()
	h := NewHandler(context.Background(), "call1", testConfig(fSTT, newFakeTTS(), &fakeLLM{}, 0, 0))

	done := make(chan struct{})

	go func() {
		defer close(done)

		for i := 0; i < 1000; i++ {
			h.ProcessFrame(context.Background(), "call1", constFrame(quietAmplitude))
		}
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("ProcessFrame blocked")
	}
}

// TestHandler_DrainTimeoutEndsTurnWhenTTSDiesWithoutDone is the dead-call
// safeguard regression: if the TTS connection dies mid-turn (or its completion
// event is lost), handleTTSDone never runs and ttsDeliveryDone is never set --
// and with the playout fix, nothing else would ever end the turn. STT would
// never be fed again on a call whose inbound audio is otherwise perfectly
// healthy (so the media plane's dead-call detection sees nothing wrong either).
// processSpeakingFrame must force the turn closed after
// speakingDrainTimeoutTicks consecutive empty ticks.
func TestHandler_DrainTimeoutEndsTurnWhenTTSDiesWithoutDone(t *testing.T) {
	fSTT := newFakeSTT()
	fTTS := newFakeTTS()
	fLLM := &fakeLLM{tokens: []string{"hi"}}

	// Guard window spans the whole test: loud frames must never barge in here,
	// the drain timeout is the only path back to Listening.
	h := NewHandler(context.Background(), "call1", testConfig(fSTT, fTTS, fLLM, time.Hour, 0))

	fSTT.sendFinal("hi")
	require.Eventually(t, func() bool { return fTTS.spokenCount() > 0 }, time.Second, time.Millisecond)

	const gen = 1

	fTTS.audio <- tts.Chunk{PCM: constFrame(loudAmplitude), Gen: gen}
	require.Eventually(t, func() bool { return h.State() == StateSpeaking }, time.Second, time.Millisecond)

	// Play out the one queued frame, then let the queue sit empty with no Done
	// ever arriving (the connection "died"). require.Eventually drives
	// ProcessFrame repeatedly, standing in for the release loop's ticks.
	require.Eventually(t, func() bool {
		h.ProcessFrame(context.Background(), "call1", constFrame(quietAmplitude))
		return h.State() == StateListening
	}, 5*time.Second, time.Millisecond, "turn must end via the drain timeout, not stay wedged in Speaking forever")

	// The point of the safeguard: the caller can be heard again.
	fedAfter := fSTT.fedCount()
	h.ProcessFrame(context.Background(), "call1", constFrame(quietAmplitude))
	assert.Equal(t, fedAfter+1, fSTT.fedCount(), "Listening must feed STT again once the wedged turn is closed")
}
