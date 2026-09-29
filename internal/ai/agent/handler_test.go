package agent

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/nitesh/vaani/internal/ai/llm"
	"github.com/nitesh/vaani/internal/ai/stt"
	"github.com/nitesh/vaani/internal/ai/tts"
)

// captureSlog swaps the default slog logger for one writing to a buffer,
// restores it on test cleanup, and returns a function that hands back that
// buffer. Same pattern as internal/media's captureSlog (unexported there, so
// duplicated rather than shared across packages).
func captureSlog(t *testing.T) func() *bytes.Buffer {
	t.Helper()

	buf := &bytes.Buffer{}
	original := slog.Default()

	slog.SetDefault(slog.New(slog.NewTextHandler(buf, nil)))
	t.Cleanup(func() { slog.SetDefault(original) })

	return func() *bytes.Buffer { return buf }
}

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

// fakeLLM streams a fixed token list, respecting context cancellation. With
// rounds set, each successive Stream call plays the next round instead
// (tokens then tool calls), so a test can script LLM -> tool -> LLM.
type fakeLLM struct {
	tokens []string
	err    error

	rounds []fakeRound

	mu    sync.Mutex
	calls int
	seen  [][]llm.Message // messages received by each Stream call
	tools [][]llm.Tool    // tools received by each Stream call
}

type fakeRound struct {
	tokens    []string
	toolCalls []llm.ToolCall
}

func (f *fakeLLM) Stream(ctx context.Context, msgs []llm.Message, tools []llm.Tool, onToken func(string)) ([]llm.ToolCall, error) {
	f.mu.Lock()
	n := f.calls
	f.calls++
	f.seen = append(f.seen, append([]llm.Message(nil), msgs...))
	f.tools = append(f.tools, tools)
	f.mu.Unlock()

	round := fakeRound{tokens: f.tokens}
	if f.rounds != nil {
		if n >= len(f.rounds) {
			return nil, nil
		}

		round = f.rounds[n]
	}

	for _, tok := range round.tokens {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		onToken(tok)
	}

	return round.toolCalls, f.err
}

func (f *fakeLLM) streamCalls() int {
	f.mu.Lock()
	defer f.mu.Unlock()

	return f.calls
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

	fTTS.audio <- tts.Chunk{PCM: constFrame(loudAmplitude), Gen: gen, Req: 1, Text: "Hi, this side Shubh."}
	require.Eventually(t, func() bool { return h.State() == StateSpeaking }, time.Second, time.Millisecond)

	fTTS.done <- gen
	drainUntilListening(t, h)

	assert.Equal(t, []llm.Message{{Role: "assistant", Content: "Hi, this side Shubh."}}, h.history,
		"the greeting must be recorded so the LLM doesn't redundantly re-greet")
}

// multiFrame returns n 640-byte frames of constant amplitude joined into one
// TTS chunk, so a single sentence spans several ProcessFrame ticks.
func multiFrame(n int, amp int16) []byte {
	var b []byte
	for i := 0; i < n; i++ {
		b = append(b, constFrame(amp)...)
	}

	return b
}

// TestHandler_FullyPlayedReplyIsRecordedInHistory is the regression test for
// the history bug: only the caller's lines and the greeting were ever kept,
// never the LLM's own answers, so every turn the LLM re-answered from scratch
// with no memory of what it had said (observed live as long, repetitive
// replies). A reply that plays out must land in history, whole.
func TestHandler_FullyPlayedReplyIsRecordedInHistory(t *testing.T) {
	fSTT := newFakeSTT()
	fTTS := newFakeTTS()
	fLLM := &fakeLLM{tokens: []string{"Hello there.", " How are you?"}}

	h := NewHandler(context.Background(), "call1", testConfig(fSTT, fTTS, fLLM, 0, 0))

	fSTT.sendFinal("hi")
	require.Eventually(t, func() bool { return fTTS.spokenCount() == 2 }, time.Second, time.Millisecond)

	const gen = 1

	fTTS.audio <- tts.Chunk{PCM: constFrame(quietAmplitude), Gen: gen, Req: 1, Text: "Hello there."}
	fTTS.audio <- tts.Chunk{PCM: constFrame(quietAmplitude), Gen: gen, Req: 2, Text: " How are you?"}
	fTTS.done <- gen

	drainUntilListening(t, h)

	assert.Equal(t, []llm.Message{
		{Role: "user", Content: "hi"},
		{Role: "assistant", Content: "Hello there. How are you?"},
	}, h.history)
}

// TestHandler_BargeInRecordsOnlyWhatWasHeard: when the caller cuts in, history
// must hold only the sentences they actually started hearing -- not the whole
// generated reply, or the LLM would believe it said things the caller never
// heard.
func TestHandler_BargeInRecordsOnlyWhatWasHeard(t *testing.T) {
	fSTT := newFakeSTT()
	fTTS := newFakeTTS()
	fLLM := &fakeLLM{tokens: []string{"First sentence.", " Second sentence."}}

	h := NewHandler(context.Background(), "call1", testConfig(fSTT, fTTS, fLLM, 0, time.Hour))

	fSTT.sendFinal("hi")
	require.Eventually(t, func() bool { return fTTS.spokenCount() == 2 }, time.Second, time.Millisecond)

	const gen = 1

	// Sentence 1 spans 5 frames, sentence 2 just 1: the barge-in below lands
	// while sentence 1 is still playing, so sentence 2 is never heard.
	fTTS.audio <- tts.Chunk{PCM: multiFrame(5, quietAmplitude), Gen: gen, Req: 1, Text: "First sentence."}
	fTTS.audio <- tts.Chunk{PCM: constFrame(quietAmplitude), Gen: gen, Req: 2, Text: " Second sentence."}

	require.Eventually(t, func() bool { return h.State() == StateSpeaking }, time.Second, time.Millisecond)

	// Wait until both chunks are queued, so the loud ticks below pop sentence
	// 1's frames rather than finding the queue empty.
	require.Eventually(t, func() bool { return len(h.outbound) == 6 }, time.Second, time.Millisecond)

	h.ProcessFrame(context.Background(), "call1", constFrame(quietAmplitude)) // plays sentence 1, frame 1
	h.ProcessFrame(context.Background(), "call1", constFrame(loudAmplitude))  // frame 2
	h.ProcessFrame(context.Background(), "call1", constFrame(loudAmplitude))  // frame 3
	h.ProcessFrame(context.Background(), "call1", constFrame(loudAmplitude))  // 3rd loud vote: barge-in

	require.Equal(t, StateTranscribing, h.State(), "barge-in must fire on the third loud frame")

	require.Eventually(t, func() bool { return fTTS.wasCancelled() }, time.Second, time.Millisecond)
	assert.Equal(t, []llm.Message{
		{Role: "user", Content: "hi"},
		{Role: "assistant", Content: "First sentence."},
	}, h.history, "only the sentence the caller started hearing may be recorded")
}

// TestHandler_RecordReplyCutBeforePlaybackRecordsNothing: nothing played means
// nothing heard -- the generated reply must not reach history at all.
func TestHandler_RecordReplyCutBeforePlaybackRecordsNothing(t *testing.T) {
	h := &Handler{callID: "call1"}
	h.turnChunks = []spokenChunk{{req: 1, text: "Never heard."}}

	h.recordReply(true)

	assert.Empty(t, h.history)
	assert.Empty(t, h.turnChunks, "tracking must be cleared for the next turn")
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

// TestHandler_OutboundBufferFullWarningLoggedOncePerGeneration is a regression
// test for a live incident: an oversized reply overflowed the outbound
// buffer and produced over 2000 individual WARN log lines within about a
// second (nothing ever drains outbound in this test, so every push past
// capacity hits the drop path). That logging burst is itself real CPU/I/O
// work competing with the media pacers for the process's attention, so
// dropped frames must be counted (Sink.Error, for metrics) without being
// logged individually.
func TestHandler_OutboundBufferFullWarningLoggedOncePerGeneration(t *testing.T) {
	getLog := captureSlog(t)

	fSTT := newFakeSTT()
	fTTS := newFakeTTS()
	fLLM := &fakeLLM{tokens: []string{"hi"}}

	NewHandler(context.Background(), "call1", testConfig(fSTT, fTTS, fLLM, 0, 0))

	fSTT.sendFinal("hi")
	require.Eventually(t, func() bool { return fTTS.spokenCount() > 0 }, time.Second, time.Millisecond)

	const gen = 1

	for i := 0; i < outboundBufferFrames+50; i++ {
		fTTS.audio <- tts.Chunk{PCM: constFrame(loudAmplitude), Gen: gen}
	}

	require.Eventually(t, func() bool {
		return strings.Contains(getLog().String(), "tts outbound buffer full")
	}, 2*time.Second, time.Millisecond)

	// Let any further drops finish being processed before counting.
	time.Sleep(100 * time.Millisecond)

	assert.Equal(t, 1, strings.Count(getLog().String(), "tts outbound buffer full"),
		"must log once per generation, not once per dropped frame")
}

// echoTool returns a fixed result and records that it ran.
func echoTool(name, result string, ran *atomic.Int32) Tool {
	return Tool{
		Def: llm.FunctionDef{Name: name, Description: "test tool"},
		Run: func(context.Context, json.RawMessage) (string, error) {
			ran.Add(1)
			return result, nil
		},
	}
}

// playTurn pushes one audio chunk per spoken sentence for gen, signals Done,
// and drives ProcessFrame until the turn ends.
func playTurn(t *testing.T, h *Handler, fTTS *fakeTTS, gen uint64, sentences ...string) {
	t.Helper()

	for i, s := range sentences {
		fTTS.audio <- tts.Chunk{PCM: constFrame(quietAmplitude), Gen: gen, Req: uint64(i + 1), Text: s}
	}

	fTTS.done <- gen
	drainUntilListening(t, h)
}

// TestHandler_ToolRoundTrip: text before a tool call is spoken right away,
// the tool runs, its result goes back to the LLM (with the assistant's call
// message before it), the LLM's follow-up is spoken, and history ends up
// with the tool exchange followed by everything actually said.
func TestHandler_ToolRoundTrip(t *testing.T) {
	fSTT := newFakeSTT()
	fTTS := newFakeTTS()

	var ran atomic.Int32

	call := llm.ToolCall{ID: "c1", Type: "function", Function: llm.FunctionCall{Name: "lookup", Arguments: `{"pin":"1"}`}}
	fLLM := &fakeLLM{rounds: []fakeRound{
		{tokens: []string{"Let me check."}, toolCalls: []llm.ToolCall{call}},
		{tokens: []string{" It is done."}},
	}}

	cfg := testConfig(fSTT, fTTS, fLLM, 0, 0)
	cfg.Tools = []Tool{echoTool("lookup", `{"ok":true}`, &ran)}
	h := NewHandler(context.Background(), "call1", cfg)

	fSTT.sendFinal("check my pin")
	require.Eventually(t, func() bool { return fTTS.spokenCount() == 2 }, time.Second, time.Millisecond)

	assert.Equal(t, int32(1), ran.Load())
	assert.Equal(t, []string{"Let me check.", " It is done."}, fTTS.spoken,
		"the pre-tool sentence must be sent to TTS before the tool result comes back")

	require.Equal(t, 2, fLLM.streamCalls())
	assert.Equal(t, "lookup", fLLM.tools[0][0].Function.Name, "tools must be offered to the LLM")

	second := fLLM.seen[1]
	require.Len(t, second, 3)
	assert.Equal(t, llm.Message{Role: "assistant", Content: "Let me check.", ToolCalls: []llm.ToolCall{call}}, second[1])
	assert.Equal(t, llm.Message{Role: "tool", ToolCallID: "c1", Content: `{"ok":true}`}, second[2])

	playTurn(t, h, fTTS, 1, "Let me check.", " It is done.")

	assert.Equal(t, []llm.Message{
		{Role: "user", Content: "check my pin"},
		{Role: "assistant", ToolCalls: []llm.ToolCall{call}},
		{Role: "tool", ToolCallID: "c1", Content: `{"ok":true}`},
		{Role: "assistant", Content: "Let me check. It is done."},
	}, h.history)
}

// TestHandler_EndCallHangsUpAfterGoodbyePlays: the LLM isn't asked again
// after end_call, and the call hangs up only once the goodbye has played.
func TestHandler_EndCallHangsUpAfterGoodbyePlays(t *testing.T) {
	fSTT := newFakeSTT()
	fTTS := newFakeTTS()

	call := llm.ToolCall{ID: "e1", Type: "function", Function: llm.FunctionCall{Name: "end_call", Arguments: "{}"}}
	fLLM := &fakeLLM{rounds: []fakeRound{{tokens: []string{"Goodbye."}, toolCalls: []llm.ToolCall{call}}}}

	var hangups atomic.Int32

	cfg := testConfig(fSTT, fTTS, fLLM, 0, 0)
	cfg.Tools = []Tool{{Def: llm.FunctionDef{Name: "end_call"}, Kind: ToolEndCall}}
	cfg.Hangup = func() { hangups.Add(1) }
	h := NewHandler(context.Background(), "call1", cfg)

	fSTT.sendFinal("bye")
	require.Eventually(t, func() bool { return fTTS.spokenCount() == 1 }, time.Second, time.Millisecond)

	fTTS.audio <- tts.Chunk{PCM: constFrame(quietAmplitude), Gen: 1, Req: 1, Text: "Goodbye."}
	require.Eventually(t, func() bool { return h.State() == StateSpeaking }, time.Second, time.Millisecond)
	assert.Zero(t, hangups.Load(), "must not hang up while the goodbye is still playing")

	fTTS.done <- 1
	drainUntilListening(t, h)

	require.Eventually(t, func() bool { return hangups.Load() == 1 }, time.Second, time.Millisecond)
	assert.Equal(t, 1, fLLM.streamCalls(), "the LLM must not be asked again after end_call")
}

// TestHandler_UnknownToolReturnsErrorToLLM: a hallucinated tool name must not
// break the turn -- the LLM gets an error result and carries on.
func TestHandler_UnknownToolReturnsErrorToLLM(t *testing.T) {
	fSTT := newFakeSTT()
	fTTS := newFakeTTS()

	call := llm.ToolCall{ID: "x", Type: "function", Function: llm.FunctionCall{Name: "nope", Arguments: "{}"}}
	fLLM := &fakeLLM{rounds: []fakeRound{{toolCalls: []llm.ToolCall{call}}, {tokens: []string{"Sorry."}}}}

	NewHandler(context.Background(), "call1", testConfig(fSTT, fTTS, fLLM, 0, 0))

	fSTT.sendFinal("hi")
	require.Eventually(t, func() bool { return fTTS.spokenCount() == 1 }, time.Second, time.Millisecond)

	result := fLLM.seen[1][2]
	assert.Equal(t, "tool", result.Role)
	assert.Contains(t, result.Content, `"status":"error"`)
	assert.Contains(t, result.Content, "nope")
}

// TestHandler_DrainTimeoutPausedWhileToolRuns: once "let me check" finishes
// playing, the queue sits empty while a slow tool runs. That is not a dead
// TTS connection, so the 10s drain safeguard must not end the turn.
func TestHandler_DrainTimeoutPausedWhileToolRuns(t *testing.T) {
	fSTT := newFakeSTT()
	fTTS := newFakeTTS()

	release := make(chan struct{})
	slow := Tool{
		Def: llm.FunctionDef{Name: "slow"},
		Run: func(ctx context.Context, _ json.RawMessage) (string, error) {
			<-release
			return `{}`, nil
		},
	}

	call := llm.ToolCall{ID: "s", Type: "function", Function: llm.FunctionCall{Name: "slow", Arguments: "{}"}}
	fLLM := &fakeLLM{rounds: []fakeRound{{tokens: []string{"One moment."}, toolCalls: []llm.ToolCall{call}}}}

	cfg := testConfig(fSTT, fTTS, fLLM, time.Hour, 0)
	cfg.Tools = []Tool{slow}
	h := NewHandler(context.Background(), "call1", cfg)

	fSTT.sendFinal("hi")
	require.Eventually(t, func() bool { return fTTS.spokenCount() == 1 }, time.Second, time.Millisecond)

	fTTS.audio <- tts.Chunk{PCM: constFrame(quietAmplitude), Gen: 1, Req: 1, Text: "One moment."}
	require.Eventually(t, func() bool { return h.State() == StateSpeaking && h.toolRunning.Load() }, time.Second, time.Millisecond)

	for i := 0; i < speakingDrainTimeoutTicks+50; i++ {
		h.ProcessFrame(context.Background(), "call1", constFrame(quietAmplitude))
	}

	assert.Equal(t, StateSpeaking, h.State(), "drain timeout must not fire while a tool is running")

	// Let the tool and the rest of the turn finish before returning, so their
	// log lines can't land in a later test that captures slog output.
	close(release)
	require.Eventually(t, func() bool { return fLLM.streamCalls() == 2 }, time.Second, time.Millisecond)
	require.Eventually(t, func() bool { return !h.toolRunning.Load() && len(h.turnToolMsgs) > 0 }, time.Second, time.Millisecond)
}

// firstSample identifies which canned sound a frame came from in the
// transfer tests (each uses a different constant amplitude).
func firstSample(t *testing.T, frames [][]byte) int16 {
	t.Helper()
	require.Len(t, frames, 1)

	return int16(frames[0][0]) | int16(frames[0][1])<<8
}

const (
	msgAmp  = 100
	holdAmp = 200
	beepAmp = 300
)

func transferTool() Tool {
	return Tool{
		Def:         llm.FunctionDef{Name: "transfer_to_support"},
		Kind:        ToolTransfer,
		Destination: "SIP/100@pbx",
		Message:     "Please hold.",
	}
}

// TestHandler_TransferHoldBeepThenConnect: the "please hold" message plays
// first, then hold audio loops while the destination rings; on answer the
// beep plays in full before connect() hands the caller over. The LLM is not
// asked again afterwards.
func TestHandler_TransferHoldBeepThenConnect(t *testing.T) {
	fSTT := newFakeSTT()
	fTTS := newFakeTTS()

	call := llm.ToolCall{ID: "t1", Type: "function", Function: llm.FunctionCall{Name: "transfer_to_support", Arguments: "{}"}}
	fLLM := &fakeLLM{rounds: []fakeRound{{toolCalls: []llm.ToolCall{call}}}}

	answer := make(chan struct{})

	var (
		connects    atomic.Int32
		gotDest     string
		gotTimeout  time.Duration
		dialStarted = make(chan struct{})
	)

	cfg := testConfig(fSTT, fTTS, fLLM, time.Hour, 0)
	cfg.Tools = []Tool{transferTool()}
	cfg.HoldAudio = constFrame(holdAmp)
	cfg.BeepAudio = constFrame(beepAmp)
	cfg.Transfer = func(ctx context.Context, dest string, timeout time.Duration) (func() error, error) {
		gotDest, gotTimeout = dest, timeout
		close(dialStarted)
		<-answer

		return func() error { connects.Add(1); return nil }, nil
	}
	h := NewHandler(context.Background(), "call1", cfg)

	fSTT.sendFinal("connect me to support")
	<-dialStarted
	assert.Equal(t, "SIP/100@pbx", gotDest)
	assert.Equal(t, defaultTransferTimeout, gotTimeout, "no configured timeout -> default")
	require.Eventually(t, func() bool { return fTTS.spokenCount() == 1 }, time.Second, time.Millisecond)

	fTTS.audio <- tts.Chunk{PCM: constFrame(msgAmp), Gen: 1, Req: 1, Text: "Please hold."}
	require.Eventually(t, func() bool { return len(h.outbound) == 1 }, time.Second, time.Millisecond)

	assert.Equal(t, int16(msgAmp), firstSample(t, h.ProcessFrame(context.Background(), "call1", constFrame(quietAmplitude))),
		"the hold message plays before the hold audio")

	for i := 0; i < 3; i++ { // one frame of hold audio, looping
		assert.Equal(t, int16(holdAmp), firstSample(t, h.ProcessFrame(context.Background(), "call1", constFrame(quietAmplitude))))
	}

	assert.Zero(t, fSTT.fedCount(), "caller audio must not reach STT while on hold")

	close(answer)
	require.Eventually(t, func() bool {
		hp := h.hold.Load()
		return hp != nil && !hp.loop
	}, time.Second, time.Millisecond)
	assert.Zero(t, connects.Load(), "must not connect before the beep has played")

	assert.Equal(t, int16(beepAmp), firstSample(t, h.ProcessFrame(context.Background(), "call1", constFrame(quietAmplitude))))

	require.Eventually(t, func() bool {
		h.ProcessFrame(context.Background(), "call1", constFrame(quietAmplitude))
		return connects.Load() == 1
	}, time.Second, time.Millisecond)

	time.Sleep(50 * time.Millisecond)
	assert.Equal(t, 1, fLLM.streamCalls(), "the LLM must not be asked again after a transfer")
}

// TestHandler_TransferFailureGoesBackToLLM: a destination that doesn't answer
// stops the hold audio and hands the reason to the LLM, which carries on.
func TestHandler_TransferFailureGoesBackToLLM(t *testing.T) {
	fSTT := newFakeSTT()
	fTTS := newFakeTTS()

	call := llm.ToolCall{ID: "t1", Type: "function", Function: llm.FunctionCall{Name: "transfer_to_support", Arguments: "{}"}}
	fLLM := &fakeLLM{rounds: []fakeRound{
		{toolCalls: []llm.ToolCall{call}},
		{tokens: []string{"Nobody is free right now."}},
	}}

	cfg := testConfig(fSTT, fTTS, fLLM, time.Hour, 0)
	cfg.Tools = []Tool{transferTool()}
	cfg.HoldAudio = constFrame(holdAmp)
	cfg.Transfer = func(context.Context, string, time.Duration) (func() error, error) {
		return nil, errors.New("transfer destination did not answer (User busy, cause 17)")
	}
	h := NewHandler(context.Background(), "call1", cfg)

	fSTT.sendFinal("connect me to support")
	require.Eventually(t, func() bool { return fTTS.spokenCount() == 2 }, time.Second, time.Millisecond)

	assert.Nil(t, h.hold.Load(), "hold audio must stop when the transfer fails")

	result := fLLM.seen[1][2]
	assert.Equal(t, "tool", result.Role)
	assert.JSONEq(t, `{"status":"failed","reason":"transfer destination did not answer (User busy, cause 17)"}`, result.Content)
	assert.Equal(t, "Nobody is free right now.", fTTS.spoken[1])
}
