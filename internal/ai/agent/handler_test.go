package agent

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
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
func captureSlog(t *testing.T) func() *lockedBuffer {
	t.Helper()

	buf := &lockedBuffer{}
	original := slog.Default()

	slog.SetDefault(slog.New(slog.NewTextHandler(buf, &slog.HandlerOptions{Level: slog.LevelDebug})))
	t.Cleanup(func() { slog.SetDefault(original) })

	return func() *lockedBuffer { return buf }
}

// lockedBuffer is a log sink a test can read while the handler's goroutines
// are still writing to it (a plain bytes.Buffer is a data race there).
type lockedBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	return b.buf.Write(p)
}

func (b *lockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()

	return b.buf.String()
}

// finalHistory stops h (cancel is its context's) and returns its history.
// h.history belongs to run()'s goroutine; once Done is closed run() has
// returned, so reading it is no longer a race. Call it once the turn has
// visibly ended: run() finishes the event it's handling (finishTurn records
// the reply right after flipping to Listening) before it sees the cancel.
func finalHistory(t *testing.T, h *Handler, cancel context.CancelFunc) []llm.Message {
	t.Helper()

	cancel()

	select {
	case <-h.Done():
	case <-time.After(time.Second):
		t.Fatal("handler didn't stop")
	}

	return h.history
}

// fakeSTT records every fed frame and lets a test push transcript results.
type fakeSTT struct {
	mu      sync.Mutex
	fed     [][]byte
	flushes int
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

func (f *fakeSTT) Flush() error {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.flushes++

	return nil
}

func (f *fakeSTT) flushCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()

	return f.flushes
}

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

// spokenList, seenAt and toolsAt read the fakes' records under their locks:
// the handler's goroutines may still be appending (a later LLM round, an
// idle prompt), and a bare read of the slice races with that.
func (f *fakeTTS) spokenList() []string {
	f.mu.Lock()
	defer f.mu.Unlock()

	return append([]string(nil), f.spoken...)
}

func (f *fakeLLM) seenAt(i int) []llm.Message {
	f.mu.Lock()
	defer f.mu.Unlock()

	return f.seen[i]
}

func (f *fakeLLM) toolsAt(i int) []llm.Tool {
	f.mu.Lock()
	defer f.mu.Unlock()

	return f.tools[i]
}

func (f *fakeTTS) endedGens() []uint64 {
	f.mu.Lock()
	defer f.mu.Unlock()

	return append([]uint64(nil), f.ended...)
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
		Start:          &Node{AllowInterrupt: true}, // barge-in tests need an interruptible node
		BargeIn:        NewEnergyDetector(floor),
		BargeInGuard:   guard,
		PostCutSilence: postCut,
		Sink:           NoopSink{},
	}
}

func TestHandler_GreetingIsSpokenWithoutWaitingForCaller(t *testing.T) {
	fSTT := newFakeSTT()
	fTTS := newFakeTTS()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	h := NewHandler(ctx, "call1", Config{
		STT:     fSTT,
		NewTTS:  func() (tts.Client, error) { return fTTS, nil },
		LLM:     &fakeLLM{}, // must never be called for the greeting
		Start:   &Node{Greeting: "Hi, this side Shubh."},
		BargeIn: NewEnergyDetector(floor),
		Sink:    NoopSink{},
	})

	require.Eventually(t, func() bool { return fTTS.spokenCount() > 0 }, time.Second, time.Millisecond)
	assert.Equal(t, []string{"Hi, this side Shubh."}, fTTS.spokenList())

	const gen = 1

	fTTS.audio <- tts.Chunk{PCM: constFrame(loudAmplitude), Gen: gen, Req: 1, Text: "Hi, this side Shubh."}
	require.Eventually(t, func() bool { return h.State() == StateSpeaking }, time.Second, time.Millisecond)

	fTTS.done <- gen
	drainUntilListening(t, h)

	assert.Equal(t, []llm.Message{{Role: "assistant", Content: "Hi, this side Shubh."}}, finalHistory(t, h, cancel),
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

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	h := NewHandler(ctx, "call1", testConfig(fSTT, fTTS, fLLM, 0, 0))

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
	}, finalHistory(t, h, cancel))
}

// TestHandler_BargeInRecordsOnlyWhatWasHeard: when the caller cuts in, history
// must hold only the sentences they actually started hearing -- not the whole
// generated reply, or the LLM would believe it said things the caller never
// heard.
func TestHandler_BargeInRecordsOnlyWhatWasHeard(t *testing.T) {
	getLog := captureSlog(t)

	fSTT := newFakeSTT()
	fTTS := newFakeTTS()
	fLLM := &fakeLLM{tokens: []string{"First sentence.", " Second sentence."}}

	h := NewHandler(context.Background(), "call1", testConfig(fSTT, fTTS, fLLM, 0, 0))

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

	require.True(t, h.paused.Load(), "barge-in must fire (pause) on the third loud frame")

	// The caller's words confirm it: the reply is cut.
	fSTT.sendFinal("wait")
	require.Eventually(t, func() bool { return fTTS.wasCancelled() }, time.Second, time.Millisecond)
	require.Eventually(t, func() bool { return fLLM.streamCalls() == 2 }, time.Second, time.Millisecond)
	assert.Equal(t, []llm.Message{
		{Role: "user", Content: "hi"},
		{Role: "assistant", Content: "First sentence."},
		{Role: "user", Content: "wait"},
	}, fLLM.seenAt(1), "only the sentence the caller started hearing may be recorded")
	assert.Contains(t, getLog().String(), `event=turn.agent turn=1 node="" gen=1 cut=true text="First sentence."`,
		"a cut reply is logged under its own turn's number")
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

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	h := NewHandler(ctx, "call1", testConfig(fSTT, newFakeTTS(), &fakeLLM{}, 0, 0))

	time.Sleep(20 * time.Millisecond) // let NewHandler's goroutines settle

	assert.Equal(t, StateListening, h.State())
	assert.Empty(t, finalHistory(t, h, cancel))
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

func TestHandler_BargeInPausesThenCuts(t *testing.T) {
	fSTT := newFakeSTT()
	fTTS := newFakeTTS()
	fLLM := &fakeLLM{tokens: []string{"a long response the caller interrupts"}}

	h := NewHandler(context.Background(), "call1", testConfig(fSTT, fTTS, fLLM, 0, 50*time.Millisecond))

	fSTT.sendFinal("hi")
	require.Eventually(t, func() bool { return fTTS.spokenCount() > 0 }, time.Second, time.Millisecond)

	fTTS.audio <- tts.Chunk{PCM: constFrame(quietAmplitude), Gen: 1}
	require.Eventually(t, func() bool { return h.State() == StateSpeaking }, time.Second, time.Millisecond)

	h.ProcessFrame(context.Background(), "call1", constFrame(quietAmplitude))
	h.ProcessFrame(context.Background(), "call1", constFrame(quietAmplitude))

	h.ProcessFrame(context.Background(), "call1", constFrame(loudAmplitude))
	h.ProcessFrame(context.Background(), "call1", constFrame(loudAmplitude))
	out := h.ProcessFrame(context.Background(), "call1", constFrame(loudAmplitude))

	assert.Nil(t, out, "the triggering tick returns no agent audio")
	assert.True(t, h.paused.Load(), "must pause immediately, not wait for the event loop")
	assert.False(t, fTTS.wasCancelled(), "a pause is not a cut yet")
	assert.Nil(t, h.ProcessFrame(context.Background(), "call1", constFrame(quietAmplitude)), "nothing plays while paused")

	// A transcript inside the post-cut gate (already on its way) doesn't confirm it...
	fSTT.sendFinal("ह")
	time.Sleep(20 * time.Millisecond)
	assert.False(t, fTTS.wasCancelled())

	// ...one after it does: all the cuts.
	time.Sleep(50 * time.Millisecond)
	fSTT.sendFinal("hold on")
	require.Eventually(t, fTTS.wasCancelled, time.Second, time.Millisecond, "cut #2: TTS must be cancelled")
	assert.False(t, h.paused.Load())
}

// TestHandler_FalseInterruptionResumesTheReply: a pause no transcript
// confirms (noise, echo) ends after falseInterruptionTimeout of quiet, and
// the reply plays on from where it stopped -- nothing cut, nothing lost.
// TestHandler_ResumeNeverReplaysThePreviousTurn: a reply paused before its
// first frame played still has the previous turn's sentence in sentFrames;
// resuming must not replay that.
func TestHandler_ResumeNeverReplaysThePreviousTurn(t *testing.T) {
	h := NewHandler(context.Background(), "call1", testConfig(newFakeSTT(), newFakeTTS(), &fakeLLM{}, 0, 0))
	old := constFrame(loudAmplitude)

	h.sentGen, h.sentReq, h.sentFrames = 1, 1, [][]byte{old} // turn 1's last sentence
	h.state.Store(int32(StateSpeaking))
	h.replayGen.Store(2) // turn 2 was paused and resumed
	h.replayPending.Store(true)

	out := h.ProcessFrame(context.Background(), "call1", constFrame(quietAmplitude))
	assert.Empty(t, out, "turn 1's audio must not play in turn 2")

	// Same generation: the interrupted sentence is replayed, as designed.
	h.replayGen.Store(1)
	h.replayPending.Store(true)
	out = h.ProcessFrame(context.Background(), "call1", constFrame(quietAmplitude))
	require.Len(t, out, 1)
	assert.Equal(t, old, out[0])
}

func TestHandler_FalseInterruptionResumesTheReply(t *testing.T) {
	fSTT := newFakeSTT()
	fTTS := newFakeTTS()
	fLLM := &fakeLLM{tokens: []string{"A reply."}}

	h := NewHandler(context.Background(), "call1", testConfig(fSTT, fTTS, fLLM, 0, 0))

	fSTT.sendFinal("hi")
	require.Eventually(t, func() bool { return fTTS.spokenCount() == 1 }, time.Second, time.Millisecond)

	// Ten frames, each with its own amplitude (101..110), so the order they
	// play in is visible.
	var pcm []byte
	for amp := int16(101); amp <= 110; amp++ {
		pcm = append(pcm, constFrame(amp)...)
	}

	fTTS.audio <- tts.Chunk{PCM: pcm, Gen: 1, Req: 1, Text: "A reply."}
	require.Eventually(t, func() bool { return h.State() == StateSpeaking && len(h.outbound) == 10 }, time.Second, time.Millisecond)

	play := func(in []byte) int16 { return firstSample(t, h.ProcessFrame(context.Background(), "call1", in)) }

	// The caller's (loud) audio plays frames 101 and 102; the third tick pauses.
	assert.Equal(t, int16(101), play(constFrame(loudAmplitude)))
	assert.Equal(t, int16(102), play(constFrame(loudAmplitude)))
	h.ProcessFrame(context.Background(), "call1", constFrame(loudAmplitude))
	require.True(t, h.paused.Load())

	require.Eventually(t, func() bool { return !h.paused.Load() }, falseInterruptionTimeout+time.Second, 10*time.Millisecond)
	assert.False(t, fTTS.wasCancelled(), "a false interruption cuts nothing")

	// Resumes from the start of the interrupted sentence, then carries on.
	for _, want := range []int16{101, 102, 103, 104} {
		assert.Equal(t, want, play(constFrame(quietAmplitude)))
	}

	fTTS.done <- 1
	drainUntilListening(t, h)
	assert.Equal(t, 1, fLLM.streamCalls())
}

// TestHandler_BargeInNeedsSustainedSpeech: speech shorter than
// BargeInMinSpeech, or broken up, never interrupts.
func TestHandler_BargeInNeedsSustainedSpeech(t *testing.T) {
	fSTT := newFakeSTT()
	fTTS := newFakeTTS()

	cfg := testConfig(fSTT, fTTS, &fakeLLM{tokens: []string{"A reply."}}, 0, 0)
	cfg.BargeInMinSpeech = 200 * time.Millisecond
	h := NewHandler(context.Background(), "call1", cfg)

	fSTT.sendFinal("hi")
	require.Eventually(t, func() bool { return fTTS.spokenCount() == 1 }, time.Second, time.Millisecond)
	fTTS.audio <- tts.Chunk{PCM: multiFrame(50, quietAmplitude), Gen: 1, Req: 1, Text: "A reply."}
	require.Eventually(t, func() bool { return h.State() == StateSpeaking }, time.Second, time.Millisecond)

	loud := func() { h.ProcessFrame(context.Background(), "call1", constFrame(loudAmplitude)) }

	for i := 0; i < 5; i++ {
		loud()
	}

	assert.False(t, h.paused.Load(), "a short burst is not an interruption")

	time.Sleep(150 * time.Millisecond)
	for i := 0; i < 5; i++ { // a quiet gap resets the run
		h.ProcessFrame(context.Background(), "call1", constFrame(quietAmplitude))
	}

	loud()
	loud()
	loud()
	time.Sleep(40 * time.Millisecond)
	loud()
	assert.False(t, h.paused.Load(), "the run restarted after the gap, so it's not long enough yet")

	time.Sleep(200 * time.Millisecond)
	loud()
	assert.True(t, h.paused.Load(), "200ms of unbroken speech interrupts")
}

// TestHandler_EndNodeAndClosingCallCantBeInterrupted: like Dograh, once the
// call is ending (end node, end_call, ending) the caller is muted, whatever
// a stored allow_interrupt says.
func TestHandler_EndNodeAndClosingCallCantBeInterrupted(t *testing.T) {
	h := &Handler{cfg: Config{}}
	h.node.Store(&Node{AllowInterrupt: true})
	assert.True(t, h.interruptible())

	h.node.Store(&Node{AllowInterrupt: true, End: true})
	assert.False(t, h.interruptible(), "an end node never")

	h.node.Store(&Node{AllowInterrupt: true})
	h.closing.Store(true)
	assert.False(t, h.interruptible(), "nor a closing call")

	h.closing.Store(false)
	h.cfg.BargeInObserveOnly = true
	assert.False(t, h.interruptible(), "nor with BARGE_IN_ENABLED=0")
}

// TestRecordReplyJoinsChunksAsSpoken: pieces spoken on their own are sent
// with a trailing space so they don't run into the reply, and chunks are
// joined exactly as spoken -- never with an added space, which split a word
// in two ("बिजल ी") when a chunk ended mid-word.
func TestRecordReplyJoinsChunksAsSpoken(t *testing.T) {
	h := &Handler{callID: "call1"}
	h.turnChunks = []spokenChunk{
		{req: 1, text: "चलिए मुख्य विषय पर बात करते हैं। "}, // transition speech, as takeEdge sends it
		{req: 2, text: "सड़क और बिजल"},                      // a chunk cut mid-word...
		{req: 3, text: "ी, या कानून व्यवस्था?"},             // ...and the rest of it
		{req: 4, text: " Next one."}, // LLM chunk with its own space
	}

	h.recordReply(false)
	assert.Equal(t, "चलिए मुख्य विषय पर बात करते हैं। सड़क और बिजली, या कानून व्यवस्था? Next one.", h.history[0].Content)

	// Two LLM rounds (text before end_call, then the end node's reply): the
	// second round's first token has no leading space (live: "नमस्ते।Thank").
	h.turnChunks = []spokenChunk{
		{req: 1, text: "आपका दिन शुभ हो। नमस्ते।"},
		{req: 2, text: "Thank you for the call."},
	}

	h.recordReply(false)
	assert.Equal(t, "आपका दिन शुभ हो। नमस्ते। Thank you for the call.", h.history[1].Content)
}

func TestHandler_STTFlushAfterQuiet(t *testing.T) {
	getLog := captureSlog(t)

	fSTT := newFakeSTT()
	vad := &fakeClockDetector{}

	cfg := testConfig(fSTT, newFakeTTS(), &fakeLLM{tokens: []string{"Okay."}}, time.Hour, 0)
	cfg.BargeIn = vad
	cfg.STTFlushAfter = 300 * time.Millisecond
	h := NewHandler(context.Background(), "call1", cfg)

	tick := func() { h.ProcessFrame(context.Background(), "call1", constFrame(quietAmplitude)) }
	startUtterance := func() {
		prev := h.sttUttStart.Load()
		fSTT.results <- stt.Result{Signal: stt.SpeechStarted}
		require.Eventually(t, func() bool { return h.sttUtterance.Load() && h.sttUttStart.Load() != prev }, time.Second, time.Millisecond)
	}

	vad.set(time.Now().Add(-time.Second))
	tick()
	assert.Zero(t, fSTT.flushCount(), "no utterance in progress")

	startUtterance()
	vad.set(time.Now())
	tick()
	assert.Zero(t, fSTT.flushCount(), "the caller is still speaking")

	vad.set(time.Now().Add(-350 * time.Millisecond))
	tick()
	assert.Equal(t, 1, fSTT.flushCount(), "300ms of quiet after speech the VAD heard")
	tick()
	assert.Equal(t, 1, fSTT.flushCount(), "once per utterance")

	fSTT.sendFinal("जी बिल्कुल")
	require.Eventually(t, func() bool { return strings.Contains(getLog().String(), "since_flush_ms=") }, time.Second, time.Millisecond)

	fSTT.results <- stt.Result{Signal: stt.SpeechEnded}
	startUtterance()
	vad.set(time.Now().Add(-2 * time.Second))
	tick()
	assert.Equal(t, 1, fSTT.flushCount(), "the VAD never heard this utterance: Sarvam decides")

	vad.set(time.Now().Add(-350 * time.Millisecond))
	tick()
	assert.Equal(t, 2, fSTT.flushCount(), "a new utterance can be flushed")
}

func TestHandler_STTFlushOffByDefault(t *testing.T) {
	fSTT := newFakeSTT()
	vad := &fakeClockDetector{}

	cfg := testConfig(fSTT, newFakeTTS(), &fakeLLM{}, time.Hour, 0)
	cfg.BargeIn = vad
	h := NewHandler(context.Background(), "call1", cfg)

	fSTT.results <- stt.Result{Signal: stt.SpeechStarted}
	require.Eventually(t, h.sttUtterance.Load, time.Second, time.Millisecond)
	vad.set(time.Now().Add(-time.Second / 2))
	h.ProcessFrame(context.Background(), "call1", constFrame(quietAmplitude))
	assert.Zero(t, fSTT.flushCount())
}

func TestHandler_SeparatelySpokenPiecesEndWithASpace(t *testing.T) {
	fSTT := newFakeSTT()
	fTTS := newFakeTTS()

	main := &Node{Name: "Main"}
	start := &Node{Name: "Start", Edges: []Edge{
		{Def: llm.FunctionDef{Name: "go"}, Speech: "चलिए आगे बढ़ते हैं।", To: main},
	}}
	fLLM := &fakeLLM{rounds: []fakeRound{
		{toolCalls: []llm.ToolCall{{ID: "e", Type: "function", Function: llm.FunctionCall{Name: "go", Arguments: "{}"}}}},
		{tokens: []string{"धन्यवाद।"}},
	}}

	cfg := testConfig(fSTT, fTTS, fLLM, 0, 0)
	cfg.Start = start
	NewHandler(context.Background(), "call1", cfg)

	fSTT.sendFinal("हाँ")
	require.Eventually(t, func() bool { return fTTS.spokenCount() == 2 }, time.Second, time.Millisecond)
	assert.Equal(t, []string{"चलिए आगे बढ़ते हैं। ", "धन्यवाद।"}, fTTS.spokenList())
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

// TestHandler_NodeWithoutAllowInterruptIsNeverCut: Dograh's allow_interrupt
// =false mutes the caller while the agent speaks at that node, even with
// barge-in enabled and the detector firing.
func TestHandler_NodeWithoutAllowInterruptIsNeverCut(t *testing.T) {
	fSTT := newFakeSTT()
	fTTS := newFakeTTS()
	fLLM := &fakeLLM{tokens: []string{"a reply the caller talks over"}}

	cfg := testConfig(fSTT, fTTS, fLLM, 0, 0)
	cfg.Start = &Node{AllowInterrupt: false}
	h := NewHandler(context.Background(), "call1", cfg)

	fSTT.sendFinal("hi")
	require.Eventually(t, func() bool { return fTTS.spokenCount() > 0 }, time.Second, time.Millisecond)

	fTTS.audio <- tts.Chunk{PCM: multiFrame(20, quietAmplitude), Gen: 1, Req: 1, Text: "a reply the caller talks over"}
	require.Eventually(t, func() bool { return h.State() == StateSpeaking }, time.Second, time.Millisecond)

	for i := 0; i < 10; i++ {
		h.ProcessFrame(context.Background(), "call1", constFrame(loudAmplitude))
	}

	assert.Equal(t, StateSpeaking, h.State(), "no cut at a node that doesn't allow interruption")
	assert.False(t, fTTS.wasCancelled())

	// Finish the turn so its log lines can't leak into a later test.
	fTTS.done <- 1
	drainUntilListening(t, h)
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
	cfg.Start = &Node{Tools: []Tool{echoTool("lookup", `{"ok":true}`, &ran)}}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	h := NewHandler(ctx, "call1", cfg)

	fSTT.sendFinal("check my pin")
	require.Eventually(t, func() bool { return fTTS.spokenCount() == 2 }, time.Second, time.Millisecond)

	assert.Equal(t, int32(1), ran.Load())
	assert.Equal(t, []string{"Let me check.", " It is done."}, fTTS.spokenList(),
		"the pre-tool sentence must be sent to TTS before the tool result comes back")

	require.Equal(t, 2, fLLM.streamCalls())
	assert.Equal(t, "lookup", fLLM.toolsAt(0)[0].Function.Name, "tools must be offered to the LLM")

	second := fLLM.seenAt(1)
	require.Len(t, second, 3)
	assert.Equal(t, llm.Message{Role: "assistant", Content: "Let me check.", ToolCalls: []llm.ToolCall{call}}, second[1])
	assert.Equal(t, llm.Message{Role: "tool", ToolCallID: "c1", Content: `{"ok":true}`}, second[2])

	playTurn(t, h, fTTS, 1, "Let me check.", " It is done.")

	assert.Equal(t, []llm.Message{
		{Role: "user", Content: "check my pin"},
		{Role: "assistant", ToolCalls: []llm.ToolCall{call}},
		{Role: "tool", ToolCallID: "c1", Content: `{"ok":true}`},
		{Role: "assistant", Content: "Let me check. It is done."},
	}, finalHistory(t, h, cancel))
}

// TestHandler_EndCallHangsUpAfterGoodbyePlays: the LLM isn't asked again
// after end_call, and the call hangs up only once the goodbye has played.
func TestHandler_EndCallHangsUpAfterGoodbyePlays(t *testing.T) {
	fSTT := newFakeSTT()
	fTTS := newFakeTTS()

	call := llm.ToolCall{ID: "e1", Type: "function", Function: llm.FunctionCall{Name: "end_call", Arguments: "{}"}}
	fLLM := &fakeLLM{rounds: []fakeRound{{tokens: []string{"Goodbye."}, toolCalls: []llm.ToolCall{call}}}}

	var hangups atomic.Int32

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cfg := testConfig(fSTT, fTTS, fLLM, 0, 0)
	cfg.Start = &Node{ID: "n1", Name: "Start", Tools: []Tool{{Def: llm.FunctionDef{Name: "end_call"}, Kind: ToolEndCall}}}
	cfg.Hangup = func() { hangups.Add(1) }
	cfg.Log = &CallLog{}
	h := NewHandler(ctx, "call1", cfg)

	fSTT.sendFinal("bye")
	require.Eventually(t, func() bool { return fTTS.spokenCount() == 1 }, time.Second, time.Millisecond)

	fTTS.audio <- tts.Chunk{PCM: constFrame(quietAmplitude), Gen: 1, Req: 1, Text: "Goodbye."}
	require.Eventually(t, func() bool { return h.State() == StateSpeaking }, time.Second, time.Millisecond)
	assert.Zero(t, hangups.Load(), "must not hang up while the goodbye is still playing")

	fTTS.done <- 1
	drainUntilListening(t, h)

	require.Eventually(t, func() bool { return hangups.Load() == 1 }, time.Second, time.Millisecond)
	assert.Equal(t, 1, fLLM.streamCalls(), "the LLM must not be asked again after end_call")

	// The call log, complete once the agent has stopped.
	cancel()
	<-h.Done()

	s := cfg.Log.Summary()
	assert.Equal(t, EndReasonEndCallTool, s.EndReason)
	assert.Equal(t, []string{"Start"}, s.NodesVisited)

	var types []string
	for _, e := range s.Events {
		types = append(types, fmt.Sprintf("%s %v %v", e.Type, e.Payload["text"], e.Payload["function_name"]))
	}

	assert.Equal(t, []string{
		"rtf-node-transition <nil> <nil>",
		"rtf-user-transcription bye <nil>",
		"rtf-function-call-start <nil> end_call",
		"rtf-function-call-end <nil> end_call",
		"rtf-bot-text Goodbye. <nil>",
	}, types)
}

// TestHandler_UnknownToolReturnsErrorToLLM: a hallucinated tool name must not
// break the turn -- the LLM gets an error result and carries on. On a node
// that offers tools it's an ordinary tool result; on one that offers none,
// text (plainHistory), since a tool result without tools is a 400 on Sarvam.
func TestHandler_UnknownToolReturnsErrorToLLM(t *testing.T) {
	call := llm.ToolCall{ID: "x", Type: "function", Function: llm.FunctionCall{Name: "nope", Arguments: "{}"}}

	run := func(t *testing.T, start *Node) []llm.Message {
		fSTT := newFakeSTT()
		fTTS := newFakeTTS()
		fLLM := &fakeLLM{rounds: []fakeRound{{toolCalls: []llm.ToolCall{call}}, {tokens: []string{"Sorry."}}}}

		cfg := testConfig(fSTT, fTTS, fLLM, 0, 0)
		cfg.Start = start
		NewHandler(context.Background(), "call1", cfg)

		fSTT.sendFinal("hi")
		require.Eventually(t, func() bool { return fTTS.spokenCount() == 1 }, time.Second, time.Millisecond)

		return fLLM.seenAt(1)
	}

	t.Run("node with tools", func(t *testing.T) {
		next := &Node{Name: "Next"}
		msgs := run(t, &Node{AllowInterrupt: true, Edges: []Edge{{Def: llm.FunctionDef{Name: "go_next"}, To: next}}})

		result := msgs[2]
		assert.Equal(t, "tool", result.Role)
		assert.Contains(t, result.Content, `"status":"error"`)
		assert.Contains(t, result.Content, "nope")
	})

	t.Run("node without tools", func(t *testing.T) {
		msgs := run(t, &Node{AllowInterrupt: true})

		require.Len(t, msgs, 2)
		assert.Equal(t, llm.Message{Role: "user", Content: "hi"}, msgs[0])
		assert.Equal(t, "assistant", msgs[1].Role)
		assert.Contains(t, msgs[1].Content, "[Tool Response: nope]")
		assert.Empty(t, msgs[1].ToolCalls)
	})
}

// TestHandler_DeadTTSIsReplacedNextTurn: the provider closing the TTS
// connection mid-turn must not leave the call on a dead client -- observed
// as dead air for the rest of the call. The turn ends, and the next one
// opens a fresh connection.
func TestHandler_DeadTTSIsReplacedNextTurn(t *testing.T) {
	fSTT := newFakeSTT()
	first, second := newFakeTTS(), newFakeTTS()

	var opened atomic.Int32

	cfg := testConfig(fSTT, first, &fakeLLM{tokens: []string{"Hello."}}, 0, 0)
	cfg.NewTTS = func() (tts.Client, error) {
		if opened.Add(1) == 1 {
			return first, nil
		}

		return second, nil
	}
	h := NewHandler(context.Background(), "call1", cfg)

	fSTT.sendFinal("hi")
	require.Eventually(t, func() bool { return first.spokenCount() == 1 }, time.Second, time.Millisecond)
	require.Equal(t, StateThinking, h.State(), "no audio yet")

	_ = first.Close() // the provider drops the connection before any audio
	require.Eventually(t, func() bool { return h.State() == StateListening }, time.Second, time.Millisecond,
		"the turn must end instead of waiting forever for audio")

	fSTT.sendFinal("hello?")
	require.Eventually(t, func() bool { return second.spokenCount() == 1 }, time.Second, time.Millisecond,
		"the next turn must open a fresh TTS connection")
	assert.Equal(t, int32(2), opened.Load())
}

func TestSignalNeverBlocksAndCoalesces(t *testing.T) {
	ch := make(chan struct{}, 1)

	signal(ch)
	signal(ch) // already pending: must not block, must not be lost

	assert.Len(t, ch, 1)
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
	cfg.Start = &Node{Tools: []Tool{slow}}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	h := NewHandler(ctx, "call1", cfg)

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
	// EndGeneration is the turn goroutine's last act; then stop run() too.
	// (Not h.turnToolMsgs: that's run()'s alone -- reading it here raced.)
	close(release)
	require.Eventually(t, func() bool { return len(fTTS.endedGens()) > 0 }, time.Second, time.Millisecond)
	finalHistory(t, h, cancel)
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
	cfg.Start = &Node{Tools: []Tool{transferTool()}}
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
	cfg.Start = &Node{Tools: []Tool{transferTool()}}
	cfg.HoldAudio = constFrame(holdAmp)
	cfg.Transfer = func(context.Context, string, time.Duration) (func() error, error) {
		return nil, errors.New("transfer destination did not answer (User busy, cause 17)")
	}
	h := NewHandler(context.Background(), "call1", cfg)

	fSTT.sendFinal("connect me to support")
	require.Eventually(t, func() bool { return fTTS.spokenCount() == 2 }, time.Second, time.Millisecond)

	assert.Nil(t, h.hold.Load(), "hold audio must stop when the transfer fails")

	result := fLLM.seenAt(1)[2]
	assert.Equal(t, "tool", result.Role)
	assert.JSONEq(t, `{"status":"failed","reason":"transfer destination did not answer (User busy, cause 17)"}`, result.Content)
	assert.Equal(t, "Nobody is free right now.", fTTS.spokenList()[1])
}

// TestHandler_WorkflowWalk follows a Dograh-shaped workflow end to end: the
// LLM opens the call from the start node's prompt, an edge call moves it to
// the next node (whose prompt and tools the very next round uses, in the
// same turn), and reaching the end node makes its closing line the last
// thing said before the call hangs up.
func TestHandler_WorkflowWalk(t *testing.T) {
	getLog := captureSlog(t)

	fSTT := newFakeSTT()
	fTTS := newFakeTTS()

	end := &Node{Name: "End Call", Prompt: "end prompt", End: true}
	main := &Node{Name: "Main", Prompt: "main prompt", AllowInterrupt: true, Edges: []Edge{
		{Def: llm.FunctionDef{Name: "end_call", Description: "when done"}, To: end},
	}}
	start := &Node{Name: "Start", Prompt: "start prompt", Edges: []Edge{
		{Def: llm.FunctionDef{Name: "move_to_main", Description: "when ready"}, To: main},
	}}

	edgeCall := func(id, name string) []llm.ToolCall {
		return []llm.ToolCall{{ID: id, Type: "function", Function: llm.FunctionCall{Name: name, Arguments: "{}"}}}
	}

	fLLM := &fakeLLM{rounds: []fakeRound{
		{tokens: []string{"Hi there."}},             // opening
		{toolCalls: edgeCall("t1", "move_to_main")}, // caller: "yes"
		{tokens: []string{"Here is the topic."}},    // now at Main
		{toolCalls: edgeCall("t2", "end_call")},     // caller: "bye"
		{tokens: []string{"Thanks, goodbye."}},      // now at End Call
	}}

	var hangups atomic.Int32

	cfg := testConfig(fSTT, fTTS, fLLM, time.Hour, 0)
	cfg.Start = start
	cfg.Hangup = func() { hangups.Add(1) }
	h := NewHandler(context.Background(), "call1", cfg)

	// Opening: the LLM speaks first, from the start node's prompt.
	require.Eventually(t, func() bool { return fTTS.spokenCount() == 1 }, time.Second, time.Millisecond)
	assert.Equal(t, []llm.Message{{Role: "system", Content: "start prompt"}, {Role: "user", Content: "start prompt"}},
		fLLM.seenAt(0), "opening: system prompt, repeated as the only user message")
	assert.Equal(t, "move_to_main", fLLM.toolsAt(0)[0].Function.Name, "the start node's edges are offered")
	playTurn(t, h, fTTS, 1, "Hi there.")

	// Edge call: switch to Main within the same turn.
	fSTT.sendFinal("yes")
	require.Eventually(t, func() bool { return fTTS.spokenCount() == 2 }, time.Second, time.Millisecond)
	assert.Equal(t, "main prompt", fLLM.seenAt(2)[0].Content, "the round after the edge uses the new node's prompt")
	assert.Equal(t, llm.Message{Role: "tool", ToolCallID: "t1", Content: `{"status":"done"}`}, fLLM.seenAt(2)[len(fLLM.seenAt(2))-1])
	assert.Equal(t, "end_call", fLLM.toolsAt(2)[0].Function.Name, "and its edges")
	assert.Equal(t, "Main", h.node.Load().Name)
	playTurn(t, h, fTTS, 2, "Here is the topic.")
	assert.Zero(t, hangups.Load())

	// End node: its closing line plays, then the call hangs up.
	fSTT.sendFinal("bye")
	require.Eventually(t, func() bool { return fTTS.spokenCount() == 3 }, time.Second, time.Millisecond)
	assert.Equal(t, "end prompt", fLLM.seenAt(4)[0].Content)
	// The end node offers no tools, so its request carries no tool calls or
	// results either: Sarvam rejects those without tools (see plainHistory).
	assert.Empty(t, fLLM.toolsAt(4))
	for _, m := range fLLM.seenAt(4) {
		assert.NotEqual(t, "tool", m.Role)
		assert.Empty(t, m.ToolCalls)
	}
	assert.Zero(t, hangups.Load(), "must not hang up before the closing line has played")
	playTurn(t, h, fTTS, 3, "Thanks, goodbye.")

	require.Eventually(t, func() bool { return hangups.Load() == 1 }, time.Second, time.Millisecond)
	assert.Equal(t, 5, fLLM.streamCalls())

	// Each transition logs whether the caller can interrupt at the new node.
	log := getLog().String()
	assert.Contains(t, log, `from=Start to=Main via=move_to_main allow_interrupt=true interrupt=on`)
	assert.Contains(t, log, `from=Main to="End Call" via=end_call allow_interrupt=false interrupt=off`)
}

func TestInterruptMode(t *testing.T) {
	cases := []struct {
		enabled, allow bool
		want           string
	}{
		{true, true, "on"},
		{true, false, "off"},
		{false, true, "off"}, // BARGE_IN_ENABLED=0 wins
		{false, false, "off"},
	}

	for _, c := range cases {
		assert.Equal(t, c.want, InterruptMode(c.enabled, &Node{AllowInterrupt: c.allow}), "%+v", c)
	}
}

func lastMessage(msgs []llm.Message) llm.Message { return msgs[len(msgs)-1] }

// TestHandler_SilentCallerIsAskedThenCallEnds mirrors Dograh's idle handling:
// after the agent speaks, IdleTimeout of silence makes the LLM check whether
// the caller is still there; a second silence gets a goodbye, then a hangup.
func TestHandler_SilentCallerIsAskedThenCallEnds(t *testing.T) {
	fSTT := newFakeSTT()
	fTTS := newFakeTTS()
	fLLM := &fakeLLM{rounds: []fakeRound{
		{tokens: []string{"Are you there?"}},
		{tokens: []string{"Have a good day."}},
	}}

	var hangups atomic.Int32

	cfg := testConfig(fSTT, fTTS, fLLM, time.Hour, 0)
	cfg.Start = &Node{Greeting: "Hello."}
	cfg.IdleTimeout = 50 * time.Millisecond
	cfg.Hangup = func() { hangups.Add(1) }
	h := NewHandler(context.Background(), "call1", cfg)

	require.Eventually(t, func() bool { return fTTS.spokenCount() == 1 }, time.Second, time.Millisecond)
	playTurn(t, h, fTTS, 1, "Hello.")

	require.Eventually(t, func() bool { return fTTS.spokenCount() == 2 }, time.Second, time.Millisecond)
	assert.Equal(t, llm.Message{Role: "user", Content: idleFirstPrompt}, lastMessage(fLLM.seenAt(0)))
	playTurn(t, h, fTTS, 2, "Are you there?")

	require.Eventually(t, func() bool { return fTTS.spokenCount() == 3 }, time.Second, time.Millisecond)
	assert.Equal(t, llm.Message{Role: "user", Content: idleFinalPrompt}, lastMessage(fLLM.seenAt(1)))
	assert.Zero(t, hangups.Load(), "the goodbye must play first")
	playTurn(t, h, fTTS, 3, "Have a good day.")

	require.Eventually(t, func() bool { return hangups.Load() == 1 }, time.Second, time.Millisecond)
	time.Sleep(150 * time.Millisecond)
	assert.Equal(t, 2, fLLM.streamCalls(), "no more silence prompts once the call is ending")
}

// TestHandler_CallerSpeechPausesIdleClock: while the caller is speaking the
// silence clock is stopped; if what they said never becomes a transcript
// (noise), it restarts when they stop.
func TestHandler_CallerSpeechPausesIdleClock(t *testing.T) {
	fSTT := newFakeSTT()
	fTTS := newFakeTTS()
	fLLM := &fakeLLM{rounds: []fakeRound{{tokens: []string{"Still there?"}}}}

	cfg := testConfig(fSTT, fTTS, fLLM, time.Hour, 0)
	cfg.Start = &Node{Greeting: "Hello."}
	cfg.IdleTimeout = 80 * time.Millisecond
	h := NewHandler(context.Background(), "call1", cfg)

	require.Eventually(t, func() bool { return fTTS.spokenCount() == 1 }, time.Second, time.Millisecond)
	playTurn(t, h, fTTS, 1, "Hello.")

	fSTT.results <- stt.Result{Signal: stt.SpeechStarted}
	time.Sleep(250 * time.Millisecond)
	assert.Zero(t, fLLM.streamCalls(), "no silence prompt while the caller is speaking")

	fSTT.results <- stt.Result{Signal: stt.SpeechEnded} // no transcript follows
	require.Eventually(t, func() bool { return fLLM.streamCalls() == 1 }, time.Second, time.Millisecond)
	assert.Equal(t, idleFirstPrompt, lastMessage(fLLM.seenAt(0)).Content)
}

func TestHandler_MaxDurationHangsUpAtOnceWhenListening(t *testing.T) {
	var hangups atomic.Int32

	cfg := testConfig(newFakeSTT(), newFakeTTS(), &fakeLLM{}, time.Hour, 0)
	cfg.MaxDuration = 30 * time.Millisecond
	cfg.Hangup = func() { hangups.Add(1) }
	NewHandler(context.Background(), "call1", cfg)

	require.Eventually(t, func() bool { return hangups.Load() == 1 }, time.Second, time.Millisecond)
}

func TestHandler_MaxDurationLetsCurrentReplyFinish(t *testing.T) {
	fSTT := newFakeSTT()
	fTTS := newFakeTTS()

	var hangups atomic.Int32

	cfg := testConfig(fSTT, fTTS, &fakeLLM{tokens: []string{"A long answer."}}, time.Hour, 0)
	cfg.MaxDuration = 100 * time.Millisecond
	cfg.Hangup = func() { hangups.Add(1) }
	h := NewHandler(context.Background(), "call1", cfg)

	fSTT.sendFinal("tell me")
	require.Eventually(t, func() bool { return fTTS.spokenCount() == 1 }, time.Second, time.Millisecond)
	fTTS.audio <- tts.Chunk{PCM: constFrame(quietAmplitude), Gen: 1, Req: 1, Text: "A long answer."}
	require.Eventually(t, func() bool { return h.State() == StateSpeaking }, time.Second, time.Millisecond)

	time.Sleep(200 * time.Millisecond)
	assert.Zero(t, hangups.Load(), "the reply in progress plays out first")

	fTTS.done <- 1
	drainUntilListening(t, h)
	require.Eventually(t, func() bool { return hangups.Load() == 1 }, time.Second, time.Millisecond)
}

// TestHandler_LogsLatencyFromCallerSpeechEnd: a transcript preceded by the
// STT's end-of-speech signal logs endpoint_ms, and its reply's first audio
// logs since_speech_end_ms; a transcript with no signal of its own logs
// neither (a stale end-of-speech must never be borrowed).
func TestHandler_LogsLatencyFromCallerSpeechEnd(t *testing.T) {
	getLog := captureSlog(t)

	fSTT := newFakeSTT()
	fTTS := newFakeTTS()
	fLLM := &fakeLLM{tokens: []string{"Okay."}}
	h := NewHandler(context.Background(), "call1", testConfig(fSTT, fTTS, fLLM, time.Hour, 0))

	fSTT.results <- stt.Result{Signal: stt.SpeechStarted}
	fSTT.results <- stt.Result{Signal: stt.SpeechEnded}
	fSTT.sendFinal("first")
	require.Eventually(t, func() bool { return fTTS.spokenCount() == 1 }, time.Second, time.Millisecond)
	playTurn(t, h, fTTS, 1, "Okay.")

	fSTT.sendFinal("second") // no START/END for this one
	require.Eventually(t, func() bool { return fTTS.spokenCount() == 2 }, time.Second, time.Millisecond)
	playTurn(t, h, fTTS, 2, "Okay.")

	var first, second []string

	for _, line := range strings.Split(getLog().String(), "\n") {
		switch {
		case strings.Contains(line, "gen=1") && (strings.Contains(line, "event=turn.user") || strings.Contains(line, "tts first audio")):
			first = append(first, line)
		case strings.Contains(line, "gen=2") && (strings.Contains(line, "event=turn.user") || strings.Contains(line, "tts first audio")):
			second = append(second, line)
		}
	}

	require.Len(t, first, 2)
	assert.Contains(t, first[0], "endpoint_ms=")
	assert.Contains(t, first[1], "since_speech_end_ms=")

	require.Len(t, second, 2)
	assert.NotContains(t, second[0], "endpoint_ms=")
	assert.NotContains(t, second[1], "since_speech_end_ms=")
}

// fakeClockDetector is a BargeInDetector that can say when it last heard
// speech (like TenVadDetector), with that time set by the test.
type fakeClockDetector struct {
	mu      sync.Mutex
	last    time.Time
	detects int
}

func (d *fakeClockDetector) Detect([]byte) bool {
	d.mu.Lock()
	defer d.mu.Unlock()

	d.detects++

	return false
}

func (d *fakeClockDetector) Reset() {}

func (d *fakeClockDetector) LastSpeechAt() time.Time {
	d.mu.Lock()
	defer d.mu.Unlock()

	return d.last
}

func (d *fakeClockDetector) set(t time.Time) {
	d.mu.Lock()
	defer d.mu.Unlock()

	d.last = t
}

func (d *fakeClockDetector) detectCount() int {
	d.mu.Lock()
	defer d.mu.Unlock()

	return d.detects
}

// TestHandler_LogsCallerLastSoundAndConversation: with a VAD that knows when
// it last heard speech, the turn logs end_detect_ms (last sound -> Sarvam's
// end-of-speech) and since_last_speech_ms (last sound -> first audio); a
// last sound older than the previous end-of-speech is never used. The
// conversation is logged as turn.user/turn.agent lines.
func TestHandler_LogsCallerLastSoundAndConversation(t *testing.T) {
	getLog := captureSlog(t)

	fSTT := newFakeSTT()
	fTTS := newFakeTTS()
	vad := &fakeClockDetector{}

	cfg := testConfig(fSTT, fTTS, &fakeLLM{tokens: []string{"Okay."}}, time.Hour, 0)
	cfg.BargeIn = vad
	h := NewHandler(context.Background(), "call1", cfg)

	h.ProcessFrame(context.Background(), "call1", constFrame(quietAmplitude))
	assert.Equal(t, 1, vad.detectCount(), "the VAD runs while listening too")

	// Utterance 1: the VAD heard the caller just before Sarvam's END_SPEECH.
	fSTT.results <- stt.Result{Signal: stt.SpeechStarted}
	vad.set(time.Now())
	fSTT.results <- stt.Result{Signal: stt.SpeechEnded}
	fSTT.sendFinal("first")
	require.Eventually(t, func() bool { return fTTS.spokenCount() == 1 }, time.Second, time.Millisecond)
	playTurn(t, h, fTTS, 1, "Okay.")

	// Utterance 2: Sarvam ends speech, but the VAD heard nothing new since
	// utterance 1 -- its last sound is stale and must not be used.
	fSTT.results <- stt.Result{Signal: stt.SpeechStarted}
	fSTT.results <- stt.Result{Signal: stt.SpeechEnded}
	fSTT.sendFinal("second")
	require.Eventually(t, func() bool { return fTTS.spokenCount() == 2 }, time.Second, time.Millisecond)
	playTurn(t, h, fTTS, 2, "Okay.")

	lines := map[string]string{}

	for _, line := range strings.Split(getLog().String(), "\n") {
		for _, key := range []string{"gen=1", "gen=2"} {
			for _, msg := range []string{"event=turn.user", "tts first audio"} {
				if strings.Contains(line, key) && strings.Contains(line, msg) {
					lines[key+" "+msg] = line
				}
			}
		}
	}

	assert.Contains(t, lines["gen=1 event=turn.user"], "end_detect_ms=")
	assert.Contains(t, lines["gen=1 tts first audio"], "since_last_speech_ms=")
	assert.Contains(t, lines["gen=2 event=turn.user"], "endpoint_ms=", "Sarvam's own figure is still there")
	assert.NotContains(t, lines["gen=2 event=turn.user"], "end_detect_ms=")
	assert.NotContains(t, lines["gen=2 tts first audio"], "since_last_speech_ms=")

	log := getLog().String()
	assert.Contains(t, log, `event=turn.user turn=1 node="" gen=1 text=first`)
	assert.Contains(t, log, `event=turn.agent turn=1 node="" gen=1 cut=false text=Okay.`)
	assert.Regexp(t, `event=turn\.agent turn=1 .* llm_ttft_ms=\d+ tts_ttfa_ms=\d+ reply_latency_ms=\d+`, log,
		"the reply's timings are on its turn.agent line")
	assert.NotContains(t, log, "agent reply recorded")
}

func TestJunkTranscript(t *testing.T) {
	cases := map[string]bool{
		"ह":     true, // a lone letter: what noise transcribes to
		"म।":    true, // plus punctuation
		"।":     true, // punctuation only
		"":      true,
		"जी":    false, // letter + vowel sign: a real answer
		"ना।":   false,
		"हाँ।":  false,
		"5":     false, // a digit answer
		"no":    false,
		"बेनी।": false, // live: a real short answer the old 1.2s gate threw away
	}

	for text, want := range cases {
		assert.Equal(t, want, junkTranscript(text), "%q", text)
	}
}

// TestHandler_PausedJunkDoesNotConfirmButAShortAnswerDoes: during a pause a
// lone-letter transcript is noise and leaves the pause in place; a real
// one-word answer like "जी" confirms the interruption.
func TestHandler_PausedJunkDoesNotConfirmButAShortAnswerDoes(t *testing.T) {
	fSTT := newFakeSTT()
	fTTS := newFakeTTS()
	fLLM := &fakeLLM{tokens: []string{"A long question."}}

	h := NewHandler(context.Background(), "call1", testConfig(fSTT, fTTS, fLLM, 0, 0))

	fSTT.sendFinal("hi")
	require.Eventually(t, func() bool { return fTTS.spokenCount() == 1 }, time.Second, time.Millisecond)
	fTTS.audio <- tts.Chunk{PCM: multiFrame(20, quietAmplitude), Gen: 1, Req: 1, Text: "A long question."}
	require.Eventually(t, func() bool { return h.State() == StateSpeaking }, time.Second, time.Millisecond)

	for i := 0; i < 3; i++ {
		h.ProcessFrame(context.Background(), "call1", constFrame(loudAmplitude))
	}

	require.True(t, h.paused.Load())

	fSTT.sendFinal("म")
	time.Sleep(30 * time.Millisecond)
	assert.True(t, h.paused.Load(), "junk leaves the pause in place")
	assert.False(t, fTTS.wasCancelled())

	fSTT.sendFinal("जी")
	require.Eventually(t, fTTS.wasCancelled, time.Second, time.Millisecond, "a real short answer confirms")
	require.Eventually(t, func() bool { return fLLM.streamCalls() == 2 }, time.Second, time.Millisecond)
	assert.Equal(t, llm.Message{Role: "user", Content: "जी"}, lastMessage(fLLM.seenAt(1)))
}

// TestHandler_ReplyHeardWhenTheCallEndsIsLogged: the caller hanging up
// mid-reply still leaves a turn.agent line with what they heard.
func TestHandler_ReplyHeardWhenTheCallEndsIsLogged(t *testing.T) {
	getLog := captureSlog(t)

	fSTT := newFakeSTT()
	fTTS := newFakeTTS()

	ctx, hangUp := context.WithCancel(context.Background())
	defer hangUp()

	h := NewHandler(ctx, "call1", testConfig(fSTT, fTTS, &fakeLLM{tokens: []string{"Goodbye now.", " Take care."}}, 0, 0))

	fSTT.sendFinal("bye")
	require.Eventually(t, func() bool { return fTTS.spokenCount() == 2 }, time.Second, time.Millisecond)
	fTTS.audio <- tts.Chunk{PCM: multiFrame(3, quietAmplitude), Gen: 1, Req: 1, Text: "Goodbye now."}
	fTTS.audio <- tts.Chunk{PCM: multiFrame(3, quietAmplitude), Gen: 1, Req: 2, Text: " Take care."}
	require.Eventually(t, func() bool { return len(h.outbound) == 6 }, time.Second, time.Millisecond)

	h.ProcessFrame(context.Background(), "call1", constFrame(quietAmplitude)) // the first sentence starts playing
	hangUp()

	require.Eventually(t, func() bool {
		return strings.Contains(getLog().String(), `event=turn.agent turn=1 node="" gen=1 cut=true text="Goodbye now."`)
	}, time.Second, time.Millisecond)
}

// TestHandler_STTHearsTheCallerInEveryState: the caller's audio reaches the
// STT on every tick -- listening, preparing a reply, speaking, paused -- so
// an answer begun over the agent is transcribed from its first syllable
// (live: "महंगाई" clipped to "हाँ जी" / "नहीं" when the STT only listened
// between replies). Each frame goes once, nothing extra.
func TestHandler_STTHearsTheCallerInEveryState(t *testing.T) {
	fSTT := newFakeSTT()
	fTTS := newFakeTTS()
	release := make(chan struct{})

	slowLLM := &blockingLLM{release: release, tokens: []string{"A reply."}}
	h := NewHandler(context.Background(), "call1", testConfig(fSTT, fTTS, slowLLM, 0, 0))

	tick := func() { h.ProcessFrame(context.Background(), "call1", constFrame(quietAmplitude)) }

	tick() // listening
	assert.Equal(t, 1, fSTT.fedCount())

	fSTT.sendFinal("hi")
	require.Eventually(t, func() bool { return h.State() == StateThinking }, time.Second, time.Millisecond)
	tick() // preparing the reply
	assert.Equal(t, 2, fSTT.fedCount())

	close(release)
	require.Eventually(t, func() bool { return fTTS.spokenCount() == 1 }, time.Second, time.Millisecond)
	fTTS.audio <- tts.Chunk{PCM: multiFrame(10, quietAmplitude), Gen: 1, Req: 1, Text: "A reply."}
	require.Eventually(t, func() bool { return h.State() == StateSpeaking }, time.Second, time.Millisecond)
	tick() // speaking
	assert.Equal(t, 3, fSTT.fedCount())

	for i := 0; i < 3; i++ { // the caller talks over it: paused
		h.ProcessFrame(context.Background(), "call1", constFrame(loudAmplitude))
	}

	require.True(t, h.paused.Load())
	tick()
	assert.Equal(t, 7, fSTT.fedCount(), "paused: still one frame per tick")
}

// TestHandler_TranscriptWhileSpeakingIsMuted: a transcript of something said
// entirely while the agent spoke, with no interruption, is discarded and
// logged (Dograh's mute) -- not silently dropped.
func TestHandler_TranscriptWhileSpeakingIsMuted(t *testing.T) {
	getLog := captureSlog(t)

	fSTT := newFakeSTT()
	fTTS := newFakeTTS()
	fLLM := &fakeLLM{tokens: []string{"A reply."}}

	cfg := testConfig(fSTT, fTTS, fLLM, 0, 0)
	cfg.Start = &Node{AllowInterrupt: false}
	h := NewHandler(context.Background(), "call1", cfg)

	fSTT.sendFinal("hi")
	require.Eventually(t, func() bool { return fTTS.spokenCount() == 1 }, time.Second, time.Millisecond)
	fTTS.audio <- tts.Chunk{PCM: multiFrame(10, quietAmplitude), Gen: 1, Req: 1, Text: "A reply."}
	require.Eventually(t, func() bool { return h.State() == StateSpeaking }, time.Second, time.Millisecond)

	fSTT.sendFinal("जी")
	require.Eventually(t, func() bool {
		return strings.Contains(getLog().String(), "transcript ignored: the agent is speaking")
	}, time.Second, time.Millisecond)
	assert.Equal(t, 1, fLLM.streamCalls(), "no new turn")

	fTTS.done <- 1
	drainUntilListening(t, h)
}

// blockingLLM holds its reply until release is closed, so a test can
// observe the Thinking state.
type blockingLLM struct {
	release chan struct{}
	tokens  []string
}

func (b *blockingLLM) Stream(ctx context.Context, _ []llm.Message, _ []llm.Tool, onToken func(string)) ([]llm.ToolCall, error) {
	select {
	case <-b.release:
	case <-ctx.Done():
		return nil, ctx.Err()
	}

	for _, tok := range b.tokens {
		onToken(tok)
	}

	return nil, nil
}
