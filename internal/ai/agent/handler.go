package agent

import (
	"context"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/nitesh/vaani/internal/ai/llm"
	"github.com/nitesh/vaani/internal/ai/stt"
	"github.com/nitesh/vaani/internal/ai/tts"
	"github.com/nitesh/vaani/internal/media"
)

// State is the agent conversation state. ProcessFrame's behavior depends
// entirely on this: it decides whether inbound audio goes to STT, is ignored
// (agent is thinking), or is scanned for barge-in (agent is speaking).
type State int32

const (
	StateListening State = iota
	StateTranscribing
	StateThinking
	StateSpeaking
)

func (s State) String() string {
	switch s {
	case StateListening:
		return "listening"
	case StateTranscribing:
		return "transcribing"
	case StateThinking:
		return "thinking"
	case StateSpeaking:
		return "speaking"
	default:
		return "unknown"
	}
}

// prerollFrames is how many trailing 20ms SPEAKING-state frames are kept (200ms):
// when barge-in fires, STT never saw these frames (it isn't fed during
// SPEAKING, to avoid transcribing the agent's own voice), so they're flushed to
// STT first, recovering the start of the interrupting utterance.
const prerollFrames = 10

// outboundBufferFrames sizes Handler.outbound. This is not a jitter margin --
// it's the gap between how TTS audio arrives and how it can be played out.
// Sarvam delivers a whole turn's synthesized audio over the network in a
// fraction of a second (far faster than real time), but the phone leg can
// only ever consume it at exactly one 20ms frame at a time; that mismatch has
// to sit somewhere. A too-small buffer here doesn't smooth anything, it just
// drops the excess -- observed live: a 5-frame (100ms) buffer silently
// dropped 1915 frames (~38s of audio) across a handful of short turns via the
// select-default branch in handleTTSAudio. Sized for 30s of audio, comfortably
// past any single realistic reply; barge-in already drains this queue
// entirely (handleBargeIn), so a larger buffer doesn't risk playing stale
// audio past an interruption, only avoids dropping it before one.
const outboundBufferFrames = int(30 * time.Second / media.FrameInterval)

// speakingDrainTimeoutTicks is the dead-call safeguard horizon for the
// Speaking state: 500 consecutive 20ms release ticks (10s) with an empty
// outbound queue and no ttsDeliveryDone means the TTS connection died
// mid-turn (or its completion event was lost) -- handleTTSDone will never
// run, and without this the turn would never end and STT would never be fed
// again (inbound RTP keeps flowing, so the media plane's own dead-call
// detection sees a healthy call). A synthesis stall legitimately holds the
// queue empty for at most a couple of seconds -- observed first-audio
// latency is 0.4-0.7s. See processSpeakingFrame.
const speakingDrainTimeoutTicks = 500

// llmStreamer is the subset of *llm.Client that Handler needs, as an
// interface so tests can inject a fake without a live LLM endpoint.
type llmStreamer interface {
	Stream(ctx context.Context, messages []llm.Message, onToken func(string)) error
}

// Sink receives Handler observability events, adapted to Prometheus metrics at
// the call site (same pattern as media.Sink) so this package stays free of a
// prometheus dependency.
type Sink interface {
	BargeIn()
	TurnStarted()
	PrerollFlushed(frames int)
	Error(stage string)
}

// NoopSink discards every event; useful for tests and as a safe default.
type NoopSink struct{}

func (NoopSink) BargeIn()           {}
func (NoopSink) TurnStarted()       {}
func (NoopSink) PrerollFlushed(int) {}
func (NoopSink) Error(string)       {}

// Config bundles one call's agent dependencies.
type Config struct {
	STT stt.Client
	// NewTTS is a factory, not a live client: the TTS connection is billed on
	// open (docs/AI_PROVIDERS.md), so it must open lazily at the first
	// THINKING transition, never at call start. It is called at most once per
	// call: the resulting Client is reused across every turn (see
	// tts.Client's doc comment) and only replaced if a barge-in cancels it.
	NewTTS       func() (tts.Client, error)
	LLM          llmStreamer
	SystemPrompt string
	// Greeting, if set, is spoken once at the start of the call, before the
	// caller says anything -- bypasses the LLM entirely (it's fixed text, not
	// generated) and is recorded into history as the agent's own turn so the
	// LLM doesn't redundantly re-greet on the caller's first real reply.
	Greeting string

	BargeIn BargeInDetector
	// BargeInGuard ignores barge-in detection for this long after entering
	// SPEAKING, so the agent's own voice leaking into the mic at the start of
	// playback can't immediately trigger a false self-interrupt.
	BargeInGuard time.Duration
	// PostCutSilence is how long a barge-in cut requires quiet before a new
	// final transcript is accepted -- otherwise the tail of the interrupting
	// utterance the STT was already mid-transcribing when it flushed could
	// double-fire a turn.
	PostCutSilence time.Duration

	Sink Sink
}

type finalTranscriptEvent struct{ text string }
type llmDoneEvent struct{ err error }
type ttsAudioEvent struct {
	pcm []byte
	gen uint64
}
type ttsDoneEvent struct{ gen uint64 }
type bargeInEvent struct{}
type greetingEvent struct{}
type ttsPlaybackDoneEvent struct{}
type ttsDrainTimeoutEvent struct{}

// Handler implements media.Handler: Sarvam STT -> LLM -> Sarvam TTS with local
// barge-in detection. See docs/AUDIO_PIPELINE.md's Handler Contract and
// docs/AI_PROVIDERS.md.
//
// Concurrency design: every field below has exactly one owning goroutine, so
// there is no lock in the hot path and nothing to get wrong under -race (which
// this project's toolchain can't run locally -- see CLAUDE.md conventions):
//   - state/speakingSince/silenceUntil/ttsDeliveryDone: atomics, written by
//     whichever goroutine reaches the transition, read by any.
//   - tts/turnCancel/history: touched only by run()'s goroutine.
//   - preroll/emptyTicks: touched only by ProcessFrame's goroutine.
//   - per-turn chunker: a local variable inside runLLMTurn, never shared.
//   - outbound: a channel (safe for concurrent send/receive by construction).
type Handler struct {
	callID  string
	cfg     Config
	baseCtx context.Context

	state         atomic.Int32
	speakingSince atomic.Int64
	silenceUntil  atomic.Int64
	// ttsDeliveryDone is set once handleTTSDone has run for the current
	// generation while still Speaking (Sarvam has finished sending audio, but
	// outbound may still hold unplayed frames -- Sarvam delivers far faster
	// than real time). processSpeakingFrame (a different goroutine) watches
	// this and only ends the turn once outbound has also fully drained, so a
	// reply is never cut off before the caller has actually heard all of it.
	ttsDeliveryDone atomic.Bool

	events chan any

	// run()-owned only.
	tts        tts.Client
	turnCancel context.CancelFunc
	history    []llm.Message
	ttsBuf     []byte
	// curGen increments once per turn. Every Speak call and every inbound
	// ttsAudioEvent/ttsDoneEvent is tagged with the generation active when it
	// was created; handlers drop anything that doesn't match curGen, which is
	// what makes a barge-in's stale, already-in-flight TTS audio harmless even
	// though the connection itself is reused across turns (see tts.Client's
	// doc comment).
	curGen uint64
	// turnStartedAt/ttfaLoggedGen are run()-owned bookkeeping for the per-turn
	// latency logs (handleFinalTranscript/handleTTSAudio/handleTTSDone):
	// turnStartedAt marks when the current generation's transcript was
	// accepted, and ttfaLoggedGen records which generation's first TTS audio
	// chunk has already been logged, so repeat chunks don't repeat the log.
	turnStartedAt time.Time
	ttfaLoggedGen uint64

	// ProcessFrame-goroutine-owned only.
	preroll [][]byte
	// emptyTicks counts consecutive Speaking-state release ticks that found
	// the outbound queue empty without ttsDeliveryDone set -- the drain
	// dead-call safeguard in processSpeakingFrame. A counter, not a stored
	// timestamp: a wall-clock stamp from the previous turn's drain would
	// mis-time the next turn's first-audio window (Listening can last
	// minutes). Reset on every pop, barge-in, and playback-done.
	emptyTicks int

	// TTS PCM re-sliced to 640B frames, drained by ProcessFrame at real-time
	// pace. See outboundBufferFrames for why this is sized in seconds, not
	// CallMedia's 5-frame jitter margin.
	outbound chan []byte
}

// NewHandler creates a Handler and starts its background goroutines (STT
// reader, event loop), both of which stop when ctx is cancelled -- pass the
// call's own context (the same one CallMedia.Run receives).
func NewHandler(ctx context.Context, callID string, cfg Config) *Handler {
	if cfg.Sink == nil {
		cfg.Sink = NoopSink{}
	}

	h := &Handler{
		callID:   callID,
		cfg:      cfg,
		baseCtx:  ctx,
		events:   make(chan any, 8),
		outbound: make(chan []byte, outboundBufferFrames),
	}
	h.state.Store(int32(StateListening))

	if cfg.SystemPrompt != "" {
		h.history = append(h.history, llm.Message{Role: "system", Content: cfg.SystemPrompt})
	}

	go h.readSTT()
	go h.run()

	if cfg.Greeting != "" {
		h.events <- greetingEvent{} // buffer is fresh; guaranteed not to block
	}

	return h
}

// State reports the current conversation state.
func (h *Handler) State() State { return State(h.state.Load()) }

// Close ends the STT connection. The current TTS connection (if any) and the
// background goroutines are cleaned up when the call's context is cancelled,
// not here -- see the concurrency-design comment on Handler.
func (h *Handler) Close() error {
	return h.cfg.STT.Close()
}

// ProcessFrame implements media.Handler. Never blocks, never allocates beyond
// a pooled-size copy for the pre-roll buffer during SPEAKING.
func (h *Handler) ProcessFrame(_ context.Context, _ string, pcm []byte) [][]byte {
	switch State(h.state.Load()) {
	case StateListening, StateTranscribing:
		if err := h.cfg.STT.Feed(pcm); err != nil {
			h.cfg.Sink.Error("stt")
		}

		return nil

	case StateThinking:
		// Silence fallback (CallMedia's writeLoop) handles this tick. Do not
		// feed STT: there's nothing new to transcribe until the agent responds.
		return nil

	case StateSpeaking:
		return h.processSpeakingFrame(pcm)

	default:
		return nil
	}
}

func (h *Handler) processSpeakingFrame(pcm []byte) [][]byte {
	h.appendPreroll(pcm)

	elapsed := time.Duration(time.Now().UnixNano() - h.speakingSince.Load())
	if elapsed >= h.cfg.BargeInGuard && h.cfg.BargeIn.Detect(pcm) {
		// Order matters: flip state and flush BEFORE signaling run() to cut
		// the turn, so the very next ProcessFrame call (20ms later) already
		// resumes feeding STT live instead of re-detecting the same barge-in.
		h.state.Store(int32(StateTranscribing))
		h.flushPrerollToSTT()
		h.cfg.BargeIn.Reset()
		h.silenceUntil.Store(time.Now().Add(h.cfg.PostCutSilence).UnixNano())
		h.cfg.Sink.BargeIn()
		slog.Info("agent barge-in detected", "call_id", h.callID)
		h.emptyTicks = 0

		select {
		case h.events <- bargeInEvent{}:
		default:
			slog.Error("agent: events channel full dropping bargeInEvent", "call_id", h.callID)
		}

		return nil
	}

	select {
	case frame := <-h.outbound:
		h.emptyTicks = 0

		return [][]byte{frame}
	default:
	}

	// Nothing queued this tick. If TTS already finished delivering this
	// generation's audio too, the reply is now fully played -- end the turn.
	// CompareAndSwap makes the detect-and-consume atomic so this can only fire
	// once per generation, even if several ticks in a row find an empty queue
	// before run() gets around to processing the event.
	if h.ttsDeliveryDone.CompareAndSwap(true, false) {
		h.emptyTicks = 0

		select {
		case h.events <- ttsPlaybackDoneEvent{}:
		default:
			slog.Error("agent: events channel full dropping ttsPlaybackDoneEvent", "call_id", h.callID)
		}

		return nil
	}

	// Dead-call safeguard (see speakingDrainTimeoutTicks): TTS gone silent
	// mid-turn without delivery-done. Like a barge-in, the state flip happens
	// right here (this goroutine) so the very next tick resumes feeding STT,
	// and an event tells run() to clear its turn state; the guard means this
	// can't stomp on a barge-in that already advanced us to Transcribing.
	h.emptyTicks++
	if h.emptyTicks >= speakingDrainTimeoutTicks {
		h.emptyTicks = 0
		h.cfg.Sink.Error("tts_drain_timeout")
		slog.Warn("speaking with empty outbound queue and no tts delivery-done for 10s; forcing turn end (tts connection likely dead)",
			"call_id", h.callID)

		if h.state.CompareAndSwap(int32(StateSpeaking), int32(StateListening)) {
			select {
			case h.events <- ttsDrainTimeoutEvent{}:
			default:
				slog.Error("agent: events channel full dropping ttsDrainTimeoutEvent", "call_id", h.callID)
			}
		}
	}

	return nil
}

func (h *Handler) appendPreroll(pcm []byte) {
	cp := make([]byte, len(pcm))
	copy(cp, pcm)

	if len(h.preroll) >= prerollFrames {
		h.preroll = h.preroll[1:]
	}

	h.preroll = append(h.preroll, cp)
}

func (h *Handler) flushPrerollToSTT() {
	for _, frame := range h.preroll {
		if err := h.cfg.STT.Feed(frame); err != nil {
			h.cfg.Sink.Error("stt")
			break
		}
	}

	h.cfg.Sink.PrerollFlushed(len(h.preroll))
	h.preroll = h.preroll[:0]
}

func (h *Handler) readSTT() {
	for {
		select {
		case <-h.baseCtx.Done():
			return
		case r, ok := <-h.cfg.STT.Results():
			if !ok {
				return
			}

			if !r.Final {
				continue
			}

			select {
			case h.events <- finalTranscriptEvent{text: r.Text}:
			case <-h.baseCtx.Done():
				return
			}
		}
	}
}

func (h *Handler) run() {
	for {
		select {
		case <-h.baseCtx.Done():
			if h.tts != nil {
				_ = h.tts.Close()
			}

			return
		case ev := <-h.events:
			h.handleEvent(ev)
		}
	}
}

func (h *Handler) handleEvent(ev any) {
	switch e := ev.(type) {
	case finalTranscriptEvent:
		h.handleFinalTranscript(e.text)
	case llmDoneEvent:
		h.handleLLMDone(e.err)
	case ttsAudioEvent:
		h.handleTTSAudio(e.pcm, e.gen)
	case ttsDoneEvent:
		h.handleTTSDone(e.gen)
	case bargeInEvent:
		h.handleBargeIn()
	case greetingEvent:
		h.handleGreeting()
	case ttsPlaybackDoneEvent:
		h.finishTurn(h.curGen)
	case ttsDrainTimeoutEvent:
		// State was already flipped by processSpeakingFrame; this only clears
		// run()-owned turn state so a dead turn's <640B ttsBuf tail can't leak
		// into the front of the next turn's audio.
		h.ttsBuf = nil
	}
}

// openTTS lazily opens h.tts if not already open, logging and reverting to
// Listening on failure. Returns false if the caller should abandon whatever
// turn it was starting.
func (h *Handler) openTTS(gen uint64) bool {
	if h.tts != nil {
		return true
	}

	t, err := h.cfg.NewTTS()
	if err != nil {
		h.cfg.Sink.Error("tts_open")
		h.state.Store(int32(StateListening))
		slog.Error("tts open failed", "call_id", h.callID, "gen", gen, "error", err)

		return false
	}

	h.tts = t

	go h.readTTS(t)

	return true
}

// handleGreeting speaks Config.Greeting once, at call start, before the
// caller has said anything -- fixed text, so it bypasses the LLM entirely.
// Recorded into history as the agent's own turn so the LLM doesn't
// redundantly re-greet on the caller's first real reply.
func (h *Handler) handleGreeting() {
	h.state.Store(int32(StateThinking))

	h.curGen++
	gen := h.curGen
	h.turnStartedAt = time.Now()

	slog.Info("agent greeting", "call_id", h.callID, "gen", gen, "text", h.cfg.Greeting)

	if !h.openTTS(gen) {
		return
	}

	h.history = append(h.history, llm.Message{Role: "assistant", Content: h.cfg.Greeting})

	chunker := &SentenceChunker{}
	for _, chunk := range chunker.Feed(h.cfg.Greeting) {
		h.sendToTTS(h.tts, chunk, gen)
	}

	if remainder := chunker.Flush(); remainder != "" {
		h.sendToTTS(h.tts, remainder, gen)
	}

	h.tts.EndGeneration(gen)
}

func (h *Handler) handleFinalTranscript(text string) {
	if time.Now().UnixNano() < h.silenceUntil.Load() {
		slog.Debug("agent: dropping final transcript inside post-cut silence gate", "call_id", h.callID)
		return
	}

	switch State(h.state.Load()) {
	case StateThinking, StateSpeaking:
		return // already mid-turn; STT shouldn't produce a final here, but guard anyway
	}

	h.state.Store(int32(StateThinking))
	h.history = append(h.history, llm.Message{Role: "user", Content: text})
	h.cfg.Sink.TurnStarted()

	h.curGen++
	gen := h.curGen
	h.turnStartedAt = time.Now()

	slog.Info("stt final transcript", "call_id", h.callID, "gen", gen, "text", text)

	if !h.openTTS(gen) {
		return
	}

	ctx, cancel := context.WithCancel(h.baseCtx)
	h.turnCancel = cancel

	history := append([]llm.Message(nil), h.history...)

	go h.runLLMTurn(ctx, history, h.tts, gen, h.turnStartedAt)
}

func (h *Handler) runLLMTurn(ctx context.Context, history []llm.Message, ttsClient tts.Client, gen uint64, turnStartedAt time.Time) {
	chunker := &SentenceChunker{}

	var firstTokenAt time.Time

	err := h.cfg.LLM.Stream(ctx, history, func(tok string) {
		if firstTokenAt.IsZero() {
			firstTokenAt = time.Now()
			slog.Info("llm first token", "call_id", h.callID, "gen", gen,
				"latency_ms", firstTokenAt.Sub(turnStartedAt).Milliseconds())
		}

		for _, chunk := range chunker.Feed(tok) {
			h.sendToTTS(ttsClient, chunk, gen)
		}
	})

	if ctx.Err() != nil {
		// Cancelled by barge-in or call teardown; a barge-in already called
		// Cancel on this exact ttsClient, so no further Speak calls for gen
		// are possible and there's nothing to end.
		slog.Info("llm turn cancelled", "call_id", h.callID, "gen", gen,
			"duration_ms", time.Since(turnStartedAt).Milliseconds())

		return
	}

	if err == nil {
		if remainder := chunker.Flush(); remainder != "" {
			h.sendToTTS(ttsClient, remainder, gen)
		}

		slog.Info("llm turn complete", "call_id", h.callID, "gen", gen,
			"duration_ms", time.Since(turnStartedAt).Milliseconds())
	} else {
		slog.Warn("llm turn error", "call_id", h.callID, "gen", gen,
			"duration_ms", time.Since(turnStartedAt).Milliseconds(), "error", err)
	}

	// No further Speak(gen) calls are coming. ttsClient.Done() will yield gen
	// once its audio (if any was sent at all) has fully arrived.
	ttsClient.EndGeneration(gen)

	select {
	case h.events <- llmDoneEvent{err: err}:
	case <-h.baseCtx.Done():
	}
}

func (h *Handler) sendToTTS(ttsClient tts.Client, text string, gen uint64) {
	if err := ttsClient.Speak(text, gen); err != nil {
		h.cfg.Sink.Error("tts_speak")
		slog.Warn("tts speak failed", "call_id", h.callID, "gen", gen, "error", err)

		return
	}

	slog.Info("tts speak", "call_id", h.callID, "gen", gen, "chars", len([]rune(text)))
	// The Thinking -> Speaking transition happens in handleTTSAudio, once real
	// audio for gen actually arrives -- not here, when text is merely sent.
	// Flipping here left a silence gap exactly as long as Sarvam's
	// synthesis+network round trip (ProcessFrame would already be draining an
	// empty outbound queue).
}

// readTTS drains ttsClient's Audio and Done channels for as long as either
// still has values to deliver -- including whatever was already buffered
// before the connection closed -- then returns once both are closed. One
// instance runs per tts.Client (i.e. per call, not per turn): setting a
// closed channel's local variable to nil makes that select case block
// forever without spinning, so the other channel keeps draining normally
// until it closes too.
//
// audioCh is drained with strict priority over doneCh (the non-blocking peek
// below, tried before the real select). The TTS client always pushes a
// generation's audio onto Audio() before pushing its Done signal -- that's a
// single-goroutine happens-before at the source (sarvamClient.receiveLoop),
// so whenever Done is ready, every audio chunk sent before it is already
// available too. Without the priority peek, a plain `select` with both cases
// ready picks between them at random, and once in a while forwards Done
// first; handleTTSDone would then flip Thinking/Speaking straight to
// Listening before handleTTSAudio's first chunk for that generation ever
// runs its Thinking->Speaking CompareAndSwap, which then fails (state is no
// longer Thinking) and never retries -- the rest of that generation's audio
// still gets buffered by handleTTSAudio, but ProcessFrame never drains it
// (State never reached Speaking), so it just sits in outbound until it
// eventually overflows on a later turn. Observed live: turns 1-2 of a call
// produced zero dropped-frame warnings (their audio silently never played,
// but hadn't yet overflowed the buffer) and turn 3 suddenly produced
// hundreds, once the accumulated undrained backlog from all three turns
// finally exceeded outboundBufferFrames.
func (h *Handler) readTTS(ttsClient tts.Client) {
	audioCh := ttsClient.Audio()
	doneCh := ttsClient.Done()

	for audioCh != nil || doneCh != nil {
		select {
		case chunk, ok := <-audioCh:
			if !ok {
				audioCh = nil
				continue
			}

			select {
			case h.events <- ttsAudioEvent{pcm: chunk.PCM, gen: chunk.Gen}:
			case <-h.baseCtx.Done():
				return
			}

			continue
		default:
		}

		select {
		case <-h.baseCtx.Done():
			return

		case chunk, ok := <-audioCh:
			if !ok {
				audioCh = nil
				continue
			}

			select {
			case h.events <- ttsAudioEvent{pcm: chunk.PCM, gen: chunk.Gen}:
			case <-h.baseCtx.Done():
				return
			}

		case gen, ok := <-doneCh:
			if !ok {
				doneCh = nil
				continue
			}

			select {
			case h.events <- ttsDoneEvent{gen: gen}:
			case <-h.baseCtx.Done():
				return
			}
		}
	}
}

// ttsFrameBuf is run()-owned (only ever appended to inside handleTTSAudio,
// which only runs on run()'s goroutine): raw TTS PCM chunks arrive in
// provider-defined sizes, not necessarily 640 bytes, so partial bytes carry
// over between chunks until a full frame accumulates.
//
// ponytail: plain make([]byte) per frame here, not the sync.Pool frames use on
// the 20ms hot path -- this only allocates once per TTS network chunk (far
// less frequent), and CallMedia's own releaseLoop copies whatever ProcessFrame
// returns into a pooled buffer anyway. Move to the pool if profiling ever
// shows this matters.
func (h *Handler) handleTTSAudio(pcm []byte, gen uint64) {
	if gen != h.curGen {
		return // stale generation from an interrupted turn -- drop it
	}

	if h.ttfaLoggedGen != gen {
		h.ttfaLoggedGen = gen
		slog.Info("tts first audio", "call_id", h.callID, "gen", gen,
			"latency_ms", time.Since(h.turnStartedAt).Milliseconds())

		// Only start draining outbound to the caller once real audio has
		// actually arrived -- see sendToTTS's comment for why this doesn't
		// happen when Speak() merely sends text.
		if h.state.CompareAndSwap(int32(StateThinking), int32(StateSpeaking)) {
			h.speakingSince.Store(time.Now().UnixNano())
		} else {
			// Diagnostic for a suspected readTTS ordering race (see its doc
			// comment): if this ever fires, state was something other than
			// Thinking when the generation's first audio arrived -- most likely
			// Listening, because handleTTSDone already ran for this gen. That
			// would mean ProcessFrame never drains this generation's audio at
			// all (State never reaches Speaking), and it just accumulates in
			// outbound until a later turn's audio pushes the buffer over its
			// cap.
			slog.Warn("tts audio arrived but state was not Thinking; Speaking transition skipped",
				"call_id", h.callID, "gen", gen, "state", State(h.state.Load()))
		}
	}

	h.ttsBuf = append(h.ttsBuf, pcm...)

	for len(h.ttsBuf) >= media.FrameSize {
		frame := make([]byte, media.FrameSize)
		copy(frame, h.ttsBuf[:media.FrameSize])
		h.ttsBuf = h.ttsBuf[media.FrameSize:]

		select {
		case h.outbound <- frame:
		default:
			h.cfg.Sink.Error("tts_outbound_full")
			slog.Warn("tts outbound buffer full, dropping frame", "call_id", h.callID, "gen", gen)
		}
	}
}

// handleTTSDone fires once Sarvam has finished *sending* this generation's
// audio -- not once the caller has finished *hearing* it. Sarvam delivers far
// faster than real time (a multi-second reply can arrive in well under a
// second), so ending the turn here unconditionally would cut most replies off
// early: observed live, a ~3s greeting played for ~440ms before the state
// flipped back to Listening and ProcessFrame stopped draining outbound,
// leaving the rest to be played -- badly stale -- as fragments of later
// turns. If still Speaking, this only records that delivery is done;
// processSpeakingFrame (a different goroutine, draining outbound in real
// time) is what actually ends the turn, once outbound is empty too.
func (h *Handler) handleTTSDone(gen uint64) {
	if gen != h.curGen {
		return // a barge-in already moved past this generation
	}

	slog.Info("tts delivery complete", "call_id", h.callID, "gen", gen,
		"delivery_latency_ms", time.Since(h.turnStartedAt).Milliseconds())

	switch State(h.state.Load()) {
	case StateThinking:
		// No audio was ever sent for this generation (e.g. the LLM produced no
		// output) -- outbound is empty, nothing to drain, end the turn now.
		h.finishTurn(gen)
	case StateSpeaking:
		h.ttsDeliveryDone.Store(true)
	}
	// StateTranscribing: a barge-in already cut this turn short; nothing to do.
}

// finishTurn ends the turn: moves back to Listening, logs the turn's real
// end-to-end latency (transcript accepted to every frame of the reply
// actually queued for playout), and clears per-turn state. Called either
// directly from handleTTSDone (Thinking: nothing was ever queued) or via
// ttsPlaybackDoneEvent, once processSpeakingFrame observes outbound has
// fully drained after delivery finished.
func (h *Handler) finishTurn(gen uint64) {
	// Only move back to Listening if still in this turn's Speaking/Thinking
	// state -- a barge-in may have already advanced us to Transcribing, and
	// this must not stomp on that.
	switch State(h.state.Load()) {
	case StateSpeaking, StateThinking:
		h.state.Store(int32(StateListening))
	}

	slog.Info("turn complete", "call_id", h.callID, "gen", gen,
		"total_latency_ms", time.Since(h.turnStartedAt).Milliseconds())

	h.ttsBuf = nil
	// h.tts is deliberately left connected -- it's reused for the next turn
	// rather than reopened (see tts.Client's doc comment).
}

func (h *Handler) handleLLMDone(err error) {
	if err != nil {
		h.cfg.Sink.Error("llm")
	}
	// The Listening transition happens via handleTTSDone once ttsClient.Done
	// yields this turn's generation -- EndGeneration (called by runLLMTurn
	// right before this event is sent) fires that immediately when the LLM
	// produced no output at all, so there's no separate "nothing was spoken"
	// case to handle here.
}

func (h *Handler) handleBargeIn() {
	if h.turnCancel != nil {
		h.turnCancel()
		h.turnCancel = nil
	}

	h.curGen++ // invalidate the interrupted turn's in-flight audio/done events

	if h.tts != nil {
		if err := h.tts.Cancel(); err != nil {
			h.cfg.Sink.Error("tts_cancel")
		}

		h.tts = nil // reopen fresh next turn; this connection is being torn down
	}

	h.ttsBuf = nil
	// Defensive: if handleTTSDone had just set this true for the
	// now-interrupted generation (delivery finishing right as the caller
	// barged in), leaving it true would make the *next* generation's first
	// merely-transient empty tick wrongly end its turn early.
	h.ttsDeliveryDone.Store(false)

	for {
		select {
		case <-h.outbound:
		default:
			return
		}
	}
}
