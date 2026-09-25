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
	// THINKING transition, never at call start.
	NewTTS       func() (tts.Client, error)
	LLM          llmStreamer
	SystemPrompt string

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
type ttsAudioEvent struct{ pcm []byte }
type ttsDoneEvent struct{}
type bargeInEvent struct{}

// Handler implements media.Handler: Sarvam STT -> LLM -> Sarvam TTS with local
// barge-in detection. See docs/AUDIO_PIPELINE.md's Handler Contract and
// docs/AI_PROVIDERS.md.
//
// Concurrency design: every field below has exactly one owning goroutine, so
// there is no lock in the hot path and nothing to get wrong under -race (which
// this project's toolchain can't run locally -- see CLAUDE.md conventions):
//   - state/speakingSince/silenceUntil: atomics, written by whichever
//     goroutine reaches the transition, read by any.
//   - tts/turnCancel/history: touched only by run()'s goroutine.
//   - preroll: touched only by ProcessFrame's goroutine.
//   - per-turn chunker: a local variable inside runLLMTurn, never shared.
//   - outbound: a channel (safe for concurrent send/receive by construction).
type Handler struct {
	callID  string
	cfg     Config
	baseCtx context.Context

	state         atomic.Int32
	speakingSince atomic.Int64
	silenceUntil  atomic.Int64

	events chan any

	// run()-owned only.
	tts        tts.Client
	turnCancel context.CancelFunc
	history    []llm.Message
	ttsBuf     []byte

	// ProcessFrame-goroutine-owned only.
	preroll [][]byte

	// TTS PCM re-sliced to 640B frames, drained by ProcessFrame. Cap 5 per
	// spec, matching CallMedia's own outbound queue depth.
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
		outbound: make(chan []byte, 5),
	}
	h.state.Store(int32(StateListening))

	if cfg.SystemPrompt != "" {
		h.history = append(h.history, llm.Message{Role: "system", Content: cfg.SystemPrompt})
	}

	go h.readSTT()
	go h.run()

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

		select {
		case h.events <- bargeInEvent{}:
		default:
			slog.Error("agent: events channel full dropping bargeInEvent", "call_id", h.callID)
		}

		return nil
	}

	select {
	case frame := <-h.outbound:
		return [][]byte{frame}
	default:
		return nil
	}
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
		h.handleTTSAudio(e.pcm)
	case ttsDoneEvent:
		h.handleTTSDone()
	case bargeInEvent:
		h.handleBargeIn()
	}
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

	if h.tts == nil {
		t, err := h.cfg.NewTTS()
		if err != nil {
			h.cfg.Sink.Error("tts_open")
			h.state.Store(int32(StateListening))

			return
		}

		h.tts = t

		go h.readTTS(t)
	}

	ctx, cancel := context.WithCancel(h.baseCtx)
	h.turnCancel = cancel

	history := append([]llm.Message(nil), h.history...)

	go h.runLLMTurn(ctx, history, h.tts)
}

func (h *Handler) runLLMTurn(ctx context.Context, history []llm.Message, ttsClient tts.Client) {
	chunker := &SentenceChunker{}

	err := h.cfg.LLM.Stream(ctx, history, func(tok string) {
		for _, chunk := range chunker.Feed(tok) {
			h.sendToTTS(ttsClient, chunk)
		}
	})

	if ctx.Err() != nil {
		return // cancelled by barge-in or call teardown; turn abandoned
	}

	if err == nil {
		if remainder := chunker.Flush(); remainder != "" {
			h.sendToTTS(ttsClient, remainder)
		}
	}

	select {
	case h.events <- llmDoneEvent{err: err}:
	case <-h.baseCtx.Done():
	}
}

func (h *Handler) sendToTTS(ttsClient tts.Client, text string) {
	if err := ttsClient.Speak(text); err != nil {
		h.cfg.Sink.Error("tts_speak")
		return
	}

	if h.state.CompareAndSwap(int32(StateThinking), int32(StateSpeaking)) {
		h.speakingSince.Store(time.Now().UnixNano())
	}
}

func (h *Handler) readTTS(ttsClient tts.Client) {
	for {
		select {
		case <-h.baseCtx.Done():
			return
		case pcm, ok := <-ttsClient.Audio():
			if !ok {
				select {
				case h.events <- ttsDoneEvent{}:
				case <-h.baseCtx.Done():
				}

				return
			}

			select {
			case h.events <- ttsAudioEvent{pcm: pcm}:
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
func (h *Handler) handleTTSAudio(pcm []byte) {
	h.ttsBuf = append(h.ttsBuf, pcm...)

	for len(h.ttsBuf) >= media.FrameSize {
		frame := make([]byte, media.FrameSize)
		copy(frame, h.ttsBuf[:media.FrameSize])
		h.ttsBuf = h.ttsBuf[media.FrameSize:]

		select {
		case h.outbound <- frame:
		default:
			h.cfg.Sink.Error("tts_outbound_full")
		}
	}
}

func (h *Handler) handleTTSDone() {
	// Only move back to Listening if still in this turn's Speaking/Thinking
	// state -- a barge-in may have already advanced us to Transcribing, and
	// this must not stomp on that.
	switch State(h.state.Load()) {
	case StateSpeaking, StateThinking:
		h.state.Store(int32(StateListening))
	}

	h.tts = nil // ready to lazily reopen next turn
	h.ttsBuf = nil
}

func (h *Handler) handleLLMDone(err error) {
	if err != nil {
		h.cfg.Sink.Error("llm")
	}

	if h.tts == nil {
		if State(h.state.Load()) == StateThinking {
			// No TTS was ever opened this turn (LLM produced nothing, or
			// failed before any chunk was sent) -- nothing to wait for.
			h.state.Store(int32(StateListening))
		}

		return
	}

	// The turn's text is fully generated, so no further Speak calls are
	// coming on this connection -- request a graceful close. tts.Client.Close
	// waits for any already-issued Speak calls' audio to finish arriving
	// before actually closing, so this does not cut off the tail of the
	// reply; Audio() closing afterward is what fires handleTTSDone and moves
	// the state back to Listening once playback has genuinely finished.
	// Without this call nothing ever closes the connection on a normal,
	// uninterrupted turn, and the handler would stay stuck past SPEAKING.
	if err := h.tts.Close(); err != nil {
		h.cfg.Sink.Error("tts_close")
	}
}

func (h *Handler) handleBargeIn() {
	if h.turnCancel != nil {
		h.turnCancel()
		h.turnCancel = nil
	}

	if h.tts != nil {
		if err := h.tts.Cancel(); err != nil {
			h.cfg.Sink.Error("tts_cancel")
		}
	}

	for {
		select {
		case <-h.outbound:
		default:
			return
		}
	}
}
