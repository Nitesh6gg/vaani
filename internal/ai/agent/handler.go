package agent

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
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
	Stream(ctx context.Context, messages []llm.Message, tools []llm.Tool, onToken func(string)) ([]llm.ToolCall, error)
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
	NewTTS func() (tts.Client, error)
	LLM    llmStreamer
	// Start is the workflow's start node (see workflow.go). Its Greeting, if
	// set, is spoken before the caller says anything, bypassing the LLM, and
	// recorded into history as the agent's own turn so the LLM doesn't
	// re-greet. Otherwise, when it has a Prompt, the LLM speaks first. Nil
	// means an empty node: no prompt, no tools.
	Start *Node
	// Hangup ends the call; used after end_call or at an end node, once the
	// agent's last words have finished playing. Nil disables hanging up.
	Hangup func()
	// IdleTimeout is Dograh's max_user_idle_timeout: once the agent has
	// finished speaking, this much caller silence makes the LLM ask whether
	// they're still there; a second time, it says goodbye and the call ends.
	// The caller starting to speak (the STT's VAD) stops the clock. 0
	// disables.
	IdleTimeout time.Duration
	// MaxDuration is Dograh's max_call_duration: this long after the call
	// started it ends -- at once if no reply is in progress, otherwise as soon
	// as the current reply has played. 0 disables.
	MaxDuration time.Duration
	// Transfer dials destination (an Asterisk dial string) and blocks until
	// it answers (returning connect) or fails, times out, or ctx ends
	// (returning an error). Calling connect hands the caller over to the
	// destination and removes the agent from the call. Nil makes transfer
	// tools report failure.
	Transfer func(ctx context.Context, destination string, timeout time.Duration) (connect func() error, err error)
	// HoldAudio loops to the caller while a transfer rings; BeepAudio plays
	// once when it answers, just before the handover. Both 16kHz LE PCM16;
	// empty means silence.
	HoldAudio []byte
	BeepAudio []byte

	BargeIn BargeInDetector
	// BargeInObserveOnly, when true, runs the detector for observability but
	// never cuts: the speech started/ended transition logs keep flowing while
	// the agent talks over any inbound audio until its turn finishes. Set by
	// the wiring when BARGE_IN_ENABLED=0 and VAD_MODE selects a detector with
	// logging (TEN VAD) -- "observe, don't cut". With it false, a sustained
	// Detect verdict cuts playback as usual.
	BargeInObserveOnly bool
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
type llmDoneEvent struct {
	gen uint64
	err error
	// toolMsgs is the turn's tool exchange (assistant tool calls + tool
	// results), for history; endCall is set when an EndsCall tool ran.
	toolMsgs []llm.Message
	endCall  bool
}
type ttsAudioEvent struct {
	pcm  []byte
	gen  uint64
	req  uint64
	text string
}

// outFrame is one 640B frame of agent audio waiting in outbound, tagged with
// the TTS request (sentence) it came from, so ProcessFrame can record how far
// playback has got.
type outFrame struct {
	pcm []byte
	req uint64
}

// spokenChunk is one sentence of the current reply whose audio has started
// arriving, in playback order.
type spokenChunk struct {
	req  uint64
	text string
}
type ttsDoneEvent struct{ gen uint64 }
type bargeInEvent struct{}
type greetingEvent struct{}
type openingEvent struct{}
type callerSpeechEvent struct{ started bool } // STT VAD: caller started/stopped speaking
type idleEvent struct{ seq uint64 }
type maxDurationEvent struct{}
type ttsPlaybackDoneEvent struct{}
type ttsDrainTimeoutEvent struct{}

// Handler implements media.Handler: Sarvam STT -> LLM -> Sarvam TTS with local
// barge-in detection. See docs/AUDIO_PIPELINE.md's Handler Contract and
// docs/AI_PROVIDERS.md.
//
// Concurrency design: every field below has exactly one owning goroutine, so
// there is no lock in the hot path and nothing to get wrong under -race (which
// this project's toolchain can't run locally -- see CLAUDE.md conventions):
//   - state/speakingSince/silenceUntil/ttsDeliveryDone/playingReq: atomics,
//     written by whichever goroutine reaches the transition, read by any.
//   - tts/turnCancel/history/turnChunks: touched only by run()'s goroutine.
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
	// playingReq is the TTS request (sentence) whose audio ProcessFrame most
	// recently handed to the caller; 0 means nothing of the current reply has
	// played yet. Written by ProcessFrame's goroutine, read by run() to decide
	// what the caller actually heard when a reply is cut short.
	playingReq atomic.Uint64
	// toolRunning is set while a tool executes (runLLMTurn's goroutine).
	// processSpeakingFrame pauses its dead-TTS drain timeout meanwhile: an
	// empty queue is expected while the caller waits on a slow API after a
	// short "let me check" has finished playing.
	toolRunning atomic.Bool
	// hold, when set, takes over ProcessFrame during a transfer: queued agent
	// speech plays first, then the hold loop (or the one-shot beep); no STT,
	// no barge-in -- the caller is waiting, not in conversation. Set and
	// cleared by the tool goroutine; the player's position is advanced only
	// by ProcessFrame's goroutine.
	hold atomic.Pointer[holdPlayer]

	// node is the workflow node the conversation is at. Read by run() when a
	// turn starts; moved by runLLMTurn's goroutine the moment the LLM takes
	// an edge (as in Dograh, a transition sticks even if the caller then
	// interrupts the new node's first words).
	node atomic.Pointer[Node]

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
	// dropWarnedGen records which generation's outbound-buffer-full warning has
	// already been logged, so an oversized reply that overflows the buffer logs
	// once for the whole turn instead of once per dropped frame -- observed
	// live, a single long reply produced over 2000 individual WARN lines in
	// under a second. Sink.Error still counts every drop for metrics.
	dropWarnedGen uint64
	// turnChunks lists the current reply's sentences in the order their audio
	// started arriving; recordReply turns them into the assistant message in
	// history -- all of them when the reply plays out, only those up to
	// playingReq when it's cut short. Without this the LLM never saw its own
	// previous answers (only the caller's lines were kept), so every turn it
	// re-answered from scratch -- observed live as long, repetitive replies.
	turnChunks []spokenChunk
	// turnToolMsgs is the current turn's tool exchange, delivered by
	// llmDoneEvent and written to history by recordReply ahead of the spoken
	// reply. hangupAfterTurn is set when an EndsCall tool ran: finishTurn
	// hangs up once the reply has played out.
	turnToolMsgs    []llm.Message
	hangupAfterTurn bool

	// Caller-silence clock (Config.IdleTimeout), mirroring pipecat's
	// UserIdleController: armed each time the agent finishes speaking,
	// stopped when anyone starts speaking. idleSeq tags each timer so a stale
	// one that fired just as it was stopped is ignored; idleArmed means the
	// agent has spoken at least once; idleCount is how many times in a row
	// the caller has been found silent.
	idleTimer *time.Timer
	idleSeq   uint64
	idleArmed bool
	idleCount int
	// ending, once set (caller silent twice, or max call duration), is
	// sticky: the call hangs up at the end of whatever turn is in progress,
	// or at once when none is. endReason says why, for the log; hungUp
	// guards against hanging up twice.
	ending    bool
	endReason string
	hungUp    bool

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
	outbound chan outFrame
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
		outbound: make(chan outFrame, outboundBufferFrames),
	}
	h.state.Store(int32(StateListening))

	if cfg.Start == nil {
		cfg.Start = &Node{}
		h.cfg.Start = cfg.Start
	}

	h.node.Store(cfg.Start)

	go h.readSTT()
	go h.run()

	// The buffer is fresh, so these can't block.
	switch {
	case cfg.Start.Greeting != "":
		h.events <- greetingEvent{}
	case cfg.Start.Prompt != "":
		h.events <- openingEvent{} // Dograh: no greeting -> the LLM opens the call
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
	if hp := h.hold.Load(); hp != nil {
		return h.processHoldFrame(hp)
	}

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
	// The preroll buffer exists solely to recover the interrupting
	// utterance's start on a cut; in observe-only mode no cut can ever fire,
	// so skip the per-frame copy entirely.
	if !h.cfg.BargeInObserveOnly {
		h.appendPreroll(pcm)
	}

	// Detect runs on every SPEAKING frame whether or not cuts are enabled:
	// it feeds the vote window and drives the speech started/ended
	// transition logs. In observe-only mode (BARGE_IN_ENABLED=0 with a
	// logging VAD) the verdict changes nothing else.
	detected := h.cfg.BargeIn.Detect(pcm)

	elapsed := time.Duration(time.Now().UnixNano() - h.speakingSince.Load())
	if detected && !h.cfg.BargeInObserveOnly && elapsed >= h.cfg.BargeInGuard {
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
		h.playingReq.Store(frame.req)

		return [][]byte{frame.pcm}
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
	if h.toolRunning.Load() {
		// Waiting on a tool, not a dead TTS connection -- see toolRunning.
		h.emptyTicks = 0
		return nil
	}

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

			if r.Signal != stt.NoSignal {
				h.post(callerSpeechEvent{started: r.Signal == stt.SpeechStarted})
				continue
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
	defer h.stopIdleTimer()

	if h.cfg.MaxDuration > 0 {
		t := time.AfterFunc(h.cfg.MaxDuration, func() { h.post(maxDurationEvent{}) })
		defer t.Stop()
	}

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
		h.handleLLMDone(e)
	case ttsAudioEvent:
		h.handleTTSAudio(e.pcm, e.gen, e.req, e.text)
	case ttsDoneEvent:
		h.handleTTSDone(e.gen)
	case bargeInEvent:
		h.handleBargeIn()
	case greetingEvent:
		h.handleGreeting()
	case openingEvent:
		slog.Info("agent opening (llm speaks first)", "call_id", h.callID)
		h.startTurn()
	case callerSpeechEvent:
		h.handleCallerSpeech(e.started)
	case idleEvent:
		h.handleIdle(e.seq)
	case maxDurationEvent:
		slog.Info("max call duration reached; ending the call", "call_id", h.callID,
			"max_duration_s", int(h.cfg.MaxDuration.Seconds()))
		h.endCall("max_call_duration")
	case ttsPlaybackDoneEvent:
		h.finishTurn(h.curGen)
	case ttsDrainTimeoutEvent:
		// State was already flipped by processSpeakingFrame; this only clears
		// run()-owned turn state so a dead turn's <640B ttsBuf tail can't leak
		// into the front of the next turn's audio, and records whatever part
		// of the reply actually played before the TTS went silent.
		h.ttsBuf = nil
		h.recordReply(true)

		if h.ending {
			h.hangup()
		}
	}
}

// post hands an event to run() from another goroutine.
func (h *Handler) post(ev any) {
	select {
	case h.events <- ev:
	case <-h.baseCtx.Done():
	}
}

// handleCallerSpeech: the caller started speaking (stop the silence clock;
// they're here) or stopped. When they stopped without it becoming a turn --
// their transcript, which Sarvam sends right after, starts one and stops the
// clock again -- the clock restarts, so noise that never became words can't
// switch silence detection off for the rest of the call.
func (h *Handler) handleCallerSpeech(started bool) {
	if started {
		h.stopIdleTimer()
		h.idleCount = 0

		return
	}

	if h.idleArmed && State(h.state.Load()) == StateListening {
		h.startIdleTimer()
	}
}

// startIdleTimer (re)starts the caller-silence clock.
func (h *Handler) startIdleTimer() {
	h.stopIdleTimer()

	if h.cfg.IdleTimeout <= 0 || h.ending {
		return
	}

	seq := h.idleSeq
	h.idleTimer = time.AfterFunc(h.cfg.IdleTimeout, func() { h.post(idleEvent{seq: seq}) })
}

func (h *Handler) stopIdleTimer() {
	if h.idleTimer != nil {
		h.idleTimer.Stop()
		h.idleTimer = nil
	}

	h.idleSeq++ // a timer that already fired is now stale
}

// Dograh's caller-silence instructions (pipecat_engine_callbacks.py), added
// to the conversation as user messages exactly as Dograh does.
const (
	idleFirstPrompt = "The user has been quiet. Politely and briefly ask if they're still there in the language that the user has been speaking so far."
	idleFinalPrompt = "The user has been quiet. We will be disconnecting the call now. Wish them a good day in the language that the user has been speaking so far."
)

// handleIdle: the caller has said nothing for IdleTimeout since the agent
// last spoke. First time, the LLM checks whether they're still there; the
// second time in a row, it says goodbye and the call ends after that.
func (h *Handler) handleIdle(seq uint64) {
	if seq != h.idleSeq || h.ending || State(h.state.Load()) != StateListening {
		return
	}

	h.idleTimer = nil
	h.idleCount++

	prompt := idleFirstPrompt
	if h.idleCount >= 2 {
		prompt = idleFinalPrompt
		h.ending, h.endReason = true, "caller_silent"
	}

	slog.Info("caller silent", "call_id", h.callID, "times", h.idleCount,
		"idle_timeout_s", h.cfg.IdleTimeout.Seconds(), "ending_call", h.ending)

	h.history = append(h.history, llm.Message{Role: "user", Content: prompt})
	h.startTurn()
}

// endCall ends the call for reason: right away when no reply is in
// progress, otherwise once the current one has played (finishTurn).
func (h *Handler) endCall(reason string) {
	h.ending, h.endReason = true, reason
	h.stopIdleTimer()

	switch State(h.state.Load()) {
	case StateListening, StateTranscribing:
		h.hangup()
	}
}

func (h *Handler) hangup() {
	if h.hungUp {
		return
	}

	h.hungUp = true

	reason := h.endReason
	if reason == "" {
		reason = "end_call"
	}

	slog.Info("agent ending call", "call_id", h.callID, "gen", h.curGen, "reason", reason,
		"node", h.node.Load().Name)

	if h.cfg.Hangup != nil {
		go h.cfg.Hangup() // ARI REST call; never block the event loop on it
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

// handleGreeting speaks the start node's Greeting once, at call start, before the
// caller has said anything -- fixed text, so it bypasses the LLM entirely.
// Recorded into history as the agent's own turn so the LLM doesn't
// redundantly re-greet on the caller's first real reply.
func (h *Handler) handleGreeting() {
	h.state.Store(int32(StateThinking))

	h.curGen++
	gen := h.curGen
	h.turnStartedAt = time.Now()
	h.startTurnChunks()

	slog.Info("agent greeting", "call_id", h.callID, "gen", gen, "text", h.cfg.Start.Greeting)

	if !h.openTTS(gen) {
		return
	}

	// The greeting reaches history the same way LLM replies do (recordReply,
	// once it has played -- or only the heard part if cut off), so the LLM
	// knows it already greeted and doesn't do it again.
	chunker := &SentenceChunker{}
	for _, chunk := range chunker.Feed(h.cfg.Start.Greeting) {
		if gen != h.curGen {
			// A barge-in superseded the greeting mid-speak (run-goroutine
			// check, so this is race-free): stop speaking into a torn-down
			// TTS connection.
			return
		}

		h.sendToTTS(h.baseCtx, h.tts, chunk, gen)
	}

	if remainder := chunker.Flush(); remainder != "" && gen == h.curGen {
		h.sendToTTS(h.baseCtx, h.tts, remainder, gen)
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

	h.history = append(h.history, llm.Message{Role: "user", Content: text})
	slog.Info("stt final transcript", "call_id", h.callID, "gen", h.curGen+1, "text", text)
	h.idleCount = 0
	h.startTurn()
}

// startTurn runs one LLM turn over the current history: after the caller
// spoke, at call start when the LLM opens the call, or when the caller has
// gone silent.
func (h *Handler) startTurn() {
	h.stopIdleTimer() // the agent is about to speak
	h.state.Store(int32(StateThinking))
	h.cfg.Sink.TurnStarted()

	h.curGen++
	gen := h.curGen
	h.turnStartedAt = time.Now()
	h.startTurnChunks()

	if !h.openTTS(gen) {
		return
	}

	ctx, cancel := context.WithCancel(h.baseCtx)
	h.turnCancel = cancel

	history := append([]llm.Message(nil), h.history...)

	go h.runLLMTurn(ctx, history, h.tts, gen, h.turnStartedAt)
}

// requestMessages is what the LLM is sent at node: its system prompt, then
// the conversation.
func requestMessages(node *Node, history []llm.Message) []llm.Message {
	if node.Prompt == "" {
		return history
	}

	msgs := make([]llm.Message, 0, len(history)+2)
	msgs = append(msgs, llm.Message{Role: "system", Content: node.Prompt})

	if len(history) == 0 {
		// Opening turn: nobody has spoken yet. Gemini needs at least one
		// non-system message, so the prompt is repeated as the user message
		// -- the same thing pipecat's Google service does (Dograh's stack,
		// pipecat/services/google/llm.py) for a system-only context.
		msgs = append(msgs, llm.Message{Role: "user", Content: node.Prompt})
	}

	return append(msgs, history...)
}

// runLLMTurn streams the LLM's reply for one turn, speaking it sentence by
// sentence as it arrives. When the model calls tools, any text it wrote first
// ("ek second, main check kar raha hoon") is spoken immediately, the tools
// run, their results go back to the model, and it streams again -- up to
// maxToolRounds times. An EndsCall tool stops the loop: the model isn't asked
// again, and the call hangs up once the reply has played out.
func (h *Handler) runLLMTurn(ctx context.Context, history []llm.Message, ttsClient tts.Client, gen uint64, turnStartedAt time.Time) {
	chunker := &SentenceChunker{}
	node := h.node.Load()

	var (
		firstTokenAt time.Time
		toolMsgs     []llm.Message
		endCall      bool
		transferred  bool
		err          error
	)

	for round := 0; round < maxToolRounds; round++ {
		var (
			calls []llm.ToolCall
			said  strings.Builder
		)

		calls, err = h.cfg.LLM.Stream(ctx, requestMessages(node, history), node.toolDefs(), func(tok string) {
			if firstTokenAt.IsZero() {
				firstTokenAt = time.Now()
				slog.Info("llm first token", "call_id", h.callID, "gen", gen,
					"latency_ms", firstTokenAt.Sub(turnStartedAt).Milliseconds())
			}

			said.WriteString(tok)

			for _, chunk := range chunker.Feed(tok) {
				h.sendToTTS(ctx, ttsClient, chunk, gen)
			}
		})

		if ctx.Err() != nil {
			// Cancelled by barge-in or call teardown; a barge-in already
			// called Cancel on this exact ttsClient, so no further Speak calls
			// for gen are possible and there's nothing to end.
			slog.Info("llm turn cancelled", "call_id", h.callID, "gen", gen,
				"duration_ms", time.Since(turnStartedAt).Milliseconds())

			return
		}

		if err != nil {
			break
		}

		// Speak the tail now -- before any tool runs -- so the caller isn't
		// left in silence while it does.
		if remainder := chunker.Flush(); remainder != "" {
			h.sendToTTS(ctx, ttsClient, remainder, gen)
		}

		if len(calls) == 0 {
			break
		}

		if round == maxToolRounds-1 {
			slog.Warn("llm tool round limit reached; ending turn", "call_id", h.callID, "gen", gen,
				"rounds", maxToolRounds)

			break
		}

		// Within this turn the model sees what it said alongside its calls.
		// The copy kept for long-term history drops that text: recordReply
		// records everything actually spoken as one message, so keeping it
		// here too would duplicate it.
		history = append(history, llm.Message{Role: "assistant", Content: said.String(), ToolCalls: calls})
		toolMsgs = append(toolMsgs, llm.Message{Role: "assistant", ToolCalls: calls})

		for _, call := range calls {
			var (
				result string
				action toolAction
			)

			if e := node.edge(call.Function.Name); e != nil {
				node, result = h.takeEdge(ctx, ttsClient, node, e, gen)
			} else {
				result, action = h.runTool(ctx, ttsClient, node, call, gen)
			}

			msg := llm.Message{Role: "tool", ToolCallID: call.ID, Content: result}
			history = append(history, msg)
			toolMsgs = append(toolMsgs, msg)
			endCall = endCall || action == actionEndCall
			transferred = transferred || action == actionTransferred
		}

		if ctx.Err() != nil {
			slog.Info("llm turn cancelled during tool call", "call_id", h.callID, "gen", gen)
			return
		}

		// Neither needs another LLM round: end_call's goodbye has been said,
		// and after a transfer the caller is talking to a person.
		if endCall || transferred {
			break
		}
	}

	// At an end node the reply just generated is the closing line: hang up
	// once it has played (Dograh ends the call right after it).
	endCall = endCall || node.End

	if err == nil {
		slog.Info("llm turn complete", "call_id", h.callID, "gen", gen,
			"duration_ms", time.Since(turnStartedAt).Milliseconds())
	} else {
		slog.Warn("llm turn error", "call_id", h.callID, "gen", gen,
			"duration_ms", time.Since(turnStartedAt).Milliseconds(), "error", err)
	}

	// Sent BEFORE EndGeneration: EndGeneration can emit Done immediately (no
	// audio pending), and run() must already know about endCall/toolMsgs when
	// that Done ends the turn. Both travel on h.events, so order is kept.
	select {
	case h.events <- llmDoneEvent{gen: gen, err: err, toolMsgs: toolMsgs, endCall: endCall}:
	case <-h.baseCtx.Done():
		return
	}

	// No further Speak(gen) calls are coming. ttsClient.Done() will yield gen
	// once its audio (if any was sent at all) has fully arrived.
	ttsClient.EndGeneration(gen)
}

// toolAction is what a tool call means for the rest of the turn.
type toolAction int

const (
	actionNone        toolAction = iota
	actionEndCall                // hang up once the reply has played
	actionTransferred            // the caller is now with the transfer destination
)

// takeEdge moves the conversation along e (the LLM called its function):
// speaks the edge's transition speech, switches to the target node -- whose
// prompt and tools the next LLM round uses -- and returns Dograh's
// transition result.
func (h *Handler) takeEdge(ctx context.Context, ttsClient tts.Client, from *Node, e *Edge, gen uint64) (*Node, string) {
	if e.Speech != "" {
		h.sendToTTS(ctx, ttsClient, e.Speech, gen)
	}

	slog.Info("node transition", "call_id", h.callID, "gen", gen,
		"from", from.Name, "to", e.To.Name, "via", e.Def.Name)
	h.node.Store(e.To)

	return e.To, `{"status":"done"}`
}

// runTool executes one tool call of node, returning the result for the LLM
// and what it means for the turn. The tool's Message (Dograh's customMessage)
// is spoken first. Unknown tools and failures become error results the LLM
// can react to.
func (h *Handler) runTool(ctx context.Context, ttsClient tts.Client, node *Node, call llm.ToolCall, gen uint64) (string, toolAction) {
	t, ok := node.tool(call.Function.Name)
	if !ok {
		slog.Warn("llm called an unknown tool", "call_id", h.callID, "gen", gen, "tool", call.Function.Name)
		return toolErrorResult(fmt.Errorf("unknown tool %q", call.Function.Name)), actionNone
	}

	slog.Info("tool call", "call_id", h.callID, "gen", gen, "tool", call.Function.Name,
		"args", truncate(call.Function.Arguments, 200))

	if t.Message != "" {
		h.sendToTTS(ctx, ttsClient, t.Message, gen)
	}

	h.toolRunning.Store(true)
	defer h.toolRunning.Store(false)

	switch t.Kind {
	case ToolEndCall:
		var args struct {
			Reason string `json:"reason"`
		}

		_ = json.Unmarshal([]byte(call.Function.Arguments), &args)
		slog.Info("end call requested", "call_id", h.callID, "gen", gen, "reason", args.Reason)

		return `{"status":"success","action":"ending_call"}`, actionEndCall
	case ToolTransfer:
		return h.transfer(ctx, t, gen)
	}

	if t.Run == nil {
		return toolErrorResult(fmt.Errorf("tool %q has no implementation", t.Def.Name)), actionNone
	}

	tctx, cancel := context.WithTimeout(ctx, toolTimeout)
	defer cancel()

	start := time.Now()

	result, err := t.Run(tctx, json.RawMessage(call.Function.Arguments))
	if err != nil {
		slog.Warn("tool failed", "call_id", h.callID, "gen", gen, "tool", call.Function.Name,
			"duration_ms", time.Since(start).Milliseconds(), "error", err)

		return toolErrorResult(err), actionNone
	}

	slog.Info("tool result", "call_id", h.callID, "gen", gen, "tool", call.Function.Name,
		"duration_ms", time.Since(start).Milliseconds(), "result", truncate(result, 200))

	return result, actionNone
}

// holdMessageWaitTicks is how long (in 20ms ticks) hold music waits for the
// transfer's spoken message to start arriving from TTS before starting
// anyway -- so the ring doesn't start and then get interrupted by the message.
const holdMessageWaitTicks = 150

// transfer runs a ToolTransfer: ring the destination with hold music playing
// to the caller; on answer play the beep, then hand over. Mirrors Dograh's
// transfer flow (hold loop while waiting, beep, then bridge swap).
func (h *Handler) transfer(ctx context.Context, t Tool, gen uint64) (string, toolAction) {
	if h.cfg.Transfer == nil {
		return transferFailed("call transfer is not available"), actionNone
	}

	timeout := t.Timeout
	if timeout <= 0 {
		timeout = defaultTransferTimeout
	}

	slog.Info("transfer dialing", "call_id", h.callID, "gen", gen,
		"destination", t.Destination, "timeout_s", int(timeout.Seconds()))

	h.hold.Store(&holdPlayer{pcm: h.cfg.HoldAudio, loop: true, waitForMessage: t.Message != ""})

	start := time.Now()

	connect, err := h.cfg.Transfer(ctx, t.Destination, timeout)
	if err != nil {
		h.hold.Store(nil)
		slog.Warn("transfer failed", "call_id", h.callID, "gen", gen, "destination", t.Destination,
			"after_ms", time.Since(start).Milliseconds(), "error", err)

		return transferFailed(err.Error()), actionNone
	}

	slog.Info("transfer answered", "call_id", h.callID, "gen", gen, "destination", t.Destination,
		"after_ms", time.Since(start).Milliseconds())

	// Beep, and let it finish playing before the handover cuts this leg off.
	beep := &holdPlayer{pcm: h.cfg.BeepAudio}
	h.hold.Store(beep)
	waitUntil(ctx, 2*time.Second, beep.done.Load)

	if err := connect(); err != nil {
		h.hold.Store(nil)
		slog.Warn("transfer handover failed", "call_id", h.callID, "gen", gen, "error", err)

		return transferFailed(err.Error()), actionNone
	}

	slog.Info("call transferred", "call_id", h.callID, "gen", gen, "destination", t.Destination)

	return `{"status":"success","action":"transferred"}`, actionTransferred
}

func transferFailed(reason string) string {
	b, _ := json.Marshal(map[string]string{"status": "failed", "reason": reason})
	return string(b)
}

// waitUntil polls cond every 20ms until it's true, max elapses, or ctx ends.
func waitUntil(ctx context.Context, max time.Duration, cond func() bool) {
	deadline := time.NewTimer(max)
	defer deadline.Stop()

	tick := time.NewTicker(media.FrameInterval)
	defer tick.Stop()

	for !cond() {
		select {
		case <-ctx.Done():
			return
		case <-deadline.C:
			return
		case <-tick.C:
		}
	}
}

// holdPlayer feeds canned audio (hold loop or beep) to the caller frame by
// frame. pos/waited/sawMessage belong to ProcessFrame's goroutine; done is
// read by the tool goroutine.
type holdPlayer struct {
	pcm            []byte
	loop           bool
	waitForMessage bool

	pos        int
	waited     int
	sawMessage bool

	done atomic.Bool
}

// next returns the player's next 20ms frame, or nil once a one-shot player
// has finished (or there's nothing to play).
func (p *holdPlayer) next() []byte {
	if p.pos+media.FrameSize > len(p.pcm) {
		if !p.loop || len(p.pcm) < media.FrameSize {
			p.done.Store(true)
			return nil
		}

		p.pos = 0
	}

	f := p.pcm[p.pos : p.pos+media.FrameSize]
	p.pos += media.FrameSize

	return f
}

// processHoldFrame is ProcessFrame during a transfer: already-queued agent
// speech (the "please stay on the line" message) plays first, then the hold
// audio. Nothing reaches STT and barge-in is off -- the caller is waiting.
func (h *Handler) processHoldFrame(p *holdPlayer) [][]byte {
	select {
	case f := <-h.outbound:
		p.sawMessage = true
		h.playingReq.Store(f.req)

		return [][]byte{f.pcm}
	default:
	}

	if p.waitForMessage && !p.sawMessage && p.waited < holdMessageWaitTicks {
		p.waited++
		return nil
	}

	if f := p.next(); f != nil {
		return [][]byte{f}
	}

	return nil
}

// truncate caps s at max runes for logging.
func truncate(s string, max int) string {
	r := []rune(s)
	if len(r) <= max {
		return s
	}

	return string(r[:max]) + "…"
}

// sendToTTS sends one text chunk for synthesis. ctx is the owning turn's
// context: once it's cancelled (barge-in cut the turn), Speak attempts stop
// entirely and the doomed write -- the TTS connection is already torn down by
// Cancel -- is logged at Debug, not Warn. Observed live: every barge-in
// produced a "tts speak failed: use of closed network connection" warn that
// was pure cancellation noise.
func (h *Handler) sendToTTS(ctx context.Context, ttsClient tts.Client, text string, gen uint64) {
	if ctx.Err() != nil {
		slog.Debug("tts speak skipped; turn already cancelled",
			"call_id", h.callID, "gen", gen, "chars", len([]rune(text)))

		return
	}

	if err := ttsClient.Speak(text, gen); err != nil {
		h.cfg.Sink.Error("tts_speak")

		if ctx.Err() != nil {
			slog.Debug("tts speak failed after turn cancellation; expected",
				"call_id", h.callID, "gen", gen, "error", err)

			return
		}

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
			case h.events <- ttsAudioEvent{pcm: chunk.PCM, gen: chunk.Gen, req: chunk.Req, text: chunk.Text}:
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
			case h.events <- ttsAudioEvent{pcm: chunk.PCM, gen: chunk.Gen, req: chunk.Req, text: chunk.Text}:
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
func (h *Handler) handleTTSAudio(pcm []byte, gen, req uint64, text string) {
	if gen != h.curGen {
		return // stale generation from an interrupted turn -- drop it
	}

	if text != "" && (len(h.turnChunks) == 0 || h.turnChunks[len(h.turnChunks)-1].req != req) {
		h.turnChunks = append(h.turnChunks, spokenChunk{req: req, text: text})
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
		case h.outbound <- outFrame{pcm: frame, req: req}:
		default:
			h.cfg.Sink.Error("tts_outbound_full")

			if h.dropWarnedGen != gen {
				h.dropWarnedGen = gen
				slog.Warn("tts outbound buffer full, dropping frames (further drops this turn are counted but not logged individually)",
					"call_id", h.callID, "gen", gen)
			}
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

	h.recordReply(false)
	h.ttsBuf = nil

	if h.hangupAfterTurn || h.ending {
		h.hangupAfterTurn = false
		h.hangup()

		return
	}

	// The agent has finished speaking: now the caller-silence clock runs.
	h.idleArmed = true
	h.startIdleTimer()
	// h.tts is deliberately left connected -- it's reused for the next turn
	// rather than reopened (see tts.Client's doc comment).
}

// recordReply appends the current reply to history as the agent's own turn
// and clears turnChunks. When the reply played out in full (cut=false) every
// sentence goes in; when it was cut short (barge-in, dead TTS) only the
// sentences up to the one playing at the moment of the cut -- what the caller
// actually started hearing -- so the LLM's memory matches the conversation
// the caller had, not the reply it merely generated. Sentences Sarvam
// rejected never produced audio, so they never enter turnChunks at all.
//
// No "(interrupted)" marker is added to the text: the LLM tends to imitate
// markers it sees in its own past turns, and anything it writes gets spoken.
func (h *Handler) recordReply(cut bool) {
	played := h.playingReq.Load()

	var b strings.Builder

	for _, c := range h.turnChunks {
		if cut && (played == 0 || c.req > played) {
			break
		}

		b.WriteString(c.text)
	}

	h.turnChunks = nil

	// Tool calls and results go in first, even when the reply was cut: the
	// tools really ran, and the LLM should know what they returned.
	h.history = append(h.history, h.turnToolMsgs...)
	h.turnToolMsgs = nil

	text := strings.TrimSpace(b.String())
	if text == "" {
		return
	}

	h.history = append(h.history, llm.Message{Role: "assistant", Content: text})
	slog.Info("agent reply recorded", "call_id", h.callID, "gen", h.curGen,
		"cut", cut, "chars", len([]rune(text)))
}

// startTurnChunks resets reply tracking at the start of a new turn.
func (h *Handler) startTurnChunks() {
	h.turnChunks = nil
	h.turnToolMsgs = nil
	h.hangupAfterTurn = false
	h.playingReq.Store(0)
}

func (h *Handler) handleLLMDone(e llmDoneEvent) {
	if e.err != nil {
		h.cfg.Sink.Error("llm")
	}

	if e.gen != h.curGen {
		return // a barge-in already moved on; that turn's tools don't matter now
	}

	h.turnToolMsgs = e.toolMsgs
	h.hangupAfterTurn = e.endCall

	// The Listening transition happens via handleTTSDone once ttsClient.Done
	// yields this turn's generation -- EndGeneration (called by runLLMTurn
	// right after this event is sent) fires that immediately when the LLM
	// produced no output at all, so there's no separate "nothing was spoken"
	// case to handle here.
}

func (h *Handler) handleBargeIn() {
	if h.turnCancel != nil {
		h.turnCancel()
		h.turnCancel = nil
	}

	h.curGen++ // invalidate the interrupted turn's in-flight audio/done events

	// Keep only what the caller heard before cutting in. Safe to read
	// playingReq here: processSpeakingFrame flipped state to Transcribing
	// before sending bargeInEvent, so it has stopped popping frames.
	h.recordReply(true)

	// The caller cut in over a goodbye: they want to keep talking, so don't
	// hang up on them -- unless the call is being ended regardless (max
	// duration, silence), which Dograh doesn't reverse either.
	h.hangupAfterTurn = false

	if h.ending {
		h.hangup()
	}

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
