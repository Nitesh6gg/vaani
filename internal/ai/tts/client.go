// Package tts streams synthesized speech from a text-to-speech provider.
//
// NewSarvamClient's wire protocol (sarvam.go) is ported from a sibling
// project's live-verified Sarvam client (D:\go-agent-worker's internal/tts),
// not written from assumption -- see sarvam.go's doc comment for the specific
// findings (the required send_completion_event flag, the keepalive Sarvam
// actually needs, why request_id can't identify one Speak call's audio) and
// where each was confirmed against the real API.
package tts

// Client streams text in and receives synthesized 16kHz LE PCM16 audio back.
// Billing note (docs/AI_PROVIDERS.md): the connection is charged on open, so
// callers open it lazily (first THINKING transition, not at call start) --
// see AgentHandler.Config.NewTTS. Vaani opens one Client per turn rather than
// reusing one across a whole call: Sarvam's connection has no per-request
// cancel, so a reused connection can deliver an interrupted turn's stale audio
// into a later turn (a sibling project's docs/DECISIONS.md ADR-015 hit this
// live and fixed it with generation-tagging); a fresh per-turn connection has
// no later turn for stale audio to leak into in the first place.
type Client interface {
	// Speak sends one text chunk (one SentenceChunker flush) to synthesize.
	// May be called more than once per turn (once per chunker flush); all
	// calls share this one connection/turn.
	Speak(text string) error
	// Audio returns the channel of raw PCM byte chunks as they arrive. Chunks
	// are not necessarily frame-aligned; the caller re-slices them to 640-byte
	// frames. Closed when the connection ends (via Cancel, or via Close once
	// every outstanding Speak call's audio has fully arrived).
	Audio() <-chan []byte
	// Cancel closes the connection immediately, discarding any outstanding
	// Speak calls' in-flight audio -- barge-in's cut #2. Sarvam has no
	// per-request cancel message, so this is the only way to stop it: closing
	// the connection removes any chance of stale audio arriving after the
	// fact, rather than waiting on Sarvam's own end-of-request signal (see
	// sarvam.go's doc comment for why that signal alone isn't fast enough).
	Cancel() error
	// Close requests a graceful shutdown: no further Speak calls are expected
	// (the turn's LLM stream is done), but audio for already-issued Speak
	// calls is still in flight and must be allowed to finish arriving over
	// Audio() before the connection actually closes -- otherwise the tail of
	// the reply gets cut off.
	Close() error
}

// Config bundles Sarvam TTS connection parameters (internal/config.Config's
// SARVAM_*/TTS_* fields).
type Config struct {
	WSURL      string // defaults to Sarvam's production endpoint if empty
	APIKey     string
	Voice      string // e.g. "shubh" (must belong to Model's voice family)
	Model      string // e.g. "bulbul:v3"
	Language   string // e.g. "hi-IN"
	SampleRate int    // defaults to 16000 if zero
}
