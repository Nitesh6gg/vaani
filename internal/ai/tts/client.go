// Package tts streams synthesized speech from a text-to-speech provider.
//
// NewSarvamClient is deliberately unimplemented for the same reason as
// internal/ai/stt: Bulbul v3's WS message shapes, multi-turn socket reuse
// behavior, in-band cancel message (if any), actual output sample rate, idle
// timeout, and billing model are Phase 0b probes (docs/AI_PROVIDERS.md) not
// yet run against a live account. See docs/AI_PROVIDERS.md before implementing.
package tts

import "errors"

// ErrNotImplemented is returned by NewSarvamClient until Phase 0b's live
// probes confirm the actual message shapes to implement against.
var ErrNotImplemented = errors.New("tts: Sarvam client not implemented -- Phase 0b probes required first, see docs/AI_PROVIDERS.md")

// Client streams text in and receives synthesized 16kHz LE PCM16 audio back.
// Billing note (docs/AI_PROVIDERS.md): the connection is charged on open, so
// callers should open lazily (first THINKING transition, not at call start)
// and reuse across turns within a call if Phase 0b confirms the socket
// supports multiple text turns -- see AgentHandler.
type Client interface {
	// Speak sends one text chunk (one SentenceChunker flush) to synthesize.
	Speak(text string) error
	// Audio returns the channel of raw PCM byte chunks as they arrive. Chunks
	// are not necessarily frame-aligned; the caller re-slices them to 640-byte
	// frames. Closed when the connection ends.
	Audio() <-chan []byte
	// Cancel stops synthesis of the current turn (barge-in cut #2). Per Phase 0
	// finding: an in-band cancel message if Bulbul supports one, otherwise this
	// closes the underlying connection (the next Speak/turn must reconnect).
	Cancel() error
	// Close ends the connection entirely (call teardown).
	Close() error
}

// Config bundles Sarvam TTS connection parameters (internal/config.Config's
// SARVAM_*/TTS_* fields).
type Config struct {
	WSURL      string
	APIKey     string
	Voice      string
	SampleRate int
}

// NewSarvamClient will connect to Sarvam's Bulbul v3 streaming TTS WebSocket.
// Unimplemented pending Phase 0b -- see package doc comment.
func NewSarvamClient(_ Config) (Client, error) {
	return nil, ErrNotImplemented
}
