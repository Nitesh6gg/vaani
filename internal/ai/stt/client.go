// Package stt streams inbound call audio to a speech-to-text provider.
//
// NewSarvamClient's wire protocol (sarvam.go) is ported from a sibling
// project's live-verified Sarvam client (D:\go-agent-worker's internal/stt),
// not written from assumption -- see sarvam.go's doc comment for the specific
// findings (message shapes, the no-keepalive rule, which field errors land in)
// and where each was confirmed against the real API.
package stt

// Result is one event from the stream: a transcript, or (Signal set, no
// Text) the provider's own VAD noticing the caller start or stop speaking.
// Both kinds share one channel so they arrive in the order the provider sent
// them (see sarvam.go).
type Result struct {
	Text   string
	Final  bool
	Signal Signal
}

// Signal is a provider VAD event.
type Signal int

const (
	NoSignal Signal = iota
	SpeechStarted
	SpeechEnded
)

// Client streams inbound audio to a speech-to-text provider and emits partial
// and final transcript events. Implementations must tolerate Feed being called
// once per 20ms frame from a single goroutine and must never block that
// goroutine on network I/O (buffer internally and send asynchronously).
type Client interface {
	// Feed sends one 20ms LE PCM16 frame to the provider.
	Feed(pcm []byte) error
	// Flush asks the provider to finalize what it has heard so far now,
	// instead of waiting for its own end-of-speech detection (only honoured
	// when the connection was opened with Config.FlushSignal).
	Flush() error
	// Results returns the channel of transcript events for this connection.
	// Closed when the connection ends.
	Results() <-chan Result
	// Close ends the connection.
	Close() error
}

// Config bundles Sarvam STT connection parameters (internal/config.Config's
// SARVAM_* fields).
type Config struct {
	WSURL  string // defaults to Sarvam's production endpoint if empty
	APIKey string
	Model  string // e.g. "saaras:v3"

	// Language is Sarvam's language-code query param, e.g. "hi-IN". "unknown"
	// (Sarvam's auto-detect) is used if empty.
	Language string

	// FlushSignal opens the connection with flush_signal=true, so Flush's
	// {"type":"flush"} message forces a final transcript (Sarvam SDK:
	// "flush the audio buffer and force finalize partial transcriptions";
	// Dograh uses it on its local VAD's end of speech).
	FlushSignal bool
}
