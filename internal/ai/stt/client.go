// Package stt streams inbound call audio to a speech-to-text provider.
//
// NewSarvamClient is deliberately unimplemented: Sarvam's saaras realtime WS
// message shapes, endpointing behavior, idle timeout, and billing model are
// Phase 0b probes (docs/AI_PROVIDERS.md) that have not been run against a live
// account yet. Implementing the wire protocol from assumption would risk
// shipping a client that silently talks past the real API -- this project's
// standing rule is verify live, never fabricate. Wire it up once those probe
// results are recorded.
package stt

import "errors"

// ErrNotImplemented is returned by NewSarvamClient until Phase 0b's live
// probes confirm the actual message shapes to implement against.
var ErrNotImplemented = errors.New("stt: Sarvam client not implemented -- Phase 0b probes required first, see docs/AI_PROVIDERS.md")

// Result is one transcript event from the stream.
type Result struct {
	Text  string
	Final bool
}

// Client streams inbound audio to a speech-to-text provider and emits partial
// and final transcript events. Implementations must tolerate Feed being called
// once per 20ms frame from a single goroutine and must never block that
// goroutine on network I/O (buffer internally and send asynchronously).
type Client interface {
	// Feed sends one 20ms LE PCM16 frame to the provider.
	Feed(pcm []byte) error
	// Results returns the channel of transcript events for this connection.
	// Closed when the connection ends.
	Results() <-chan Result
	// Close ends the connection.
	Close() error
}

// Config bundles Sarvam STT connection parameters (internal/config.Config's
// SARVAM_* fields).
type Config struct {
	WSURL  string
	APIKey string
	Model  string
}

// NewSarvamClient will connect to Sarvam's saaras realtime STT WebSocket.
// Unimplemented pending Phase 0b -- see package doc comment.
func NewSarvamClient(_ Config) (Client, error) {
	return nil, ErrNotImplemented
}
