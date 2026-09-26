// Package tts streams synthesized speech from a text-to-speech provider.
//
// NewSarvamClient's wire protocol (sarvam.go) is ported from a sibling
// project's live-verified Sarvam client (D:\go-agent-worker's internal/tts),
// not written from assumption -- see sarvam.go's doc comment for the specific
// findings (the required send_completion_event flag, the keepalive Sarvam
// actually needs, why request_id can't identify one Speak call's audio) and
// where each was confirmed against the real API.
package tts

// Chunk is one piece of decoded PCM, tagged with the generation number the
// caller passed to the Speak call that produced it. A caller tracking "the
// current generation" can drop any Chunk whose Gen doesn't match, discarding
// audio left over from an interrupted turn no matter when it arrives --
// necessary because the connection is reused across many turns (see Client's
// doc comment) and the provider has no per-request cancel.
type Chunk struct {
	PCM []byte
	Gen uint64
}

// Client streams text in and receives synthesized 16kHz LE PCM16 audio back.
//
// One Client is opened lazily per call (first THINKING transition, never at
// call start -- the connection is billed on open, per docs/AI_PROVIDERS.md)
// and reused across every turn in that call, not reopened per turn: a fresh
// WebSocket handshake on every reply would add real, recurring latency
// against the TTFA budget, and -- since the connection is what's billed --
// multiply the connection charge by the number of turns instead of paying it
// once per call. This matches a sibling project's live-verified design
// (D:\go-agent-worker's internal/tts), which reuses one connection for a
// whole call and solves the resulting problem -- the provider has no
// per-request cancel, so a request abandoned by a barge-in can still deliver
// its audio after a newer turn has started -- with generation tagging
// (docs/DECISIONS.md ADR-015): every Chunk and Done signal carries the
// generation of the Speak call that produced it, so a caller only has to
// compare against its own current generation, never guess from timing.
type Client interface {
	// Speak sends one text chunk (one SentenceChunker flush) to synthesize,
	// tagged with gen. May be called more than once per turn (once per
	// chunker flush) and across many turns on this one connection; each
	// call's audio and completion are identified by gen, not by connection
	// state.
	Speak(text string, gen uint64) error
	// EndGeneration signals that no further Speak calls will be made for gen
	// (the turn's LLM stream is done producing text). Once every Speak(gen)
	// call's audio has fully arrived, gen is emitted on Done -- including
	// immediately, if no Speak call for gen was ever made (an LLM turn that
	// produced no output).
	EndGeneration(gen uint64)
	// Audio returns the channel of decoded PCM chunks as they arrive, each
	// tagged with its originating generation. Chunks are not necessarily
	// frame-aligned; the caller re-slices them to 640-byte frames. Closed
	// when the connection ends for good (Cancel or Close), not between turns.
	Audio() <-chan Chunk
	// Done yields a generation once its audio is fully delivered (see
	// EndGeneration). Closed alongside Audio when the connection ends.
	Done() <-chan uint64
	// Cancel closes the connection immediately, discarding any outstanding
	// generations' in-flight audio -- barge-in's cut #2. The provider has no
	// per-request cancel message, so this is the only way to stop it. The
	// caller is expected to open a fresh Client for the next turn.
	Cancel() error
	// Close ends the connection for good (call teardown).
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
