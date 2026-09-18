package media

import "context"

// Handler transforms one released, jitter-buffered, byte-order-normalized 20ms
// PCM16 (little-endian) frame into zero or more frames to transmit. It is called
// once per 20ms release tick, from a single goroutine per call -- implementations
// need no internal locking. This is the seam Phase 3 plugs the STT/LLM/TTS agent
// into; only the Handler implementation changes, none of the transport code.
type Handler interface {
	// ProcessFrame receives exactly FrameSize (640) bytes of LE PCM16 audio for
	// call callID, and returns the frames (each expected to be FrameSize bytes)
	// to send back out over RTP, in order. Returning nil or an empty slice sends
	// nothing on this tick.
	ProcessFrame(ctx context.Context, callID string, pcm []byte) [][]byte
}

// LoopbackHandler is the Phase 1 behavior preserved as the first Handler
// implementation: echo the input frame back unchanged.
type LoopbackHandler struct{}

// ProcessFrame implements Handler by returning pcm unchanged as the only output
// frame.
func (LoopbackHandler) ProcessFrame(_ context.Context, _ string, pcm []byte) [][]byte {
	return [][]byte{pcm}
}

// SilentHandler returns zero frames for every input. It's a debug hook
// (TEST_SILENT_HANDLER=1) for deterministically exercising the write tick's
// silence-fallback path -- RTP must never starve on an active call even when a
// Handler has nothing to send.
type SilentHandler struct{}

// ProcessFrame implements Handler by always returning no frames.
func (SilentHandler) ProcessFrame(_ context.Context, _ string, _ []byte) [][]byte {
	return nil
}
