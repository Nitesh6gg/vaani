package media

import "sync"

// AudioTap fans out one call's live inbound PCM frames (post-jitter-buffer,
// post-normalization -- the same bytes handed to the WAV recorder and the
// Handler) to zero or more concurrent debug listeners. It exists for
// DEBUG_AUDIO=1's /debug/audio/{callID} endpoint: real-time listening to a call
// in progress, without waiting for RecordDir's WAV file to close.
type AudioTap struct {
	mu     sync.Mutex
	closed bool
	subs   map[chan []byte]*sync.Once
}

// NewAudioTap returns an empty tap ready to Publish into and Subscribe to.
func NewAudioTap() *AudioTap {
	return &AudioTap{subs: make(map[chan []byte]*sync.Once)}
}

// Publish fans frame out to every current subscriber. Non-blocking: a slow or
// stalled subscriber drops frames rather than stalling the call's release tick,
// since this runs on the same goroutine as the media pipeline's hot path. A
// no-op after Close.
func (t *AudioTap) Publish(frame []byte) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.closed || len(t.subs) == 0 {
		return
	}

	cp := make([]byte, len(frame))
	copy(cp, frame)

	for ch := range t.subs {
		select {
		case ch <- cp:
		default:
		}
	}
}

// Subscribe registers a new listener, returning a channel of raw PCM frames and
// an unsubscribe function the caller must call exactly once when done (e.g. on
// HTTP client disconnect) to stop leaking the subscription. The channel is
// closed either by that unsubscribe call or by Close, whichever comes first --
// never by Publish -- so a range loop over it always terminates cleanly. If the
// tap is already closed, Subscribe returns an already-closed channel.
func (t *AudioTap) Subscribe() (ch <-chan []byte, unsubscribe func()) {
	t.mu.Lock()
	defer t.mu.Unlock()

	c := make(chan []byte, 32)

	if t.closed {
		close(c)
		return c, func() {}
	}

	once := &sync.Once{}
	t.subs[c] = once

	return c, func() {
		t.mu.Lock()
		o, ok := t.subs[c]
		delete(t.subs, c)
		t.mu.Unlock()

		if ok {
			o.Do(func() { close(c) })
		}
	}
}

// Close closes every current subscriber's channel and marks the tap closed, so
// Publish becomes a no-op and Subscribe hands out pre-closed channels
// thereafter. Called when the call ends: without this, a streaming
// /debug/audio/{callID} client would block forever past the call's lifetime
// instead of seeing its stream end. Idempotent.
func (t *AudioTap) Close() {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.closed {
		return
	}

	t.closed = true

	for ch, once := range t.subs {
		once.Do(func() { close(ch) })
	}

	t.subs = nil
}

var (
	tapsMu sync.RWMutex
	taps   = make(map[string]*AudioTap)
)

// RegisterAudioTap creates and registers a tap for callID, for the media
// pipeline (CallMedia/AudioSocketCallMedia) to Publish into. Call
// UnregisterAudioTap when the call ends.
func RegisterAudioTap(callID string) *AudioTap {
	t := NewAudioTap()

	tapsMu.Lock()
	taps[callID] = t
	tapsMu.Unlock()

	return t
}

// UnregisterAudioTap removes and closes callID's tap, if any -- closing it so
// any live /debug/audio/{callID} stream ends instead of hanging past the call's
// lifetime. A no-op if it was never registered (DEBUG_AUDIO was off) or already
// removed.
func UnregisterAudioTap(callID string) {
	tapsMu.Lock()
	t, ok := taps[callID]
	delete(taps, callID)
	tapsMu.Unlock()

	if ok {
		t.Close()
	}
}

// LookupAudioTap returns callID's tap, or nil if that call has no active media
// plane with DEBUG_AUDIO enabled.
func LookupAudioTap(callID string) *AudioTap {
	tapsMu.RLock()
	defer tapsMu.RUnlock()

	return taps[callID]
}
