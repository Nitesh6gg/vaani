package media

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"math"
	"net"
	"sync"
	"time"
)

// AudioSocketSink receives per-call AudioSocket observability events.
type AudioSocketSink interface {
	PacketIn(bytes int)
	PacketOut()
	SilenceSent()
	SendError()
	Malformed()
	WatchdogTimeout()
	PacerDrift(ms float64)
	AudioLevel(rms float64)
}

// AudioSocketConfig bundles an AudioSocket call's tunables. Handler defaults to
// LoopbackHandler if nil, same as Config.
type AudioSocketConfig struct {
	Handler   Handler
	RecordDir string
	// DebugAudio, when true, registers this call with the /debug/audio/{callID}
	// tap registry (see audiotap.go). Same contract as Config.DebugAudio.
	DebugAudio bool
}

// AudioSocketCallMedia runs the media pipeline for one AudioSocket call:
//
//	TCP reader -> frame parse -> inbound queue -> (20ms release tick) ->
//	Handler.ProcessFrame -> outbound queue -> (20ms write tick) -> TCP writer
//
// This deliberately mirrors CallMedia's shape (same Handler contract,
// independent release/write tickers so a slow Handler can't stall the outbound
// stream, WAV recording, watchdog) because Phase 3's Handler must not care which
// transport it's running over. It differs from CallMedia in exactly the ways
// AudioSocket itself differs from RTP: no jitter buffer (TCP already guarantees
// in-order, lossless delivery -- there is nothing to reorder or conceal), no
// byte-order normalization (the protocol's audio payload is little-endian by
// definition, not a per-deployment question), and no "remote locked" concept
// (accepting the TCP connection is itself the connection, unlike a UDP socket
// that will take packets from anywhere until an address is learned).
type AudioSocketCallMedia struct {
	callID   string
	conn     net.Conn
	sink     AudioSocketSink
	handler  Handler
	recorder *WavRecorder
	watchdog *Watchdog
	tap      *AudioTap

	releasePacer *Pacer
	writePacer   *Pacer
	inbound      chan *[]byte
	outbound     chan *[]byte

	prevRelease time.Time

	// rmsSum/rmsTicks accumulate under rmsMu: releaseLoop writes them on every
	// tick, while NearSilent can be read from the ARI teardown goroutine the
	// instant the call context is cancelled -- i.e. while a final tick may
	// still be in flight (pacer stop doesn't wait for it). Same contract as
	// CallMedia's fields.
	rmsMu    sync.Mutex
	rmsSum   float64
	rmsTicks int64
}

// NewAudioSocketCallMedia wraps conn -- already accepted from Asterisk's TCP
// connection for this call -- into a running AudioSocket media pipeline.
func NewAudioSocketCallMedia(callID string, conn net.Conn, sink AudioSocketSink, cfg AudioSocketConfig) *AudioSocketCallMedia {
	handler := cfg.Handler
	if handler == nil {
		handler = LoopbackHandler{}
	}

	var tap *AudioTap
	if cfg.DebugAudio {
		tap = RegisterAudioTap(callID)
	}

	return &AudioSocketCallMedia{
		callID:       callID,
		conn:         conn,
		sink:         sink,
		handler:      handler,
		recorder:     NewWavRecorder(cfg.RecordDir, callID),
		watchdog:     NewWatchdog(callID, WatchdogTimeout, sink),
		tap:          tap,
		releasePacer: NewPacer(FrameInterval),
		writePacer:   NewPacer(FrameInterval),
		inbound:      make(chan *[]byte, 5),
		outbound:     make(chan *[]byte, 5),
	}
}

// Run starts the pipeline's goroutines and blocks until ctx is cancelled or the
// TCP connection errors out.
func (c *AudioSocketCallMedia) Run(ctx context.Context) {
	readDone := make(chan struct{})

	var pacedLoops sync.WaitGroup
	pacedLoops.Add(2)

	go func() {
		defer pacedLoops.Done()
		c.releaseLoop(ctx)
	}()
	go func() {
		defer pacedLoops.Done()
		c.writeLoop(ctx)
	}()
	go c.watchdog.Run(ctx)
	go c.readLoop(ctx, readDone)

	<-ctx.Done()
	c.releasePacer.Stop()
	c.writePacer.Stop()
	_ = c.conn.Close()
	<-readDone
	// Pacer stop ends the release/write loops, but a tick already in flight
	// must finish before the recorder (and anything reading per-call state)
	// is touched -- Pacer.Stop does not wait for fn to return.
	pacedLoops.Wait()
	c.recorder.Close()

	if c.tap != nil {
		UnregisterAudioTap(c.callID)
	}
}

// Close tears down the call's media plane; equivalent to cancelling the context
// passed to Run, provided for callers that don't otherwise hold that cancel func.
func (c *AudioSocketCallMedia) Close() {
	c.releasePacer.Stop()
	c.writePacer.Stop()
	_ = c.conn.Close()
	c.recorder.Close()

	if c.tap != nil {
		UnregisterAudioTap(c.callID)
	}
}

// NearSilent reports whether this call's average inbound audio level, across its
// whole lifetime, stayed suspiciously low. Same contract and thresholds as
// CallMedia.NearSilent -- see the const block in loopback.go.
func (c *AudioSocketCallMedia) NearSilent() (avgRMS float64, ok bool) {
	c.rmsMu.Lock()
	defer c.rmsMu.Unlock()

	if c.rmsTicks < nearSilenceMinTicks {
		return 0, false
	}

	avg := c.rmsSum / float64(c.rmsTicks)

	return avg, avg < nearSilenceRMSThreshold
}

func (c *AudioSocketCallMedia) readLoop(ctx context.Context, done chan<- struct{}) {
	defer close(done)

	for {
		frame, err := ReadAudioSocketFrame(c.conn)
		if err != nil {
			if ctx.Err() != nil || errors.Is(err, io.EOF) {
				return
			}

			slog.Warn("audiosocket read error", "call_id", c.callID, "error", err)

			return
		}

		switch frame.Kind {
		case AudioSocketKindHangup:
			return
		case AudioSocketKindSlin16:
			if len(frame.Payload) != FrameSize {
				c.sink.Malformed()
				slog.Debug("audiosocket frame wrong size", "call_id", c.callID, "size", len(frame.Payload))

				continue
			}

			c.sink.PacketIn(len(frame.Payload))
			c.watchdog.Touch()

			buf := &frame.Payload // already a pooled FrameSize buffer, per ReadAudioSocketFrame

			select {
			case c.inbound <- buf:
			default:
				PutFrame(buf) // release isn't keeping up; drop rather than block the reader
			}
		default:
			// UUID, DTMF, error frames: no-op for now (control-plane concerns, not
			// the media pipeline). Logged for visibility.
			slog.Debug("audiosocket control frame", "call_id", c.callID, "kind", frame.Kind)
		}
	}
}

// releaseLoop pops one frame per tick from the inbound queue (or synthesizes
// silence if nothing has arrived yet), runs it through the Handler, and queues
// the result(s) for the writer.
func (c *AudioSocketCallMedia) releaseLoop(ctx context.Context) {
	c.releasePacer.Run(func(now time.Time) {
		if ctx.Err() != nil {
			return
		}

		c.observeDrift(now)

		var raw *[]byte

		select {
		case f := <-c.inbound:
			raw = f
		default:
			raw = silentFrame()
		}

		pcm := *raw

		c.recorder.WriteIn(pcm)

		level := rms(pcm)
		c.sink.AudioLevel(level)
		c.rmsMu.Lock()
		c.rmsSum += level
		c.rmsTicks++
		c.rmsMu.Unlock()

		if c.tap != nil {
			c.tap.Publish(pcm)
		}

		for _, frame := range c.handler.ProcessFrame(ctx, c.callID, pcm) {
			out := GetFrame()
			*out = (*out)[:0]
			*out = append(*out, frame...)

			select {
			case c.outbound <- out:
			default:
				PutFrame(out)
			}
		}

		PutFrame(raw)
	})
}

// writeLoop drains the outbound queue on its own tick and sends each frame as a
// paced AudioSocket audio frame. If nothing is queued yet (handler underrun, or
// startup), it still sends a silence frame on schedule, per the pacer invariant.
func (c *AudioSocketCallMedia) writeLoop(ctx context.Context) {
	c.writePacer.Run(func(_ time.Time) {
		if ctx.Err() != nil {
			return
		}

		var frame *[]byte

		isSilenceFallback := false

		select {
		case f := <-c.outbound:
			frame = f
		default:
			frame = silentFrame()
			isSilenceFallback = true
		}

		c.recorder.WriteOut(*frame)

		err := WriteAudioSocketFrame(c.conn, AudioSocketKindSlin16, *frame)

		PutFrame(frame)

		if err != nil {
			c.sink.SendError()
			slog.Warn("audiosocket write error", "call_id", c.callID, "error", err)

			return
		}

		c.sink.PacketOut()

		if isSilenceFallback {
			c.sink.SilenceSent()
		}
	})
}

// observeDrift reports the absolute difference between this release tick's
// actual interval and the nominal 20ms, for the vaani_pacer_drift_ms histogram.
func (c *AudioSocketCallMedia) observeDrift(now time.Time) {
	if !c.prevRelease.IsZero() {
		delta := now.Sub(c.prevRelease)
		driftMs := math.Abs(float64(delta-FrameInterval)) / float64(time.Millisecond)
		c.sink.PacerDrift(driftMs)
	}

	c.prevRelease = now
}
