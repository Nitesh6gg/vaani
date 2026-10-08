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
	// QueueDropped fires when handler output is dropped because the outbound
	// queue was full (the writer isn't keeping up). Same contract as Sink's.
	QueueDropped()
	WatchdogTimeout()
	PacerDrift(ms float64)
	AudioLevel(rms float64)
}

// AudioSocketConfig bundles an AudioSocket call's tunables. Handler defaults to
// LoopbackHandler if nil, same as Config.
type AudioSocketConfig struct {
	Handler   Handler
	RecordDir string
	// ToWire exists for symmetry with Config.ToWire so the write path looks
	// identical across transports; AudioSocket's payload is little-endian by
	// protocol definition, so callers must leave it at the zero value
	// (LittleEndian) -- anything else is a programming error, not a deployment
	// question.
	ToWire Endianness
	// DebugAudio, when true, registers this call with the /debug/audio/{callID}
	// tap registry (see audiotap.go). Same contract as Config.DebugAudio.
	DebugAudio bool
	// MediaDeadTimeout, if > 0, is how long inbound media may stay completely
	// silent before Dead()'s channel closes. Same contract as Config's.
	MediaDeadTimeout time.Duration
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
//
// The same pipeline also runs FreeSWITCH calls (NewEarshotCallMedia): only
// the wire -- how one 640-byte frame is read and written -- differs.
type AudioSocketCallMedia struct {
	callID   string
	wire     frameWire
	readDone chan struct{}
	sink     AudioSocketSink
	handler  Handler
	toWire   Endianness
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
	return newFramedCallMedia(callID, &audioSocketWire{conn: conn, callID: callID, sink: sink}, sink, cfg)
}

// frameWire is one call's media connection, as whole 640-byte frames.
// ReadFrame is called only from readLoop, WriteFrame only from writeLoop.
type frameWire interface {
	// ReadFrame returns the next inbound frame as a pooled FrameSize buffer,
	// nil (with a nil error) for anything that isn't audio, or io.EOF when
	// the far end hung up.
	ReadFrame() (*[]byte, error)
	// WriteFrame sends one frame. An error is fatal to the call's media.
	WriteFrame(pcm []byte) error
	Close() error
}

// audioSocketWire is Asterisk's res_audiosocket framing over TCP.
type audioSocketWire struct {
	conn   net.Conn
	callID string
	sink   AudioSocketSink
	// writeBuf is the reused header+payload scratch buffer for
	// writeAudioSocketFrameInto -- sized once for the one frame size this hot
	// path ever sends, so it never allocates (invariant #3). writeLoop-goroutine-
	// owned only.
	writeBuf [audioSocketHeaderSize + FrameSize]byte
}

func (w *audioSocketWire) ReadFrame() (*[]byte, error) {
	frame, err := ReadAudioSocketFrame(w.conn)
	if err != nil {
		return nil, err
	}

	switch frame.Kind {
	case AudioSocketKindHangup:
		return nil, io.EOF
	case AudioSocketKindSlin16:
		if len(frame.Payload) != FrameSize {
			w.sink.Malformed()
			logFrameSizeOnce(w.callID, int(frame.Kind), len(frame.Payload))
			slog.Debug("audiosocket frame wrong size", "call_id", w.callID, "size", len(frame.Payload))

			return nil, nil
		}

		return &frame.Payload, nil // already a pooled FrameSize buffer, per ReadAudioSocketFrame
	default:
		// UUID, DTMF, error frames: no-op for now (control-plane concerns, not
		// the media pipeline). Logged for visibility.
		slog.Debug("audiosocket control frame", "call_id", w.callID, "kind", frame.Kind)

		return nil, nil
	}
}

// WriteFrame writes header+payload as one Write call (see audiosocket.go's
// package doc comment for why that matters).
func (w *audioSocketWire) WriteFrame(pcm []byte) error {
	return writeAudioSocketFrameInto(w.conn, w.writeBuf[:], AudioSocketKindSlin16, pcm)
}

func (w *audioSocketWire) Close() error { return w.conn.Close() }

// quietWire is a wire whose far end plays its own audio when nothing arrives
// (Earshot): writeLoop sends it no filler silence. RTP and AudioSocket do get
// filler -- their far end needs the steady 20 ms stream.
type quietWire interface{ noFillerSilence() }

func newFramedCallMedia(callID string, wire frameWire, sink AudioSocketSink, cfg AudioSocketConfig) *AudioSocketCallMedia {
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
		wire:         wire,
		readDone:     make(chan struct{}),
		sink:         sink,
		handler:      handler,
		toWire:       cfg.ToWire,
		recorder:     NewWavRecorder(cfg.RecordDir, callID),
		watchdog:     NewWatchdog(callID, WatchdogTimeout, cfg.MediaDeadTimeout, sink),
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
	var pacedLoops sync.WaitGroup

	pacedLoops.Go(func() { c.releaseLoop(ctx) })
	pacedLoops.Go(func() { c.writeLoop(ctx) })
	go c.watchdog.Run(ctx)
	go c.readLoop(ctx, c.readDone)

	<-ctx.Done()
	c.releasePacer.Stop()
	c.writePacer.Stop()
	_ = c.wire.Close()
	<-c.readDone
	// Pacer stop ends the release/write loops, but a tick already in flight
	// must finish before the recorder (and anything reading per-call state)
	// is touched -- Pacer.Stop does not wait for fn to return.
	pacedLoops.Wait()
	c.recorder.Close()

	if c.tap != nil {
		UnregisterAudioTap(c.callID)
	}
}

// ReadDone is closed once the connection stops delivering frames: the far end
// hung up or closed it, a read failed, or Run's ctx ended. Run itself keeps
// going until its ctx is cancelled.
func (c *AudioSocketCallMedia) ReadDone() <-chan struct{} {
	return c.readDone
}

// Dead returns a channel that's closed once inbound media has been silent for
// AudioSocketConfig.MediaDeadTimeout (never closes if that was 0). Same
// contract as CallMedia.Dead.
func (c *AudioSocketCallMedia) Dead() <-chan struct{} {
	return c.watchdog.Dead()
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
		buf, err := c.wire.ReadFrame()
		if err != nil {
			if ctx.Err() != nil || errors.Is(err, io.EOF) {
				return
			}

			slog.Warn("media read error", "call_id", c.callID, "error", err)

			return
		}

		if buf == nil {
			continue
		}

		c.sink.PacketIn(len(*buf))
		c.watchdog.Touch()

		select {
		case c.inbound <- buf:
		default:
			PutFrame(buf) // release isn't keeping up; drop rather than block the reader
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

		level := RMS(pcm)
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
				c.sink.QueueDropped()
				PutFrame(out)
			}
		}

		PutFrame(raw)
	})
}

// writeLoop drains the outbound queue on its own tick and sends each frame as a
// paced AudioSocket audio frame. If nothing is queued yet (handler underrun, or
// startup), it still sends a silence frame on schedule, per the pacer invariant.
//
// A failed write is fatal to this call's media, not just a dropped frame:
// writeAudioSocketFrameInto writes header+payload as one Write call (see
// audiosocket.go's package doc comment for why that matters), but a short
// write or any error still leaves the framed stream misaligned -- every
// subsequent frame would be parsed at the wrong boundary, corrupting audio
// rather than degrading it. On the first write error the loop stops writing
// and closes the connection (which also unblocks the reader); call teardown
// follows via StasisEnd.
func (c *AudioSocketCallMedia) writeLoop(ctx context.Context) {
	dead := false

	_, quiet := c.wire.(quietWire)

	c.writePacer.Run(func(_ time.Time) {
		if ctx.Err() != nil || dead {
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

		c.recorder.WriteOut(*frame) // filler too: keeps the recording on the call's clock

		if isSilenceFallback && quiet {
			PutFrame(frame)
			return
		}

		// Symmetry hook with CallMedia.writeLoop; always LE (no-op) unless a
		// caller ignored AudioSocketConfig.ToWire's contract.
		FromLE(*frame, c.toWire)

		err := c.wire.WriteFrame(*frame)

		PutFrame(frame)

		if err != nil {
			c.sink.SendError()
			dead = true

			select {
			case <-c.readDone:
				// The far end already closed (a hangup): the write just lost
				// the race with teardown.
				slog.Debug("media write after the connection closed", "call_id", c.callID, "error", err)
			default:
				slog.Error("media write error; stopping writes and closing connection (framed stream can no longer be trusted)",
					"call_id", c.callID, "error", err)
			}
			_ = c.wire.Close()

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
