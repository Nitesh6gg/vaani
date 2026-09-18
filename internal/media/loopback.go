package media

import (
	"context"
	"encoding/binary"
	"log/slog"
	"math"
	"net"
	"time"
)

// Sink receives per-call media observability events. Implemented by the metrics
// package at the call site so this package stays free of a prometheus dependency.
type Sink interface {
	PacketIn(bytes int)
	PacketOut()
	SeqGap()
	Malformed()
	SendError()
	Late()
	Duplicate()
	SilenceInserted()
	SSRCChange()
	Reanchor()
	SilenceSent()
	WatchdogTimeout()
	PacerDrift(ms float64)
	AudioLevel(rms float64)
}

// Config bundles a call's media pipeline tunables, kept separate from the fixed
// callID/conn/sink triple so NewCallMedia's signature doesn't grow with every
// phase. Handler defaults to LoopbackHandler if nil.
type Config struct {
	JitterBufferPackets int
	FromWire            Endianness
	Handler             Handler
	RecordDir           string
}

// CallMedia runs the media pipeline for exactly one call:
//
//	UDP reader -> jitter buffer -> (20ms release tick) -> byte-order normalize ->
//	Handler.ProcessFrame -> outbound queue -> (20ms write tick) -> UDP
//
// Release and write run on independent tickers so a slow Handler (an LLM call, in
// Phase 3) never delays the paced outbound RTP stream -- a slow tick just means the
// writer falls back to silence for that frame, per the pacer invariant that it
// always emits exactly one packet per tick.
type CallMedia struct {
	callID   string
	ep       *Endpoint
	sink     Sink
	jb       *JitterBuffer
	handler  Handler
	fromWire Endianness
	recorder *WavRecorder
	watchdog *Watchdog
	seq      SeqTracker

	releasePacer *Pacer
	writePacer   *Pacer
	sender       *Sender
	outbound     chan *[]byte

	prevRelease time.Time
}

// NewCallMedia wraps conn (already bound to the call's allocated port) into a
// running media pipeline.
func NewCallMedia(callID string, conn *net.UDPConn, sink Sink, cfg Config) *CallMedia {
	handler := cfg.Handler
	if handler == nil {
		handler = LoopbackHandler{}
	}

	return &CallMedia{
		callID:       callID,
		ep:           NewEndpoint(conn),
		sink:         sink,
		jb:           NewJitterBuffer(cfg.JitterBufferPackets, sink),
		handler:      handler,
		fromWire:     cfg.FromWire,
		recorder:     NewWavRecorder(cfg.RecordDir, callID),
		watchdog:     NewWatchdog(callID, WatchdogTimeout, sink),
		releasePacer: NewPacer(FrameInterval),
		writePacer:   NewPacer(FrameInterval),
		sender:       NewSender(),
		outbound:     make(chan *[]byte, 5),
	}
}

// Run starts the pipeline's goroutines and blocks until ctx is cancelled or the
// UDP socket errors out.
func (c *CallMedia) Run(ctx context.Context) {
	readDone := make(chan struct{})

	go c.watchdog.Run(ctx)
	go c.releaseLoop(ctx)
	go c.writeLoop(ctx)
	go c.readLoop(ctx, readDone)

	<-ctx.Done()
	c.releasePacer.Stop()
	c.writePacer.Stop()
	_ = c.ep.Close()
	<-readDone
	c.recorder.Close()
}

// Close tears down the call's media plane; equivalent to cancelling the context
// passed to Run, provided for callers that don't otherwise hold that cancel func.
func (c *CallMedia) Close() {
	c.releasePacer.Stop()
	c.writePacer.Stop()
	_ = c.ep.Close()
	c.recorder.Close()
}

// RemoteLocked reports whether a valid inbound packet has ever locked the outbound
// RTP destination for this call. Callers should WARN at teardown if this is still
// false -- it means no audio was ever received from Asterisk for the whole call.
func (c *CallMedia) RemoteLocked() bool {
	return c.ep.Remote() != nil
}

func (c *CallMedia) readLoop(ctx context.Context, done chan<- struct{}) {
	defer close(done)

	buf := make([]byte, 1500) // generous UDP MTU headroom; RTP+slin16@20ms is 640B payload

	for {
		n, addr, err := c.ep.ReadFrom(buf)
		if err != nil {
			if ctx.Err() != nil {
				return // socket closed as part of teardown; not an error
			}

			slog.Warn("rtp read error", "call_id", c.callID, "error", err)

			return
		}

		if IsRTCP(buf[:n]) {
			continue
		}

		pkt, err := ParseRTP(buf[:n])
		if err != nil {
			c.sink.Malformed()
			slog.Debug("malformed rtp packet", "call_id", c.callID, "error", err)

			continue
		}

		if c.ep.LockRemote(addr) {
			slog.Info("rtp remote locked", "call_id", c.callID, "addr", addr)
		}

		c.sender.SetPayloadType(pkt.PayloadType)
		c.sink.PacketIn(len(pkt.Payload))
		c.watchdog.Touch()

		if c.seq.Observe(pkt.SequenceNumber) {
			c.sink.SeqGap()
		}

		frame := GetFrame()
		*frame = (*frame)[:0]
		*frame = append(*frame, pkt.Payload...)

		c.jb.Push(pkt.SequenceNumber, pkt.SSRC, frame)
	}
}

// releaseLoop pops one frame per tick from the jitter buffer, normalizes it to LE
// PCM16, runs it through the Handler, and queues the result(s) for the writer.
func (c *CallMedia) releaseLoop(ctx context.Context) {
	c.releasePacer.Run(func(now time.Time) {
		if ctx.Err() != nil {
			return
		}

		c.observeDrift(now)

		raw := c.jb.Release()
		pcm := *raw

		NormalizeToLE(pcm, c.fromWire)
		c.recorder.WriteIn(pcm)
		c.sink.AudioLevel(rms(pcm))

		for _, frame := range c.handler.ProcessFrame(ctx, c.callID, pcm) {
			out := GetFrame()
			*out = (*out)[:0]
			*out = append(*out, frame...)

			select {
			case c.outbound <- out:
			default:
				// Writer isn't keeping up; drop rather than block release/handler.
				PutFrame(out)
			}
		}

		PutFrame(raw)
	})
}

// writeLoop drains the outbound queue on its own tick and sends each frame as
// paced RTP. RTP must never starve on an active call: if the queue is empty --
// handler underrun, startup, or a Handler that legitimately has nothing to say
// this tick -- it still transmits a zeroed silence frame on schedule, per the
// pacer invariant that exactly one packet goes out every tick once a remote is
// locked.
func (c *CallMedia) writeLoop(ctx context.Context) {
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

		pkt := c.sender.Build(*frame)

		out, err := pkt.Marshal()

		PutFrame(frame)

		if err != nil {
			slog.Error("rtp marshal error", "call_id", c.callID, "error", err)
			return
		}

		sent, err := c.ep.WriteTo(out)
		if err != nil {
			c.sink.SendError()
			slog.Warn("rtp write error", "call_id", c.callID, "error", err)

			return
		}

		if sent {
			c.sink.PacketOut()

			if isSilenceFallback {
				c.sink.SilenceSent()
			}
		}
	})
}

// observeDrift reports the absolute difference between this release tick's actual
// interval and the nominal 20ms, for the vaani_pacer_drift_ms histogram.
func (c *CallMedia) observeDrift(now time.Time) {
	if !c.prevRelease.IsZero() {
		delta := now.Sub(c.prevRelease)
		driftMs := math.Abs(float64(delta-FrameInterval)) / float64(time.Millisecond)
		c.sink.PacerDrift(driftMs)
	}

	c.prevRelease = now
}

// rms computes the root-mean-square level of a little-endian PCM16 buffer. Not
// VAD -- just an observability signal that real audio (not silence) is flowing.
func rms(pcm []byte) float64 {
	n := len(pcm) / 2
	if n == 0 {
		return 0
	}

	var sumSq float64

	for i := 0; i < n; i++ {
		s := float64(int16(binary.LittleEndian.Uint16(pcm[i*2:])))
		sumSq += s * s
	}

	return math.Sqrt(sumSq / float64(n))
}
