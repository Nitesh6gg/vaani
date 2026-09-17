package media

import (
	"context"
	"log/slog"
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
}

// CallMedia runs the loop-back media plane for exactly one call: one reader
// goroutine (UDP -> buffered channel) and one writer goroutine (pacer -> UDP),
// per the no-goroutine-per-frame invariant. Frames are recycled through the shared
// sync.Pool on both sides.
type CallMedia struct {
	callID string
	ep     *Endpoint
	pacer  *Pacer
	sender *Sender
	sink   Sink

	frames chan *[]byte
	seq    SeqTracker
}

// NewCallMedia wraps conn (already bound to the call's allocated port) into a
// running loop-back media plane.
func NewCallMedia(callID string, conn *net.UDPConn, sink Sink) *CallMedia {
	return &CallMedia{
		callID: callID,
		ep:     NewEndpoint(conn),
		pacer:  NewPacer(FrameInterval),
		sender: NewSender(),
		sink:   sink,
		frames: make(chan *[]byte, 5),
	}
}

// Run starts the reader and writer goroutines and blocks until ctx is cancelled or
// the UDP socket is closed. Cancelling ctx (or calling Close) is the mechanism by
// which a caller tears down a call's media plane.
func (c *CallMedia) Run(ctx context.Context) {
	done := make(chan struct{})

	go c.writeLoop(ctx)
	go c.readLoop(ctx, done)

	<-ctx.Done()
	c.pacer.Stop()
	_ = c.ep.Close()
	<-done
}

// Close tears down the call's media plane; equivalent to cancelling the context
// passed to Run, provided for callers that don't otherwise hold that cancel func.
func (c *CallMedia) Close() {
	c.pacer.Stop()
	_ = c.ep.Close()
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

		c.ep.LockRemote(addr)
		c.sender.SetPayloadType(pkt.PayloadType)
		c.sink.PacketIn(len(pkt.Payload))

		if c.seq.Observe(pkt.SequenceNumber) {
			c.sink.SeqGap()
		}

		frame := GetFrame()
		*frame = (*frame)[:0]
		*frame = append(*frame, pkt.Payload...)

		select {
		case c.frames <- frame:
		default:
			// Buffered chan (capacity 5) is full: drop the oldest behavior would
			// require an extra goroutine; instead drop this frame to keep the
			// reader from ever blocking on the writer.
			PutFrame(frame)
		}
	}
}

func (c *CallMedia) writeLoop(ctx context.Context) {
	c.pacer.Run(func(_ time.Time) {
		if ctx.Err() != nil {
			return
		}

		var payload []byte

		select {
		case frame := <-c.frames:
			payload = *frame

			defer PutFrame(frame)
		default:
			// No inbound frame ready yet: still emit on schedule so the far end's
			// jitter buffer sees a steady 20ms cadence, per the pacer invariant.
			silence := GetFrame()
			defer PutFrame(silence)

			payload = *silence
			for i := range payload {
				payload[i] = 0
			}
		}

		pkt := c.sender.Build(payload)

		out, err := pkt.Marshal()
		if err != nil {
			slog.Error("rtp marshal error", "call_id", c.callID, "error", err)
			return
		}

		if err := c.ep.WriteTo(out); err != nil {
			slog.Warn("rtp write error", "call_id", c.callID, "error", err)
			return
		}

		c.sink.PacketOut()
	})
}
