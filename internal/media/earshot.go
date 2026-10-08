package media

import (
	"errors"
	"io"
	"log/slog"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

// earshotWriteTimeout bounds one frame write: a FreeSWITCH that stops reading
// must fail the call's media, not stall the write tick forever.
const earshotWriteTimeout = 2 * time.Second

// EarshotStats is what one call's earshot connection carried, for the F1 lab
// questions in docs/FREESWITCH.md (are inbound messages really 640 bytes?).
type EarshotStats struct {
	Messages     int64 // binary messages received
	Bytes        int64 // audio bytes received
	OddMessages  int64 // binary messages whose size wasn't FrameSize
	TextMessages int64 // JSON control messages received (logged at DEBUG)
	CloseCode    int   // WebSocket close code from FreeSWITCH, 0 if none
}

// Earshot is one call's mod_earshot connection, proto=native codec=l16
// rate=16000: raw little-endian L16 in binary messages both ways (see
// docs/FREESWITCH.md). Inbound messages are not guaranteed to be exactly one
// frame (earshot resamples the 8 kHz channel), so they're re-framed: a frame
// may span message boundaries.
//
// Unlike RTP or AudioSocket, it is never sent filler silence: earshot plays
// what it receives on FreeSWITCH's own clock and FreeSWITCH plays its own
// audio when nothing is queued, so a frame sent "just to keep the stream
// alive" only queues there. Audio delayed on the network and delivered in a
// burst used to stay queued for the rest of the call that way -- 160-180 ms
// of extra delay seen on live calls over a LAN (2026-10-08); with nothing
// sent while the agent is quiet, the queue empties at every pause.
type Earshot struct {
	ws     *websocket.Conn
	callID string

	r io.Reader // the binary message being read; readLoop-owned
	n int64     // its bytes so far

	statsMu sync.Mutex
	stats   EarshotStats

	writeMu sync.Mutex // gorilla allows one writer at a time: writeLoop and Clear
}

// NewEarshotCallMedia runs the media pipeline over a FreeSWITCH call's
// mod_earshot WebSocket (already upgraded). Same pipeline, pacing and
// Handler contract as an AudioSocket call; ToWire must stay LittleEndian.
func NewEarshotCallMedia(callID string, ws *websocket.Conn, sink AudioSocketSink, cfg AudioSocketConfig) (*AudioSocketCallMedia, *Earshot) {
	w := &Earshot{ws: ws, callID: callID}
	return newFramedCallMedia(callID, w, sink, cfg), w
}

// noFillerSilence marks Earshot for writeLoop (see quietWire).
func (*Earshot) noFillerSilence() {}

// clearMessage is earshot's native "drop queued playback" control message.
var clearMessage = []byte(`{"type":"clear"}`)

// Clear drops the agent audio earshot has queued but not yet played, so the
// caller stops hearing the agent at once (an interruption). Safe to call from
// any goroutine.
func (w *Earshot) Clear() error {
	w.writeMu.Lock()
	defer w.writeMu.Unlock()

	_ = w.ws.SetWriteDeadline(time.Now().Add(earshotWriteTimeout))

	return w.ws.WriteMessage(websocket.TextMessage, clearMessage)
}

// ReadFrame fills one pooled frame from as many binary messages as it takes.
func (w *Earshot) ReadFrame() (*[]byte, error) {
	out := GetFrame()
	buf := *out
	filled := 0

	for filled < FrameSize {
		if w.r == nil {
			kind, r, err := w.ws.NextReader()
			if err != nil {
				PutFrame(out)
				return nil, w.readErr(err)
			}

			if kind != websocket.BinaryMessage {
				w.text(r)
				continue
			}

			w.r, w.n = r, 0
		}

		n, err := w.r.Read(buf[filled:])
		filled += n
		w.n += int64(n)

		if errors.Is(err, io.EOF) {
			w.endMessage()
			continue
		}

		if err != nil {
			PutFrame(out)
			return nil, w.readErr(err)
		}
	}

	return out, nil
}

func (w *Earshot) endMessage() {
	w.statsMu.Lock()
	w.stats.Messages++
	w.stats.Bytes += w.n
	if w.n != FrameSize {
		w.stats.OddMessages++
	}
	w.statsMu.Unlock()
	w.r = nil
}

// text drains a JSON control message (dtmf, command_result... -- nothing
// Vaani asks earshot for yet).
func (w *Earshot) text(r io.Reader) {
	msg, _ := io.ReadAll(io.LimitReader(r, 4096))

	w.statsMu.Lock()
	w.stats.TextMessages++
	w.statsMu.Unlock()

	slog.Debug("earshot text message", "call_id", w.callID, "message", string(msg))
}

// readErr maps a WebSocket close to io.EOF (a normal end of call) and
// records its close code.
func (w *Earshot) readErr(err error) error {
	var ce *websocket.CloseError
	if errors.As(err, &ce) {
		w.statsMu.Lock()
		w.stats.CloseCode = ce.Code
		w.statsMu.Unlock()

		return io.EOF
	}

	return err
}

// WriteFrame sends one frame as one binary message.
func (w *Earshot) WriteFrame(pcm []byte) error {
	w.writeMu.Lock()
	defer w.writeMu.Unlock()

	_ = w.ws.SetWriteDeadline(time.Now().Add(earshotWriteTimeout))

	return w.ws.WriteMessage(websocket.BinaryMessage, pcm)
}

func (w *Earshot) Close() error { return w.ws.Close() }

// Stats returns a snapshot of what the connection carried so far.
func (w *Earshot) Stats() EarshotStats {
	w.statsMu.Lock()
	defer w.statsMu.Unlock()

	return w.stats
}
