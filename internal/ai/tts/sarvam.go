package tts

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

// sarvamTTSBaseURL is Sarvam's production Bulbul v3 endpoint, used when
// Config.WSURL is empty.
const sarvamTTSBaseURL = "wss://api.sarvam.ai/text-to-speech/ws"

// audioBufferChunks is deliberately large: synthesis outruns real-time
// playback, so a whole reply's audio can arrive well before AgentHandler's
// outbound queue (capacity 5, one 20ms frame at a time) drains it.
const audioBufferChunks = 256

// doneBufferSlots only ever needs to hold one entry per turn in flight, which
// in practice is one or two; sized generously so a slow consumer can't block
// the receive loop.
const doneBufferSlots = 16

// keepaliveInterval matches a sibling project's live-verified value
// (D:\go-agent-worker's internal/tts/sarvam.go) -- unlike STT, Sarvam's TTS
// socket needs this; it is not just tolerated.
const keepaliveInterval = 20 * time.Second

// sarvamClient streams text to Sarvam's Bulbul v3 TTS WebSocket and emits
// linear16 PCM, over one connection reused across every turn in a call (see
// client.go's Client doc comment for why).
//
// Protocol verified live against the API by a sibling project
// (D:\go-agent-worker's internal/tts/sarvam.go, docs/DECISIONS.md ADR-008/
// ADR-011/ADR-015/ADR-017), ported here rather than re-derived from
// assumption:
//
//   - send_completion_event=true is REQUIRED in the connect URL. Without it
//     Sarvam accepts the connection and every message and silently emits zero
//     audio -- verified directly: 0 samples without the flag, audio
//     immediately with it.
//   - Needs a ~20s {"type":"ping"} keepalive -- the opposite of STT, which
//     must never receive one.
//   - request_id on audio frames is a session identifier, not a per-request
//     one: verified constant across two sequential Speak-equivalent requests
//     on one connection, so it cannot distinguish one call's audio from the
//     next. Audio and "final" events instead arrive strictly in the order
//     their requests were sent, which is what generation tagging below relies
//     on to attribute each chunk.
//   - A whitespace-only "text" value (e.g. a bare "\n") is rejected with
//     {"type":"error",...,"message":"400: 'text' cannot be empty"} -- observed
//     live when SentenceChunker flushed a lone newline as a "sentence".
//     Sarvam sends this error INSTEAD OF a "final" for that request, never
//     both, so onFinal's bookkeeping runs on error too (see receiveLoop);
//     otherwise that request's pendingGens entry would never clear and its
//     generation's Done would never fire, wedging the call.
//   - Sarvam has no "stop synthesizing" message. Once text+flush is sent,
//     audio keeps arriving until Sarvam is done, regardless of what the
//     caller wants by then -- its own {"type":"event","event_type":"final"}
//     end-of-request signal was observed taking up to 9.5s in a live call.
//     Reusing one connection across turns means a barge-in can leave a stale
//     request's audio still in flight when a new turn starts on the same
//     connection; pendingGens (FIFO, one entry per outstanding Speak call, in
//     send order) tags every arriving chunk with the generation of the
//     oldest still-outstanding request, matching Sarvam's own in-order
//     delivery. A genuine barge-in instead calls Cancel, which throws the
//     whole connection away rather than trying to filter around the stale
//     request.
type sarvamClient struct {
	cfg    Config
	callID string
	conn   *websocket.Conn // fixed for this client's whole lifetime; no reconnect

	writeMu sync.Mutex

	genMu       sync.Mutex
	pendingGens []uint64        // FIFO: one entry per outstanding Speak call, in send order
	endedGens   map[uint64]bool // EndGeneration has been called for this gen
	lastGen     uint64          // most recent gen seen, for tagging audio if pendingGens is briefly empty

	stateMu sync.Mutex
	closed  bool // connection already torn down; guards against a double-close

	audio chan Chunk
	done  chan uint64
}

// NewSarvamClient dials Sarvam's Bulbul v3 TTS WebSocket, sends the initial
// config, and starts the receive + keepalive loops. ctx bounds the
// keepalive loop's lifetime; callID is included on every log line.
func NewSarvamClient(ctx context.Context, callID string, cfg Config) (Client, error) {
	if cfg.WSURL == "" {
		cfg.WSURL = sarvamTTSBaseURL
	}
	if cfg.SampleRate == 0 {
		cfg.SampleRate = 16000
	}

	c := &sarvamClient{
		cfg:       cfg,
		callID:    callID,
		endedGens: make(map[uint64]bool),
		audio:     make(chan Chunk, audioBufferChunks),
		done:      make(chan uint64, doneBufferSlots),
	}

	conn, err := c.dial(ctx)
	if err != nil {
		return nil, fmt.Errorf("tts: connect: %w", err)
	}

	if err := c.sendConfig(conn); err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("tts: send config: %w", err)
	}

	c.conn = conn

	go c.receiveLoop()
	go c.keepaliveLoop(ctx)

	return c, nil
}

func (c *sarvamClient) dial(ctx context.Context) (*websocket.Conn, error) {
	endpoint := fmt.Sprintf("%s?model=%s&send_completion_event=true", c.cfg.WSURL, c.cfg.Model)
	header := http.Header{}
	header.Set("api-subscription-key", c.cfg.APIKey)

	conn, _, err := websocket.DefaultDialer.DialContext(ctx, endpoint, header)
	if err != nil {
		return nil, err
	}

	return conn, nil
}

func (c *sarvamClient) sendConfig(conn *websocket.Conn) error {
	cfg := map[string]any{
		"target_language_code": c.cfg.Language,
		"speaker":              c.cfg.Voice,
		"speech_sample_rate":   fmt.Sprintf("%d", c.cfg.SampleRate),
		"enable_preprocessing": false,
		"output_audio_codec":   "linear16",
		"pace":                 1.0,
		"model":                c.cfg.Model,
	}

	return c.writeJSON(conn, map[string]any{"type": "config", "data": cfg})
}

func (c *sarvamClient) writeJSON(conn *websocket.Conn, v any) error {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()

	return conn.WriteJSON(v)
}

// Speak sends text for synthesis followed by a flush so Sarvam emits audio,
// tagging the outstanding request with gen for later attribution.
func (c *sarvamClient) Speak(text string, gen uint64) error {
	// Whitespace-only text (e.g. SentenceChunker treats "\n" as a sentence
	// delimiter, so a bare newline in the LLM's stream can flush as a
	// "sentence") isn't caught by a plain empty-string check, but Sarvam
	// trims it server-side and rejects it with a 400 -- see the doc comment
	// above: "'text' cannot be empty".
	if strings.TrimSpace(text) == "" {
		return nil
	}

	c.stateMu.Lock()
	closed := c.closed
	c.stateMu.Unlock()
	if closed {
		return fmt.Errorf("tts: client closed")
	}

	c.genMu.Lock()
	c.pendingGens = append(c.pendingGens, gen)
	c.lastGen = gen
	c.genMu.Unlock()

	if err := c.writeJSON(c.conn, map[string]any{
		"type": "text",
		"data": map[string]any{"text": text},
	}); err != nil {
		return fmt.Errorf("tts: send text: %w", err)
	}

	if err := c.writeJSON(c.conn, map[string]any{"type": "flush"}); err != nil {
		return fmt.Errorf("tts: send flush: %w", err)
	}

	return nil
}

// EndGeneration signals no further Speak calls are coming for gen. If none
// were ever pending (an LLM turn that produced no text), gen is done
// immediately.
func (c *sarvamClient) EndGeneration(gen uint64) {
	c.genMu.Lock()
	c.endedGens[gen] = true
	pending := c.genHasPendingLocked(gen)
	c.genMu.Unlock()

	if !pending {
		c.emitDone(gen)
	}
}

func (c *sarvamClient) genHasPendingLocked(gen uint64) bool {
	for _, g := range c.pendingGens {
		if g == gen {
			return true
		}
	}

	return false
}

func (c *sarvamClient) emitDone(gen uint64) {
	select {
	case c.done <- gen:
	default:
		slog.Warn("tts done buffer full, dropping signal", "call_id", c.callID, "gen", gen)
	}
}

func (c *sarvamClient) Audio() <-chan Chunk { return c.audio }
func (c *sarvamClient) Done() <-chan uint64 { return c.done }

func (c *sarvamClient) Cancel() error {
	c.forceClose()
	return nil
}

func (c *sarvamClient) Close() error {
	c.forceClose()
	return nil
}

// forceClose tears the connection down immediately; idempotent. The receive
// loop observes the closed connection and returns, closing Audio and Done.
func (c *sarvamClient) forceClose() {
	c.stateMu.Lock()
	if c.closed {
		c.stateMu.Unlock()
		return
	}
	c.closed = true
	c.stateMu.Unlock()

	_ = c.conn.Close()
}

// onFinal pops the oldest outstanding request off pendingGens (Sarvam
// delivers "final"/error events in send order) and, if that was the last
// outstanding request for its generation and EndGeneration has already been
// called for it, emits Done. Also called for a provider error (see
// receiveLoop): Sarvam sends error INSTEAD OF final for a rejected request,
// never both, but that request is just as resolved either way -- no more
// audio is coming for it, so its slot must clear the same way or its
// generation's Done would never fire.
func (c *sarvamClient) onFinal() {
	c.genMu.Lock()
	if len(c.pendingGens) == 0 {
		c.genMu.Unlock()
		return
	}
	gen := c.pendingGens[0]
	c.pendingGens = c.pendingGens[1:]
	pending := c.genHasPendingLocked(gen)
	ended := c.endedGens[gen]
	c.genMu.Unlock()

	if !pending && ended {
		c.emitDone(gen)
	}
}

// peekGen returns the generation to attribute the next arriving audio chunk
// to: the oldest outstanding request, or the most recent generation seen if
// none is currently outstanding (a chunk arriving in the gap between one
// request's last byte and the next request being sent).
func (c *sarvamClient) peekGen() uint64 {
	c.genMu.Lock()
	defer c.genMu.Unlock()

	if len(c.pendingGens) > 0 {
		return c.pendingGens[0]
	}

	return c.lastGen
}

func (c *sarvamClient) receiveLoop() {
	defer close(c.audio)
	defer close(c.done)

	for {
		_, data, err := c.conn.ReadMessage()
		if err != nil {
			return
		}

		var m ttsInboundMessage
		if err := json.Unmarshal(data, &m); err != nil {
			continue
		}

		switch {
		case m.Type == "error":
			slog.Warn("tts provider error", "call_id", c.callID, "message", m.Data.Message)
			c.onFinal() // the rejected request is resolved, not retried -- see onFinal's doc comment
		case m.Type == "event" && m.Data.EventType == "final":
			c.onFinal()
		case m.Type == "audio" && m.Data.Audio != "":
			raw, err := base64.StdEncoding.DecodeString(m.Data.Audio)
			if err != nil {
				continue
			}

			select {
			case c.audio <- Chunk{PCM: raw, Gen: c.peekGen()}:
			default:
				slog.Warn("tts audio buffer full, dropping chunk", "call_id", c.callID)
			}
		}
	}
}

func (c *sarvamClient) keepaliveLoop(ctx context.Context) {
	ticker := time.NewTicker(keepaliveInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			c.stateMu.Lock()
			closed := c.closed
			c.stateMu.Unlock()

			if closed {
				return
			}

			if err := c.writeJSON(c.conn, map[string]any{"type": "ping"}); err != nil {
				slog.Debug("tts keepalive failed", "call_id", c.callID, "error", err)
				return
			}
		}
	}
}

type ttsInboundMessage struct {
	Type string `json:"type"`
	Data struct {
		Audio     string `json:"audio"`
		Message   string `json:"message"`
		EventType string `json:"event_type"`
	} `json:"data"`
}
