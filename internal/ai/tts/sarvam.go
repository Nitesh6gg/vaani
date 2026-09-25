package tts

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
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

// keepaliveInterval matches a sibling project's live-verified value
// (D:\go-agent-worker's internal/tts/sarvam.go) -- unlike STT, Sarvam's TTS
// socket needs this; it is not just tolerated.
const keepaliveInterval = 20 * time.Second

// sarvamClient streams text to Sarvam's Bulbul v3 TTS WebSocket and emits
// linear16 PCM.
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
//     next.
//   - Sarvam has no "stop synthesizing" message. Once text+flush is sent,
//     audio keeps arriving until Sarvam is done, regardless of what the
//     caller wants by then -- its own {"type":"event","event_type":"final"}
//     end-of-request signal was observed taking up to 9.5s in a live call,
//     long enough for a fresh reply's real audio to be misattributed to a
//     stale request if a connection were reused across turns and audio were
//     filtered by a tag instead. Vaani sidesteps that entirely by giving each
//     turn its own connection (see client.go's Client doc comment) rather
//     than porting the generation-tagging scheme the sibling project needed
//     for its call-scoped, cross-turn-reused connection.
//
// What one connection here still has to get right: a single turn's
// SentenceChunker can call Speak more than once (one per sentence), so the
// connection must not close after the first "final" -- only once every
// outstanding Speak call's "final" has arrived (pendingFlushes reaches 0) AND
// Close has been requested (AgentHandler calls Close once its LLM stream
// itself is done producing text, meaning no further Speak calls are coming).
// Cancel skips all of that and closes immediately, for a genuine barge-in.
type sarvamClient struct {
	cfg    Config
	callID string
	conn   *websocket.Conn // fixed for this client's whole lifetime; no reconnect

	writeMu sync.Mutex

	stateMu        sync.Mutex
	pendingFlushes int
	closing        bool // Close was requested: shut down once pendingFlushes reaches 0
	closed         bool // connection already torn down; guards against a double-close

	audio chan []byte
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
		cfg:    cfg,
		callID: callID,
		audio:  make(chan []byte, audioBufferChunks),
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

// Speak sends text for synthesis followed by a flush so Sarvam emits audio.
func (c *sarvamClient) Speak(text string) error {
	if text == "" {
		return nil
	}

	c.stateMu.Lock()
	if c.closing || c.closed {
		c.stateMu.Unlock()
		return fmt.Errorf("tts: client closing")
	}
	c.pendingFlushes++
	c.stateMu.Unlock()

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

func (c *sarvamClient) Audio() <-chan []byte { return c.audio }

func (c *sarvamClient) Cancel() error {
	c.forceClose()
	return nil
}

func (c *sarvamClient) Close() error {
	c.stateMu.Lock()
	c.closing = true
	done := c.pendingFlushes <= 0
	c.stateMu.Unlock()

	if done {
		c.forceClose()
	}

	return nil
}

// forceClose tears the connection down immediately; idempotent. The receive
// loop observes the closed connection and returns, closing Audio().
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

// onFinal accounts for one Speak call's audio having fully arrived, and
// completes a graceful Close once every outstanding one has.
func (c *sarvamClient) onFinal() {
	c.stateMu.Lock()
	if c.pendingFlushes > 0 {
		c.pendingFlushes--
	}
	done := c.closing && c.pendingFlushes <= 0
	c.stateMu.Unlock()

	if done {
		c.forceClose()
	}
}

func (c *sarvamClient) receiveLoop() {
	defer close(c.audio)

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
		case m.Type == "event" && m.Data.EventType == "final":
			c.onFinal()
		case m.Type == "audio" && m.Data.Audio != "":
			raw, err := base64.StdEncoding.DecodeString(m.Data.Audio)
			if err != nil {
				continue
			}

			select {
			case c.audio <- raw:
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
