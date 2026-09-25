package stt

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"strconv"
	"sync"
	"sync/atomic"

	"github.com/gorilla/websocket"
)

// sarvamSTTBaseURL is Sarvam's production saaras realtime endpoint, used when
// Config.WSURL is empty.
const sarvamSTTBaseURL = "wss://api.sarvam.ai/speech-to-text/ws"

// sarvamSampleRate is fixed: Vaani's Handler contract feeds ProcessFrame's
// already-16kHz LE PCM16 frames straight through, never anything else.
const sarvamSampleRate = 16000

// sarvamClient streams audio to Sarvam's saaras realtime STT WebSocket.
//
// Protocol verified live against the API by a sibling project
// (D:\go-agent-worker's internal/stt/sarvam.go, docs/DECISIONS.md ADR-009/
// ADR-010), ported here rather than re-derived from assumption:
//
//   - Audio goes as JSON, not raw binary WebSocket frames:
//     {"audio":{"data":<base64 PCM>,"sample_rate":16000,"encoding":"audio/wav"}}
//   - No keepalive. Sarvam rejects any message without an "audio" field
//     ('audio' must not be None) and closes the connection -- a
//     {"type":"ping"} on an idle socket destroys it rather than preserving
//     it. An untouched idle socket survived 75s+ in the same live probe.
//   - Sarvam reports failures in data.message, not data.error -- both are
//     logged, since which field is populated is undocumented.
//   - vad_signals=true is requested so Sarvam's own segmentation is active,
//     but its START_SPEECH/END_SPEECH signals are not surfaced as Results:
//     Vaani's barge-in is local (docs/AUDIO_PIPELINE.md), and mixing VAD
//     signals with transcripts on two different channels is exactly what a
//     sibling project's ADR-009 found corrupts turn-taking (Go's select
//     picks among ready channels at random, reordering events Sarvam sent in
//     sequence). If VAD signals are ever needed here, they must go through
//     this same Results channel, not a second one.
//
// Reconnect is lazy and one-shot, matching the same project's ADR-011: Feed
// never retries in a loop (a standing retry loop exhausted Sarvam's rate
// limit in that project's own history) -- a failed send drops that frame,
// invalidates the connection, and kicks off one background redial for the
// next Feed to use.
type sarvamClient struct {
	cfg    Config
	callID string
	ctx    context.Context

	connMu sync.RWMutex
	conn   *websocket.Conn

	writeMu      sync.Mutex
	reconnecting atomic.Bool

	results chan Result
}

// NewSarvamClient connects to Sarvam's saaras realtime STT WebSocket and
// starts streaming results. ctx bounds the connection's lifetime (redials
// stop once it's cancelled); callID is included on every log line.
func NewSarvamClient(ctx context.Context, callID string, cfg Config) (Client, error) {
	if cfg.WSURL == "" {
		cfg.WSURL = sarvamSTTBaseURL
	}
	if cfg.Language == "" {
		cfg.Language = "unknown"
	}

	c := &sarvamClient{
		cfg:     cfg,
		callID:  callID,
		ctx:     ctx,
		results: make(chan Result, 16),
	}

	conn, err := c.dial(ctx)
	if err != nil {
		return nil, fmt.Errorf("stt: connect: %w", err)
	}

	c.setConn(conn)
	go c.receiveLoop(ctx, conn)

	return c, nil
}

func (c *sarvamClient) dial(ctx context.Context) (*websocket.Conn, error) {
	q := url.Values{}
	q.Set("model", c.cfg.Model)
	q.Set("language-code", c.cfg.Language)
	q.Set("mode", "transcribe")
	q.Set("sample_rate", strconv.Itoa(sarvamSampleRate))
	q.Set("vad_signals", "true")

	endpoint := c.cfg.WSURL + "?" + q.Encode()
	header := http.Header{}
	header.Set("Api-Subscription-Key", c.cfg.APIKey)

	conn, _, err := websocket.DefaultDialer.DialContext(ctx, endpoint, header)
	if err != nil {
		return nil, err
	}

	return conn, nil
}

func (c *sarvamClient) setConn(conn *websocket.Conn) {
	c.connMu.Lock()
	c.conn = conn
	c.connMu.Unlock()
}

func (c *sarvamClient) getConn() *websocket.Conn {
	c.connMu.RLock()
	defer c.connMu.RUnlock()

	return c.conn
}

// invalidate clears conn if it's still the active one, so the next Feed
// triggers a redial rather than writing to a dead socket.
func (c *sarvamClient) invalidate(conn *websocket.Conn) {
	c.connMu.Lock()
	if c.conn == conn {
		c.conn = nil
	}
	c.connMu.Unlock()

	_ = conn.Close()
}

func (c *sarvamClient) triggerReconnect() {
	if !c.reconnecting.CompareAndSwap(false, true) {
		return
	}

	go func() {
		defer c.reconnecting.Store(false)

		if c.ctx.Err() != nil {
			return
		}

		conn, err := c.dial(c.ctx)
		if err != nil {
			slog.Warn("stt reconnect failed", "call_id", c.callID, "error", err)
			return
		}

		c.setConn(conn)
		slog.Info("stt reconnected", "call_id", c.callID)

		go c.receiveLoop(c.ctx, conn)
	}()
}

// Feed sends one PCM frame to Sarvam. Never blocks on a redial: a dead
// connection drops this frame and kicks off one background reconnect for the
// next Feed call, rather than retrying in a loop.
func (c *sarvamClient) Feed(pcm []byte) error {
	conn := c.getConn()
	if conn == nil {
		c.triggerReconnect()
		return fmt.Errorf("stt: not connected")
	}

	msg := sttAudioMessage{}
	msg.Audio.Data = base64.StdEncoding.EncodeToString(pcm)
	msg.Audio.SampleRate = sarvamSampleRate
	msg.Audio.Encoding = "audio/wav"

	c.writeMu.Lock()
	err := conn.WriteJSON(msg)
	c.writeMu.Unlock()

	if err != nil {
		c.invalidate(conn)
		return fmt.Errorf("stt: send: %w", err)
	}

	return nil
}

func (c *sarvamClient) Results() <-chan Result { return c.results }

// Close closes the underlying connection. The receive loop's own exit closes
// Results.
func (c *sarvamClient) Close() error {
	conn := c.getConn()
	if conn == nil {
		return nil
	}

	return conn.Close()
}

// receiveLoop owns one connection and exits when that connection dies; the
// next Feed redials. Results is deliberately never closed here -- only Close
// or ctx cancellation end the client for good, and Handler.readSTT already
// selects on its own context alongside Results, so a channel close isn't
// needed for shutdown to work. Closing it here would either need multi-
// goroutine close-once tracking across reconnects for no real benefit, or
// risk a double-close panic; matching a sibling project's proven design
// (D:\go-agent-worker's internal/stt/sarvam.go) instead of inventing one.
func (c *sarvamClient) receiveLoop(ctx context.Context, conn *websocket.Conn) {
	for {
		if ctx.Err() != nil {
			return
		}

		_, data, err := conn.ReadMessage()
		if err != nil {
			c.invalidate(conn)
			return
		}

		var m sttInboundMessage
		if err := json.Unmarshal(data, &m); err != nil {
			continue
		}

		if m.Data.Error != "" {
			slog.Warn("stt provider error", "call_id", c.callID, "error", m.Data.Error)
		}

		if m.Data.Message != "" {
			slog.Warn("stt provider error", "call_id", c.callID, "message", m.Data.Message)
		}

		if m.Type != "data" || m.Data.Transcript == "" {
			continue
		}

		select {
		case c.results <- Result{Text: m.Data.Transcript, Final: true}:
		case <-ctx.Done():
			return
		}
	}
}

type sttAudioMessage struct {
	Audio struct {
		Data       string `json:"data"`
		SampleRate int    `json:"sample_rate"`
		Encoding   string `json:"encoding"`
	} `json:"audio"`
}

type sttInboundMessage struct {
	Type string `json:"type"`
	Data struct {
		Transcript string `json:"transcript"`
		Error      string `json:"error"`
		// Sarvam reports failures here, not in Error -- a sibling project's
		// ADR-010 found reading only Error hid every STT error.
		Message string `json:"message"`
	} `json:"data"`
}
