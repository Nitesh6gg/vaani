package media

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type nopAudioSocketSink struct{}

func (nopAudioSocketSink) PacketIn(int)       {}
func (nopAudioSocketSink) PacketOut()         {}
func (nopAudioSocketSink) SilenceSent()       {}
func (nopAudioSocketSink) SendError()         {}
func (nopAudioSocketSink) Malformed()         {}
func (nopAudioSocketSink) QueueDropped()      {}
func (nopAudioSocketSink) WatchdogTimeout()   {}
func (nopAudioSocketSink) PacerDrift(float64) {}
func (nopAudioSocketSink) AudioLevel(float64) {}

// Earshot's inbound messages needn't be one frame each: the loopback call
// must re-frame them, echo the same audio back as 640-byte binary messages,
// and end its reads when earshot closes the socket.
func TestEarshotCallMedia_ReframesAndEchoes(t *testing.T) {
	type result struct {
		earshot *Earshot
		media   *AudioSocketCallMedia
	}
	started := make(chan result, 1)
	// Frames that arrive in a clump behind their tick are heard but not echoed (see LateHearer), so
	// the audio is checked as the handler hears it: tick frames and late ones, in order.
	heard := &orderedHandler{}

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ws, err := (&websocket.Upgrader{}).Upgrade(w, r, nil)
		if !assert.NoError(t, err) {
			return
		}
		m, stats := NewEarshotCallMedia("call-1", ws, nopAudioSocketSink{}, AudioSocketConfig{Handler: heard})
		started <- result{stats, m}

		ctx, cancel := context.WithCancel(context.Background())
		go func() { <-m.ReadDone(); cancel() }()
		m.Run(ctx)
	}))
	defer srv.Close()

	ws, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(srv.URL, "http")+"/call", nil)
	require.NoError(t, err)
	defer func() { _ = ws.Close() }()
	res := <-started

	// 4 frames of non-silent audio, split unevenly: 100+1180+640+320+320.
	sent := make([]byte, 4*FrameSize)
	for i := range sent {
		sent[i] = byte(i%250 + 1)
	}
	rest := sent
	for _, n := range []int{100, 1180, 640, 320, 320} {
		require.NoError(t, ws.WriteMessage(websocket.BinaryMessage, rest[:n]))
		rest = rest[n:]
	}

	require.Eventually(t, func() bool { return len(heard.audio()) >= len(sent) }, 3*time.Second, 5*time.Millisecond)
	assert.Equal(t, sent, heard.audio(), "the audio is heard byte for byte, in order")

	require.NoError(t, ws.WriteMessage(websocket.CloseMessage,
		websocket.FormatCloseMessage(websocket.CloseNormalClosure, "hangup")))

	select {
	case <-res.media.ReadDone():
	case <-time.After(2 * time.Second):
		t.Fatal("ReadDone not closed after earshot closed the socket")
	}

	st := res.earshot.Stats()
	assert.Equal(t, int64(5), st.Messages)
	assert.Equal(t, int64(4), st.OddMessages)
	assert.Equal(t, int64(4*FrameSize), st.Bytes)
	assert.Equal(t, websocket.CloseNormalClosure, st.CloseCode)
}

// startEarshot runs one earshot call media with handler against a test
// WebSocket client, returning the client end and the call's Earshot.
func startEarshot(t *testing.T, handler Handler) (*websocket.Conn, *Earshot) {
	t.Helper()
	got := make(chan *Earshot, 1)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ws, err := (&websocket.Upgrader{}).Upgrade(w, r, nil)
		if !assert.NoError(t, err) {
			return
		}
		m, e := NewEarshotCallMedia("call-1", ws, nopAudioSocketSink{}, AudioSocketConfig{Handler: handler})
		got <- e

		ctx, cancel := context.WithCancel(context.Background())
		go func() { <-m.ReadDone(); cancel() }()
		m.Run(ctx)
	}))
	t.Cleanup(srv.Close)

	ws, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(srv.URL, "http")+"/call", nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = ws.Close() })

	return ws, <-got
}

// A quiet agent sends earshot nothing: FreeSWITCH plays its own silence, and
// filler would only queue in earshot's play buffer (delay that never drains).
func TestEarshotCallMedia_NoFillerSilence(t *testing.T) {
	ws, _ := startEarshot(t, SilentHandler{})

	_ = ws.SetReadDeadline(time.Now().Add(300 * time.Millisecond)) // 15 ticks
	_, _, err := ws.ReadMessage()
	var ne interface{ Timeout() bool }
	require.ErrorAs(t, err, &ne, "nothing may arrive while the agent has nothing to say")
	assert.True(t, ne.Timeout())
}

func TestEarshot_ClearSendsEarshotsClearMessage(t *testing.T) {
	ws, e := startEarshot(t, SilentHandler{})

	require.NoError(t, e.Clear())

	_ = ws.SetReadDeadline(time.Now().Add(time.Second))
	kind, msg, err := ws.ReadMessage()
	require.NoError(t, err)
	assert.Equal(t, websocket.TextMessage, kind)
	assert.JSONEq(t, `{"type":"clear"}`, string(msg))
}

// orderedHandler records the non-silent audio it hears, tick frames and late ones, in order.
type orderedHandler struct {
	mu  sync.Mutex
	pcm []byte
}

func (h *orderedHandler) ProcessFrame(_ context.Context, _ string, pcm []byte) [][]byte {
	h.HearLate(pcm)

	return nil
}

func (h *orderedHandler) HearLate(pcm []byte) {
	if bytes.Equal(pcm, make([]byte, FrameSize)) {
		return
	}

	h.mu.Lock()
	defer h.mu.Unlock()

	h.pcm = append(h.pcm, pcm...)
}

func (h *orderedHandler) audio() []byte {
	h.mu.Lock()
	defer h.mu.Unlock()

	return append([]byte(nil), h.pcm...)
}
