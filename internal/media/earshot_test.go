package media

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
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
		stats func() EarshotStats
		media *AudioSocketCallMedia
	}
	started := make(chan result, 1)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ws, err := (&websocket.Upgrader{}).Upgrade(w, r, nil)
		if !assert.NoError(t, err) {
			return
		}
		m, stats := NewEarshotCallMedia("call-1", ws, nopAudioSocketSink{}, AudioSocketConfig{})
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

	var echoed []byte
	_ = ws.SetReadDeadline(time.Now().Add(3 * time.Second))
	for len(echoed) < len(sent) {
		kind, msg, err := ws.ReadMessage()
		require.NoError(t, err)
		require.Equal(t, websocket.BinaryMessage, kind)
		require.Len(t, msg, FrameSize, "every outbound message is exactly one frame")
		if !bytes.Equal(msg, make([]byte, FrameSize)) { // skip the pacer's silence
			echoed = append(echoed, msg...)
		}
	}
	assert.Equal(t, sent, echoed, "audio comes back byte for byte, in order")

	require.NoError(t, ws.WriteMessage(websocket.CloseMessage,
		websocket.FormatCloseMessage(websocket.CloseNormalClosure, "hangup")))

	select {
	case <-res.media.ReadDone():
	case <-time.After(2 * time.Second):
		t.Fatal("ReadDone not closed after earshot closed the socket")
	}

	st := res.stats()
	assert.Equal(t, int64(5), st.Messages)
	assert.Equal(t, int64(4), st.OddMessages)
	assert.Equal(t, int64(4*FrameSize), st.Bytes)
	assert.Equal(t, websocket.CloseNormalClosure, st.CloseCode)
}
