package stt

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// sttServer runs a minimal fake Sarvam STT WS endpoint for one connection at a
// time, handing each accepted connection to handle on its own goroutine.
func sttServer(t *testing.T, handle func(conn *websocket.Conn, r *http.Request)) *httptest.Server {
	t.Helper()

	upgrader := websocket.Upgrader{}

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		require.NoError(t, err)

		go handle(conn, r)
	}))

	t.Cleanup(srv.Close)

	return srv
}

func wsURL(httpURL string) string {
	return "ws" + strings.TrimPrefix(httpURL, "http")
}

func TestSarvamClient_DialSendsExpectedQueryParamsAndHeader(t *testing.T) {
	gotQuery := make(chan map[string][]string, 1)
	gotKey := make(chan string, 1)

	srv := sttServer(t, func(conn *websocket.Conn, r *http.Request) {
		gotQuery <- r.URL.Query()
		gotKey <- r.Header.Get("Api-Subscription-Key")
		_ = conn.Close()
	})

	c, err := NewSarvamClient(context.Background(), "call1", Config{
		WSURL:    wsURL(srv.URL),
		APIKey:   "test-key",
		Model:    "saaras:v3",
		Language: "hi-IN",
	})
	require.NoError(t, err)
	defer func() { _ = c.Close() }()

	select {
	case q := <-gotQuery:
		assert.Equal(t, []string{"saaras:v3"}, q["model"])
		assert.Equal(t, []string{"hi-IN"}, q["language-code"])
		assert.Equal(t, []string{"transcribe"}, q["mode"])
		assert.Equal(t, []string{"16000"}, q["sample_rate"])
		assert.Equal(t, []string{"true"}, q["vad_signals"])
	case <-time.After(time.Second):
		t.Fatal("server never observed a connection")
	}

	assert.Equal(t, "test-key", <-gotKey)
}

func TestSarvamClient_DialDefaultsLanguageToUnknown(t *testing.T) {
	gotQuery := make(chan map[string][]string, 1)

	srv := sttServer(t, func(conn *websocket.Conn, r *http.Request) {
		gotQuery <- r.URL.Query()
		_ = conn.Close()
	})

	c, err := NewSarvamClient(context.Background(), "call1", Config{WSURL: wsURL(srv.URL), APIKey: "k", Model: "saaras:v3"})
	require.NoError(t, err)
	defer func() { _ = c.Close() }()

	q := <-gotQuery
	assert.Equal(t, []string{"unknown"}, q["language-code"])
}

func TestSarvamClient_FeedSendsBase64AudioJSON(t *testing.T) {
	type audioPayload struct {
		Audio struct {
			Data       string `json:"data"`
			SampleRate int    `json:"sample_rate"`
			Encoding   string `json:"encoding"`
		} `json:"audio"`
	}

	gotMsg := make(chan audioPayload, 1)

	srv := sttServer(t, func(conn *websocket.Conn, r *http.Request) {
		_, data, err := conn.ReadMessage()
		if err != nil {
			return
		}

		var p audioPayload
		require.NoError(t, json.Unmarshal(data, &p))
		gotMsg <- p
	})

	c, err := NewSarvamClient(context.Background(), "call1", Config{WSURL: wsURL(srv.URL), APIKey: "k", Model: "saaras:v3"})
	require.NoError(t, err)
	defer func() { _ = c.Close() }()

	pcm := []byte{1, 2, 3, 4, 5, 6}
	require.NoError(t, c.Feed(pcm))

	select {
	case p := <-gotMsg:
		assert.Equal(t, 16000, p.Audio.SampleRate)
		assert.Equal(t, "audio/wav", p.Audio.Encoding)
		assert.NotEmpty(t, p.Audio.Data)
	case <-time.After(time.Second):
		t.Fatal("server never received a Feed message")
	}
}

func TestSarvamClient_TranscriptBecomesFinalResult(t *testing.T) {
	srv := sttServer(t, func(conn *websocket.Conn, r *http.Request) {
		_ = conn.WriteJSON(map[string]any{
			"type": "data",
			"data": map[string]any{"transcript": "hello there"},
		})
	})

	c, err := NewSarvamClient(context.Background(), "call1", Config{WSURL: wsURL(srv.URL), APIKey: "k", Model: "saaras:v3"})
	require.NoError(t, err)
	defer func() { _ = c.Close() }()

	select {
	case r := <-c.Results():
		assert.Equal(t, "hello there", r.Text)
		assert.True(t, r.Final)
	case <-time.After(time.Second):
		t.Fatal("no result received")
	}
}

func TestSarvamClient_EmptyTranscriptIgnored(t *testing.T) {
	delivered := make(chan struct{})

	srv := sttServer(t, func(conn *websocket.Conn, r *http.Request) {
		_ = conn.WriteJSON(map[string]any{"type": "data", "data": map[string]any{"transcript": ""}})
		_ = conn.WriteJSON(map[string]any{"type": "data", "data": map[string]any{"transcript": "real one"}})
		close(delivered)
	})

	c, err := NewSarvamClient(context.Background(), "call1", Config{WSURL: wsURL(srv.URL), APIKey: "k", Model: "saaras:v3"})
	require.NoError(t, err)
	defer func() { _ = c.Close() }()

	<-delivered

	select {
	case r := <-c.Results():
		assert.Equal(t, "real one", r.Text, "the empty transcript must be skipped, not delivered as a blank Result")
	case <-time.After(time.Second):
		t.Fatal("no result received")
	}
}

func TestSarvamClient_VadSignalMessageIsNotDeliveredAsResult(t *testing.T) {
	srv := sttServer(t, func(conn *websocket.Conn, r *http.Request) {
		_ = conn.WriteJSON(map[string]any{
			"type": "events",
			"data": map[string]any{"signal_type": "START_SPEECH"},
		})
		_ = conn.WriteJSON(map[string]any{"type": "data", "data": map[string]any{"transcript": "after vad"}})
	})

	c, err := NewSarvamClient(context.Background(), "call1", Config{WSURL: wsURL(srv.URL), APIKey: "k", Model: "saaras:v3"})
	require.NoError(t, err)
	defer func() { _ = c.Close() }()

	select {
	case r := <-c.Results():
		assert.Equal(t, "after vad", r.Text, "only the transcript message should produce a Result")
	case <-time.After(time.Second):
		t.Fatal("no result received")
	}
}

func TestSarvamClient_FeedAfterCloseReconnectsInBackground(t *testing.T) {
	connections := make(chan *websocket.Conn, 4)

	srv := sttServer(t, func(conn *websocket.Conn, r *http.Request) {
		connections <- conn
		// Keep the connection open until the test closes it, so ReadMessage
		// blocks instead of erroring immediately.
		for {
			if _, _, err := conn.ReadMessage(); err != nil {
				return
			}
		}
	})

	c, err := NewSarvamClient(context.Background(), "call1", Config{WSURL: wsURL(srv.URL), APIKey: "k", Model: "saaras:v3"})
	require.NoError(t, err)
	defer func() { _ = c.Close() }()

	var first *websocket.Conn
	select {
	case first = <-connections:
	case <-time.After(time.Second):
		t.Fatal("no initial connection")
	}

	// Kill the connection from the server side to simulate a drop.
	require.NoError(t, first.Close())

	// A write can succeed into the local socket buffer even after the peer
	// has already closed its end, so a single Feed call's return value isn't
	// a reliable "has the drop been noticed yet" signal on its own -- keep
	// calling it (so whichever call does notice, via its own failed write or
	// because the read loop already invalidated the connection, triggers a
	// reconnect) until the server actually accepts a second connection.
	require.Eventually(t, func() bool {
		_ = c.Feed([]byte{1, 2, 3, 4})

		select {
		case <-connections:
			return true
		default:
			return false
		}
	}, 2*time.Second, 10*time.Millisecond, "client never reconnected after the drop")

	// And Feed should work again now that the reconnect completed.
	require.Eventually(t, func() bool {
		return c.Feed([]byte{1, 2, 3, 4}) == nil
	}, time.Second, 5*time.Millisecond, "Feed never succeeded again after reconnecting")
}
