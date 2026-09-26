package tts

import (
	"context"
	"encoding/base64"
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

func ttsServer(t *testing.T, handle func(conn *websocket.Conn, r *http.Request)) *httptest.Server {
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

func sendFinal(conn *websocket.Conn) error {
	return conn.WriteJSON(map[string]any{"type": "event", "data": map[string]any{"event_type": "final"}})
}

func TestSarvamClient_DialRequestsCompletionEventAndModel(t *testing.T) {
	gotQuery := make(chan map[string][]string, 1)
	gotKey := make(chan string, 1)

	srv := ttsServer(t, func(conn *websocket.Conn, r *http.Request) {
		gotQuery <- r.URL.Query()
		gotKey <- r.Header.Get("api-subscription-key")

		// Drain the config message so sendConfig doesn't block/error.
		_, _, _ = conn.ReadMessage()
	})

	c, err := NewSarvamClient(context.Background(), "call1", Config{
		WSURL:  wsURL(srv.URL),
		APIKey: "test-key",
		Model:  "bulbul:v3",
		Voice:  "shubh",
	})
	require.NoError(t, err)
	defer func() { _ = c.Close() }()

	select {
	case q := <-gotQuery:
		assert.Equal(t, []string{"bulbul:v3"}, q["model"])
		assert.Equal(t, []string{"true"}, q["send_completion_event"])
	case <-time.After(time.Second):
		t.Fatal("server never observed a connection")
	}

	assert.Equal(t, "test-key", <-gotKey)
}

func TestSarvamClient_SendsConfigOnConnect(t *testing.T) {
	type configMsg struct {
		Type string `json:"type"`
		Data struct {
			Speaker          string `json:"speaker"`
			SpeechSampleRate string `json:"speech_sample_rate"`
			OutputAudioCodec string `json:"output_audio_codec"`
			Model            string `json:"model"`
		} `json:"data"`
	}

	got := make(chan configMsg, 1)

	srv := ttsServer(t, func(conn *websocket.Conn, r *http.Request) {
		_, data, err := conn.ReadMessage()
		if err != nil {
			return
		}

		var m configMsg
		require.NoError(t, json.Unmarshal(data, &m))
		got <- m
	})

	c, err := NewSarvamClient(context.Background(), "call1", Config{
		WSURL: wsURL(srv.URL), APIKey: "k", Model: "bulbul:v3", Voice: "shubh", SampleRate: 16000,
	})
	require.NoError(t, err)
	defer func() { _ = c.Close() }()

	select {
	case m := <-got:
		assert.Equal(t, "config", m.Type)
		assert.Equal(t, "shubh", m.Data.Speaker)
		assert.Equal(t, "16000", m.Data.SpeechSampleRate)
		assert.Equal(t, "linear16", m.Data.OutputAudioCodec)
		assert.Equal(t, "bulbul:v3", m.Data.Model)
	case <-time.After(time.Second):
		t.Fatal("server never received the config message")
	}
}

func TestSarvamClient_SpeakSendsTextThenFlush(t *testing.T) {
	msgs := make(chan map[string]any, 4)

	srv := ttsServer(t, func(conn *websocket.Conn, r *http.Request) {
		_, _, _ = conn.ReadMessage() // config
		for i := 0; i < 2; i++ {
			_, data, err := conn.ReadMessage()
			if err != nil {
				return
			}
			var m map[string]any
			_ = json.Unmarshal(data, &m)
			msgs <- m
		}
	})

	c, err := NewSarvamClient(context.Background(), "call1", Config{WSURL: wsURL(srv.URL), APIKey: "k", Model: "bulbul:v3", Voice: "shubh"})
	require.NoError(t, err)
	defer func() { _ = c.Close() }()

	require.NoError(t, c.Speak("hello there", 1))

	m1 := <-msgs
	assert.Equal(t, "text", m1["type"])
	data, _ := m1["data"].(map[string]any)
	assert.Equal(t, "hello there", data["text"])

	m2 := <-msgs
	assert.Equal(t, "flush", m2["type"])
}

func TestSarvamClient_SpeakEmptyTextIsNoop(t *testing.T) {
	srv := ttsServer(t, func(conn *websocket.Conn, r *http.Request) {
		_, _, _ = conn.ReadMessage() // config
	})

	c, err := NewSarvamClient(context.Background(), "call1", Config{WSURL: wsURL(srv.URL), APIKey: "k", Model: "bulbul:v3", Voice: "shubh"})
	require.NoError(t, err)
	defer func() { _ = c.Close() }()

	assert.NoError(t, c.Speak("", 1))
}

func TestSarvamClient_AudioEventTaggedWithOutstandingGeneration(t *testing.T) {
	payload := []byte{10, 20, 30, 40}

	srv := ttsServer(t, func(conn *websocket.Conn, r *http.Request) {
		_, _, _ = conn.ReadMessage() // config
		_, _, _ = conn.ReadMessage() // text
		_, _, _ = conn.ReadMessage() // flush
		_ = conn.WriteJSON(map[string]any{
			"type": "audio",
			"data": map[string]any{"audio": base64.StdEncoding.EncodeToString(payload)},
		})
	})

	c, err := NewSarvamClient(context.Background(), "call1", Config{WSURL: wsURL(srv.URL), APIKey: "k", Model: "bulbul:v3", Voice: "shubh"})
	require.NoError(t, err)
	defer func() { _ = c.Close() }()

	require.NoError(t, c.Speak("hello", 42))

	select {
	case chunk := <-c.Audio():
		assert.Equal(t, payload, chunk.PCM)
		assert.Equal(t, uint64(42), chunk.Gen)
	case <-time.After(time.Second):
		t.Fatal("no audio chunk received")
	}
}

func TestSarvamClient_DoneFiresOnceItsFinalArrives(t *testing.T) {
	finalNow := make(chan struct{})

	srv := ttsServer(t, func(conn *websocket.Conn, r *http.Request) {
		_, _, _ = conn.ReadMessage() // config
		_, _, _ = conn.ReadMessage() // text
		_, _, _ = conn.ReadMessage() // flush
		<-finalNow
		_ = sendFinal(conn)
	})

	c, err := NewSarvamClient(context.Background(), "call1", Config{WSURL: wsURL(srv.URL), APIKey: "k", Model: "bulbul:v3", Voice: "shubh"})
	require.NoError(t, err)
	defer func() { _ = c.Close() }()

	require.NoError(t, c.Speak("one sentence", 7))
	c.EndGeneration(7)

	select {
	case <-c.Done():
		t.Fatal("Done fired before the outstanding flush's final event arrived")
	case <-time.After(50 * time.Millisecond):
	}

	close(finalNow)

	select {
	case gen := <-c.Done():
		assert.Equal(t, uint64(7), gen)
	case <-time.After(time.Second):
		t.Fatal("Done never fired after the final event arrived")
	}
}

func TestSarvamClient_EndGenerationWithNoSpeakCallsFiresDoneImmediately(t *testing.T) {
	srv := ttsServer(t, func(conn *websocket.Conn, r *http.Request) {
		_, _, _ = conn.ReadMessage() // config
	})

	c, err := NewSarvamClient(context.Background(), "call1", Config{WSURL: wsURL(srv.URL), APIKey: "k", Model: "bulbul:v3", Voice: "shubh"})
	require.NoError(t, err)
	defer func() { _ = c.Close() }()

	c.EndGeneration(3) // an LLM turn that produced no text at all

	select {
	case gen := <-c.Done():
		assert.Equal(t, uint64(3), gen)
	case <-time.After(time.Second):
		t.Fatal("Done never fired for a generation with no outstanding Speak calls")
	}
}

func TestSarvamClient_MultipleSpeaksOnlyDoneAfterAllFinalsForThatGeneration(t *testing.T) {
	sendFinalNow := make(chan struct{}, 2)

	srv := ttsServer(t, func(conn *websocket.Conn, r *http.Request) {
		_, _, _ = conn.ReadMessage() // config
		for i := 0; i < 2; i++ {
			_, _, _ = conn.ReadMessage() // text
			_, _, _ = conn.ReadMessage() // flush
			<-sendFinalNow
			_ = sendFinal(conn)
		}
	})

	c, err := NewSarvamClient(context.Background(), "call1", Config{WSURL: wsURL(srv.URL), APIKey: "k", Model: "bulbul:v3", Voice: "shubh"})
	require.NoError(t, err)
	defer func() { _ = c.Close() }()

	require.NoError(t, c.Speak("sentence one", 5))
	require.NoError(t, c.Speak("sentence two", 5))
	c.EndGeneration(5)

	sendFinalNow <- struct{}{} // first sentence's final

	select {
	case <-c.Done():
		t.Fatal("Done fired after only one of two pending finals arrived")
	case <-time.After(100 * time.Millisecond):
	}

	sendFinalNow <- struct{}{} // second sentence's final

	select {
	case gen := <-c.Done():
		assert.Equal(t, uint64(5), gen)
	case <-time.After(time.Second):
		t.Fatal("Done never fired once both finals arrived")
	}
}

func TestSarvamClient_ReusedAcrossTurnsSecondSpeakUsesSameConnection(t *testing.T) {
	connCount := make(chan struct{}, 4)

	srv := ttsServer(t, func(conn *websocket.Conn, r *http.Request) {
		connCount <- struct{}{}
		_, _, _ = conn.ReadMessage() // config
		for i := 0; i < 2; i++ {
			_, _, _ = conn.ReadMessage() // text
			_, _, _ = conn.ReadMessage() // flush
			_ = sendFinal(conn)
		}
	})

	c, err := NewSarvamClient(context.Background(), "call1", Config{WSURL: wsURL(srv.URL), APIKey: "k", Model: "bulbul:v3", Voice: "shubh"})
	require.NoError(t, err)
	defer func() { _ = c.Close() }()

	require.NoError(t, c.Speak("turn one", 1))
	c.EndGeneration(1)
	<-c.Done() // wait for the first turn's flush+final round trip

	require.NoError(t, c.Speak("turn two", 2))
	c.EndGeneration(2)
	<-c.Done()

	assert.Len(t, connCount, 1, "a second Speak call on the same Client must not open a new connection")
}

func TestSarvamClient_CancelClosesImmediatelyRegardlessOfPending(t *testing.T) {
	srv := ttsServer(t, func(conn *websocket.Conn, r *http.Request) {
		_, _, _ = conn.ReadMessage() // config
		_, _, _ = conn.ReadMessage() // text
		_, _, _ = conn.ReadMessage() // flush
		// Never sends "final" -- Cancel must not wait for it.
		<-r.Context().Done()
	})

	c, err := NewSarvamClient(context.Background(), "call1", Config{WSURL: wsURL(srv.URL), APIKey: "k", Model: "bulbul:v3", Voice: "shubh"})
	require.NoError(t, err)

	require.NoError(t, c.Speak("interrupted mid-flight", 1))
	require.NoError(t, c.Cancel())

	require.Eventually(t, func() bool {
		select {
		case _, open := <-c.Audio():
			return !open
		default:
			return false
		}
	}, time.Second, 5*time.Millisecond, "Cancel must close Audio() immediately, not wait for a final that never comes")
}
