package freeswitch

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/nitesh/vaani/internal/config"
)

const testUUID = "0f1e2d3c-4b5a-6978-8796-a5b4c3d2e1f0"

// fakeSwitch is a minimal mod_event_socket: auth, +OK to every command, and
// events pushed by the test. It records the api commands it got.
type fakeSwitch struct {
	addr string

	mu   sync.Mutex
	apis []string
	conn net.Conn
}

func newFakeSwitch(t *testing.T) *fakeSwitch {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = ln.Close() })

	fs := &fakeSwitch{addr: ln.Addr().String()}
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		fs.mu.Lock()
		fs.conn = conn
		fs.mu.Unlock()
		fs.send("Content-Type: auth/request\n\n")

		r := bufio.NewReader(conn)
		for {
			line, err := r.ReadString('\n')
			if err != nil {
				return
			}
			_, _ = r.ReadString('\n') // the blank line ending the command
			cmd := strings.TrimSuffix(line, "\n")

			if api, ok := strings.CutPrefix(cmd, "api "); ok {
				fs.mu.Lock()
				fs.apis = append(fs.apis, api)
				fs.mu.Unlock()
				fs.send("Content-Type: api/response\nContent-Length: 4\n\n+OK\n")

				continue
			}
			fs.send("Content-Type: command/reply\nReply-Text: +OK\n\n")
		}
	}()

	return fs
}

func (fs *fakeSwitch) send(s string) {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	_, _ = fs.conn.Write([]byte(s))
}

func (fs *fakeSwitch) hangupEvent(uuid string) {
	body := fmt.Sprintf("Event-Name: CHANNEL_HANGUP_COMPLETE\nUnique-ID: %s\nHangup-Cause: NORMAL_CLEARING\n\n", uuid)
	fs.send(fmt.Sprintf("Content-Length: %d\nContent-Type: text/event-plain\n\n%s", len(body), body))
}

func (fs *fakeSwitch) apiCalls() []string {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	return append([]string(nil), fs.apis...)
}

// start runs a Manager's ESL side against fs and serves earshot on an
// httptest server; it returns the earshot URL.
func start(t *testing.T, fs *fakeSwitch, token string) (*Manager, string) {
	t.Helper()
	m := New(config.Config{AppMode: "loopback", EslAddr: fs.addr, EslPassword: "pw", EarshotAuthToken: token})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { m.eslLoop(ctx); close(done) }()
	t.Cleanup(func() { cancel(); <-done })
	require.Eventually(t, m.Connected, 2*time.Second, 10*time.Millisecond)

	srv := httptest.NewServer(m)
	t.Cleanup(srv.Close)

	return m, "ws" + strings.TrimPrefix(srv.URL, "http") + "/call"
}

func dialCall(t *testing.T, url string, h http.Header) *websocket.Conn {
	t.Helper()
	ws, resp, err := websocket.DefaultDialer.Dial(url, h)
	require.NoError(t, err)
	_ = resp.Body.Close()
	t.Cleanup(func() { _ = ws.Close() })

	return ws
}

func callHeaders(token string) http.Header {
	h := http.Header{"X-Channel-Uuid": {testUUID}, "X-Earshot-Meta": {`{"from":"1001","to":"1002"}`}}
	if token != "" {
		h.Set("Authorization", token)
	}

	return h
}

func waitNoCalls(t *testing.T, m *Manager) {
	t.Helper()
	require.Eventually(t, func() bool {
		m.mu.Lock()
		defer m.mu.Unlock()
		return len(m.calls) == 0
	}, 3*time.Second, 10*time.Millisecond)
}

func TestRejectsBadConnections(t *testing.T) {
	_, url := start(t, newFakeSwitch(t), "secret")

	for _, tc := range []struct {
		name string
		h    http.Header
		code int
	}{
		{"no token", callHeaders(""), http.StatusUnauthorized},
		{"wrong token", callHeaders("nope"), http.StatusUnauthorized},
		{"no channel uuid", http.Header{"Authorization": {"secret"}}, http.StatusBadRequest},
		{"uuid with an ESL command in it", http.Header{"Authorization": {"secret"},
			"X-Channel-Uuid": {testUUID + " NORMAL_CLEARING\n\napi fsctl shutdown"}}, http.StatusBadRequest},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, resp, err := websocket.DefaultDialer.Dial(url, tc.h)
			require.Error(t, err)
			assert.Equal(t, tc.code, resp.StatusCode)
			_ = resp.Body.Close()
		})
	}
}

// The caller hangs up: FreeSWITCH reports it, the call ends, and Vaani
// doesn't try to hang up a channel that's already gone.
func TestCallerHangupEndsCall(t *testing.T) {
	fs := newFakeSwitch(t)
	m, url := start(t, fs, "")
	ws := dialCall(t, url, callHeaders(""))

	_, msg, err := ws.ReadMessage() // media is flowing (the pacer's first frame)
	require.NoError(t, err)
	assert.Len(t, msg, 640)

	fs.hangupEvent(testUUID)
	waitNoCalls(t, m)
	assert.Empty(t, fs.apiCalls(), "no uuid_kill for a channel FreeSWITCH already hung up")
}

// Earshot drops the socket while the channel may still be up: Vaani hangs
// the channel up itself.
func TestSocketCloseHangsUpChannel(t *testing.T) {
	fs := newFakeSwitch(t)
	m, url := start(t, fs, "")
	ws := dialCall(t, url, callHeaders(""))

	_, _, err := ws.ReadMessage()
	require.NoError(t, err)
	_ = ws.Close()

	waitNoCalls(t, m)
	assert.Equal(t, []string{"uuid_kill " + testUUID + " NORMAL_CLEARING"}, fs.apiCalls())
}

// What a real hangup looks like (F1 lab): earshot drops the socket first and
// the hangup event follows. That's a hangup, not ours to kill.
func TestSocketCloseThenHangupEventIsAHangup(t *testing.T) {
	fs := newFakeSwitch(t)
	m, url := start(t, fs, "")
	ws := dialCall(t, url, callHeaders(""))

	_, _, err := ws.ReadMessage()
	require.NoError(t, err)
	_ = ws.Close()
	time.Sleep(100 * time.Millisecond)
	fs.hangupEvent(testUUID)

	waitNoCalls(t, m)
	assert.Empty(t, fs.apiCalls())
}

func TestDuplicateConnectionRejected(t *testing.T) {
	_, url := start(t, newFakeSwitch(t), "")
	ws := dialCall(t, url, callHeaders(""))
	_, _, err := ws.ReadMessage()
	require.NoError(t, err)

	_, resp, err := websocket.DefaultDialer.Dial(url, callHeaders(""))
	require.Error(t, err)
	assert.Equal(t, http.StatusConflict, resp.StatusCode)
	_ = resp.Body.Close()
}
