package esl

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeFS is a scripted mod_event_socket: it does the auth handshake, then
// hands each command line it reads to handle, which writes the replies.
func fakeFS(t *testing.T, password string, handle func(cmd string, w *bytes.Buffer)) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = ln.Close() })

	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		defer func() { _ = conn.Close() }()
		r := bufio.NewReader(conn)
		var w bytes.Buffer
		flush := func() { _, _ = conn.Write(w.Bytes()); w.Reset() }
		w.WriteString("Content-Type: auth/request\n\n")
		flush()
		for {
			cmd, err := readCommand(r)
			if err != nil {
				return
			}
			if strings.HasPrefix(cmd, "auth ") {
				if cmd == "auth "+password {
					w.WriteString("Content-Type: command/reply\nReply-Text: +OK accepted\n\n")
				} else {
					w.WriteString("Content-Type: command/reply\nReply-Text: -ERR invalid\n\n")
				}
				flush()
				continue
			}
			handle(cmd, &w)
			flush()
		}
	}()
	return ln.Addr().String()
}

// readCommand reads one "line\n\n" command.
func readCommand(r *bufio.Reader) (string, error) {
	line, err := r.ReadString('\n')
	if err != nil {
		return "", err
	}
	if _, err := r.ReadString('\n'); err != nil {
		return "", err
	}
	return strings.TrimSuffix(line, "\n"), nil
}

func writeEvent(w *bytes.Buffer, headers string) {
	fmt.Fprintf(w, "Content-Length: %d\nContent-Type: text/event-plain\n\n%s", len(headers), headers)
}

func writeAPI(w *bytes.Buffer, body string) {
	fmt.Fprintf(w, "Content-Type: api/response\nContent-Length: %d\n\n%s", len(body), body)
}

func dial(t *testing.T, addr string) *Client {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	c, err := Dial(ctx, addr, "ClueCon")
	require.NoError(t, err)
	t.Cleanup(func() { _ = c.Close() })
	return c
}

func TestDialRejectsWrongPassword(t *testing.T) {
	addr := fakeFS(t, "secret", func(string, *bytes.Buffer) {})
	_, err := Dial(context.Background(), addr, "ClueCon")
	assert.ErrorContains(t, err, "auth rejected")
}

func TestAPIAndEventsStayInOrder(t *testing.T) {
	addr := fakeFS(t, "ClueCon", func(cmd string, w *bytes.Buffer) {
		switch cmd {
		case "event plain CHANNEL_ANSWER CHANNEL_HANGUP_COMPLETE":
			w.WriteString("Content-Type: command/reply\nReply-Text: +OK event listener enabled plain\n\n")
		case "api uuid_kill abc":
			// events interleaved before the API response, like a real switch
			for i := range 50 {
				writeEvent(w, fmt.Sprintf("Event-Name: CHANNEL_ANSWER\nEvent-Sequence: %d\nUnique-ID: abc\n\n", i))
			}
			writeEvent(w, "Event-Name: CHANNEL_HANGUP_COMPLETE\nEvent-Sequence: 50\nHangup-Cause: NORMAL_CLEARING\n\n")
			writeAPI(w, "+OK\n")
		case "api uuid_kill missing":
			writeAPI(w, "-ERR No such channel!\n")
		}
	})
	c := dial(t, addr)
	ctx := context.Background()

	require.NoError(t, c.Subscribe(ctx, "CHANNEL_ANSWER", "CHANNEL_HANGUP_COMPLETE"))
	out, err := c.API(ctx, "uuid_kill abc")
	require.NoError(t, err)
	assert.Equal(t, "+OK", out)

	_, err = c.API(ctx, "uuid_kill missing")
	assert.ErrorContains(t, err, "uuid_kill: -ERR No such channel!")

	for i := range 51 {
		ev := <-c.Events()
		assert.Equal(t, fmt.Sprint(i), ev.Get("Event-Sequence"))
		if i == 50 {
			assert.Equal(t, "CHANNEL_HANGUP_COMPLETE", ev.Name())
			assert.Equal(t, "NORMAL_CLEARING", ev.Get("Hangup-Cause"))
		}
	}
}

func TestBgAPIAndCustomEventWithBody(t *testing.T) {
	addr := fakeFS(t, "ClueCon", func(cmd string, w *bytes.Buffer) {
		if strings.HasPrefix(cmd, "bgapi ") {
			w.WriteString("Content-Type: command/reply\nReply-Text: +OK Job-UUID: job-1\nJob-UUID: job-1\n\n")
			body := "-ERR USER_BUSY\n"
			writeEvent(w, fmt.Sprintf("Event-Name: BACKGROUND_JOB\nJob-UUID: job-1\nContent-Length: %d\n\n%s", len(body), body))
			writeEvent(w, "Event-Name: CUSTOM\nEvent-Subclass: earshot%3A%3Aconnected\nurl: ws%3A%2F%2Fvaani%3A9095%2Fcall\n\n")
		}
	})
	c := dial(t, addr)

	job, err := c.BgAPI(context.Background(), "originate {origination_uuid=x}user/1002 &park()")
	require.NoError(t, err)
	assert.Equal(t, "job-1", job)

	ev := <-c.Events()
	assert.Equal(t, "BACKGROUND_JOB", ev.Name())
	assert.Equal(t, "job-1", ev.Get("Job-UUID"))
	assert.Equal(t, "-ERR USER_BUSY\n", string(ev.Body))

	ev = <-c.Events()
	assert.Equal(t, "earshot::connected", ev.Name())
	assert.Equal(t, "ws://vaani:9095/call", ev.Get("url"))
}

func TestRejectsLineBreaks(t *testing.T) {
	addr := fakeFS(t, "ClueCon", func(string, *bytes.Buffer) { t.Error("nothing may reach the switch") })
	c := dial(t, addr)

	for _, cmd := range []string{"uuid_transfer abc 1002\n\napi fsctl shutdown", "x\ry"} {
		_, err := c.API(context.Background(), cmd)
		assert.ErrorContains(t, err, "line break")
	}
	assert.NoError(t, c.Err(), "a rejected command must not close the connection")
}

// A reply that misses its deadline would be read as the next command's
// reply, so the connection is closed instead.
func TestTimeoutClosesConnection(t *testing.T) {
	addr := fakeFS(t, "ClueCon", func(string, *bytes.Buffer) {}) // never replies
	c := dial(t, addr)

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	_, err := c.API(ctx, "status")
	assert.ErrorIs(t, err, context.DeadlineExceeded)

	<-c.Done()
	_, err = c.API(context.Background(), "status")
	assert.Error(t, err)
	_, open := <-c.Events()
	assert.False(t, open, "Events closes with the connection")
}

func TestDisconnectNotice(t *testing.T) {
	addr := fakeFS(t, "ClueCon", func(cmd string, w *bytes.Buffer) {
		w.WriteString("Content-Type: text/disconnect-notice\nContent-Length: 9\n\nGoodbye!\n")
	})
	c := dial(t, addr)

	_, err := c.API(context.Background(), "status")
	assert.ErrorContains(t, err, "disconnect-notice")
	assert.ErrorContains(t, c.Err(), "disconnect-notice")
}
