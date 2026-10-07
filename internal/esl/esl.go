// Package esl is a minimal FreeSWITCH Event Socket client (inbound mode: we
// connect to mod_event_socket, one connection for the whole process).
//
// Wire format (FreeSWITCH v1.10.12 mod_event_socket.c): each message is
// "Name: value\n" headers, a blank line, then Content-Length bytes of body
// when that header is present. Commands are one line ending in "\n\n".
//
// Two guarantees third-party clients don't give (see docs/FREESWITCH.md):
//   - Events are delivered in the order FreeSWITCH sent them, on one channel.
//   - Commands run one at a time and each gets its own reply. A command whose
//     context expires before its reply closes the connection: a late reply
//     would otherwise be taken as the next command's.
package esl

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/textproto"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Message is one ESL message: a reply, an API response or an event.
type Message struct {
	Header textproto.MIMEHeader
	Body   []byte
}

// Get returns header name, URL-decoded (event headers are URL-encoded).
func (m *Message) Get(name string) string {
	v := m.Header.Get(name)
	if d, err := url.PathUnescape(v); err == nil {
		return d
	}
	return v
}

// Name is the event's name; for CUSTOM events, its subclass
// (e.g. "earshot::connected").
func (m *Message) Name() string {
	if n := m.Get("Event-Name"); n != "CUSTOM" {
		return n
	}
	return m.Get("Event-Subclass")
}

// ErrClosed is returned once the connection is gone.
var ErrClosed = errors.New("esl: connection closed")

// Client is one authenticated ESL connection. Safe for concurrent use.
type Client struct {
	conn net.Conn

	cmdMu   sync.Mutex // one command in flight at a time
	replies chan *Message

	events chan *Message
	qMu    sync.Mutex
	queue  []*Message // unbounded, so the reader never blocks on a slow consumer
	qReady chan struct{}

	done    chan struct{}
	errOnce sync.Once
	err     error
}

// Dial connects to addr and authenticates with password. ctx bounds the
// connect and the auth handshake only.
func Dial(ctx context.Context, addr, password string) (*Client, error) {
	var d net.Dialer
	conn, err := d.DialContext(ctx, "tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("esl: dial %s: %w", addr, err)
	}
	if dl, ok := ctx.Deadline(); ok {
		_ = conn.SetDeadline(dl)
	}

	r := textproto.NewReader(bufio.NewReader(conn))
	if err := auth(r, conn, password); err != nil {
		_ = conn.Close()
		return nil, err
	}
	_ = conn.SetDeadline(time.Time{})

	c := &Client{
		conn:    conn,
		replies: make(chan *Message, 1),
		events:  make(chan *Message),
		qReady:  make(chan struct{}, 1),
		done:    make(chan struct{}),
	}
	go c.readLoop(r)
	go c.pumpEvents()
	return c, nil
}

func auth(r *textproto.Reader, w io.Writer, password string) error {
	m, err := readMessage(r)
	if err != nil {
		return fmt.Errorf("esl: waiting for auth request: %w", err)
	}
	if ct := m.Header.Get("Content-Type"); ct != "auth/request" {
		return fmt.Errorf("esl: expected auth/request, got %q", ct)
	}
	if err := checkLine(password); err != nil {
		return err
	}
	if _, err := io.WriteString(w, "auth "+password+"\n\n"); err != nil {
		return fmt.Errorf("esl: sending auth: %w", err)
	}
	m, err = readMessage(r)
	if err != nil {
		return fmt.Errorf("esl: reading auth reply: %w", err)
	}
	if reply := m.Get("Reply-Text"); !strings.HasPrefix(reply, "+OK") {
		return fmt.Errorf("esl: auth rejected: %s", reply)
	}
	return nil
}

func readMessage(r *textproto.Reader) (*Message, error) {
	h, err := r.ReadMIMEHeader()
	if err != nil {
		return nil, err
	}
	m := &Message{Header: h}
	if cl := h.Get("Content-Length"); cl != "" {
		n, err := strconv.Atoi(cl)
		if err != nil || n < 0 {
			return nil, fmt.Errorf("esl: bad Content-Length %q", cl)
		}
		m.Body = make([]byte, n)
		if _, err := io.ReadFull(r.R, m.Body); err != nil {
			return nil, err
		}
	}
	return m, nil
}

func (c *Client) readLoop(r *textproto.Reader) {
	for {
		m, err := readMessage(r)
		if err != nil {
			c.fail(err)
			return
		}
		switch ct := m.Header.Get("Content-Type"); ct {
		case "command/reply", "api/response":
			select {
			case c.replies <- m:
			default: // nobody asked: protocol out of step, stop trusting it
				c.fail(fmt.Errorf("esl: unexpected %s", ct))
				return
			}
		case "text/event-plain":
			ev, err := readMessage(textproto.NewReader(bufio.NewReader(bytes.NewReader(m.Body))))
			if err != nil {
				c.fail(fmt.Errorf("esl: bad event: %w", err))
				return
			}
			c.qMu.Lock()
			c.queue = append(c.queue, ev)
			c.qMu.Unlock()
			select {
			case c.qReady <- struct{}{}:
			default:
			}
		case "text/disconnect-notice":
			c.fail(errors.New("esl: freeswitch sent disconnect-notice"))
			return
		}
		// Anything else (log/data, text/rude-rejection...) is ignored.
	}
}

func (c *Client) pumpEvents() {
	defer close(c.events)
	for {
		c.qMu.Lock()
		q := c.queue
		c.queue = nil
		c.qMu.Unlock()
		for _, ev := range q {
			select {
			case c.events <- ev:
			case <-c.done:
				return
			}
		}
		select {
		case <-c.qReady:
		case <-c.done:
			return
		}
	}
}

func (c *Client) fail(err error) {
	c.errOnce.Do(func() {
		c.err = err
		close(c.done)
		_ = c.conn.Close()
	})
}

// Events delivers subscribed events in FreeSWITCH's order. Closed once the
// connection is gone.
func (c *Client) Events() <-chan *Message { return c.events }

// Done is closed when the connection is gone; Err then says why.
func (c *Client) Done() <-chan struct{} { return c.done }

// Err is why the connection closed (nil while it's open).
func (c *Client) Err() error {
	select {
	case <-c.done:
		return c.err
	default:
		return nil
	}
}

// Close closes the connection.
func (c *Client) Close() error {
	c.fail(ErrClosed)
	return nil
}

// checkLine rejects CR/LF: a newline inside an argument would end the
// command early and run the rest as a second ESL command.
func checkLine(s string) error {
	if strings.ContainsAny(s, "\r\n") {
		return errors.New("esl: command contains a line break")
	}
	return nil
}

// Send runs one raw command line and returns its reply.
func (c *Client) Send(ctx context.Context, cmd string) (*Message, error) {
	if err := checkLine(cmd); err != nil {
		return nil, err
	}
	c.cmdMu.Lock()
	defer c.cmdMu.Unlock()

	if err := c.Err(); err != nil {
		return nil, err
	}
	if dl, ok := ctx.Deadline(); ok {
		_ = c.conn.SetWriteDeadline(dl)
		defer func() { _ = c.conn.SetWriteDeadline(time.Time{}) }()
	}
	if _, err := io.WriteString(c.conn, cmd+"\n\n"); err != nil {
		c.fail(err)
		return nil, fmt.Errorf("esl: send: %w", err)
	}

	select {
	case m := <-c.replies:
		return m, nil
	case <-c.done:
		return nil, c.err
	case <-ctx.Done():
		c.fail(fmt.Errorf("esl: no reply to %q: %w", verb(cmd), ctx.Err()))
		return nil, ctx.Err()
	}
}

// API runs "api <cmd>" and returns its output; output starting with "-ERR"
// (or "-USAGE") is returned as an error.
func (c *Client) API(ctx context.Context, cmd string) (string, error) {
	m, err := c.Send(ctx, "api "+cmd)
	if err != nil {
		return "", err
	}
	out := strings.TrimSpace(string(m.Body))
	if strings.HasPrefix(out, "-") {
		return "", fmt.Errorf("esl: %s: %s", verb(cmd), out)
	}
	return out, nil
}

// BgAPI runs "bgapi <cmd>" and returns its Job-UUID; the result arrives
// later as a BACKGROUND_JOB event with that Job-UUID (subscribe to it).
func (c *Client) BgAPI(ctx context.Context, cmd string) (string, error) {
	m, err := c.command(ctx, "bgapi "+cmd)
	if err != nil {
		return "", err
	}
	return m.Get("Job-UUID"), nil
}

// Subscribe adds events, e.g. "CHANNEL_ANSWER", "CUSTOM", "earshot::connected"
// (FreeSWITCH syntax: "event plain CHANNEL_ANSWER CUSTOM earshot::connected").
func (c *Client) Subscribe(ctx context.Context, events ...string) error {
	_, err := c.command(ctx, "event plain "+strings.Join(events, " "))
	return err
}

// command sends cmd and fails unless the reply is "+OK".
func (c *Client) command(ctx context.Context, cmd string) (*Message, error) {
	m, err := c.Send(ctx, cmd)
	if err != nil {
		return nil, err
	}
	if reply := m.Get("Reply-Text"); !strings.HasPrefix(reply, "+OK") {
		return nil, fmt.Errorf("esl: %s: %s", verb(cmd), reply)
	}
	return m, nil
}

// verb is cmd's command name ("api uuid_kill <uuid>" -> "uuid_kill") for
// errors; the arguments can hold phone numbers, so they're left out.
func verb(cmd string) string {
	cmd = strings.TrimPrefix(strings.TrimPrefix(cmd, "api "), "bgapi ")
	if i := strings.IndexByte(cmd, ' '); i >= 0 {
		return cmd[:i]
	}
	return cmd
}
