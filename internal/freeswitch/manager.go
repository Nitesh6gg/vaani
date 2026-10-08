// Package freeswitch runs Vaani's calls on FreeSWITCH: mod_earshot opens one
// WebSocket per call to Vaani (media), and one Event Socket connection for the
// whole process carries control (hangup events, uuid_kill). The design and
// every protocol fact it relies on are in docs/FREESWITCH.md.
package freeswitch

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"

	"github.com/nitesh/vaani/internal/callagent"
	"github.com/nitesh/vaani/internal/config"
	"github.com/nitesh/vaani/internal/esl"
	"github.com/nitesh/vaani/internal/media"
	"github.com/nitesh/vaani/internal/metrics"
)

const (
	eslDialTimeout    = 10 * time.Second
	eslCommandTimeout = 5 * time.Second
	eslMaxBackoff     = 30 * time.Second
	// shutdownDrain bounds how long Run waits for calls to end after its ctx
	// is cancelled.
	shutdownDrain = 10 * time.Second
	// hangupEventGrace: how long a closed earshot socket waits for the
	// hangup event before the call is ended as "socket closed".
	hangupEventGrace = time.Second
	// statusInterval: how often each call logs earshot's play buffer (the F1
	// clock-drift question in docs/FREESWITCH.md).
	// ponytail: one ESL command per call per interval; drop it (or make it
	// DEBUG-only) once F1's drift numbers are in.
	statusInterval = 30 * time.Second
)

// Why a call ended (its context's cause).
var (
	errHangup      = errors.New("hangup")
	errWSClosed    = errors.New("earshot closed the media connection")
	errMediaDead   = errors.New("no inbound audio")
	errMaxDuration = errors.New("maximum call duration reached")
	errShutdown    = errors.New("vaani shutting down")
)

// channelUUID is FreeSWITCH's channel UUID format. Checked before the value
// goes into any ESL command or log line.
var channelUUID = regexp.MustCompile(`^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$`)

// Earshot sends no Origin header; a browser can't open this socket anyway
// without the auth token, so the origin check adds nothing.
var upgrader = websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}

// Manager owns the process's ESL connection and every live FreeSWITCH call.
type Manager struct {
	cfg    config.Config
	agents *callagent.Builder
	esl    atomic.Pointer[esl.Client]

	mu      sync.Mutex
	calls   map[string]*call
	closing bool
	wg      sync.WaitGroup
}

type call struct {
	id     string
	cancel context.CancelCauseFunc
	ctx    context.Context
}

// New returns a Manager for cfg (TELEPHONY=freeswitch); agents builds each
// call's agent (APP_MODE=agent) or leaves it a loopback.
func New(cfg config.Config, agents *callagent.Builder) *Manager {
	return &Manager{cfg: cfg, agents: agents, calls: make(map[string]*call)}
}

// Connected reports whether the Event Socket is up, for /healthz.
func (m *Manager) Connected() bool { return m.esl.Load() != nil }

// Run listens for earshot connections and keeps the Event Socket connected
// until ctx is cancelled, then ends every call (hanging each up) and returns.
// It fails at once if EarshotListenAddr can't be bound.
func (m *Manager) Run(ctx context.Context) error {
	ln, err := net.Listen("tcp", m.cfg.EarshotListenAddr)
	if err != nil {
		return fmt.Errorf("freeswitch: listen for earshot on %s: %w", m.cfg.EarshotListenAddr, err)
	}

	m.agents.SetProcessContext(ctx) // before the first call can arrive

	srv := &http.Server{Handler: m, ReadHeaderTimeout: 5 * time.Second}
	go func() {
		if err := srv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.Error("earshot listener stopped", "error", err)
		}
	}()

	// The Event Socket outlives ctx: draining calls still need it to hang up.
	eslCtx, stopESL := context.WithCancel(context.Background())
	eslDone := make(chan struct{})
	go func() {
		defer close(eslDone)
		m.eslLoop(eslCtx)
	}()

	slog.Info("listening for earshot", "addr", m.cfg.EarshotListenAddr, "event", "earshot.listening")

	<-ctx.Done()

	_ = srv.Close() // no new calls; live ones are hijacked and unaffected

	m.mu.Lock()
	m.closing = true
	for _, c := range m.calls {
		c.cancel(errShutdown)
	}
	m.mu.Unlock()

	drained := make(chan struct{})
	go func() {
		m.wg.Wait()
		m.agents.Wait() // the calls' Dograh records
		close(drained)
	}()
	select {
	case <-drained:
	case <-time.After(shutdownDrain):
		slog.Warn("calls still ending at shutdown deadline", "deadline", shutdownDrain)
	}

	stopESL()
	<-eslDone

	return nil
}

// eslLoop keeps one Event Socket connection up, reconnecting with backoff,
// and feeds its events to onEvent.
func (m *Manager) eslLoop(ctx context.Context) {
	backoff := time.Second

	for ctx.Err() == nil {
		c, err := m.connectESL(ctx)
		if err != nil {
			slog.Warn("event socket connect failed; retrying", "addr", m.cfg.EslAddr, "error", err,
				"retry_in", backoff, "event", "esl.connect_failed")

			select {
			case <-time.After(backoff):
			case <-ctx.Done():
				return
			}

			backoff = min(backoff*2, eslMaxBackoff)

			continue
		}

		backoff = time.Second
		m.esl.Store(c)
		slog.Info("connected to FreeSWITCH event socket", "addr", m.cfg.EslAddr, "event", "esl.connected")

		stop := context.AfterFunc(ctx, func() { _ = c.Close() })
		for ev := range c.Events() {
			m.onEvent(ev)
		}
		stop()

		m.esl.Store(nil)

		if ctx.Err() == nil {
			slog.Error("event socket lost; reconnecting (live calls keep running)", "error", c.Err(),
				"event", "esl.disconnected")
		}
	}
}

func (m *Manager) connectESL(ctx context.Context) (*esl.Client, error) {
	dctx, cancel := context.WithTimeout(ctx, eslDialTimeout)
	defer cancel()

	c, err := esl.Dial(dctx, m.cfg.EslAddr, m.cfg.EslPassword)
	if err != nil {
		return nil, err
	}

	// CHANNEL_HANGUP, not _COMPLETE: it fires as the hangup starts
	// (switch_channel_perform_hangup, Hangup-Cause already set), while
	// _COMPLETE waits for the channel's cleanup -- measured 5.4 s after
	// earshot dropped the socket in the F1 lab.
	// earshot::error: earshot couldn't connect to (or was refused by) Vaani.
	if err := c.Subscribe(dctx, "CHANNEL_HANGUP", "CUSTOM", "earshot::error"); err != nil {
		_ = c.Close()
		return nil, err
	}

	return c, nil
}

func (m *Manager) onEvent(ev *esl.Message) {
	id := ev.Get("Unique-ID")

	m.mu.Lock()
	c := m.calls[id]
	m.mu.Unlock()

	switch ev.Name() {
	case "CHANNEL_HANGUP":
		if c != nil {
			c.cancel(fmt.Errorf("%w: %s", errHangup, ev.Get("Hangup-Cause")))
		}
	case "earshot::error":
		// Earshot couldn't connect to Vaani, or Vaani refused it (bad token,
		// a duplicate...): with no reconnect, the dialplan's silence_stream
		// would hold that caller in silence until the call's time cap.
		// Assumes every earshot stream on this switch points at Vaani.
		if c != nil || !channelUUID.MatchString(id) {
			return
		}

		slog.Warn("earshot couldn't reach Vaani; hanging the call up", "call_id", id,
			"reason", ev.Get("reason"), "component", "telephony", "event", "earshot.connect_failed")

		go m.hangup(id) // not on the event loop: it waits for the switch's reply
	}
}

// ServeHTTP accepts one earshot connection (dialplan: "earshot start
// ws://<EARSHOT_LISTEN_ADDR>/call proto=native codec=l16 rate=16000") and
// runs that call until it ends.
func (m *Manager) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/call" {
		http.NotFound(w, r)
		return
	}

	if tok := m.cfg.EarshotAuthToken; tok != "" &&
		subtle.ConstantTimeCompare([]byte(r.Header.Get("Authorization")), []byte(tok)) != 1 {
		slog.Warn("earshot connection rejected: bad or missing auth token", "remote", r.RemoteAddr,
			"event", "earshot.unauthorized")
		http.Error(w, "unauthorized", http.StatusUnauthorized)

		return
	}

	id := r.Header.Get("X-Channel-UUID")
	if !channelUUID.MatchString(id) {
		slog.Warn("earshot connection rejected: no valid X-Channel-UUID", "remote", r.RemoteAddr,
			"event", "earshot.bad_request")
		http.Error(w, "X-Channel-UUID required", http.StatusBadRequest)

		return
	}

	c, err := m.register(id)
	if err != nil {
		slog.Warn("earshot connection rejected", "call_id", id, "error", err, "event", "earshot.rejected")
		http.Error(w, err.Error(), http.StatusConflict)

		return
	}
	defer m.unregister(c)

	ws, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		return // Upgrade already replied with the error
	}

	m.runCall(c, ws, r.Header)
}

func (m *Manager) register(id string) (*call, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.closing {
		return nil, errShutdown
	}

	if _, dup := m.calls[id]; dup {
		return nil, errors.New("call already connected") // earshot reconnecting: see EARSHOT_NO_RECONNECT
	}

	ctx, cancel := context.WithCancelCause(context.Background())
	if d := m.cfg.MaxCallDuration; d > 0 {
		var stop context.CancelFunc
		ctx, stop = context.WithTimeoutCause(ctx, d, errMaxDuration)
		parent := cancel
		cancel = func(cause error) { parent(cause); stop() }
	}

	c := &call{id: id, ctx: ctx, cancel: cancel}
	m.calls[id] = c
	m.wg.Add(1)

	return c, nil
}

func (m *Manager) unregister(c *call) {
	c.cancel(nil)

	m.mu.Lock()
	delete(m.calls, c.id)
	m.mu.Unlock()

	m.wg.Done()
}

// phoneNumberRE is what a caller or called number may look like: it goes into
// Dograh's call history and the agent's prompt ({{caller_number}}), and it
// arrives from the dialplan, i.e. from the caller's SIP headers.
var phoneNumberRE = regexp.MustCompile(`^\+?[0-9*#]{1,32}$`)

// phoneNumber returns n if it looks like a phone number, else "".
func phoneNumber(n string) string {
	if phoneNumberRE.MatchString(n) {
		return n
	}

	return ""
}

// earshotMeta is the dialplan's EARSHOT_META, which earshot forwards as the
// X-Earshot-Meta header: {"from":"${caller_id_number}","to":"${destination_number}"}.
type earshotMeta struct {
	From string `json:"from"`
	To   string `json:"to"`
}

func (m *Manager) runCall(c *call, ws *websocket.Conn, h http.Header) {
	config.RegisterCallTrace(c.id, config.NewTraceID())
	defer config.UnregisterCallTrace(c.id)

	var meta earshotMeta
	_ = json.Unmarshal([]byte(h.Get("X-Earshot-Meta")), &meta)
	from, to := phoneNumber(meta.From), phoneNumber(meta.To)

	slog.Info("call started", "call_id", c.id, "sip_call_id", h.Get("X-Call-ID"),
		"caller_number", from, "called_number", to, "app_mode", m.cfg.AppMode,
		"component", "telephony", "event", "call.started")

	// The agent (APP_MODE=agent) runs on the call's context: it stops when
	// the call ends. nil = loopback. Transfer is F3 (docs/FREESWITCH.md):
	// until then the agent tells the caller it isn't available.
	// earshot is set just below, before the media (and so the agent) runs.
	var earshot atomic.Pointer[media.Earshot]

	handler := m.agents.Handler(c.ctx, callagent.Call{ID: c.id, CallerNumber: from, CalledNumber: to},
		callagent.Hooks{
			Hangup: func() { m.hangup(c.id) },
			// The caller started talking over the agent: drop the reply audio
			// earshot still has queued, so the agent goes quiet at once. A
			// false interruption replays from the sentence's start anyway, so
			// nothing is lost. Off the agent's goroutine: it's a network write.
			Interrupted: func() {
				if e := earshot.Load(); e != nil {
					go func() {
						if err := e.Clear(); err != nil {
							slog.Debug("earshot clear failed", "call_id", c.id, "error", err)
						}
					}()
				}
			},
		})

	asm, es := media.NewEarshotCallMedia(c.id, ws, metrics.AudioSocketSink{}, media.AudioSocketConfig{
		Handler:          handler,
		RecordDir:        m.cfg.RecordDir,
		ToWire:           media.LittleEndian, // earshot's L16 is host order: LE on x86-64/ARM64
		DebugAudio:       m.cfg.DebugAudio,
		MediaDeadTimeout: m.cfg.MediaDeadTimeout,
	})
	earshot.Store(es)

	go func() {
		select {
		case <-asm.ReadDone():
			// On hangup earshot drops the socket (close code 1006) about when
			// CHANNEL_HANGUP reaches us: give the event a moment, so a hangup
			// is reported as one and the gone channel isn't killed again.
			select {
			case <-c.ctx.Done():
			case <-time.After(hangupEventGrace):
				c.cancel(errWSClosed)
			}
		case <-asm.Dead():
			c.cancel(errMediaDead)
		case <-c.ctx.Done():
		}
	}()

	go m.logStatus(c)

	started := time.Now()
	asm.Run(c.ctx)

	reason := context.Cause(c.ctx)
	if !errors.Is(reason, errHangup) {
		m.hangup(c.id) // the channel may still be up: ours to end
	}

	st := es.Stats()
	slog.Info("call ended", "call_id", c.id, "reason", reason.Error(),
		"duration_ms", time.Since(started).Milliseconds(),
		"earshot_messages", st.Messages, "earshot_odd_size_messages", st.OddMessages,
		"earshot_bytes", st.Bytes, "earshot_text_messages", st.TextMessages, "earshot_close_code", st.CloseCode,
		"component", "telephony", "event", "call.ended")
}

// hangup ends the FreeSWITCH channel. A channel that's already gone is fine.
func (m *Manager) hangup(id string) {
	out, err := m.api("uuid_kill " + id + " NORMAL_CLEARING")
	switch {
	case err == nil:
		slog.Debug("hung up channel", "call_id", id, "reply", out)
	case strings.Contains(err.Error(), "No such channel"):
		slog.Debug("channel already gone", "call_id", id)
	default:
		slog.Warn("could not hang up channel", "call_id", id, "error", err, "event", "call.hangup_failed")
	}
}

// logStatus logs earshot's play buffer every statusInterval: if Vaani's 20ms
// clock runs faster than FreeSWITCH's, play_buffered grows over the call.
func (m *Manager) logStatus(c *call) {
	t := time.NewTicker(statusInterval)
	defer t.Stop()

	for {
		select {
		case <-c.ctx.Done():
			return
		case <-t.C:
		}

		out, err := m.api("earshot " + c.id + " status")
		if err != nil {
			slog.Debug("earshot status failed", "call_id", c.id, "error", err)
			continue
		}

		var st struct {
			PlayBuffered int64 `json:"play_buffered"`
			RxFrames     int64 `json:"rx_frames"`
			WSConnected  int   `json:"ws_connected"`
		}
		if err := json.Unmarshal([]byte(out), &st); err != nil {
			slog.Debug("earshot status unreadable", "call_id", c.id, "status", out)
			continue
		}

		slog.Info("earshot status", "call_id", c.id, "play_buffered_bytes", st.PlayBuffered,
			"rx_frames", st.RxFrames, "ws_connected", st.WSConnected, "event", "earshot.status")
	}
}

func (m *Manager) api(cmd string) (string, error) {
	c := m.esl.Load()
	if c == nil {
		return "", errors.New("event socket not connected")
	}

	ctx, cancel := context.WithTimeout(context.Background(), eslCommandTimeout)
	defer cancel()

	return c.API(ctx, cmd)
}
