package ari

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"sync"
	"time"

	"github.com/CyCoreSystems/ari/v5"
	"github.com/CyCoreSystems/ari/v5/rid"

	"github.com/nitesh/vaani/internal/callagent"
	"github.com/nitesh/vaani/internal/config"
	"github.com/nitesh/vaani/internal/dograh"
	"github.com/nitesh/vaani/internal/media"
	"github.com/nitesh/vaani/internal/metrics"
	"github.com/nitesh/vaani/internal/session"
)

// audioSocketAcceptTimeout bounds how long completeBridge waits for Asterisk to
// actually open the AudioSocket TCP connection after the channel is bridged.
const audioSocketAcceptTimeout = 10 * time.Second

// connectivityPollInterval is how often watchConnectivity samples the ARI
// client's WebSocket state. Same 1s cadence as the client's own reconnect
// watcher; it only needs to be fast relative to how long a zombie would
// otherwise survive.
const connectivityPollInterval = time.Second

// call tracks the ARI/media-specific state of one bridged caller<->externalMedia
// pair; *session.Call carries the lifecycle state and the once-guaranteed
// teardown that used to be prone to double- or zero-firing in Phase 1.
type call struct {
	*session.Call

	// Exactly one of these is set, depending on Manager.cfg.MediaEncapsulation,
	// bound/listening before Asterisk is ever told the address -- see startCall.
	udpConn  *net.UDPConn
	listener net.Listener

	bridge *ari.BridgeHandle
	cm     *media.CallMedia            // set once the RTP media plane starts
	asm    *media.AudioSocketCallMedia // set once the AudioSocket media plane starts

	// mediaCtx scopes the media plane and the agent (a child of Call.Ctx,
	// created in completeBridge). A transfer cancels it to take the agent out
	// of a call that carries on without it; teardown cancels it via Call.Ctx.
	mediaCtx    context.Context
	mediaCancel context.CancelFunc

	// transferChannel is the transfer destination's channel once a transfer
	// has connected (written under Manager.mu): from then on the call is
	// caller <-> destination, and teardown hangs that channel up too.
	transferChannel string

	// maxDur, when MAX_CALL_DURATION_SECONDS is set, hangs the call up when the
	// cap is reached however healthy it looks -- a backstop against a wedged
	// call burning billable telephony time forever. Stopped inside teardown.
	maxDur *time.Timer

	// Synchronization: cm and bridge are written on the Stasis event loop
	// before any other goroutine can discover this call. asm is written by the
	// AudioSocket accept goroutine, so both it and any reader (teardown, which
	// can run on the event loop, at shutdown, or on that goroutine itself) must
	// go through Manager.mu.
}

// closeMediaSocket closes whichever of the call's media sockets is set. Safe to
// call more than once and regardless of which encapsulation was used.
func closeMediaSocket(c *call) {
	if c.udpConn != nil {
		_ = c.udpConn.Close()
	}

	if c.listener != nil {
		_ = c.listener.Close()
	}
}

// hangupChannel hangs a channel up, logging a failure instead of swallowing it.
// A hangup error is routine when the far end beat us to it (Asterisk answers
// with a 404 for an already-gone channel -- the normal case on teardown, since
// the StasisEnd that triggered it usually means one channel just hung up), so
// it's Debug-level: visible when hunting a leak of live channels, silent in
// normal operation.
func hangupChannel(cl ari.Client, callID, channelID string) {
	if err := cl.Channel().Hangup(ari.NewKey(ari.ChannelKey, channelID), ""); err != nil {
		slog.Debug("hangup failed", "call_id", callID, "channel_id", channelID, "error", err)
	}
}

// deleteBridge deletes a mixing bridge, warning on failure: unlike a failed
// hangup, a bridge that survives its teardown holds Asterisk-side resources
// with no event that would ever clean it up.
func deleteBridge(callID string, bh *ari.BridgeHandle) {
	if err := bh.Delete(); err != nil {
		slog.Warn("failed to delete bridge", "call_id", callID, "error", err)
	}
}

// Manager owns the Stasis event loop: it answers incoming calls, wires each one to
// an externalMedia channel and a bridge, runs the media plane for its duration,
// and tears everything down on hangup.
type Manager struct {
	cl    ari.Client
	cfg   config.Config
	ports *media.PortAllocator
	// agents builds each call's agent and records it in Dograh.
	agents *callagent.Builder

	mu        sync.Mutex
	pending   map[string]*call            // keyed by the externalMedia channel ID, awaiting its StasisStart
	active    map[string]*call            // keyed by callerID and externalID once bridged (plus transferChannel after a transfer)
	transfers map[string]*pendingTransfer // keyed by the ringing transfer destination's channel ID
}

// NewManager creates a call Manager bound to cl. store and storage may be nil.
func NewManager(cl ari.Client, cfg config.Config, ports *media.PortAllocator, store *dograh.Store, storage *dograh.Storage) *Manager {
	m := &Manager{
		cl:        cl,
		cfg:       cfg,
		ports:     ports,
		agents:    callagent.New(cfg, store, storage),
		pending:   make(map[string]*call),
		active:    make(map[string]*call),
		transfers: make(map[string]*pendingTransfer),
	}

	return m
}

// Run subscribes to the Stasis events this app cares about and dispatches them
// until ctx is cancelled.
func (m *Manager) Run(ctx context.Context) {
	// Set before any call exists (calls only start from this loop's events),
	// so every call's Dograh run sees it.
	m.agents.SetProcessContext(ctx)

	sub := m.cl.Bus().Subscribe(nil, "StasisStart", "StasisEnd", "ChannelDtmfReceived", "ChannelDestroyed")
	defer sub.Cancel()

	go m.watchConnectivity(ctx)

	for {
		select {
		case <-ctx.Done():
			m.teardownAll()
			m.agents.Wait() // the calls' Dograh records (bounded by finishRun's timeout)

			return
		case evt, ok := <-sub.Events():
			if !ok {
				return
			}

			switch e := evt.(type) {
			case *ari.StasisStart:
				m.onStasisStart(ctx, e)
			case *ari.StasisEnd:
				m.onStasisEnd(e)
			case *ari.ChannelDtmfReceived:
				slog.Debug("dtmf received", "call_id", e.Channel.ID, "digit", e.Digit)
			case *ari.ChannelDestroyed:
				// StasisEnd alone doesn't say WHY a channel left.
				// ChannelDestroyed carries the hangup cause, which
				// distinguishes a normal caller hangup (16, "Normal
				// Clearing") from carrier drops, busy/congestion, and
				// Asterisk-initiated kicks (e.g. its AudioSocket read
				// timeout) -- without it, every teardown looks identical.
				// channel_id is whichever leg this is; call_id the call it
				// belongs to (when it's still known), so the line groups with
				// the call's others.
				slog.Info("channel destroyed", "call_id", m.callIDFor(e.Channel.ID), "channel_id", e.Channel.ID,
					"cause", e.Cause, "cause_txt", e.CauseTxt, "event", "channel.destroyed")
				m.onTransferLegDestroyed(e)
			}
		}
	}
}

// watchConnectivity polls the ARI client's WebSocket state and tears down every
// call when it reconnects after a drop. Events that fire while the WebSocket is
// down -- StasisEnd above all -- are lost, so any call spanning the gap may
// have ended without Vaani ever finding out: a billable Asterisk channel and a
// permanently leaked media port. Tearing everything down on reconnect is the
// conservative recovery (a brief blip sacrifices healthy calls too); Phase 3's
// agent mode can revisit this with a proper re-sync if it becomes a problem.
func (m *Manager) watchConnectivity(ctx context.Context) {
	ticker := time.NewTicker(connectivityPollInterval)
	defer ticker.Stop()

	up := m.cl.Connected()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			now := m.cl.Connected()
			if now && !up && m.hasCalls() {
				slog.Warn("ari websocket reconnected after a drop; tearing down all calls (events during the gap, including StasisEnd, were lost)")
				m.teardownAll()
			}

			up = now
		}
	}
}

// callIDFor is the call a channel belongs to (caller, externalMedia or
// transfer leg), or the channel id itself once the call is no longer
// tracked -- a caller channel's id is its call's id anyway.
func (m *Manager) callIDFor(channelID string) string {
	m.mu.Lock()
	defer m.mu.Unlock()

	if c := m.active[channelID]; c != nil {
		return c.ID
	}

	if c := m.pending[channelID]; c != nil {
		return c.ID
	}

	return channelID
}

// hasCalls reports whether any call is currently tracked (bridged or staged).
func (m *Manager) hasCalls() bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	return len(m.active) > 0 || len(m.pending) > 0
}

func (m *Manager) onStasisStart(ctx context.Context, e *ari.StasisStart) {
	id := e.Channel.ID

	// A transfer destination we dialed has answered (see transfer.go). It
	// enters this same Stasis app, so it must not be mistaken for a new
	// inbound call.
	if len(e.Args) > 0 && e.Args[0] == transferAppArg {
		m.onTransferLegAnswered(id)
		return
	}

	m.mu.Lock()
	pc, found := m.pending[id]
	// pending is keyed by both IDs (so a hangup during setup can find the call by
	// either), so confirm this really is the externalMedia leg arriving.
	isExternal := found && pc.ExternalID == id

	if isExternal {
		delete(m.pending, pc.ID)
		delete(m.pending, pc.ExternalID)
	}
	m.mu.Unlock()

	if isExternal {
		m.completeBridge(pc)
		return
	}

	m.startCall(ctx, e)
}

// startCall answers a newly-arrived caller channel and stages its externalMedia
// counterpart. The bridge is completed once that channel's own StasisStart arrives
// (see completeBridge).
func (m *Manager) startCall(ctx context.Context, e *ari.StasisStart) {
	id := e.Channel.ID
	channel := m.cl.Channel().Get(e.Key(ari.ChannelKey, id))

	if err := channel.Answer(); err != nil {
		slog.Error("failed to answer channel", "call_id", id, "event", "call.setup_failed", "step", "answer", "error", err)
		// A channel we failed to answer is still live in Stasis; leaving it
		// without hanging it up would strand it in the dialplan.
		hangupChannel(m.cl, id, id)

		return
	}

	port, err := m.ports.Alloc()
	if err != nil {
		slog.Error("failed to allocate media port", "call_id", id, "event", "call.setup_failed", "step", "port", "error", err)
		_ = channel.Hangup()

		return
	}

	metrics.MediaPortsInUse.Set(float64(m.ports.InUse()))

	// Bind/listen on the media socket BEFORE telling Asterisk where to send, so
	// it's already ready the instant Asterisk's first packet or connection
	// attempt can possibly arrive. Binding later (once the bridge is up, several
	// ARI round-trips away) leaves a window where audio hits nothing listening:
	// for UDP/RTP that triggers an ICMP port-unreachable which can kill the flow
	// entirely across a routed/firewalled path (a real production incident --
	// see docs/AUDIO_PIPELINE.md); for TCP/AudioSocket, Asterisk's connection
	// attempt would simply be refused outright.
	var (
		udpConn   *net.UDPConn
		listener  net.Listener
		encap     string
		transport string
	)

	switch m.cfg.MediaEncapsulation {
	case "audiosocket":
		encap, transport = "audiosocket", "tcp"
		listener, err = net.Listen("tcp", fmt.Sprintf(":%d", port))
	default:
		encap, transport = "rtp", "udp"
		udpConn, err = net.ListenUDP("udp", &net.UDPAddr{Port: port})
	}

	if err != nil {
		slog.Error("failed to bind media socket", "call_id", id, "event", "call.setup_failed", "step", "bind", "port", port, "encapsulation", encap, "error", err)

		m.ports.Free(port)
		metrics.MediaPortsInUse.Set(float64(m.ports.InUse()))
		_ = channel.Hangup()

		return
	}

	externalID := rid.New(rid.Channel)
	info := session.Info{
		CallerNumber: callerNumber(e.Channel),
		CalledNumber: calledNumber(e.Channel),
		// Every channel that reaches this Stasis app got here by someone dialing
		// in -- there's no outbound-origination code path yet, so this is a
		// constant rather than derived from channel/endpoint naming that would
		// vary per deployment's trunk conventions.
		Direction: "inbound",
	}
	c := &call{Call: session.New(id, externalID, port, info, ctx), udpConn: udpConn, listener: listener}

	// Every log line carrying call_id=id is now stamped with this call's
	// W3C trace_id by the logging handler (see internal/config/logging.go);
	// unregistered at teardown so the registry stays bounded by live calls.
	config.RegisterCallTrace(id, config.NewTraceID())

	m.mu.Lock()
	m.pending[externalID] = c
	m.pending[id] = c // so a hangup during setup can still find and clean up this call
	m.mu.Unlock()

	// Armed after the pending insert so the cap timer can never fire on a call
	// that isn't yet discoverable for teardown. The timer survives into the
	// active map (c is the same object); teardown stops it.
	if m.cfg.MaxCallDuration > 0 {
		c.maxDur = time.AfterFunc(m.cfg.MaxCallDuration, func() {
			slog.Warn("max call duration reached; hanging up", "call_id", id, "event", "call.max_duration_backstop",
				"external_id", externalID, "max_seconds", int(m.cfg.MaxCallDuration.Seconds()))
			m.teardown(c)
		})
	}

	c.SetState(session.StateStaged)

	externalHost := fmt.Sprintf("%s:%d", m.cfg.MediaIP, port)

	if encap == "audiosocket" {
		// The vendored ARI library's ExternalMediaOptions has no "data" field,
		// but Asterisk's chan_audiosocket rejects the request without one -- so
		// this one case makes the raw REST call itself. See externalmedia.go.
		var uuid string

		uuid, err = newUUIDv4()
		if err == nil {
			err = createAudioSocketExternalMedia(m.cfg, externalID, m.cfg.AriApp, externalHost, uuid)
		}
	} else {
		_, err = m.cl.Channel().ExternalMedia(e.Key(ari.ChannelKey, externalID), ari.ExternalMediaOptions{
			ChannelID:     externalID,
			App:           m.cfg.AriApp,
			ExternalHost:  externalHost,
			Encapsulation: encap,
			Transport:     transport,
			Format:        "slin16",
		})
	}

	if err != nil {
		slog.Error("failed to create externalMedia channel", "call_id", id, "event", "call.setup_failed", "step", "external_media", "error", err)

		m.mu.Lock()
		delete(m.pending, externalID)
		delete(m.pending, id)
		m.mu.Unlock()

		closeMediaSocket(c)
		m.ports.Free(port)
		metrics.MediaPortsInUse.Set(float64(m.ports.InUse()))
		_ = channel.Hangup()

		return
	}

	slog.Info("call answered, media socket listening, externalMedia staged",
		"call_id", id, "external_id", externalID, "port", port, "encapsulation", encap,
		"caller_number", info.CallerNumber, "called_number", info.CalledNumber, "direction", info.Direction,
		"event", "call.answered")
}

// completeBridge runs once the externalMedia channel itself enters Stasis: it
// creates the bridge, adds both channels, and starts the media plane appropriate
// to the configured encapsulation.
func (m *Manager) completeBridge(c *call) {
	bridgeKey := ari.NewKey(ari.BridgeKey, rid.New(rid.Bridge))

	bh, err := m.cl.Bridge().Create(bridgeKey, "mixing", bridgeKey.ID)
	if err != nil {
		slog.Error("failed to create bridge", "call_id", c.ID, "event", "call.setup_failed", "step", "bridge", "error", err)
		m.abort(c)

		return
	}

	if err := bh.AddChannel(c.ID); err != nil {
		slog.Error("failed to add caller to bridge", "call_id", c.ID, "event", "call.setup_failed", "step", "bridge_caller", "error", err)
		deleteBridge(c.ID, bh)
		m.abort(c)

		return
	}

	if err := bh.AddChannel(c.ExternalID); err != nil {
		slog.Error("failed to add externalMedia channel to bridge", "call_id", c.ID, "event", "call.setup_failed", "step", "bridge_media", "error", err)
		deleteBridge(c.ID, bh)
		m.abort(c)

		return
	}

	c.bridge = bh
	c.mediaCtx, c.mediaCancel = context.WithCancel(c.Ctx)
	c.SetState(session.StateBridged)

	// Added to active (and CallsActive incremented) here, before the media plane
	// itself has necessarily started -- AudioSocket's Accept() below can take a
	// moment, and a hangup during that window must still be found by
	// onStasisEnd and cleanly torn down (which is also why CallsActive.Dec()
	// always lives in teardown(), paired with this Inc(), rather than with
	// "media actually running").
	m.mu.Lock()
	m.active[c.ID] = c
	m.active[c.ExternalID] = c
	m.mu.Unlock()

	metrics.CallsActive.Inc()
	slog.Info("call bridged", "event", "call.bridged",
		"call_id", c.ID, "external_id", c.ExternalID, "port", c.Port,
		"caller_number", c.Info.CallerNumber, "called_number", c.Info.CalledNumber, "direction", c.Info.Direction)

	// Both off the Stasis event loop: building an agent call's handler reads
	// Dograh's database and dials the STT (seconds, worst case), and every
	// other call's events wait on this loop meanwhile.
	if m.cfg.MediaEncapsulation == "audiosocket" {
		go m.startAudioSocketMedia(c)
		return
	}

	go m.startRTPMedia(c)
}

// mediaHandler builds the Handler for a new call from config: SilentHandler if
// TEST_SILENT_HANDLER=1 (a debug hook -- never set in production, since it
// means the call carries no audio at all); agent.Handler if AppMode is
// "agent"; otherwise nil (CallMedia/AudioSocketCallMedia both default that to
// LoopbackHandler, which is what AppMode "loopback" relies on for
// telephony-only testing, e.g. SIPp load tests, with no AI dependency at all).
// The agent runs on c.mediaCtx: its background goroutines and STT/TTS
// connections all stop when that's cancelled -- at hangup, or when a
// transfer hands the caller over -- so there is nothing further to close here.
func (m *Manager) mediaHandler(c *call) media.Handler {
	return m.agents.Handler(c.mediaCtx, callagent.Call{
		ID: c.ID, CallerNumber: c.Info.CallerNumber, CalledNumber: c.Info.CalledNumber,
	}, callagent.Hooks{
		// Hanging up the caller's channel fires StasisEnd, which runs the
		// normal teardown (media plane, bridge, externalMedia channel).
		Hangup: func() { hangupChannel(m.cl, c.ID, c.ID) },
		Transfer: func(ctx context.Context, destination string, timeout time.Duration) (func() error, error) {
			return m.transfer(ctx, c, destination, timeout)
		},
	})
}

// startRTPMedia constructs and runs the RTP media plane for an already-bridged
// call, on its own goroutine (see completeBridge). Inbound RTP arriving
// meanwhile waits in the already-bound socket's buffer.
func (m *Manager) startRTPMedia(c *call) {
	fromWire, err := media.ParseEndianness(m.cfg.AudioL16Endianness)
	if err != nil {
		// Already validated at config.Load() time; unreachable in practice.
		slog.Error("invalid AUDIO_L16_ENDIANNESS", "call_id", c.ID, "error", err)
	}

	// Built before taking m.mu: it can take seconds (see mediaHandler), and
	// every other call's events need m.mu.
	handler := m.mediaHandler(c)

	// Under m.mu, like c.asm: teardown snapshots c.cm under it. A call torn
	// down meanwhile (the caller hung up) gets no media plane; its handler
	// already stopped with the call's context.
	m.mu.Lock()
	if c.State() == session.StateTornDown {
		m.mu.Unlock()
		return
	}

	c.cm = media.NewCallMedia(c.ID, c.udpConn, metrics.Sink{}, media.Config{
		JitterBufferPackets: m.cfg.JitterBufferPackets,
		FromWire:            fromWire,
		// The verified inbound order is also the outbound order: Asterisk's
		// externalMedia RTP L16 is symmetric, so a "be" deployment must swap
		// handler output (LE by contract) back to BE on the way out too, or
		// the caller hears byte-swapped noise while the WAV sounds fine.
		ToWire:           fromWire,
		RecordDir:        m.cfg.RecordDir,
		Handler:          handler,
		DebugAudio:       m.cfg.DebugAudio,
		MediaDeadTimeout: m.cfg.MediaDeadTimeout,
	})
	cm := c.cm
	m.mu.Unlock()

	c.SetState(session.StateMediaActive)
	slog.Info("rtp media plane running", "call_id", c.ID, "component", "media", "event", "media.started", "port", c.Port)

	go m.watchMediaDead(c, cm.Dead())
	cm.Run(c.mediaCtx) // already on its own goroutine
}

// watchMediaDead hangs a call up as soon as its media plane declares itself
// dead (media.CallMedia.Dead / AudioSocketCallMedia.Dead -- sustained zero
// inbound audio past MEDIA_DEAD_TIMEOUT_SECONDS), instead of leaving it running
// until Asterisk's own rtptimeout eventually notices or the process exits.
// Exits without doing anything if the call ends normally first (dead never
// closes in that case). Teardown's once-guard makes calling it here always
// safe, whichever path gets there first.
func (m *Manager) watchMediaDead(c *call, dead <-chan struct{}) {
	select {
	case <-dead:
		slog.Warn("hanging up call: media dead, no inbound audio past MEDIA_DEAD_TIMEOUT_SECONDS", "call_id", c.ID, "component", "media", "event", "media.dead")
		m.teardown(c)
	case <-c.mediaCtx.Done(): // hangup, or the media plane was stopped by a transfer
	}
}

// startAudioSocketMedia waits for Asterisk to open the AudioSocket TCP
// connection, then constructs and runs the media plane. Always called in its
// own goroutine (from completeBridge): Accept blocks, and blocking the shared
// Stasis event loop would stall every other call.
func (m *Manager) startAudioSocketMedia(c *call) {
	ln, ok := c.listener.(*net.TCPListener)
	if !ok {
		slog.Error("audiosocket listener has unexpected type", "call_id", c.ID)
		m.teardown(c)

		return
	}

	_ = ln.SetDeadline(time.Now().Add(audioSocketAcceptTimeout))

	conn, err := ln.Accept()
	if err != nil {
		// Either genuinely timed out, or the listener was closed by a concurrent
		// teardown (e.g. the caller hung up before Asterisk connected) -- either
		// way, Teardown's once-guard makes calling it here safe and idempotent.
		slog.Warn("audiosocket connection never arrived", "call_id", c.ID, "component", "media", "event", "media.no_connection", "error", err)
		m.teardown(c)

		return
	}

	_ = ln.Close() // one connection is all a call needs; free the OS listener now

	// Built before taking m.mu: it can take seconds (see mediaHandler), and
	// holding m.mu meanwhile stalled every other call's events -- calls were
	// set up one at a time.
	handler := m.mediaHandler(c)

	// Assigned under m.mu: this runs on its own goroutine while the Stasis event
	// loop can concurrently run teardown, which reads c.asm. Without the lock
	// that's a data race per the Go memory model, not just a lost update. A
	// call torn down meanwhile gets no media plane.
	m.mu.Lock()
	if c.State() == session.StateTornDown {
		m.mu.Unlock()
		_ = conn.Close()

		return
	}

	c.asm = media.NewAudioSocketCallMedia(c.ID, conn, metrics.AudioSocketSink{}, media.AudioSocketConfig{
		RecordDir:        m.cfg.RecordDir,
		Handler:          handler,
		ToWire:           media.LittleEndian, // AudioSocket payload is LE by protocol; see AudioSocketConfig.ToWire
		DebugAudio:       m.cfg.DebugAudio,
		MediaDeadTimeout: m.cfg.MediaDeadTimeout,
	})
	asm := c.asm
	m.mu.Unlock()

	c.SetState(session.StateMediaActive)
	slog.Info("audiosocket connected, media plane running", "call_id", c.ID, "component", "media", "event", "media.started", "port", c.Port)

	go m.watchMediaDead(c, asm.Dead())
	asm.Run(c.mediaCtx) // blocking is fine: already running in its own goroutine
}

// abort releases a call's port when bridging fails before the media plane starts.
// It goes through the same Teardown once-guard as a normal hangup so a subsequent
// StasisEnd for either channel (which Asterisk will still deliver once we hang
// them up below) can never re-run cleanup or double-free the port.
func (m *Manager) abort(c *call) {
	m.mu.Lock()
	delete(m.pending, c.ExternalID)
	delete(m.pending, c.ID)
	m.mu.Unlock()

	c.Teardown(func() {
		closeMediaSocket(c) // the media plane never started; nothing else will close this

		m.ports.Free(c.Port)
		metrics.MediaPortsInUse.Set(float64(m.ports.InUse()))
		config.UnregisterCallTrace(c.ID)

		hangupChannel(m.cl, c.ID, c.ID)
		hangupChannel(m.cl, c.ID, c.ExternalID)
	})
}

func (m *Manager) onStasisEnd(e *ari.StasisEnd) {
	id := e.Channel.ID

	m.mu.Lock()
	c, ok := m.active[id]
	if ok {
		delete(m.active, c.ID)
		delete(m.active, c.ExternalID)
	}
	m.mu.Unlock()

	if ok {
		// Reaching the active map means this is the call's FIRST StasisEnd
		// (the maps were just cleared; the second leg's StasisEnd finds
		// nothing) -- so this leg is the one that ended the call, and
		// teardown is hanging up the other side in response. Paired with
		// ChannelDestroyed's hangup cause, this says who ended the call.
		leg := "externalMedia"
		if id == c.ID {
			leg = "caller"
		}

		slog.Info("call ended by leg", "call_id", c.ID, "leg", leg, "channel_id", id, "external_id", c.ExternalID,
			"event", "call.hangup")
		m.teardown(c)

		return
	}

	// Not bridged: the call may still be staged (externalMedia requested but its
	// StasisStart never arrived, or the caller hung up mid-setup). abort() runs
	// the same once-guarded cleanup, which matters because the media port and its
	// socket are already allocated by this point -- both would otherwise leak for
	// the life of the process.
	m.mu.Lock()
	pc, staged := m.pending[id]
	m.mu.Unlock()

	if staged {
		slog.Info("call ended before bridging completed", "call_id", pc.ID, "event", "call.abandoned", "external_id", pc.ExternalID, "port", pc.Port)
		m.abort(pc)
	}
}

// teardown performs the full call cleanup -- media plane, bridge, remaining
// channel, media socket, and the duration/active-count metrics -- exactly once,
// via session.Call's once-guard. Both the caller's and the externalMedia
// channel's StasisEnd route here (via the m.active lookup keyed by both IDs), so
// without that guard this is exactly the kind of path that used to be able to
// double-fire. It also runs for a call whose media plane never actually started
// (e.g. AudioSocket's Accept timed out), since completeBridge adds a call to
// active -- and increments CallsActive -- before that's guaranteed to happen.
func (m *Manager) teardown(c *call) {
	c.Teardown(func() {
		if c.maxDur != nil {
			c.maxDur.Stop()
		}

		// Remove the call from both maps: teardown can be triggered from paths
		// that never went through onStasisEnd/abort's map cleanup (the max
		// duration cap, watchConnectivity's post-reconnect purge, and the
		// maxDur timer firing on a still-pending call).
		m.mu.Lock()
		delete(m.active, c.ID)
		delete(m.active, c.ExternalID)
		delete(m.pending, c.ID)
		delete(m.pending, c.ExternalID)
		transferChannel := c.transferChannel
		if transferChannel != "" {
			delete(m.active, transferChannel)
		}
		m.mu.Unlock()

		// After a transfer the call is caller <-> destination: whichever side
		// hung up, the other goes too.
		if transferChannel != "" {
			hangupChannel(m.cl, c.ID, transferChannel)
		}

		// Snapshot the media-plane fields under m.mu: c.asm in particular is
		// written by the AudioSocket accept goroutine (see startAudioSocketMedia),
		// which can race this closure running on the event loop or at shutdown.
		m.mu.Lock()
		cm, asm := c.cm, c.asm
		m.mu.Unlock()

		switch {
		case cm != nil && !cm.RemoteLocked():
			slog.Warn("rtp remote never locked; no audio ever received", "call_id", c.ID, "component", "media", "event", "media.no_audio", "external_id", c.ExternalID)
		case cm == nil && asm == nil:
			slog.Warn("media plane never started; no audio ever received", "call_id", c.ID, "component", "media", "event", "media.no_audio", "external_id", c.ExternalID)
		}

		// Independent of the checks above: a call can have transport-healthy audio
		// (remote locked, packets flowing) and still carry near-silent audio the
		// whole way through -- exactly the signature of the jitter-buffer
		// production incident (see docs/AUDIO_PIPELINE.md), which neither check
		// above catches since Asterisk really was sending packets.
		if avgRMS, ok := nearSilentRMS(cm, asm); ok {
			slog.Warn("call carried near-silent audio throughout; audio pipeline may be misconfigured",
				"call_id", c.ID, "component", "media", "event", "media.silent", "external_id", c.ExternalID, "avg_rms", avgRMS)
		}

		closeMediaSocket(c)

		if c.bridge != nil {
			deleteBridge(c.ID, c.bridge)
		}

		hangupChannel(m.cl, c.ID, c.ID)
		hangupChannel(m.cl, c.ID, c.ExternalID)

		m.ports.Free(c.Port)
		metrics.MediaPortsInUse.Set(float64(m.ports.InUse()))
		metrics.CallsActive.Dec()
		metrics.CallDurationSeconds.Observe(time.Since(c.Started).Seconds())

		slog.Info("call torn down",
			"call_id", c.ID, "external_id", c.ExternalID, "port", c.Port,
			"caller_number", c.Info.CallerNumber, "called_number", c.Info.CalledNumber, "direction", c.Info.Direction,
			"event", "call.teardown")

		config.UnregisterCallTrace(c.ID)
	})
}

// nearSilentRMS reports whichever media plane the call actually ran's
// NearSilent() verdict, or (0, false) if neither started. Takes the already
// snapshotted fields so the read stays synchronized with the AudioSocket
// accept goroutine's assignment (see teardown).
func nearSilentRMS(cm *media.CallMedia, asm *media.AudioSocketCallMedia) (avgRMS float64, ok bool) {
	switch {
	case cm != nil:
		return cm.NearSilent()
	case asm != nil:
		return asm.NearSilent()
	default:
		return 0, false
	}
}

// teardownAll tears down every tracked call -- active (bridged) and pending
// (staged, awaiting its externalMedia StasisStart) -- and empties both maps.
// Used on process shutdown, and by watchConnectivity after an ARI WebSocket
// reconnect (events missed during the gap may have included StasisEnd, so any
// surviving call is a potential zombie: a billable channel and a leaked media
// port). Teardown's once-guard makes this safe for calls already mid-teardown.
func (m *Manager) teardownAll() {
	m.mu.Lock()
	seen := make(map[*call]struct{})

	for _, c := range m.active {
		seen[c] = struct{}{}
	}

	for _, c := range m.pending {
		seen[c] = struct{}{}
	}

	m.active = make(map[string]*call)
	m.pending = make(map[string]*call)
	m.mu.Unlock()

	for c := range seen {
		m.teardown(c)
	}
}

// callerNumber extracts the calling party's number from a StasisStart's channel
// data. Caller is a pointer and can be nil (e.g. certain channel types never
// populate it), so this returns "" rather than risking a nil dereference.
func callerNumber(ch ari.ChannelData) string {
	if ch.Caller == nil {
		return ""
	}

	return ch.Caller.Number
}

// calledNumber extracts the extension that routed the call into this Stasis app.
// Dialplan is a pointer and can be nil; returns "" in that case.
func calledNumber(ch ari.ChannelData) string {
	if ch.Dialplan == nil {
		return ""
	}

	return ch.Dialplan.Exten
}
