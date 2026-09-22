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

	"github.com/nitesh/vaani/internal/config"
	"github.com/nitesh/vaani/internal/media"
	"github.com/nitesh/vaani/internal/metrics"
	"github.com/nitesh/vaani/internal/session"
)

// audioSocketAcceptTimeout bounds how long completeBridge waits for Asterisk to
// actually open the AudioSocket TCP connection after the channel is bridged.
const audioSocketAcceptTimeout = 10 * time.Second

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

// Manager owns the Stasis event loop: it answers incoming calls, wires each one to
// an externalMedia channel and a bridge, runs the media plane for its duration,
// and tears everything down on hangup.
type Manager struct {
	cl    ari.Client
	cfg   config.Config
	ports *media.PortAllocator

	mu      sync.Mutex
	pending map[string]*call // keyed by the externalMedia channel ID, awaiting its StasisStart
	active  map[string]*call // keyed by both callerID and externalID once bridged
}

// NewManager creates a call Manager bound to cl.
func NewManager(cl ari.Client, cfg config.Config, ports *media.PortAllocator) *Manager {
	return &Manager{
		cl:      cl,
		cfg:     cfg,
		ports:   ports,
		pending: make(map[string]*call),
		active:  make(map[string]*call),
	}
}

// Run subscribes to the Stasis events this app cares about and dispatches them
// until ctx is cancelled.
func (m *Manager) Run(ctx context.Context) {
	sub := m.cl.Bus().Subscribe(nil, "StasisStart", "StasisEnd", "ChannelDtmfReceived")
	defer sub.Cancel()

	for {
		select {
		case <-ctx.Done():
			m.shutdown()
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
			}
		}
	}
}

func (m *Manager) onStasisStart(ctx context.Context, e *ari.StasisStart) {
	id := e.Channel.ID

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
		slog.Error("failed to answer channel", "call_id", id, "error", err)
		return
	}

	port, err := m.ports.Alloc()
	if err != nil {
		slog.Error("failed to allocate media port", "call_id", id, "error", err)
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
		slog.Error("failed to bind media socket", "call_id", id, "port", port, "encapsulation", encap, "error", err)

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

	m.mu.Lock()
	m.pending[externalID] = c
	m.pending[id] = c // so a hangup during setup can still find and clean up this call
	m.mu.Unlock()

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
		slog.Error("failed to create externalMedia channel", "call_id", id, "error", err)

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
		"caller_number", info.CallerNumber, "called_number", info.CalledNumber, "direction", info.Direction)
}

// completeBridge runs once the externalMedia channel itself enters Stasis: it
// creates the bridge, adds both channels, and starts the media plane appropriate
// to the configured encapsulation.
func (m *Manager) completeBridge(c *call) {
	bridgeKey := ari.NewKey(ari.BridgeKey, rid.New(rid.Bridge))

	bh, err := m.cl.Bridge().Create(bridgeKey, "mixing", bridgeKey.ID)
	if err != nil {
		slog.Error("failed to create bridge", "call_id", c.ID, "error", err)
		m.abort(c)

		return
	}

	if err := bh.AddChannel(c.ID); err != nil {
		slog.Error("failed to add caller to bridge", "call_id", c.ID, "error", err)
		_ = bh.Delete()
		m.abort(c)

		return
	}

	if err := bh.AddChannel(c.ExternalID); err != nil {
		slog.Error("failed to add externalMedia channel to bridge", "call_id", c.ID, "error", err)
		_ = bh.Delete()
		m.abort(c)

		return
	}

	c.bridge = bh
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
	slog.Info("call bridged",
		"call_id", c.ID, "external_id", c.ExternalID, "port", c.Port,
		"caller_number", c.Info.CallerNumber, "called_number", c.Info.CalledNumber, "direction", c.Info.Direction)

	if m.cfg.MediaEncapsulation == "audiosocket" {
		go m.startAudioSocketMedia(c)
		return
	}

	m.startRTPMedia(c)
}

// mediaHandler builds the Handler for a new call from config: SilentHandler if
// TEST_SILENT_HANDLER=1 (a debug hook -- never set in production, since it means
// the call carries no audio at all), otherwise nil (CallMedia/AudioSocketCallMedia
// both default that to LoopbackHandler).
func (m *Manager) mediaHandler(callID string) media.Handler {
	if !m.cfg.TestSilentHandler {
		return nil
	}

	slog.Warn("TEST_SILENT_HANDLER=1: this call will carry no audio", "call_id", callID)

	return media.SilentHandler{}
}

// startRTPMedia constructs and runs the RTP media plane for an already-bridged
// call. Called synchronously from completeBridge -- unlike AudioSocket, there's
// no blocking accept step, so this doesn't need its own goroutine.
func (m *Manager) startRTPMedia(c *call) {
	fromWire, err := media.ParseEndianness(m.cfg.AudioL16Endianness)
	if err != nil {
		// Already validated at config.Load() time; unreachable in practice.
		slog.Error("invalid AUDIO_L16_ENDIANNESS", "call_id", c.ID, "error", err)
	}

	c.cm = media.NewCallMedia(c.ID, c.udpConn, metrics.Sink{}, media.Config{
		JitterBufferPackets: m.cfg.JitterBufferPackets,
		FromWire:            fromWire,
		RecordDir:           m.cfg.RecordDir,
		Handler:             m.mediaHandler(c.ID),
	})

	c.SetState(session.StateMediaActive)
	slog.Info("rtp media plane running", "call_id", c.ID, "port", c.Port)

	go c.cm.Run(c.Ctx)
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
		slog.Warn("audiosocket connection never arrived", "call_id", c.ID, "error", err)
		m.teardown(c)

		return
	}

	_ = ln.Close() // one connection is all a call needs; free the OS listener now

	c.asm = media.NewAudioSocketCallMedia(c.ID, conn, metrics.AudioSocketSink{}, media.AudioSocketConfig{
		RecordDir: m.cfg.RecordDir,
		Handler:   m.mediaHandler(c.ID),
	})

	c.SetState(session.StateMediaActive)
	slog.Info("audiosocket connected, media plane running", "call_id", c.ID, "port", c.Port)

	c.asm.Run(c.Ctx) // blocking is fine: already running in its own goroutine
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

		_ = m.cl.Channel().Hangup(ari.NewKey(ari.ChannelKey, c.ID), "")
		_ = m.cl.Channel().Hangup(ari.NewKey(ari.ChannelKey, c.ExternalID), "")
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
		slog.Info("call ended before bridging completed", "call_id", pc.ID, "external_id", pc.ExternalID, "port", pc.Port)
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
		switch {
		case c.cm != nil && !c.cm.RemoteLocked():
			slog.Warn("rtp remote never locked; no audio ever received", "call_id", c.ID, "external_id", c.ExternalID)
		case c.cm == nil && c.asm == nil:
			slog.Warn("media plane never started; no audio ever received", "call_id", c.ID, "external_id", c.ExternalID)
		}

		closeMediaSocket(c)

		if c.bridge != nil {
			_ = c.bridge.Delete()
		}

		_ = m.cl.Channel().Hangup(ari.NewKey(ari.ChannelKey, c.ID), "")
		_ = m.cl.Channel().Hangup(ari.NewKey(ari.ChannelKey, c.ExternalID), "")

		m.ports.Free(c.Port)
		metrics.MediaPortsInUse.Set(float64(m.ports.InUse()))
		metrics.CallsActive.Dec()
		metrics.CallDurationSeconds.Observe(time.Since(c.Started).Seconds())

		slog.Info("call torn down",
			"call_id", c.ID, "external_id", c.ExternalID, "port", c.Port,
			"caller_number", c.Info.CallerNumber, "called_number", c.Info.CalledNumber, "direction", c.Info.Direction)
	})
}

// shutdown tears down every active call, used on process shutdown.
func (m *Manager) shutdown() {
	m.mu.Lock()
	seen := make(map[string]*call)

	for _, c := range m.active {
		seen[c.ID] = c
	}

	m.active = make(map[string]*call)
	m.mu.Unlock()

	for _, c := range seen {
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
