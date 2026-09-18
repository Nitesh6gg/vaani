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

// call tracks the ARI/media-specific state of one bridged caller<->externalMedia
// pair; *session.Call carries the lifecycle state and the once-guaranteed
// teardown that used to be prone to double- or zero-firing in Phase 1.
type call struct {
	*session.Call

	bridge *ari.BridgeHandle
	cm     *media.CallMedia
}

// Manager owns the Stasis event loop: it answers incoming calls, wires each one to
// an externalMedia RTP channel and a bridge, runs the loop-back media plane for its
// duration, and tears everything down on hangup.
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
	pending, isExternal := m.pending[id]
	if isExternal {
		delete(m.pending, id)
	}
	m.mu.Unlock()

	if isExternal {
		m.completeBridge(pending)
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
	c := &call{Call: session.New(id, externalID, port, info, ctx)}

	m.mu.Lock()
	m.pending[externalID] = c
	m.mu.Unlock()

	c.SetState(session.StateStaged)

	_, err = m.cl.Channel().ExternalMedia(e.Key(ari.ChannelKey, externalID), ari.ExternalMediaOptions{
		ChannelID:     externalID,
		App:           m.cfg.AriApp,
		ExternalHost:  fmt.Sprintf("%s:%d", m.cfg.MediaIP, port),
		Encapsulation: "rtp",
		Transport:     "udp",
		Format:        "slin16",
	})
	if err != nil {
		slog.Error("failed to create externalMedia channel", "call_id", id, "error", err)

		m.mu.Lock()
		delete(m.pending, externalID)
		m.mu.Unlock()

		m.ports.Free(port)
		metrics.MediaPortsInUse.Set(float64(m.ports.InUse()))
		_ = channel.Hangup()

		return
	}

	slog.Info("call answered, externalMedia staged",
		"call_id", id, "external_id", externalID, "port", port,
		"caller_number", info.CallerNumber, "called_number", info.CalledNumber, "direction", info.Direction)
}

// completeBridge runs once the externalMedia channel itself enters Stasis: it
// creates the bridge, adds both channels, binds the call's UDP socket, and starts
// the media plane.
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

	c.SetState(session.StateBridged)

	conn, err := net.ListenUDP("udp", &net.UDPAddr{Port: c.Port})
	if err != nil {
		slog.Error("failed to bind media socket", "call_id", c.ID, "port", c.Port, "error", err)
		_ = bh.Delete()
		m.abort(c)

		return
	}

	fromWire, err := media.ParseEndianness(m.cfg.AudioL16Endianness)
	if err != nil {
		// Already validated at config.Load() time; unreachable in practice.
		slog.Error("invalid AUDIO_L16_ENDIANNESS", "call_id", c.ID, "error", err)
	}

	c.bridge = bh
	c.cm = media.NewCallMedia(c.ID, conn, metrics.Sink{}, media.Config{
		JitterBufferPackets: m.cfg.JitterBufferPackets,
		FromWire:            fromWire,
		RecordDir:           m.cfg.RecordDir,
	})

	m.mu.Lock()
	m.active[c.ID] = c
	m.active[c.ExternalID] = c
	m.mu.Unlock()

	c.SetState(session.StateMediaActive)
	metrics.CallsActive.Inc()
	slog.Info("call bridged",
		"call_id", c.ID, "external_id", c.ExternalID, "port", c.Port,
		"caller_number", c.Info.CallerNumber, "called_number", c.Info.CalledNumber, "direction", c.Info.Direction)

	go c.cm.Run(c.Ctx)
}

// abort releases a call's port when bridging fails before the media plane starts.
// It goes through the same Teardown once-guard as a normal hangup so a subsequent
// StasisEnd for either channel (which Asterisk will still deliver once we hang
// them up below) can never re-run cleanup or double-free the port.
func (m *Manager) abort(c *call) {
	m.mu.Lock()
	delete(m.pending, c.ExternalID)
	m.mu.Unlock()

	c.Teardown(func() {
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

	if !ok {
		// Caller (or its externalMedia peer) hung up before the bridge completed;
		// nothing to tear down beyond what abort() already did.
		return
	}

	m.teardown(c)
}

// teardown performs the full call cleanup -- media plane, bridge, remaining
// channel, UDP port, and the duration metric -- exactly once, via session.Call's
// once-guard. Both the caller's and the externalMedia channel's StasisEnd route
// here (via the m.active lookup keyed by both IDs), so without that guard this is
// exactly the kind of path that used to be able to double-fire.
func (m *Manager) teardown(c *call) {
	c.Teardown(func() {
		if c.cm != nil && !c.cm.RemoteLocked() {
			slog.Warn("rtp remote never locked; no audio ever received", "call_id", c.ID, "external_id", c.ExternalID)
		}

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
