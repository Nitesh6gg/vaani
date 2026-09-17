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
)

// call tracks the state of one bridged caller<->externalMedia pair.
type call struct {
	callerID   string
	externalID string
	port       int
	started    time.Time

	bridge *ari.BridgeHandle
	cm     *media.CallMedia
	cancel context.CancelFunc
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
		m.completeBridge(ctx, pending)
		return
	}

	m.startCall(e)
}

// startCall answers a newly-arrived caller channel and stages its externalMedia
// counterpart. The bridge is completed once that channel's own StasisStart arrives
// (see completeBridge).
func (m *Manager) startCall(e *ari.StasisStart) {
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
	c := &call{callerID: id, externalID: externalID, port: port, started: time.Now()}

	m.mu.Lock()
	m.pending[externalID] = c
	m.mu.Unlock()

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

	slog.Info("call answered, externalMedia staged", "call_id", id, "external_id", externalID, "port", port)
}

// completeBridge runs once the externalMedia channel itself enters Stasis: it
// creates the bridge, adds both channels, binds the call's UDP socket, and starts
// the loop-back media plane.
func (m *Manager) completeBridge(ctx context.Context, c *call) {
	bridgeKey := ari.NewKey(ari.BridgeKey, rid.New(rid.Bridge))

	bh, err := m.cl.Bridge().Create(bridgeKey, "mixing", bridgeKey.ID)
	if err != nil {
		slog.Error("failed to create bridge", "call_id", c.callerID, "error", err)
		m.abort(c)

		return
	}

	if err := bh.AddChannel(c.callerID); err != nil {
		slog.Error("failed to add caller to bridge", "call_id", c.callerID, "error", err)
		_ = bh.Delete()
		m.abort(c)

		return
	}

	if err := bh.AddChannel(c.externalID); err != nil {
		slog.Error("failed to add externalMedia channel to bridge", "call_id", c.callerID, "error", err)
		_ = bh.Delete()
		m.abort(c)

		return
	}

	conn, err := net.ListenUDP("udp", &net.UDPAddr{Port: c.port})
	if err != nil {
		slog.Error("failed to bind media socket", "call_id", c.callerID, "port", c.port, "error", err)
		_ = bh.Delete()
		m.abort(c)

		return
	}

	callCtx, cancel := context.WithCancel(ctx)
	c.bridge = bh
	c.cancel = cancel
	c.cm = media.NewCallMedia(c.callerID, conn, metrics.Sink{})

	m.mu.Lock()
	m.active[c.callerID] = c
	m.active[c.externalID] = c
	m.mu.Unlock()

	metrics.CallsActive.Inc()
	slog.Info("call bridged", "call_id", c.callerID, "external_id", c.externalID, "port", c.port)

	go c.cm.Run(callCtx)
}

// abort releases a call's port when bridging fails before the media plane starts.
func (m *Manager) abort(c *call) {
	m.mu.Lock()
	delete(m.pending, c.externalID)
	m.mu.Unlock()

	m.ports.Free(c.port)
	metrics.MediaPortsInUse.Set(float64(m.ports.InUse()))

	_ = m.cl.Channel().Hangup(ari.NewKey(ari.ChannelKey, c.callerID), "")
	_ = m.cl.Channel().Hangup(ari.NewKey(ari.ChannelKey, c.externalID), "")
}

func (m *Manager) onStasisEnd(e *ari.StasisEnd) {
	id := e.Channel.ID

	m.mu.Lock()
	c, ok := m.active[id]
	if ok {
		delete(m.active, c.callerID)
		delete(m.active, c.externalID)
	}
	m.mu.Unlock()

	if !ok {
		// Caller (or its externalMedia peer) hung up before the bridge completed;
		// nothing to tear down beyond what abort() already did.
		return
	}

	m.teardown(c)
}

// teardown performs the full call cleanup: media plane, bridge, remaining channel,
// and the UDP port.
func (m *Manager) teardown(c *call) {
	if c.cancel != nil {
		c.cancel()
	}

	if c.bridge != nil {
		_ = c.bridge.Delete()
	}

	_ = m.cl.Channel().Hangup(ari.NewKey(ari.ChannelKey, c.callerID), "")
	_ = m.cl.Channel().Hangup(ari.NewKey(ari.ChannelKey, c.externalID), "")

	m.ports.Free(c.port)
	metrics.MediaPortsInUse.Set(float64(m.ports.InUse()))
	metrics.CallsActive.Dec()
	metrics.CallDurationSeconds.Observe(time.Since(c.started).Seconds())

	slog.Info("call torn down", "call_id", c.callerID, "external_id", c.externalID, "port", c.port)
}

// shutdown tears down every active call, used on process shutdown.
func (m *Manager) shutdown() {
	m.mu.Lock()
	seen := make(map[string]*call)

	for _, c := range m.active {
		seen[c.callerID] = c
	}

	m.active = make(map[string]*call)
	m.mu.Unlock()

	for _, c := range seen {
		m.teardown(c)
	}
}
