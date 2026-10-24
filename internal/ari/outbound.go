package ari

import (
	"context"
	"fmt"
	"log/slog"
	"strconv"
	"sync"
	"time"

	"github.com/CyCoreSystems/ari/v5"

	"github.com/CyCoreSystems/ari/v5/rid"
	"github.com/nitesh/vaani/internal/dograh"
)

// outboundAppArg is the first Stasis argument of a channel Vaani dialed. Like a
// transfer leg, an originated channel enters the Stasis app only once the
// callee answers, so from there the call follows the inbound path
// (startCall: media socket bound before Asterisk is told its address,
// externalMedia, bridge, agent).
const outboundAppArg = "outbound"

// outboundAnswerSlack is added to the ring timeout for our own watchdog, so
// Asterisk's timeout (which ends the ringing channel with a clear hangup
// cause) normally fires first. The watchdog is for events that never arrive,
// such as ones lost while the ARI WebSocket was down.
var outboundAnswerSlack = 5 * time.Second // a variable for the tests

// Outcome is how a dialed call ended up. Done gets exactly one.
type Outcome struct {
	// Connected: the callee answered and the agent ran the call; its Dograh
	// run is completed by the agent's own record. Otherwise the call never
	// connected and Status says why (busy, no-answer, rejected, failed), for
	// the caller to complete the run (Store.MarkNotConnected).
	Connected bool
	Status    string
	Cause     int // Asterisk's hangup cause, if there was one
}

// DialRequest is an outbound call to place.
type DialRequest struct {
	Endpoint    string // Asterisk dial string, e.g. PJSIP/9876543210@trunk
	CallerID    string
	Called      string // the dialed number, as the callee is known to Dograh
	RingTimeout time.Duration

	// WorkflowID and RunID: the call's Dograh workflow and its run, which the
	// caller created before dialing. Variables are the contact's own.
	WorkflowID int
	RunID      int64
	Variables  map[string]any

	// Done is called once, on its own goroutine, when the call is over or never
	// connected.
	Done func(Outcome)
}

// outboundCall is a dialed call from origination until it is over. The fields
// after the Once are guarded by Manager.mu.
type outboundCall struct {
	req  DialRequest
	once sync.Once

	watchdog     *time.Timer
	answered     bool
	agentStarted bool
}

func (o *outboundCall) finish(out Outcome) {
	o.once.Do(func() {
		if o.req.Done != nil {
			go o.req.Done(out)
		}
	})
}

// Dial originates the call and returns its channel id. The result arrives
// through req.Done. An error means nothing was dialed and Done won't be
// called.
func (m *Manager) Dial(ctx context.Context, req DialRequest) (string, error) {
	id := rid.New(rid.Channel)
	oc := &outboundCall{req: req}

	m.mu.Lock()
	m.outbound[id] = oc
	m.mu.Unlock()

	_, err := m.cl.Channel().Originate(ari.NewKey(ari.ChannelKey, id), ari.OriginateRequest{
		Endpoint:  req.Endpoint,
		App:       m.cfg.AriApp,
		AppArgs:   outboundAppArg + "," + strconv.FormatInt(req.RunID, 10),
		Timeout:   int(req.RingTimeout.Seconds()),
		ChannelID: id,
		CallerID:  req.CallerID,
	})
	if err != nil {
		m.mu.Lock()
		delete(m.outbound, id)
		m.mu.Unlock()

		return "", fmt.Errorf("could not dial %s: %w", req.Endpoint, err)
	}

	m.mu.Lock()
	if m.outbound[id] == oc && !oc.answered {
		oc.watchdog = time.AfterFunc(req.RingTimeout+outboundAnswerSlack, func() { m.outboundTimedOut(id) })
	}
	m.mu.Unlock()

	slog.Info("outbound call dialing", "call_id", id, "event", "outbound.dialing", "endpoint", req.Endpoint,
		"called_number", req.Called, "caller_id", req.CallerID, "workflow_id", req.WorkflowID, "run_id", req.RunID,
		"ring_timeout_s", int(req.RingTimeout.Seconds()))

	return id, nil
}

// outboundFor is the dialed call on channel id, or nil.
func (m *Manager) outboundFor(id string) *outboundCall {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.outbound[id]
}

// takeRinging removes and returns the dialed call on channel id if it has not
// been answered; nil otherwise (not ours, answered, or already taken).
func (m *Manager) takeRinging(id string) *outboundCall {
	m.mu.Lock()
	defer m.mu.Unlock()

	oc := m.outbound[id]
	if oc == nil || oc.answered {
		return nil
	}

	delete(m.outbound, id)

	if oc.watchdog != nil {
		oc.watchdog.Stop()
	}

	return oc
}

// onOutboundAnswered handles the StasisStart of a channel we dialed.
func (m *Manager) onOutboundAnswered(ctx context.Context, e *ari.StasisStart) {
	id := e.Channel.ID

	m.mu.Lock()
	oc := m.outbound[id]
	if oc != nil {
		oc.answered = true

		if oc.watchdog != nil {
			oc.watchdog.Stop()
		}
	}
	m.mu.Unlock()

	if oc == nil {
		// Answered after we gave up on it (timeout, restart): nobody wants it.
		slog.Warn("outbound leg answered but no call is waiting; hanging it up", "call_id", id, "event", "outbound.orphan")
		hangupChannel(m.cl, id, id)

		return
	}

	slog.Info("outbound call answered", "call_id", id, "event", "outbound.answered", "called_number", oc.req.Called)
	m.startCall(ctx, e)
}

// onOutboundDestroyed reports a dialed call that ended without being
// answered, with the status its hangup cause stands for. Asterisk sends every
// app the destroyed events of every channel, so only channels in m.outbound
// count.
func (m *Manager) onOutboundDestroyed(e *ari.ChannelDestroyed) {
	oc := m.takeRinging(e.Channel.ID)
	if oc == nil {
		return
	}

	status := dograh.StatusForCause(e.Cause)

	slog.Info("outbound call not answered", "call_id", e.Channel.ID, "event", "outbound.unanswered",
		"status", status, "cause", e.Cause, "cause_txt", e.CauseTxt, "called_number", oc.req.Called)
	oc.finish(Outcome{Status: status, Cause: e.Cause})
}

// outboundTimedOut: the ring timeout passed and Asterisk never reported.
func (m *Manager) outboundTimedOut(id string) {
	oc := m.takeRinging(id)
	if oc == nil {
		return
	}

	slog.Warn("outbound call timed out without a result; hanging it up", "call_id", id, "event", "outbound.timeout",
		"called_number", oc.req.Called)
	hangupChannel(m.cl, id, id)
	oc.finish(Outcome{Status: dograh.StatusNoAnswer})
}

// outboundSetupFailed ends an answered outbound call whose media setup failed
// before the agent was reached: the callee heard nothing, so it never
// connected.
func (m *Manager) outboundSetupFailed(id string) {
	m.mu.Lock()
	oc := m.outbound[id]
	delete(m.outbound, id)
	m.mu.Unlock()

	if oc != nil {
		oc.finish(Outcome{Status: dograh.StatusFailed})
	}
}

// finishOutbound is the end of an answered outbound call (from teardown).
func (m *Manager) finishOutbound(c *call) {
	if c.out == nil {
		return
	}

	m.mu.Lock()
	started := c.out.agentStarted
	delete(m.outbound, c.ID)
	m.mu.Unlock()

	if started {
		c.out.finish(Outcome{Connected: true})
	} else {
		c.out.finish(Outcome{Status: dograh.StatusFailed})
	}
}

// hangupRingingOutbound hangs up every dialed call still ringing (shutdown).
func (m *Manager) hangupRingingOutbound() {
	m.mu.Lock()

	var ringing []string

	for id, oc := range m.outbound {
		if !oc.answered {
			ringing = append(ringing, id)
		}
	}
	m.mu.Unlock()

	for _, id := range ringing {
		if oc := m.takeRinging(id); oc != nil {
			hangupChannel(m.cl, id, id)
			oc.finish(Outcome{Status: dograh.StatusFailed})
		}
	}
}
