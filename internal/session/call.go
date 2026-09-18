// Package session tracks per-call lifecycle state, independent of ARI and media
// transport details, so a call's teardown can be structurally guaranteed to run
// exactly once no matter how many events (caller hangup, externalMedia hangup,
// process shutdown) try to trigger it.
package session

import (
	"context"
	"sync"
	"time"
)

// State is a call's position in its lifecycle.
type State int

const (
	// StateNew: caller channel has entered Stasis, not yet answered.
	StateNew State = iota
	// StateStaged: answered, externalMedia channel requested, awaiting its own
	// StasisStart.
	StateStaged
	// StateBridged: both channels are in a bridge, but the media plane hasn't
	// necessarily started running yet.
	StateBridged
	// StateMediaActive: the media pipeline (CallMedia) is running.
	StateMediaActive
	// StateTornDown: terminal. A call never leaves this state.
	StateTornDown
)

// String renders the state for logging.
func (s State) String() string {
	switch s {
	case StateNew:
		return "new"
	case StateStaged:
		return "staged"
	case StateBridged:
		return "bridged"
	case StateMediaActive:
		return "media_active"
	case StateTornDown:
		return "torn_down"
	default:
		return "unknown"
	}
}

// Call is a per-call state machine. It carries its own context, cancelled when
// the call tears down, and guarantees that a caller-supplied teardown action runs
// exactly once regardless of how many times Teardown is invoked -- this is what
// makes the Phase 1 class of bug (a duration metric observed zero or two times
// because two different events both thought they owned cleanup) structurally
// impossible: whichever event calls Teardown first runs the action; every other
// caller, including ones racing concurrently, is a guaranteed no-op.
type Call struct {
	ID         string
	ExternalID string
	Port       int
	Started    time.Time
	Info       Info

	Ctx    context.Context
	Cancel context.CancelFunc

	mu    sync.Mutex
	state State

	teardownOnce sync.Once
}

// Info is the call detail metadata captured when a call arrives, kept for the
// whole call lifecycle (logging now; available to a Phase 3 Handler later for
// routing/personalization decisions). Deliberately not exposed as Prometheus
// metric labels: phone numbers are high-cardinality and often PII, so they
// belong in structured logs, not a time series.
type Info struct {
	CallerNumber string
	CalledNumber string
	Direction    string
}

// New creates a Call in StateNew, deriving its context from parent.
func New(id, externalID string, port int, info Info, parent context.Context) *Call {
	ctx, cancel := context.WithCancel(parent)

	return &Call{
		ID:         id,
		ExternalID: externalID,
		Info:       info,
		Port:       port,
		Started:    time.Now(),
		Ctx:        ctx,
		Cancel:     cancel,
		state:      StateNew,
	}
}

// SetState records a forward lifecycle transition. It does not validate that the
// transition is legal (the caller -- ari.Manager -- already only calls it at the
// right points); it exists so State() has something meaningful to report.
func (c *Call) SetState(s State) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.state = s
}

// State returns the call's current lifecycle state.
func (c *Call) State() State {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.state
}

// Teardown runs fn exactly once for this Call's lifetime: the first caller (from
// any goroutine) runs it; every subsequent call, including ones already in
// flight, is a no-op. It also cancels Ctx and moves to StateTornDown before
// running fn, so fn can rely on both already being true.
func (c *Call) Teardown(fn func()) {
	c.teardownOnce.Do(func() {
		c.SetState(StateTornDown)
		c.Cancel()
		fn()
	})
}
