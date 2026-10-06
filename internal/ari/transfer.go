package ari

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/CyCoreSystems/ari/v5"
	"github.com/CyCoreSystems/ari/v5/rid"

	"github.com/nitesh/vaani/internal/session"
)

// transferAppArg is the first Stasis argument on a transfer destination's
// channel, so onStasisStart can tell it apart from a new inbound call.
// (Dograh tags its ARI transfer legs the same way: "transfer,<id>".)
const transferAppArg = "transfer"

// transferAnswerSlack is added to the ring timeout for our own wait, so
// Asterisk's timeout (which hangs the ringing channel up and yields a clear
// ChannelDestroyed cause) normally fires first.
const transferAnswerSlack = 5 * time.Second

// pendingTransfer is a transfer destination that is ringing. The event loop
// signals it; transfer() waits on it. Both channels are buffered so the
// event loop never blocks.
type pendingTransfer struct {
	answered chan struct{}
	failed   chan string // hangup cause, when it's destroyed without answering
}

// transfer dials destination for call c and waits until it answers, fails,
// or times out. On answer it returns connect, which swaps the destination
// into the bridge in place of the agent's media leg and stops the agent.
// Mirrors Dograh's ARI transfer: originate into the same Stasis app with
// "transfer,<id>" args, then swap the bridge on answer.
func (m *Manager) transfer(ctx context.Context, c *call, destination string, timeout time.Duration) (func() error, error) {
	destID := rid.New(rid.Channel)
	pt := &pendingTransfer{answered: make(chan struct{}, 1), failed: make(chan string, 1)}

	m.mu.Lock()
	m.transfers[destID] = pt
	m.mu.Unlock()

	forget := func() {
		m.mu.Lock()
		delete(m.transfers, destID)
		m.mu.Unlock()
	}

	_, err := m.cl.Channel().Originate(ari.NewKey(ari.ChannelKey, destID), ari.OriginateRequest{
		Endpoint:  destination,
		App:       m.cfg.AriApp,
		AppArgs:   transferAppArg + "," + c.ID,
		Timeout:   int(timeout.Seconds()),
		ChannelID: destID,
		CallerID:  c.Info.CallerNumber,
	})
	if err != nil {
		forget()
		return nil, fmt.Errorf("could not dial %s: %w", destination, err)
	}

	slog.Info("transfer leg dialing", "call_id", c.ID, "transfer_channel", destID, "destination", destination)

	wait := time.NewTimer(timeout + transferAnswerSlack)
	defer wait.Stop()

	select {
	case <-pt.answered:
		forget()
		return func() error { return m.connectTransfer(c, destID) }, nil
	case cause := <-pt.failed:
		forget()
		return nil, fmt.Errorf("transfer destination did not answer (%s)", cause)
	case <-wait.C:
		forget()
		hangupChannel(m.cl, c.ID, destID)

		return nil, errors.New("transfer destination did not answer in time")
	case <-ctx.Done():
		// The caller hung up (or the call is otherwise ending) while it rang.
		forget()
		hangupChannel(m.cl, c.ID, destID)

		return nil, ctx.Err()
	}
}

// connectTransfer puts the answered destination into the call's bridge,
// takes the agent's externalMedia leg out, and stops the agent. From here
// the call is caller <-> destination; teardown ends both together.
func (m *Manager) connectTransfer(c *call, destID string) error {
	m.mu.Lock()
	bridge, tornDown := c.bridge, c.State() == session.StateTornDown
	m.mu.Unlock()

	if tornDown || bridge == nil {
		hangupChannel(m.cl, c.ID, destID)
		return errors.New("call ended before the transfer could connect")
	}

	// Add the destination before removing the agent, so the caller never
	// sits in a bridge alone.
	if err := bridge.AddChannel(destID); err != nil {
		hangupChannel(m.cl, c.ID, destID)
		return fmt.Errorf("could not connect the transfer: %w", err)
	}

	// Re-checked here, under the lock teardown reads transferChannel with: the
	// caller can hang up during AddChannel's round trip above. session.Teardown
	// marks the call torn down before its cleanup takes m.mu, so either this
	// sees the teardown and hangs the destination up itself, or the teardown's
	// cleanup sees transferChannel and does. Without it the destination stayed
	// up, billable, and its m.active entry was never removed.
	m.mu.Lock()
	if c.State() == session.StateTornDown {
		m.mu.Unlock()
		hangupChannel(m.cl, c.ID, destID)

		return errors.New("call ended while the transfer was connecting")
	}

	c.transferChannel = destID
	m.active[destID] = c
	// The agent's leg is about to be hung up on purpose; its StasisEnd must
	// not tear the whole call down.
	delete(m.active, c.ExternalID)
	m.mu.Unlock()

	if err := bridge.RemoveChannel(c.ExternalID); err != nil {
		slog.Debug("removing agent leg from bridge failed", "call_id", c.ID, "error", err)
	}

	c.mediaCancel() // stops the media plane and the agent (STT/TTS connections close)
	hangupChannel(m.cl, c.ID, c.ExternalID)

	return nil
}

// onTransferLegAnswered handles the StasisStart of a transfer destination
// (it only enters Stasis once answered).
func (m *Manager) onTransferLegAnswered(channelID string) {
	m.mu.Lock()
	pt := m.transfers[channelID]
	m.mu.Unlock()

	if pt == nil {
		// Answered after we gave up (timeout, caller hung up): nobody is
		// waiting for it, so don't leave it connected to nothing.
		slog.Warn("transfer leg answered but no transfer is waiting; hanging it up", "channel_id", channelID)
		hangupChannel(m.cl, channelID, channelID)

		return
	}

	select {
	case pt.answered <- struct{}{}:
	default:
	}
}

// onTransferLegDestroyed reports a ringing transfer destination that went
// away without answering (busy, rejected, no answer), with the cause.
func (m *Manager) onTransferLegDestroyed(e *ari.ChannelDestroyed) {
	m.mu.Lock()
	pt := m.transfers[e.Channel.ID]
	m.mu.Unlock()

	if pt == nil {
		return
	}

	select {
	case pt.failed <- fmt.Sprintf("%s, cause %d", e.CauseTxt, e.Cause):
	default:
	}
}
