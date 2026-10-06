package ari

import (
	"context"
	"sync"
	"testing"

	"github.com/CyCoreSystems/ari/v5"
	"github.com/CyCoreSystems/ari/v5/client/arimocks"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/nitesh/vaani/internal/config"
	"github.com/nitesh/vaani/internal/media"
	"github.com/nitesh/vaani/internal/session"
)

// TestConnectTransfer_CallerHangsUpDuringHandover: the caller hanging up
// while the destination is being added to the bridge (an ARI round trip)
// must not leave the answered destination up, nor its m.active entry behind.
func TestConnectTransfer_CallerHangsUpDuringHandover(t *testing.T) {
	var (
		mu     sync.Mutex
		hungUp []string
	)

	channels := &arimocks.Channel{}
	channels.On("Hangup", mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
		mu.Lock()
		hungUp = append(hungUp, args.Get(0).(*ari.Key).ID)
		mu.Unlock()
	}).Return(nil)

	cl := &arimocks.Client{}
	cl.On("Channel").Return(channels)

	ports := media.NewPortAllocator(20000, 4)
	port, err := ports.Alloc()
	require.NoError(t, err)

	m := NewManager(cl, config.Config{}, ports, nil, nil)

	c := &call{Call: session.New("caller-1", "ext-1", port, session.Info{}, context.Background())}
	_, c.mediaCancel = context.WithCancel(context.Background())

	bridges := &arimocks.Bridge{}
	bridges.On("Delete", mock.Anything).Return(nil)
	bridges.On("RemoveChannel", mock.Anything, mock.Anything).Return(nil)
	// The caller hangs up during AddChannel's round trip.
	bridges.On("AddChannel", mock.Anything, "dest-1").Run(func(mock.Arguments) { m.teardown(c) }).Return(nil)
	c.bridge = ari.NewBridgeHandle(ari.NewKey(ari.BridgeKey, "b1"), bridges, nil)

	m.active[c.ID], m.active[c.ExternalID] = c, c

	err = m.connectTransfer(c, "dest-1")
	require.Error(t, err)

	mu.Lock()
	assert.Contains(t, hungUp, "dest-1", "the answered destination must be hung up")
	mu.Unlock()
	assert.Empty(t, m.active, "no call may stay registered")
	assert.Zero(t, ports.InUse())
}
