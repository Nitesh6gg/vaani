package session

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCall_TeardownRunsExactlyOnce(t *testing.T) {
	c := New("caller1", "external1", 20000, Info{}, context.Background())

	var count atomic.Int32

	c.Teardown(func() { count.Add(1) })
	c.Teardown(func() { count.Add(1) })
	c.Teardown(func() { count.Add(1) })

	assert.Equal(t, int32(1), count.Load())
	assert.Equal(t, StateTornDown, c.State())
	assert.ErrorIs(t, c.Ctx.Err(), context.Canceled)
}

// TestCall_TeardownRunsExactlyOnceConcurrently is the regression test for the
// exact Phase 1 bug class: multiple goroutines (representing e.g. the caller's
// StasisEnd and the externalMedia channel's own StasisEnd arriving at nearly the
// same time) racing to tear down the same call must never double-run cleanup,
// which is what would have double-observed (or, with a differently-shaped race,
// zero-observed) vaani_call_duration_seconds in the old code.
func TestCall_TeardownRunsExactlyOnceConcurrently(t *testing.T) {
	c := New("caller1", "external1", 20000, Info{}, context.Background())

	var count atomic.Int32

	const goroutines = 50

	var wg sync.WaitGroup

	wg.Add(goroutines)

	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			c.Teardown(func() { count.Add(1) })
		}()
	}

	wg.Wait()

	assert.Equal(t, int32(1), count.Load(), "teardown action must run exactly once under concurrent callers")
}

func TestCall_InfoIsPreserved(t *testing.T) {
	info := Info{CallerNumber: "+15551234567", CalledNumber: "1001", Direction: "inbound"}
	c := New("caller1", "external1", 20000, info, context.Background())

	assert.Equal(t, info, c.Info)
}

func TestCall_StateTransitions(t *testing.T) {
	c := New("caller1", "external1", 20000, Info{}, context.Background())
	assert.Equal(t, StateNew, c.State())

	c.SetState(StateStaged)
	assert.Equal(t, StateStaged, c.State())

	c.SetState(StateBridged)
	c.SetState(StateMediaActive)
	assert.Equal(t, StateMediaActive, c.State())

	c.Teardown(func() {})
	assert.Equal(t, StateTornDown, c.State())
}
