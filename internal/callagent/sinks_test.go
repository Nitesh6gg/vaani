package callagent

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/nitesh/vaani/internal/ai/agent"
)

// The FreeSWITCH manager clears earshot's queued audio through this hook;
// without the hook (Asterisk) the events go where they always did.
func TestSinksCallInterruptedOnPause(t *testing.T) {
	var calls int
	log := &agent.CallLog{}
	s := sinks(log, Hooks{Interrupted: func() { calls++ }})

	s.InterruptionPaused()
	s.BargeIn() // a confirmed cut is not a second pause
	assert.Equal(t, 1, calls)
	assert.Equal(t, 1, log.Summary().Pauses, "the call's own log still counts it")

	assert.NotPanics(t, func() { sinks(&agent.CallLog{}, Hooks{}).InterruptionPaused() })
}
