package ari

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSerial_OneKeyRunsInOrder(t *testing.T) {
	s := newSerial()

	var (
		mu  sync.Mutex
		got []int
	)

	for i := 0; i < 200; i++ {
		s.do("call", func() {
			if i%50 == 0 {
				time.Sleep(time.Millisecond) // a slow handler must not let later ones overtake it
			}

			mu.Lock()
			got = append(got, i)
			mu.Unlock()
		})
	}

	s.idle(5 * time.Second)

	require.Len(t, got, 200)

	for i, v := range got {
		assert.Equal(t, i, v)
	}
}

func TestSerial_DifferentKeysDoNotWaitForEachOther(t *testing.T) {
	s := newSerial()

	release := make(chan struct{})

	var done atomic.Int32

	s.do("slow", func() { <-release }) // one call stuck on a slow Asterisk request

	for i := 0; i < 50; i++ {
		s.do("call-"+string(rune('a'+i%26))+string(rune('a'+i/26)), func() { done.Add(1) })
	}

	require.Eventually(t, func() bool { return done.Load() == 50 }, 2*time.Second, time.Millisecond,
		"the other calls' events ran while one handler was blocked")

	close(release)
	s.idle(5 * time.Second)
}

func TestSerial_AKeyCanBeUsedAgainAfterItsQueueDrained(t *testing.T) {
	s := newSerial()

	var n atomic.Int32

	for round := 0; round < 3; round++ {
		s.do("k", func() { n.Add(1) })
		s.idle(5 * time.Second)
	}

	assert.EqualValues(t, 3, n.Load())
	assert.Empty(t, s.qs, "no goroutine or queue is left behind for a finished call")
}
