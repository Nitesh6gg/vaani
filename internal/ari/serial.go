package ari

import (
	"sync"
	"time"
)

// serial runs functions one after another per key and in parallel across keys.
// The Stasis event loop only hands events to it: the handlers make blocking
// calls to Asterisk (answer, externalMedia, bridge, hangups), and when they ran
// inline on the one loop a burst of calls queued behind each other, and the
// ARI library silently dropped the events past its 100-event buffer. Events of
// one call stay in order (a call's key is its caller channel's ID); different
// calls no longer wait for each other.
type serial struct {
	mu   sync.Mutex
	qs   map[string][]func() // a key is present while a goroutine is draining it
	wait sync.WaitGroup
}

func newSerial() *serial { return &serial{qs: map[string][]func(){}} }

// do queues fn under key; a goroutine drains the key's queue and ends when it is empty.
func (s *serial) do(key string, fn func()) {
	s.mu.Lock()
	q, running := s.qs[key]
	s.qs[key] = append(q, fn)
	s.mu.Unlock()

	if running {
		return
	}

	s.wait.Add(1)

	go func() {
		defer s.wait.Done()

		for {
			s.mu.Lock()

			q := s.qs[key]
			if len(q) == 0 {
				delete(s.qs, key)
				s.mu.Unlock()

				return
			}

			next := q[0]
			s.qs[key] = q[1:]
			s.mu.Unlock()

			next()
		}
	}()
}

// idle waits up to d for every queued function to have run.
func (s *serial) idle(d time.Duration) {
	done := make(chan struct{})

	go func() { s.wait.Wait(); close(done) }()

	select {
	case <-done:
	case <-time.After(d):
	}
}
