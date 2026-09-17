package media

import (
	"fmt"
	"sync"
)

// PortAllocator hands out UDP ports from a fixed, configured range. One port is
// bound to exactly one call's UDP socket at a time.
type PortAllocator struct {
	mu    sync.Mutex
	base  int
	count int
	inUse map[int]bool
}

// NewPortAllocator creates an allocator over [base, base+count).
func NewPortAllocator(base, count int) *PortAllocator {
	return &PortAllocator{
		base:  base,
		count: count,
		inUse: make(map[int]bool, count),
	}
}

// Alloc reserves and returns a free port, or an error if the range is exhausted.
func (a *PortAllocator) Alloc() (int, error) {
	a.mu.Lock()
	defer a.mu.Unlock()

	for i := 0; i < a.count; i++ {
		port := a.base + i
		if !a.inUse[port] {
			a.inUse[port] = true
			return port, nil
		}
	}

	return 0, fmt.Errorf("media: no free ports in range %d-%d", a.base, a.base+a.count-1)
}

// Free releases a port back to the pool. Freeing a port not currently allocated is
// a no-op.
func (a *PortAllocator) Free(port int) {
	a.mu.Lock()
	defer a.mu.Unlock()
	delete(a.inUse, port)
}

// InUse returns the number of currently-allocated ports.
func (a *PortAllocator) InUse() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return len(a.inUse)
}
