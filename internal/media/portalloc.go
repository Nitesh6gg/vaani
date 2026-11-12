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
	// free holds the unallocated ports, oldest-freed first: a returned port goes to the back, so
	// the call that just released it has the longest time to finish closing its socket before the
	// port is handed out again (an immediate reuse can fail to bind: "address already in use").
	free []int
}

// NewPortAllocator creates an allocator over [base, base+count).
func NewPortAllocator(base, count int) *PortAllocator {
	free := make([]int, count)
	for i := range free {
		free[i] = base + i
	}

	return &PortAllocator{
		base:  base,
		count: count,
		inUse: make(map[int]bool, count),
		free:  free,
	}
}

// Alloc reserves and returns a free port, or an error if the range is exhausted.
func (a *PortAllocator) Alloc() (int, error) {
	a.mu.Lock()
	defer a.mu.Unlock()

	if len(a.free) > 0 {
		port := a.free[0]
		a.free = a.free[1:]
		a.inUse[port] = true

		return port, nil
	}

	return 0, fmt.Errorf("media: no free ports in range %d-%d", a.base, a.base+a.count-1)
}

// Free releases a port back to the pool. Freeing a port not currently allocated is
// a no-op.
func (a *PortAllocator) Free(port int) {
	a.mu.Lock()
	defer a.mu.Unlock()

	if !a.inUse[port] {
		return
	}

	delete(a.inUse, port)
	a.free = append(a.free, port)
}

// InUse returns the number of currently-allocated ports.
func (a *PortAllocator) InUse() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return len(a.inUse)
}
