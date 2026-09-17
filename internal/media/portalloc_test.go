package media

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPortAllocator_AllocIsUniqueAndInRange(t *testing.T) {
	a := NewPortAllocator(20000, 4)

	seen := make(map[int]bool)
	for i := 0; i < 4; i++ {
		p, err := a.Alloc()
		require.NoError(t, err)
		assert.GreaterOrEqual(t, p, 20000)
		assert.Less(t, p, 20004)
		assert.False(t, seen[p], "port %d allocated twice while both were in use", p)
		seen[p] = true
	}

	assert.Equal(t, 4, a.InUse())
}

func TestPortAllocator_ExhaustionErrors(t *testing.T) {
	a := NewPortAllocator(20000, 2)

	_, err := a.Alloc()
	require.NoError(t, err)
	_, err = a.Alloc()
	require.NoError(t, err)

	_, err = a.Alloc()
	assert.Error(t, err, "allocating beyond the configured range must fail")
}

func TestPortAllocator_FreeThenAllocReuses(t *testing.T) {
	a := NewPortAllocator(20000, 1)

	p1, err := a.Alloc()
	require.NoError(t, err)

	_, err = a.Alloc()
	require.Error(t, err, "single-port range must be exhausted after one alloc")

	a.Free(p1)
	assert.Equal(t, 0, a.InUse())

	p2, err := a.Alloc()
	require.NoError(t, err)
	assert.Equal(t, p1, p2, "freeing the only port must make it available again")
}

func TestPortAllocator_ConcurrentAllocNeverDoubleAllocates(t *testing.T) {
	const (
		count      = 64
		goroutines = 16
	)

	a := NewPortAllocator(20000, count)

	results := make(chan int, count)

	for g := 0; g < goroutines; g++ {
		go func() {
			for {
				p, err := a.Alloc()
				if err != nil {
					return
				}
				results <- p
			}
		}()
	}

	seen := make(map[int]bool)
	for i := 0; i < count; i++ {
		p := <-results
		require.False(t, seen[p], "port %d was double-allocated", p)
		seen[p] = true
	}

	assert.Len(t, seen, count)
}
