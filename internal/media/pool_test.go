package media

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGetFrame_SizeAndReuse(t *testing.T) {
	f := GetFrame()
	assert.Len(t, *f, FrameSize)

	PutFrame(f)

	f2 := GetFrame()
	assert.Len(t, *f2, FrameSize)
	PutFrame(f2)
}

// BenchmarkFramePool_SteadyState must show 0 allocs/op: get-then-put in a tight
// loop should never touch the allocator once the pool is warm.
func BenchmarkFramePool_SteadyState(b *testing.B) {
	// Warm the pool.
	warm := GetFrame()
	PutFrame(warm)

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		f := GetFrame()
		PutFrame(f)
	}
}
