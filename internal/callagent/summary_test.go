package callagent

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPercentiles(t *testing.T) {
	cases := []struct {
		name     string
		in       []int64
		p50, p95 int64
		ok       bool
	}{
		{"none", nil, 0, 0, false},
		{"one", []int64{700}, 700, 700, true},
		{"unsorted", []int64{900, 400, 1200, 500, 600}, 600, 1200, true},
		{"twenty", []int64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20}, 10, 19, true},
	}

	for _, tc := range cases {
		in := append([]int64(nil), tc.in...)
		p50, p95, ok := percentiles(in)

		assert.Equal(t, tc.ok, ok, tc.name)
		assert.Equal(t, tc.p50, p50, tc.name)
		assert.Equal(t, tc.p95, p95, tc.name)
		assert.Equal(t, tc.in, in, "%s: the input must not be reordered", tc.name)
	}
}
