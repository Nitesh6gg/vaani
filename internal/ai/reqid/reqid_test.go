package reqid

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestFromHeader(t *testing.T) {
	tests := []struct {
		name string
		h    http.Header
		want string
	}{
		{"none", http.Header{}, ""},
		{"request id", http.Header{"X-Request-Id": {"abc"}}, "abc"},
		{"first name wins", http.Header{"Cf-Ray": {"ray"}, "X-Request-Id": {"abc"}}, "abc"},
		{"fallback", http.Header{"Cf-Ray": {"ray"}}, "ray"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, FromHeader(tt.h))
		})
	}
}
