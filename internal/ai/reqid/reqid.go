// Package reqid finds the id a provider put on a response: the one its
// support asks for when a request went wrong.
package reqid

import "net/http"

// headers are the names providers and gateways commonly use, best first.
var headers = []string{"X-Request-Id", "Request-Id", "X-Correlation-Id", "Cf-Ray"}

// FromHeader returns the first request id header present, or "".
func FromHeader(h http.Header) string {
	for _, name := range headers {
		if v := h.Get(name); v != "" {
			return v
		}
	}

	return ""
}
