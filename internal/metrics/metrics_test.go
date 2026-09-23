package metrics

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHealthHandler_AriConnected_Returns200(t *testing.T) {
	handler := healthHandler(func() bool { return true }, time.Now().Add(-5*time.Second))

	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	rec := httptest.NewRecorder()

	handler(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)

	var resp healthResponse
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	assert.Equal(t, "ok", resp.Status)
	assert.True(t, resp.AriConnected)
	assert.GreaterOrEqual(t, resp.UptimeSeconds, 5.0)
}

func TestHealthHandler_AriDisconnected_Returns503(t *testing.T) {
	handler := healthHandler(func() bool { return false }, time.Now())

	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	rec := httptest.NewRecorder()

	handler(rec, req)

	assert.Equal(t, http.StatusServiceUnavailable, rec.Code)

	var resp healthResponse
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	assert.False(t, resp.AriConnected)
	assert.NotEqual(t, "ok", resp.Status)
}

func TestHealthHandler_ReportsActiveCalls(t *testing.T) {
	CallsActive.Set(3)
	defer CallsActive.Set(0)

	handler := healthHandler(func() bool { return true }, time.Now())

	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	rec := httptest.NewRecorder()

	handler(rec, req)

	var resp healthResponse
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	assert.Equal(t, 3, resp.ActiveCalls)
}

func TestHealthHandler_ContentTypeIsJSON(t *testing.T) {
	handler := healthHandler(func() bool { return true }, time.Now())

	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	rec := httptest.NewRecorder()

	handler(rec, req)

	assert.Equal(t, "application/json", rec.Header().Get("Content-Type"))
}
