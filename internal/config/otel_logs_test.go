package config

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestOtelLogsHandlerUnset: the default, no-OTEL_EXPORTER_OTLP_*_ENDPOINT
// deployment gets no handler at all and a no-op shutdown -- behaviour must
// stay unchanged (stderr only) unless an operator opts in.
func TestOtelLogsHandlerUnset(t *testing.T) {
	t.Setenv("OTEL_EXPORTER_OTLP_LOGS_ENDPOINT", "")
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "")

	handler, shutdown, err := otelLogsHandler(context.Background(), "test")
	require.NoError(t, err)
	assert.Nil(t, handler)
	require.NotNil(t, shutdown)
	assert.NoError(t, shutdown(context.Background()))
}

// TestOtelLogsHandlerConfigured: with the endpoint env var set (as a Spring
// Boot app's otlp.logging.endpoint would be), a handler and a working
// shutdown are built -- otlploghttp.New doesn't dial out until something is
// actually exported, so this doesn't need a live collector.
func TestOtelLogsHandlerConfigured(t *testing.T) {
	t.Setenv("OTEL_EXPORTER_OTLP_LOGS_ENDPOINT", "http://127.0.0.1:4318/v1/logs")

	handler, shutdown, err := otelLogsHandler(context.Background(), "test")
	require.NoError(t, err)
	assert.NotNil(t, handler)
	require.NotNil(t, shutdown)
	assert.NoError(t, shutdown(context.Background()))
}
