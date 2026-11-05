package config

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
)

func TestCallIDs_UsesTheCallsTraceID(t *testing.T) {
	traceID := NewTraceID()
	want, err := trace.TraceIDFromHex(traceID)
	require.NoError(t, err)

	ctx := context.WithValue(context.Background(), callTraceKey{}, want)
	got, sid := callIDs{}.NewIDs(ctx)

	assert.Equal(t, want, got)
	assert.True(t, sid.IsValid())

	other, _ := callIDs{}.NewIDs(context.Background())
	assert.True(t, other.IsValid())
	assert.NotEqual(t, want, other)
}

func TestStartCallSpan_NoExporterIsHarmless(t *testing.T) {
	RegisterCallTrace("c1", NewTraceID())
	defer UnregisterCallTrace("c1")

	ctx, span := StartCallSpan(context.Background(), "c1")
	assert.NotNil(t, ctx)
	span.End()
}

func TestStartCallSpan_SpanCarriesTheCallsTraceID(t *testing.T) {
	rec := tracetest.NewSpanRecorder()
	prev := otel.GetTracerProvider()
	otel.SetTracerProvider(sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(rec), sdktrace.WithIDGenerator(callIDs{})))

	defer otel.SetTracerProvider(prev)

	traceID := NewTraceID()
	RegisterCallTrace("c2", traceID)

	defer UnregisterCallTrace("c2")

	ctx, span := StartCallSpan(context.Background(), "c2")
	_, child := otel.Tracer("t").Start(ctx, "child")
	child.End()
	span.End()

	assert.Equal(t, traceID, span.SpanContext().TraceID().String())
	assert.Equal(t, traceID, child.SpanContext().TraceID().String())
	assert.Len(t, rec.Ended(), 2)
}
