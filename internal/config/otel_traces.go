package config

import (
	"context"
	crand "crypto/rand"
	"os"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"

	"github.com/nitesh/vaani/internal/version"
)

// otelTraces installs the global tracer provider when the OTLP endpoint is
// set (the same variables the log export uses); without them spans are no-ops
// and this costs nothing. shutdown flushes the batch exporter.
func otelTraces(ctx context.Context, env string) (shutdown func(context.Context) error, err error) {
	if os.Getenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT") == "" && os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT") == "" {
		return func(context.Context) error { return nil }, nil
	}

	exporter, err := otlptracehttp.New(ctx)
	if err != nil {
		return nil, err
	}

	res, err := resource.New(ctx, resource.WithAttributes(
		attribute.String("service.name", ServiceName),
		attribute.String("service.version", version.Version),
		attribute.String("deployment.environment", env),
	))
	if err != nil {
		return nil, err
	}

	provider := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter),
		sdktrace.WithResource(res),
		sdktrace.WithIDGenerator(callIDs{}),
	)
	otel.SetTracerProvider(provider)

	return provider.Shutdown, nil
}

type callTraceKey struct{}

// callIDs gives a call's root span the call's own trace id (the one its log
// lines carry as trace_id), so a log line and the call's trace find each
// other; every other root span gets a random one.
type callIDs struct{}

func (callIDs) NewIDs(ctx context.Context) (trace.TraceID, trace.SpanID) {
	tid, ok := ctx.Value(callTraceKey{}).(trace.TraceID)
	if !ok {
		_, _ = crand.Read(tid[:])
	}

	return tid, callIDs{}.NewSpanID(ctx, tid)
}

func (callIDs) NewSpanID(context.Context, trace.TraceID) trace.SpanID {
	var sid trace.SpanID

	for sid == (trace.SpanID{}) { // all-zero is not a valid span id
		_, _ = crand.Read(sid[:])
	}

	return sid
}

// StartCallSpan starts the root span of call callID, in the call's trace (see
// callIDs). Spans started from the returned context, like each LLM request,
// become its children. End the span when the call's agent has finished.
func StartCallSpan(ctx context.Context, callID string) (context.Context, trace.Span) {
	if tid, err := trace.TraceIDFromHex(lookupTrace(callID)); err == nil {
		ctx = context.WithValue(ctx, callTraceKey{}, tid)
	}

	return otel.Tracer(ServiceName).Start(ctx, "call", trace.WithAttributes(attribute.String("call_id", callID)))
}
