package config

import (
	"context"
	"log/slog"
	"os"

	"go.opentelemetry.io/contrib/bridges/otelslog"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploghttp"
	sdklog "go.opentelemetry.io/otel/sdk/log"
	"go.opentelemetry.io/otel/sdk/resource"

	"github.com/nitesh/vaani/internal/version"
)

// otelLogsHandler builds the OTLP log bridge handler described in
// ConfigureLogging's doc comment: nil, nil when OTEL_EXPORTER_OTLP_LOGS_ENDPOINT
// (and OTEL_EXPORTER_OTLP_ENDPOINT, which otlploghttp also honours) isn't set --
// every deployment that doesn't opt in gets today's stderr-only behaviour, at
// no cost. shutdown flushes the batch exporter; call it once, on process exit,
// after the last log line that should reach SigNoz.
func otelLogsHandler(ctx context.Context, env string) (handler slog.Handler, shutdown func(context.Context) error, err error) {
	if os.Getenv("OTEL_EXPORTER_OTLP_LOGS_ENDPOINT") == "" && os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT") == "" {
		return nil, func(context.Context) error { return nil }, nil
	}

	exporter, err := otlploghttp.New(ctx)
	if err != nil {
		return nil, nil, err
	}

	res, err := resource.New(ctx, resource.WithAttributes(
		attribute.String("service.name", ServiceName),
		attribute.String("service.version", version.Version),
		attribute.String("deployment.environment", env),
	))
	if err != nil {
		return nil, nil, err
	}

	provider := sdklog.NewLoggerProvider(
		sdklog.WithProcessor(sdklog.NewBatchProcessor(exporter)),
		sdklog.WithResource(res),
	)

	return otelslog.NewHandler(ServiceName, otelslog.WithLoggerProvider(provider)), provider.Shutdown, nil
}
