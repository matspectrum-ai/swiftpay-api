package observability

import (
	"context"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
)

var Tracer = otel.Tracer("swiftpay-pix-api")

func StartSpan(ctx context.Context, name string, attrs ...trace.SpanStartOption) (context.Context, trace.Span) {
	return Tracer.Start(ctx, name, attrs...)
}
