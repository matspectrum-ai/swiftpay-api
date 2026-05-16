package domain

import "context"

type contextKey string

const (
	CtxKeyIdempotencyKey contextKey = "idempotency_key"
	CtxKeyEndpointPath   contextKey = "endpoint_path"
)

func IdempotencyKeyFromContext(ctx context.Context) string {
	if v, ok := ctx.Value(CtxKeyIdempotencyKey).(string); ok {
		return v
	}
	return ""
}

func EndpointPathFromContext(ctx context.Context) string {
	if v, ok := ctx.Value(CtxKeyEndpointPath).(string); ok {
		return v
	}
	return ""
}
