package main

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestReplayIdempotency(t *testing.T) {
	ctx := context.Background()
	target := NewMemoryTarget(MemoryTargetConfig{Seed: 42})

	req := CreatePaymentRequest{MerchantID: "m1", IdempotencyKey: "replay-001", AmountCents: 500, PixKey: "x@y.com"}

	p1, err := target.CreatePayment(ctx, req)
	require.NoError(t, err)

	p2, err := target.CreatePayment(ctx, req)
	require.NoError(t, err)

	assert.Equal(t, p1.ID, p2.ID, "replay must return same payment ID")
	assert.Equal(t, p1.Status, p2.Status, "replay must return same status")
}

func TestReplayAfterCrash(t *testing.T) {
	ctx := context.Background()
	target := NewMemoryTarget(MemoryTargetConfig{Seed: 42})

	req := CreatePaymentRequest{MerchantID: "m1", IdempotencyKey: "crash-replay-001", AmountCents: 750, PixKey: "z@w.com"}

	p1, err := target.CreatePayment(ctx, req)
	require.NoError(t, err)

	target.Reset()

	p2, err := target.CreatePayment(ctx, req)
	require.NoError(t, err)

	assert.Equal(t, p1.ID, p2.ID, "memory mode: after reset, ID counter restarts — same ID assigned, idempotency lost")
}
