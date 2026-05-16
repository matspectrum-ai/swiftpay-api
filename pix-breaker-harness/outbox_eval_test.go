package main

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestOutboxMessageWritten(t *testing.T) {
	ctx := context.Background()
	target := NewMemoryTarget(MemoryTargetConfig{Seed: 42})

	req := CreatePaymentRequest{MerchantID: "m1", IdempotencyKey: "outbox-001", AmountCents: 100, PixKey: "p@q.com"}

	_, err := target.CreatePayment(ctx, req)
	require.NoError(t, err)

	snap := target.Snapshot()
	assert.Equal(t, 1, snap.Payments, "payment must be created")
	assert.Equal(t, 1, snap.OutboxQueued, "outbox message must be queued")
}

func TestOutboxNoDuplicates(t *testing.T) {
	ctx := context.Background()
	target := NewMemoryTarget(MemoryTargetConfig{Seed: 42})

	req := CreatePaymentRequest{MerchantID: "m1", IdempotencyKey: "outbox-nodup-001", AmountCents: 200, PixKey: "r@s.com"}

	_, err := target.CreatePayment(ctx, req)
	require.NoError(t, err)

	_, err = target.CreatePayment(ctx, req)
	require.NoError(t, err)

	snap := target.Snapshot()
	assert.Equal(t, 1, snap.Payments, "only 1 payment")
	assert.Equal(t, 1, snap.OutboxQueued, "only 1 outbox message")
}
