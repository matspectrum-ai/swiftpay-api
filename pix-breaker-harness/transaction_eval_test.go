package main

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTransactionRollback(t *testing.T) {
	ctx := context.Background()
	target := NewMemoryTarget(MemoryTargetConfig{Seed: 42})

	req := CreatePaymentRequest{MerchantID: "", IdempotencyKey: "bad-001", AmountCents: 0, PixKey: ""}
	_, err := target.CreatePayment(ctx, req)
	assert.Error(t, err, "invalid request must fail")

	snap := target.Snapshot()
	assert.Equal(t, 0, snap.Payments, "no payment should be created for invalid request")
	assert.Equal(t, 0, snap.OutboxQueued, "no outbox message for invalid request")
}

func TestPartialFailureCleanup(t *testing.T) {
	ctx := context.Background()
	target := NewMemoryTarget(MemoryTargetConfig{Seed: 42})

	req := CreatePaymentRequest{MerchantID: "m1", IdempotencyKey: "partial-001", AmountCents: 800, PixKey: "e1@f1.com"}
	_, err := target.CreatePayment(ctx, req)
	assert.NoError(t, err)

	snap := target.Snapshot()
	assert.Equal(t, 1, snap.Payments)
	assert.Equal(t, 1, snap.OutboxQueued, "outbox must be consistent with payment count")
}
