package main

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCrashRecoveryNoStateGrowth(t *testing.T) {
	ctx := context.Background()
	target := NewMemoryTarget(MemoryTargetConfig{Seed: 42})

	req := CreatePaymentRequest{MerchantID: "m1", IdempotencyKey: "crash-001", AmountCents: 600, PixKey: "a1@b1.com"}
	_, err := target.CreatePayment(ctx, req)
	assert.NoError(t, err)

	snap1 := target.Snapshot()
	target.Reset()
	snap2 := target.Snapshot()

	assert.Equal(t, 1, snap1.Payments, "pre-crash: 1 payment")
	assert.Equal(t, 0, snap2.Payments, "post-reset: 0 payments (memory mode)")
	assert.Equal(t, 1, snap1.OutboxQueued, "pre-crash: outbox message queued")
}

func TestRecoveryPreservesInvariants(t *testing.T) {
	ctx := context.Background()
	target := NewMemoryTarget(MemoryTargetConfig{Seed: 42})

	req := CreatePaymentRequest{MerchantID: "m1", IdempotencyKey: "recovery-001", AmountCents: 700, PixKey: "c1@d1.com"}
	p1, err := target.CreatePayment(ctx, req)
	assert.NoError(t, err)

	target.Reset()

	p2, err := target.CreatePayment(ctx, req)
	assert.NoError(t, err)

	assert.Equal(t, p1.ID, p2.ID, "memory mode: after reset, ID counter restarts at 1 — same ID assigned but different payment")
}
