package main

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPSPFailureDoesNotCorruptState(t *testing.T) {
	ctx := context.Background()
	target := NewMemoryTarget(MemoryTargetConfig{Seed: 42, DemoBugs: true})

	req := CreatePaymentRequest{MerchantID: "m1", IdempotencyKey: "psp-fail-001", AmountCents: 1200, PixKey: "k1@l1.com"}
	p, err := target.CreatePayment(ctx, req)
	require.NoError(t, err)
	assert.NotEmpty(t, p.ID)

	p2, err := target.GetPayment(ctx, p.ID)
	require.NoError(t, err)
	assert.Equal(t, p.ID, p2.ID)
}

func TestInvalidPaymentRejected(t *testing.T) {
	ctx := context.Background()
	target := NewMemoryTarget(MemoryTargetConfig{Seed: 42})

	_, err := target.CreatePayment(ctx, CreatePaymentRequest{MerchantID: "", IdempotencyKey: "k1", AmountCents: 100, PixKey: "x"})
	assert.Error(t, err)

	_, err = target.CreatePayment(ctx, CreatePaymentRequest{MerchantID: "m1", IdempotencyKey: "k2", AmountCents: 0, PixKey: "x"})
	assert.Error(t, err)

	_, err = target.CreatePayment(ctx, CreatePaymentRequest{MerchantID: "m1", IdempotencyKey: "k3", AmountCents: 100, PixKey: ""})
	assert.Error(t, err)

	snap := target.Snapshot()
	assert.Equal(t, 0, snap.Payments, "no payments for invalid requests")
}
