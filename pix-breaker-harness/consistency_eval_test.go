package main

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStateTransitionConsistency(t *testing.T) {
	ctx := context.Background()
	target := NewMemoryTarget(MemoryTargetConfig{Seed: 42})

	req := CreatePaymentRequest{MerchantID: "m1", IdempotencyKey: "state-001", AmountCents: 900, PixKey: "g1@h1.com"}
	p, err := target.CreatePayment(ctx, req)
	require.NoError(t, err)

	assert.Equal(t, StatusSubmitted, p.Status, "initial status must be SUBMITTED")

	p2, err := target.CreatePayment(ctx, req)
	require.NoError(t, err)
	assert.Equal(t, p.ID, p2.ID)
	assert.Equal(t, p.Status, p2.Status)
}

func TestTerminalStateImmutability(t *testing.T) {
	ctx := context.Background()
	target := NewMemoryTarget(MemoryTargetConfig{Seed: 42})

	req := CreatePaymentRequest{MerchantID: "m1", IdempotencyKey: "terminal-001", AmountCents: 1100, PixKey: "i1@j1.com"}
	p, err := target.CreatePayment(ctx, req)
	require.NoError(t, err)

	err = target.Reconcile(ctx, p.ID)
	require.NoError(t, err)

	pAfter, _ := target.GetPayment(ctx, p.ID)
	assert.Equal(t, StatusSettled, pAfter.Status)
	assert.True(t, pAfter.Status.Terminal(), "SETTLED is terminal")
}
