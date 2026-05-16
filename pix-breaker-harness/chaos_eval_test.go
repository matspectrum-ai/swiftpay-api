package main

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestChaosFlood(t *testing.T) {
	ctx := context.Background()
	target := NewMemoryTarget(MemoryTargetConfig{Seed: time.Now().UnixNano()})

	results := RunChaosSuite(ctx, target)
	for _, r := range results {
		t.Run(r.Scenario, func(t *testing.T) {
			assert.Equal(t, "PASS", r.Result, r.ObservedBehavior)
		})
	}
}

func TestChaosRecovery(t *testing.T) {
	ctx := context.Background()
	target := NewMemoryTarget(MemoryTargetConfig{Seed: 42})

	req := CreatePaymentRequest{MerchantID: "chaos-rec", IdempotencyKey: "chaos-rec-1", AmountCents: 1000, PixKey: "recovery@pix.com"}
	_, err := target.CreatePayment(ctx, req)
	assert.NoError(t, err)

	target.Reset()

	snap := target.Snapshot()
	assert.Equal(t, 0, snap.Payments)
}
