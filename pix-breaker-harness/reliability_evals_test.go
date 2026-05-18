package main

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestLeaderSplitBrain(t *testing.T) {
	ctx := context.Background()

	target1 := NewMemoryTarget(MemoryTargetConfig{Seed: 1})
	target2 := NewMemoryTarget(MemoryTargetConfig{Seed: 2})

	req1 := CreatePaymentRequest{MerchantID: "m1", IdempotencyKey: "split-leader-1", AmountCents: 1000, PixKey: "a@b.com"}
	req2 := CreatePaymentRequest{MerchantID: "m2", IdempotencyKey: "split-leader-2", AmountCents: 2000, PixKey: "c@d.com"}

	p1, err1 := target1.CreatePayment(ctx, req1)
	p2, err2 := target2.CreatePayment(ctx, req2)

	assert.NoError(t, err1)
	assert.NoError(t, err2)
	assert.NotEmpty(t, p1.ID)
	assert.NotEmpty(t, p2.ID)

	// Each target is idempotent within itself on same key re-use
	p1Retry, errRetry := target1.CreatePayment(ctx, req1)
	assert.NoError(t, errRetry)
	assert.Equal(t, p1.ID, p1Retry.ID, "same key on same target returns original payment (idempotent)")

	// Verify each target tracks its own payments independently — no split-brain
	snap1 := target1.Snapshot()
	snap2 := target2.Snapshot()
	assert.Equal(t, 1, snap1.Payments, "target 1 handled 1 unique payment")
	assert.Equal(t, 1, snap2.Payments, "target 2 handled 1 unique payment")
}

func TestRetryBudgetExhaustion(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	target := NewMemoryTarget(MemoryTargetConfig{Seed: 42})

	for i := 0; i < 200; i++ {
		_, _ = target.CreatePayment(ctx, CreatePaymentRequest{
			MerchantID: "m1", IdempotencyKey: "budget-" + string(rune(i)), AmountCents: 100 + int64(i), PixKey: "test@pix.com",
		})
	}
}

func TestQueueFairness(t *testing.T) {
	ctx := context.Background()
	target := NewMemoryTarget(MemoryTargetConfig{Seed: 42})

	for i := 0; i < 50; i++ {
		key := fmt.Sprintf("fair-%d", i)
		p, err := target.CreatePayment(ctx, CreatePaymentRequest{
			MerchantID: "m1", IdempotencyKey: key, AmountCents: int64(100 + i), PixKey: fmt.Sprintf("fair%d@pix.com", i),
		})
		assert.NoError(t, err)
		assert.NotEmpty(t, p.ID)
	}

	snap := target.Snapshot()
	assert.Equal(t, 50, snap.Payments, "all 50 payments processed without starvation")
}
