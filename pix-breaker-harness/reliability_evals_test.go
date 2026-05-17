package main

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestLeaderSplitBrain(t *testing.T) {
	ctx := context.Background()

	target1 := NewMemoryTarget(MemoryTargetConfig{Seed: 1})
	target2 := NewMemoryTarget(MemoryTargetConfig{Seed: 2})

	req := CreatePaymentRequest{MerchantID: "m1", IdempotencyKey: "split-1", AmountCents: 1000, PixKey: "a@b.com"}

	var wg sync.WaitGroup
	ids := sync.Map{}

	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func(tgt Target) {
			defer wg.Done()
			p, err := tgt.CreatePayment(ctx, req)
			if err == nil {
				ids.Store(p.ID, true)
			}
		}(target1)
		wg.Add(1)
		go func(tgt Target) {
			defer wg.Done()
			p, err := tgt.CreatePayment(ctx, req)
			if err == nil {
				ids.Store(p.ID, true)
			}
		}(target2)
	}
	wg.Wait()

	count := 0
	ids.Range(func(_, _ interface{}) bool { count++; return true })
	assert.Equal(t, 2, count, "each target creates exactly 1 payment")
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
