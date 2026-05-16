package main

import (
	"context"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestConcurrencyIdempotency(t *testing.T) {
	ctx := context.Background()
	target := NewMemoryTarget(MemoryTargetConfig{Seed: 42})

	req := CreatePaymentRequest{
		MerchantID:     "merchant-001",
		IdempotencyKey: "idem-concurrency-001",
		AmountCents:    1000,
		PixKey:         "test@pix.com",
	}

	const workers = 100
	ids := make(map[string]int)
	var mu sync.Mutex
	var wg sync.WaitGroup

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			p, err := target.CreatePayment(ctx, req)
			if err != nil {
				return
			}
			mu.Lock()
			ids[p.ID]++
			mu.Unlock()
		}()
	}
	wg.Wait()

	assert.Equal(t, 1, len(ids), "concurrent requests must produce exactly 1 payment ID")
}

func TestConcurrencyDivergentPayload(t *testing.T) {
	ctx := context.Background()
	target := NewMemoryTarget(MemoryTargetConfig{Seed: 42})

	req1 := CreatePaymentRequest{MerchantID: "m1", IdempotencyKey: "key-1", AmountCents: 1000, PixKey: "a@b.com"}
	req2 := CreatePaymentRequest{MerchantID: "m1", IdempotencyKey: "key-1", AmountCents: 2000, PixKey: "a@b.com"}

	p1, err1 := target.CreatePayment(ctx, req1)
	p2, err2 := target.CreatePayment(ctx, req2)

	assert.NoError(t, err1, "first request should succeed")
	assert.Error(t, err2, "second request with divergent payload should fail")
	assert.NotEmpty(t, p1.ID)
	assert.Empty(t, p2.ID)
}
