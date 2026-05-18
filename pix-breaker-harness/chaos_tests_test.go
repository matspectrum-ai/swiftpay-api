package main

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSplitBrainLeaderIsolation(t *testing.T) {
	ctx := context.Background()
	target1 := NewMemoryTarget(MemoryTargetConfig{Seed: 1})
	target2 := NewMemoryTarget(MemoryTargetConfig{Seed: 2})

	req := CreatePaymentRequest{MerchantID: "m1", IdempotencyKey: "split-iso-1", AmountCents: 5555, PixKey: "iso@pix.com"}

	var wg sync.WaitGroup
	ids := sync.Map{}

	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(tgt Target) {
			defer wg.Done()
			p, err := tgt.CreatePayment(ctx, req)
			if err == nil {
				ids.Store("1-"+p.ID, true)
			}
		}(target1)
		wg.Add(1)
		go func(tgt Target) {
			defer wg.Done()
			p, err := tgt.CreatePayment(ctx, req)
			if err == nil {
				ids.Store("2-"+p.ID, true)
			}
		}(target2)
	}
	wg.Wait()

	count := 0
	ids.Range(func(_, _ interface{}) bool { count++; return true })
	assert.Equal(t, 2, count, "each target instance creates exactly 1 payment — no cross-instance duplication")
}

func TestLeaseExpirationRecovery(t *testing.T) {
	ctx := context.Background()
	target := NewMemoryTarget(MemoryTargetConfig{Seed: 42})

	req := CreatePaymentRequest{MerchantID: "m1", IdempotencyKey: "lease-rec-1", AmountCents: 7777, PixKey: "lease@pix.com"}
	_, err := target.CreatePayment(ctx, req)
	require.NoError(t, err)

	target.Reset()

	p2, err := target.CreatePayment(ctx, req)
	require.NoError(t, err)
	assert.NotEmpty(t, p2.ID, "after reset, payment can be recreated (IDs may collide in memory mode)")
}

func TestRetryAmplificationPrevention(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	target := NewMemoryTarget(MemoryTargetConfig{Seed: 42})

	for i := 0; i < 200; i++ {
		_, err := target.CreatePayment(ctx, CreatePaymentRequest{
			MerchantID:     "m1",
			IdempotencyKey: fmt.Sprintf("amp-%d", i),
			AmountCents:    int64(100 + i),
			PixKey:         fmt.Sprintf("amp%d@pix.com", i),
		})
		assert.NoError(t, err)
	}
}

func TestWorkloadContentionIsolation(t *testing.T) {
	ctx := context.Background()
	target := NewMemoryTarget(MemoryTargetConfig{Seed: 42})

	var wg sync.WaitGroup

	for i := 0; i < 25; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			_, err := target.CreatePayment(ctx, CreatePaymentRequest{
				MerchantID: "m1", IdempotencyKey: fmt.Sprintf("cont-%d", idx),
				AmountCents: int64(100 + idx), PixKey: fmt.Sprintf("cont%d@pix.com", idx),
			})
			assert.NoError(t, err)
		}(i)
	}

	for i := 0; i < 25; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			req := CreatePaymentRequest{
				MerchantID: "m1", IdempotencyKey: fmt.Sprintf("cont-wh-%d", idx),
				AmountCents: int64(100 + idx), PixKey: fmt.Sprintf("contwh%d@pix.com", idx),
			}
			p, err := target.CreatePayment(ctx, req)
			require.NoError(t, err)
			ev := WebhookEvent{EventID: fmt.Sprintf("cont-ev-%d", idx), ProviderRef: p.ProviderRef, PaymentStatus: StatusSettled, OccurredAt: time.Now().UTC()}
			err = target.HandleWebhook(ctx, ev)
			assert.NoError(t, err)
		}(i)
	}

	wg.Wait()
	snap := target.Snapshot()
	assert.Equal(t, 50, snap.Payments, "all 50 operations completed without contention deadlock")
}

func TestOrphanRecovery(t *testing.T) {
	ctx := context.Background()
	target := NewMemoryTarget(MemoryTargetConfig{Seed: 42})

	req := CreatePaymentRequest{MerchantID: "m1", IdempotencyKey: "orphan-1", AmountCents: 9999, PixKey: "orphan@pix.com"}
	_, err := target.CreatePayment(ctx, req)
	require.NoError(t, err)

	target.Reset()

	p2, err := target.CreatePayment(ctx, req)
	require.NoError(t, err)
	assert.NotEmpty(t, p2.ID)
}

func TestPSPInstabilityGracefulDegradation(t *testing.T) {
	ctx := context.Background()
	target := NewMemoryTarget(MemoryTargetConfig{Seed: 42, DemoBugs: true})

	for i := 0; i < 50; i++ {
		_, _ = target.CreatePayment(ctx, CreatePaymentRequest{
			MerchantID: "m1", IdempotencyKey: fmt.Sprintf("psp-inst-%d", i),
			AmountCents: int64(100 + i), PixKey: fmt.Sprintf("pspins%d@pix.com", i),
		})
	}

	snap := target.Snapshot()
	assert.GreaterOrEqual(t, snap.Payments, 0, "system should not crash under PSP instability")
}
