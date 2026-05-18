package main

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWebhookDurabilityAfterACK(t *testing.T) {
	ctx := context.Background()
	target := NewMemoryTarget(MemoryTargetConfig{Seed: 42})

	req := CreatePaymentRequest{MerchantID: "m1", IdempotencyKey: "dur-ack-1", AmountCents: 1500, PixKey: "ack@pix.com"}
	p, err := target.CreatePayment(ctx, req)
	require.NoError(t, err)

	ev := WebhookEvent{EventID: "evt-ack-1", ProviderRef: p.ProviderRef, PaymentStatus: StatusSettled, OccurredAt: time.Now().UTC()}
	err = target.HandleWebhook(ctx, ev)
	require.NoError(t, err)

	p2, err := target.GetPayment(ctx, p.ID)
	require.NoError(t, err)
	assert.Equal(t, StatusSettled, p2.Status, "webhook must persist before ACK")
}

func TestReplayDeterminism(t *testing.T) {
	ctx := context.Background()
	target := NewMemoryTarget(MemoryTargetConfig{Seed: 42})

	req := CreatePaymentRequest{MerchantID: "m1", IdempotencyKey: "replay-det-1", AmountCents: 2000, PixKey: "det@pix.com"}

	p1, err := target.CreatePayment(ctx, req)
	require.NoError(t, err)
	p2, err := target.CreatePayment(ctx, req)
	require.NoError(t, err)

	assert.Equal(t, p1.ID, p2.ID)
	assert.Equal(t, p1.Status, p2.Status)
}

func TestLeaderSplitBrainIsolation(t *testing.T) {
	ctx := context.Background()
	target1 := NewMemoryTarget(MemoryTargetConfig{Seed: 1})
	target2 := NewMemoryTarget(MemoryTargetConfig{Seed: 2})

	req1 := CreatePaymentRequest{MerchantID: "m1", IdempotencyKey: "leader-1", AmountCents: 1000, PixKey: "a@b.com"}
	req2 := CreatePaymentRequest{MerchantID: "m2", IdempotencyKey: "leader-2", AmountCents: 2000, PixKey: "c@d.com"}

	p1, err1 := target1.CreatePayment(ctx, req1)
	require.NoError(t, err1)
	p2, err2 := target2.CreatePayment(ctx, req2)
	require.NoError(t, err2)

	snap1 := target1.Snapshot()
	snap2 := target2.Snapshot()
	assert.Equal(t, 1, snap1.Payments, "target1 handles its own payment")
	assert.Equal(t, 1, snap2.Payments, "target2 handles its own payment")

	ev := WebhookEvent{EventID: "evt-split-1", ProviderRef: p1.ProviderRef, PaymentStatus: StatusSettled, OccurredAt: time.Now().UTC()}
	err := target1.HandleWebhook(ctx, ev)
	require.NoError(t, err)

	got1, _ := target1.GetPayment(ctx, p1.ID)
	assert.Equal(t, StatusSettled, got1.Status, "target1 processed webhook")

	got2, _ := target2.GetPayment(ctx, p2.ID)
	assert.Equal(t, StatusSubmitted, got2.Status, "target2 payment untouched by target1 webhook")
}

func TestRefundConsistency(t *testing.T) {
	ctx := context.Background()
	target := NewMemoryTarget(MemoryTargetConfig{Seed: 42})

	req := CreatePaymentRequest{MerchantID: "m1", IdempotencyKey: "ref-cons-1", AmountCents: 3000, PixKey: "ref@pix.com"}
	p, err := target.CreatePayment(ctx, req)
	require.NoError(t, err)

	ev := WebhookEvent{EventID: "evt-ref-1", ProviderRef: p.ProviderRef, PaymentStatus: StatusSettled, OccurredAt: time.Now().UTC()}
	err = target.HandleWebhook(ctx, ev)
	require.NoError(t, err)

	p2, _ := target.GetPayment(ctx, p.ID)
	assert.Equal(t, StatusSettled, p2.Status)
}
