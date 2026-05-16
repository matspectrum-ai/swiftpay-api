package main

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWebhookDeduplication(t *testing.T) {
	ctx := context.Background()
	target := NewMemoryTarget(MemoryTargetConfig{Seed: 42})

	req := CreatePaymentRequest{MerchantID: "m1", IdempotencyKey: "wh-001", AmountCents: 300, PixKey: "t@u.com"}
	p, err := target.CreatePayment(ctx, req)
	require.NoError(t, err)

	ev := WebhookEvent{
		EventID:       "evt-001",
		ProviderRef:   p.ProviderRef,
		PaymentStatus: StatusSettled,
		OccurredAt:    time.Now(),
	}

	err = target.HandleWebhook(ctx, ev)
	require.NoError(t, err)

	err = target.HandleWebhook(ctx, ev)
	require.NoError(t, err)

	pAfter, err := target.GetPayment(ctx, p.ID)
	require.NoError(t, err)
	assert.Equal(t, StatusSettled, pAfter.Status, "duplicate webhook must not change state")
}

func TestWebhookOutOfOrder(t *testing.T) {
	ctx := context.Background()
	target := NewMemoryTarget(MemoryTargetConfig{Seed: 42})

	req := CreatePaymentRequest{MerchantID: "m1", IdempotencyKey: "wh-ooo-001", AmountCents: 400, PixKey: "v@x.com"}
	p, err := target.CreatePayment(ctx, req)
	require.NoError(t, err)

	ev1 := WebhookEvent{EventID: "evt-settled", ProviderRef: p.ProviderRef, PaymentStatus: StatusSettled, OccurredAt: time.Now()}
	err = target.HandleWebhook(ctx, ev1)
	require.NoError(t, err)

	ev2 := WebhookEvent{EventID: "evt-submitted", ProviderRef: p.ProviderRef, PaymentStatus: StatusSubmitted, OccurredAt: time.Now().Add(-1 * time.Hour)}
	err = target.HandleWebhook(ctx, ev2)
	require.NoError(t, err)

	pAfter, _ := target.GetPayment(ctx, p.ID)
	assert.Equal(t, StatusSettled, pAfter.Status, "terminal state must not regress")
}

func TestWebhookAfterCancel(t *testing.T) {
	ctx := context.Background()
	target := NewMemoryTarget(MemoryTargetConfig{Seed: 42})

	req := CreatePaymentRequest{MerchantID: "m1", IdempotencyKey: "wh-cancel-001", AmountCents: 500, PixKey: "y@z.com"}
	p, err := target.CreatePayment(ctx, req)
	require.NoError(t, err)

	err = target.Reconcile(ctx, p.ID)
	require.NoError(t, err)

	ev := WebhookEvent{EventID: "evt-late", ProviderRef: p.ProviderRef, PaymentStatus: StatusFailed, OccurredAt: time.Now()}
	err = target.HandleWebhook(ctx, ev)
	require.NoError(t, err)

	pAfter, _ := target.GetPayment(ctx, p.ID)
	assert.Equal(t, StatusSettled, pAfter.Status, "terminal state must protect against late webhooks")
}
