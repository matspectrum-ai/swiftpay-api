package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"time"
)

type MemoryTargetConfig struct {
	DemoBugs bool
	Seed     int64
}

type MemoryTarget struct {
	mu          sync.Mutex
	rng         *rand.Rand
	demoBugs    bool
	nextID      int64
	payments    map[string]*Payment
	idem        map[string]string
	webhookSeen map[string]struct{}
	outbox      []string
	processed   map[string]struct{}
}

func NewMemoryTarget(cfg MemoryTargetConfig) *MemoryTarget {
	return &MemoryTarget{
		rng:         rand.New(rand.NewSource(cfg.Seed)),
		demoBugs:    cfg.DemoBugs,
		payments:    map[string]*Payment{},
		idem:        map[string]string{},
		webhookSeen: map[string]struct{}{},
		processed:   map[string]struct{}{},
	}
}

func (m *MemoryTarget) CreatePayment(ctx context.Context, req CreatePaymentRequest) (Payment, error) {
	if req.MerchantID == "" || req.IdempotencyKey == "" || req.AmountCents <= 0 || req.PixKey == "" {
		return Payment{}, errors.New("invalid payment request")
	}
	hash := hashRequest(req)

	if m.demoBugs && m.rng.Intn(20) == 0 {
		m.mu.Lock()
		defer m.mu.Unlock()
		m.nextID++
		id := fmt.Sprintf("pay-%d", m.nextID)
		now := time.Now().UTC()
		p := &Payment{
			ID:             id,
			MerchantID:     req.MerchantID,
			IdempotencyKey: req.IdempotencyKey,
			IdempotencyHash: hash,
			AmountCents:    req.AmountCents,
			PixKey:         req.PixKey,
			Description:    req.Description,
			Status:         StatusCreated,
			ProviderRef:    fmt.Sprintf("prov-%s", id),
			Version:        1,
			CreatedAt:      now,
			UpdatedAt:      now,
		}
		m.payments[id] = p
		return *clonePayment(p), nil
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	if existingID, ok := m.idem[idemKey(req.MerchantID, req.IdempotencyKey)]; ok {
		existing := m.payments[existingID]
		if existing == nil {
			return Payment{}, errors.New("idempotency reference missing")
		}
		if existing.IdempotencyHash != hash {
			return Payment{}, errors.New("idempotency key reused with divergent payload")
		}
		return *clonePayment(existing), nil
	}

	m.nextID++
	id := fmt.Sprintf("pay-%d", m.nextID)
	now := time.Now().UTC()
	p := &Payment{
		ID:             id,
		MerchantID:     req.MerchantID,
		IdempotencyKey: req.IdempotencyKey,
		IdempotencyHash: hash,
		AmountCents:    req.AmountCents,
		PixKey:         req.PixKey,
		Description:    req.Description,
		Status:         StatusSubmissionPending,
		ProviderRef:    fmt.Sprintf("prov-%s", id),
		Version:        1,
		CreatedAt:      now,
		UpdatedAt:      now,
	}
	if err := transitionPayment(p, StatusSubmitted); err != nil {
		return Payment{}, err
	}
	m.payments[id] = p
	m.idem[idemKey(req.MerchantID, req.IdempotencyKey)] = id
	m.outbox = append(m.outbox, id)
	return *clonePayment(p), nil
}

func (m *MemoryTarget) GetPayment(ctx context.Context, paymentID string) (Payment, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	p, ok := m.payments[paymentID]
	if !ok {
		return Payment{}, errors.New("payment not found")
	}
	return *clonePayment(p), nil
}

func (m *MemoryTarget) HandleWebhook(ctx context.Context, ev WebhookEvent) error {
	if ev.EventID == "" || ev.ProviderRef == "" {
		return errors.New("invalid webhook event")
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	if _, seen := m.webhookSeen[ev.EventID]; seen && !m.demoBugs {
		return nil
	}
	m.webhookSeen[ev.EventID] = struct{}{}

	p := m.findByProviderRef(ev.ProviderRef)
	if p == nil {
		return errors.New("provider reference not found")
	}

	if p.Status.Terminal() && !m.demoBugs {
		return nil
	}
	return transitionPayment(p, ev.PaymentStatus)
}

func (m *MemoryTarget) Reconcile(ctx context.Context, paymentID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	p, ok := m.payments[paymentID]
	if !ok {
		return errors.New("payment not found")
	}
	if p.Status.Terminal() && !m.demoBugs {
		return nil
	}
	return transitionPayment(p, StatusSettled)
}

func (m *MemoryTarget) Reset() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.payments = map[string]*Payment{}
	m.idem = map[string]string{}
	m.webhookSeen = map[string]struct{}{}
	m.outbox = nil
	m.processed = map[string]struct{}{}
	m.nextID = 0
}

func (m *MemoryTarget) Snapshot() TargetSnapshot {
	m.mu.Lock()
	defer m.mu.Unlock()
	return TargetSnapshot{
		Payments:        len(m.payments),
		WebhooksSeen:    len(m.webhookSeen),
		OutboxQueued:    len(m.outbox),
		OutboxProcessed: len(m.processed),
	}
}

func (m *MemoryTarget) findByProviderRef(ref string) *Payment {
	for _, p := range m.payments {
		if p.ProviderRef == ref {
			return p
		}
	}
	return nil
}

func hashRequest(req CreatePaymentRequest) string {
	sum := sha256.Sum256([]byte(fmt.Sprintf("%s|%s|%d|%s|%s", req.MerchantID, req.IdempotencyKey, req.AmountCents, req.PixKey, req.Description)))
	return hex.EncodeToString(sum[:])
}

func idemKey(merchantID, key string) string {
	return merchantID + "::" + key
}

func clonePayment(p *Payment) *Payment {
	if p == nil {
		return nil
	}
	cp := *p
	return &cp
}

func transitionPayment(p *Payment, next PaymentStatus) error {
	if p == nil {
		return errors.New("nil payment")
	}
	if p.Status == next {
		return nil
	}
	if p.Status.Terminal() && !next.Terminal() {
		return errors.New("terminal state regression blocked")
	}
	switch p.Status {
	case StatusCreated:
		if next != StatusSubmissionPending && next != StatusSubmitted {
			return fmt.Errorf("invalid transition %s -> %s", p.Status, next)
		}
	case StatusSubmissionPending:
		if next != StatusSubmitted && next != StatusRejected && next != StatusFailed {
			return fmt.Errorf("invalid transition %s -> %s", p.Status, next)
		}
	case StatusSubmitted:
		if next != StatusConfirmed && next != StatusSettled && next != StatusRejected && next != StatusFailed {
			return fmt.Errorf("invalid transition %s -> %s", p.Status, next)
		}
	case StatusConfirmed:
		if next != StatusSettled {
			return fmt.Errorf("invalid transition %s -> %s", p.Status, next)
		}
	case StatusSettled, StatusRejected, StatusFailed, StatusCanceled:
		if next != p.Status {
			return fmt.Errorf("cannot transition terminal %s -> %s", p.Status, next)
		}
	default:
		return fmt.Errorf("unknown state %s", p.Status)
	}
	p.Status = next
	p.Version++
	p.UpdatedAt = time.Now().UTC()
	return nil
}
