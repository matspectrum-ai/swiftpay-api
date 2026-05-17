package worker

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"sync"
	"time"
)

type RetryClass string

const (
	RetryClassTransient     RetryClass = "transient"
	RetryClassOverload      RetryClass = "overload"
	RetryClassSerialization RetryClass = "serialization"
	RetryClassPSPUnstable   RetryClass = "psp_unstable"
	RetryClassNetwork       RetryClass = "network"
	RetryClassPermanent     RetryClass = "permanent"
)

func ClassifyError(err error) RetryClass {
	if err == nil {
		return ""
	}
	errStr := err.Error()

	if containsAny(errStr, "serialization", "could not serialize", "deadlock") {
		return RetryClassSerialization
	}
	if containsAny(errStr, "rate limit", "too many requests", "429", "503", "Service Unavailable") {
		return RetryClassOverload
	}
	if containsAny(errStr, "timeout", "connection refused", "connection reset", "EOF", "broken pipe") {
		return RetryClassNetwork
	}
	if containsAny(errStr, "magicpay error 5", "PSP", "provider") {
		return RetryClassPSPUnstable
	}
	if containsAny(errStr, "invalid", "validation", "not found", "400", "401", "403", "404") {
		return RetryClassPermanent
	}
	return RetryClassTransient
}

type RetryConfig struct {
	BaseDelay    time.Duration
	MaxDelay     time.Duration
	MaxAttempts  int
	JitterFactor float64
}

func DefaultRetryConfig() RetryConfig {
	return RetryConfig{
		BaseDelay:    100 * time.Millisecond,
		MaxDelay:     30 * time.Second,
		MaxAttempts:  5,
		JitterFactor: 0.25,
	}
}

type RetryBudget struct {
	mu         sync.Mutex
	window     time.Duration
	maxRetries int
	counts     []retrySlot
}

type retrySlot struct {
	ts time.Time
}

func NewRetryBudget(window time.Duration, maxRetries int) *RetryBudget {
	return &RetryBudget{window: window, maxRetries: maxRetries}
}

func (b *RetryBudget) Allow() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	cutoff := time.Now().Add(-b.window)
	active := b.counts[:0]
	for _, s := range b.counts {
		if s.ts.After(cutoff) {
			active = append(active, s)
		}
	}
	b.counts = active
	if len(b.counts) >= b.maxRetries {
		return false
	}
	b.counts = append(b.counts, retrySlot{ts: time.Now()})
	return true
}

var globalRetryBudget = NewRetryBudget(1*time.Minute, 100)

func RetryWithBackoff(ctx context.Context, cfg RetryConfig, operation func() error) error {
	var lastErr error
	for attempt := 0; attempt < cfg.MaxAttempts; attempt++ {
		if attempt > 0 {
			if !globalRetryBudget.Allow() {
				return fmt.Errorf("retry budget exhausted after %d attempts: %w", attempt, lastErr)
			}
			delay := time.Duration(float64(cfg.BaseDelay) * math.Pow(2, float64(attempt-1)))
			if delay > cfg.MaxDelay {
				delay = cfg.MaxDelay
			}
			jitter := time.Duration(float64(delay) * cfg.JitterFactor * (rand.Float64()*2 - 1))
			delay += jitter

			select {
			case <-ctx.Done():
				return fmt.Errorf("retry cancelled: %w", ctx.Err())
			case <-time.After(delay):
			}
		}

		lastErr = operation()
		if lastErr == nil {
			return nil
		}

		class := ClassifyError(lastErr)
		if class == RetryClassPermanent {
			return fmt.Errorf("permanent error: %w", lastErr)
		}
		if class == RetryClassOverload {
			delay := cfg.MaxDelay / 2
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(delay):
			}
		}
	}
	return fmt.Errorf("max retries (%d) exceeded: %w", cfg.MaxAttempts, lastErr)
}

func containsAny(s string, substrs ...string) bool {
	for _, sub := range substrs {
		for i := 0; i <= len(s)-len(sub); i++ {
			if s[i:i+len(sub)] == sub {
				return true
			}
		}
	}
	return false
}
