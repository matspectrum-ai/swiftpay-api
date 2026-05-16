package worker

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"time"
)

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

func RetryWithBackoff(ctx context.Context, cfg RetryConfig, operation func() error) error {
	var lastErr error
	for attempt := 0; attempt < cfg.MaxAttempts; attempt++ {
		if attempt > 0 {
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

		if !isTransient(lastErr) {
			return fmt.Errorf("permanent error: %w", lastErr)
		}
	}
	return fmt.Errorf("max retries (%d) exceeded: %w", cfg.MaxAttempts, lastErr)
}

func isTransient(err error) bool {
	errStr := err.Error()
	return !containsAny(errStr, "permanent", "validation", "invalid", "not found")
}

func containsAny(s string, substrs ...string) bool {
	for _, sub := range substrs {
		if len(s) >= len(sub) {
			for i := 0; i <= len(s)-len(sub); i++ {
				if s[i:i+len(sub)] == sub {
					return true
				}
			}
		}
	}
	return false
}
