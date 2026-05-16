package worker

import (
	"context"
	"fmt"
	"sync"
	"time"
)

type CircuitState int

const (
	CircuitClosed CircuitState = iota
	CircuitHalfOpen
	CircuitOpen
)

type CircuitBreaker struct {
	mu            sync.Mutex
	state         CircuitState
	failureCount  int
	lastFailure   time.Time
	threshold     int
	resetTimeout  time.Duration
	halfOpenMax   int
	halfOpenCount int
}

func NewCircuitBreaker(threshold int, resetTimeout time.Duration) *CircuitBreaker {
	return &CircuitBreaker{
		state:        CircuitClosed,
		threshold:    threshold,
		resetTimeout: resetTimeout,
		halfOpenMax:  3,
	}
}

func (cb *CircuitBreaker) Execute(ctx context.Context, fn func() error) error {
	cb.mu.Lock()

	if cb.state == CircuitOpen {
		if time.Since(cb.lastFailure) > cb.resetTimeout {
			cb.state = CircuitHalfOpen
			cb.halfOpenCount = 0
		} else {
			cb.mu.Unlock()
			return fmt.Errorf("circuit breaker open")
		}
	}

	if cb.state == CircuitHalfOpen && cb.halfOpenCount >= cb.halfOpenMax {
		cb.mu.Unlock()
		return fmt.Errorf("circuit breaker half-open limit reached")
	}

	if cb.state == CircuitHalfOpen {
		cb.halfOpenCount++
	}
	cb.mu.Unlock()

	err := fn()

	cb.mu.Lock()
	defer cb.mu.Unlock()

	if err != nil {
		cb.failureCount++
		cb.lastFailure = time.Now().UTC()
		if cb.failureCount >= cb.threshold {
			cb.state = CircuitOpen
		}
		return err
	}

	cb.failureCount = 0
	if cb.state == CircuitHalfOpen {
		cb.state = CircuitClosed
	}
	return nil
}
