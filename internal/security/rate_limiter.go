package security

import (
	"context"
	"sync"
	"time"
)

type RateLimiter struct {
	mu       sync.Mutex
	buckets  map[string]*tokenBucket
	rate     float64
	capacity int
}

type tokenBucket struct {
	tokens   float64
	lastTime time.Time
}

func NewRateLimiter(ratePerSec float64, capacity int) *RateLimiter {
	return &RateLimiter{
		buckets:  make(map[string]*tokenBucket),
		rate:     ratePerSec,
		capacity: capacity,
	}
}

func (rl *RateLimiter) Allow(key string) bool {
	rl.mu.Lock()
	defer rl.mu.Unlock()

	b, ok := rl.buckets[key]
	if !ok {
		b = &tokenBucket{tokens: float64(rl.capacity), lastTime: time.Now()}
		rl.buckets[key] = b
	}

	now := time.Now()
	elapsed := now.Sub(b.lastTime).Seconds()
	b.tokens += elapsed * rl.rate
	if b.tokens > float64(rl.capacity) {
		b.tokens = float64(rl.capacity)
	}
	b.lastTime = now

	if b.tokens < 1 {
		return false
	}
	b.tokens--
	return true
}

func (rl *RateLimiter) Cleanup(ctx context.Context, maxAge time.Duration) {
	ticker := time.NewTicker(maxAge)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			rl.mu.Lock()
			cutoff := time.Now().Add(-maxAge)
			for k, b := range rl.buckets {
				if b.lastTime.Before(cutoff) {
					delete(rl.buckets, k)
				}
			}
			rl.mu.Unlock()
		}
	}
}
