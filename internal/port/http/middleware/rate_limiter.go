package middleware

import (
	"net/http"

	"github.com/matspectrum/swiftpay-api/internal/security"
)

func RateLimiterMiddleware(limiter *security.RateLimiter) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			key := r.Header.Get("Idempotency-Key")
			if key == "" {
				key = r.RemoteAddr
			}
			if !limiter.Allow(key) {
				w.Header().Set("Retry-After", "1")
				http.Error(w, `{"type":"https://pix.bcb.gov.br/api/v2/error/RateLimited","title":"Rate Limited","status":429,"detail":"Limite de requisições excedido"}`, http.StatusTooManyRequests)
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}
