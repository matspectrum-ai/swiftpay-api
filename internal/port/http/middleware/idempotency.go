package middleware

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"log/slog"
	"net/http"

	"github.com/matspectrum/swiftpay-api/internal/domain"
	"github.com/matspectrum/swiftpay-api/internal/observability"
	"github.com/matspectrum/swiftpay-api/internal/store/postgres"
)

type captureResponseWriter struct {
	http.ResponseWriter
	body       *bytes.Buffer
	statusCode int
}

func newCaptureResponseWriter(w http.ResponseWriter) *captureResponseWriter {
	return &captureResponseWriter{
		ResponseWriter: w,
		body:           new(bytes.Buffer),
		statusCode:     http.StatusOK,
	}
}

func (rw *captureResponseWriter) WriteHeader(statusCode int) {
	rw.statusCode = statusCode
	rw.ResponseWriter.WriteHeader(statusCode)
}

func (rw *captureResponseWriter) Write(b []byte) (int, error) {
	rw.body.Write(b)
	return rw.ResponseWriter.Write(b)
}

func IdempotencyMiddleware(repo *postgres.IdempotencyRepo) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method == http.MethodGet || r.Method == http.MethodDelete || r.Method == http.MethodHead || r.Method == http.MethodOptions {
				next.ServeHTTP(w, r)
				return
			}

			key := r.Header.Get("Idempotency-Key")
			if key == "" {
				next.ServeHTTP(w, r)
				return
			}

			bodyBytes, err := io.ReadAll(r.Body)
			if err != nil {
				http.Error(w, `{"status":400,"detail":"Erro lendo body"}`, http.StatusBadRequest)
				return
			}
			r.Body.Close()

			hash := sha256.Sum256(bodyBytes)
			requestHash := hex.EncodeToString(hash[:])

			endpointPath := r.URL.Path
			observability.IdempotencyMisses.Inc()

			record, err := repo.Acquire(r.Context(), key, endpointPath, requestHash)

			if err != nil {
				if _, ok := domain.IsProblemDetail(err); ok {
					w.Header().Set("Content-Type", "application/problem+json; charset=utf-8")
					w.WriteHeader(http.StatusBadRequest)
					w.Write([]byte(`{"type":"https://pix.bcb.gov.br/api/v2/error/RequestIdAlreadyUsed","title":"RequestId já utilizado","status":400,"detail":"Idempotency-Key já utilizada com payload diferente"}`))
					return
				}
				http.Error(w, `{"status":500,"detail":"Erro de idempotência"}`, http.StatusInternalServerError)
				return
			}

			if record.Status == "completed" {
				observability.IdempotencyHits.Inc()
				w.Header().Set("Content-Type", "application/json; charset=utf-8")
				w.WriteHeader(record.ResponseStatus)
				w.Write(record.ResponseBody)
				slog.InfoContext(r.Context(), "idempotency cache hit",
					"key", key,
					"path", endpointPath,
				)
				return
			}

			if record.Status == "in_progress" {
				w.Header().Set("Retry-After", "2")
				http.Error(w, "Requisição em processamento", http.StatusConflict)
				return
			}

			ctx := context.WithValue(r.Context(), domain.CtxKeyIdempotencyKey, key)
			ctx = context.WithValue(ctx, domain.CtxKeyEndpointPath, endpointPath)
			r = r.WithContext(ctx)

			r.Body = io.NopCloser(bytes.NewReader(bodyBytes))
			rw := newCaptureResponseWriter(w)
			next.ServeHTTP(rw, r)
		})
	}
}
