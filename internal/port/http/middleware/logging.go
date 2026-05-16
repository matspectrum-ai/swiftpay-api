package middleware

import (
	"log/slog"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
)

type responseWriter struct {
	http.ResponseWriter
	statusCode  int
	bytesCount  int64
	wroteHeader bool
}

func newResponseWriter(w http.ResponseWriter) *responseWriter {
	return &responseWriter{ResponseWriter: w, statusCode: http.StatusOK}
}

func (rw *responseWriter) WriteHeader(code int) {
	if !rw.wroteHeader {
		rw.statusCode = code
		rw.wroteHeader = true
	}
	rw.ResponseWriter.WriteHeader(code)
}

func (rw *responseWriter) Write(b []byte) (int, error) {
	if !rw.wroteHeader {
		rw.wroteHeader = true
	}
	n, err := rw.ResponseWriter.Write(b)
	rw.bytesCount += int64(n)
	return n, err
}

func LoggingMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now().UTC()
		rw := newResponseWriter(w)

		requestID := GetRequestID(r.Context())
		correlationID := r.Header.Get("X-Correlation-ID")
		if correlationID == "" {
			correlationID = requestID
		}
		txid := chi.URLParam(r, "txid")

		logAttrs := []any{
			"method", r.Method,
			"path", r.URL.Path,
			"request_id", requestID,
			"correlation_id", correlationID,
		}
		if txid != "" {
			logAttrs = append(logAttrs, "txid", txid)
		}
		slog.InfoContext(r.Context(), "request_start", logAttrs...)

		next.ServeHTTP(rw, r)

		duration := time.Since(start)
		endAttrs := []any{
			"method", r.Method,
			"path", r.URL.Path,
			"status", rw.statusCode,
			"duration_ms", duration.Milliseconds(),
			"request_id", requestID,
			"correlation_id", correlationID,
		}
		if txid != "" {
			endAttrs = append(endAttrs, "txid", txid)
		}
		if rw.bytesCount > 0 {
			endAttrs = append(endAttrs, "bytes_written", rw.bytesCount)
		}

		if rw.statusCode >= 500 {
			slog.ErrorContext(r.Context(), "request_end", endAttrs...)
		} else if rw.statusCode >= 400 {
			slog.WarnContext(r.Context(), "request_end", endAttrs...)
		} else {
			slog.InfoContext(r.Context(), "request_end", endAttrs...)
		}
	})
}
