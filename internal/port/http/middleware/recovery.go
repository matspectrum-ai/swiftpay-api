package middleware

import (
	"log/slog"
	"net/http"
	"runtime/debug"
)

func RecoveryMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if rec := recover(); rec != nil {
				slog.ErrorContext(r.Context(), "panic recuperado",
					"panic", rec,
					"stack", string(debug.Stack()),
					"request_id", GetRequestID(r.Context()),
				)
				http.Error(w, `{"status":500,"title":"Erro interno","detail":"Erro interno do servidor"}`, http.StatusInternalServerError)
			}
		}()
		next.ServeHTTP(w, r)
	})
}
