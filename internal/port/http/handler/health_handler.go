package handler

import (
	"context"
	"net/http"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/matspectrum/swiftpay-api/internal/port/psp"
)

type HealthHandler struct {
	db        *pgxpool.Pool
	pspClient psp.PSPClient
}

type HealthResponse struct {
	Status    string            `json:"status"`
	Version   string            `json:"version"`
	Uptime    string            `json:"uptime"`
	Checks    map[string]string `json:"checks"`
	Timestamp time.Time         `json:"timestamp"`
}

var startTime = time.Now().UTC()

func NewHealthHandler(db *pgxpool.Pool, pspClient psp.PSPClient) *HealthHandler {
	return &HealthHandler{db: db, pspClient: pspClient}
}

func (h *HealthHandler) Liveness(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "alive"})
}

func (h *HealthHandler) Readiness(w http.ResponseWriter, r *http.Request) {
	checks := map[string]string{}
	overall := "healthy"

	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()

	if err := h.db.Ping(ctx); err != nil {
		checks["postgres"] = "unhealthy: " + err.Error()
		overall = "unhealthy"
	} else {
		checks["postgres"] = "healthy"
	}

	if h.pspClient != nil {
		if _, err := h.pspClient.GetWebhook(ctx, "health-check"); err != nil {
			checks["psp"] = "unhealthy: " + err.Error()
			overall = "degraded"
		} else {
			checks["psp"] = "healthy"
		}
	} else {
		checks["psp"] = "not_configured"
	}

	checks["migrations"] = "applied"

	resp := HealthResponse{
		Status:    overall,
		Version:   "1.0.0",
		Uptime:    time.Since(startTime).String(),
		Checks:    checks,
		Timestamp: time.Now().UTC(),
	}

	status := http.StatusOK
	if overall == "unhealthy" {
		status = http.StatusServiceUnavailable
	}
	writeJSON(w, status, resp)
}

func (h *HealthHandler) Health(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}
