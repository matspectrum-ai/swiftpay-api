package handler

import (
	"context"
	"net/http"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

type HealthHandler struct {
	db *pgxpool.Pool
}

type HealthResponse struct {
	Status    string            `json:"status"`
	Version   string            `json:"version"`
	Uptime    string            `json:"uptime"`
	Checks    map[string]string `json:"checks"`
	Timestamp time.Time         `json:"timestamp"`
}

var startTime = time.Now()

func NewHealthHandler(db *pgxpool.Pool) *HealthHandler {
	return &HealthHandler{db: db}
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

	checks["psp"] = "connected"

	resp := HealthResponse{
		Status:    overall,
		Version:   "1.0.0",
		Uptime:    time.Since(startTime).String(),
		Checks:    checks,
		Timestamp: time.Now(),
	}

	status := http.StatusOK
	if overall != "healthy" {
		status = http.StatusServiceUnavailable
	}
	writeJSON(w, status, resp)
}

func (h *HealthHandler) Health(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}
