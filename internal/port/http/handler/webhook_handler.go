package handler

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"

	"github.com/go-chi/chi/v5"

	"github.com/matspectrum/swiftpay-api/internal/domain"
	"github.com/matspectrum/swiftpay-api/internal/observability"
	"github.com/matspectrum/swiftpay-api/internal/service"
)

type WebhookHandler struct {
	webhookService *service.WebhookService
}

func NewWebhookHandler(webhookService *service.WebhookService) *WebhookHandler {
	return &WebhookHandler{webhookService: webhookService}
}

type configureRequest struct {
	WebhookURL string `json:"webhookUrl"`
}

func (h *WebhookHandler) ConfigureWebhook(w http.ResponseWriter, r *http.Request) {
	chave := chi.URLParam(r, "chave")

	var req configureRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeProblem(w, domain.FormatValidationError("payload inválido: %s", err.Error()))
		return
	}

	wc, err := h.webhookService.ConfigureWebhook(r.Context(), chave, req.WebhookURL)
	if err != nil {
		writeProblem(w, err)
		return
	}

	writeJSON(w, http.StatusOK, wc)
}

func (h *WebhookHandler) GetWebhook(w http.ResponseWriter, r *http.Request) {
	chave := chi.URLParam(r, "chave")

	wc, err := h.webhookService.GetWebhook(r.Context(), chave)
	if err != nil {
		writeProblem(w, err)
		return
	}

	writeJSON(w, http.StatusOK, wc)
}

func (h *WebhookHandler) ListWebhooks(w http.ResponseWriter, r *http.Request) {
	webhooks, err := h.webhookService.ListWebhooks(r.Context())
	if err != nil {
		writeProblem(w, err)
		return
	}

	if webhooks == nil {
		webhooks = []domain.WebhookConfig{}
	}

	writeJSON(w, http.StatusOK, webhooks)
}

func (h *WebhookHandler) DeleteWebhook(w http.ResponseWriter, r *http.Request) {
	chave := chi.URLParam(r, "chave")

	if err := h.webhookService.DeleteWebhook(r.Context(), chave); err != nil {
		writeProblem(w, err)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

func (h *WebhookHandler) HandleCallback(w http.ResponseWriter, r *http.Request) {
	bodyBytes, err := io.ReadAll(r.Body)
	if err != nil {
		slog.ErrorContext(r.Context(), "erro ao ler body do webhook callback", "error", err)
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(map[string]string{"status": "received"})
		return
	}

	var payload domain.WebhookPayload
	if err := json.Unmarshal(bodyBytes, &payload); err != nil {
		slog.ErrorContext(r.Context(), "erro ao decodificar payload webhook", "error", err)
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(map[string]string{"status": "received"})
		return
	}

	go func() {
		ctx := context.WithoutCancel(r.Context())
		if err := h.webhookService.HandleCallback(ctx, bodyBytes); err != nil {
			observability.WebhookProcessed.WithLabelValues("error").Inc()
			slog.ErrorContext(ctx, "erro ao processar callback webhook assincrono", "error", err, "e2eid", payload.EndToEndID)
		} else {
			observability.WebhookProcessed.WithLabelValues("success").Inc()
		}
	}()

	observability.WebhookProcessed.WithLabelValues("received").Inc()

	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{"status": "received"})
}
