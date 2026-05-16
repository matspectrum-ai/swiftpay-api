package handler

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"

	"github.com/matspectrum/swiftpay-api/internal/domain"
	"github.com/matspectrum/swiftpay-api/internal/service"
)

type CobHandler struct {
	cobService *service.CobService
}

func NewCobHandler(cobService *service.CobService) *CobHandler {
	return &CobHandler{cobService: cobService}
}

func writeJSON(w http.ResponseWriter, status int, data interface{}) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(data)
}

func writeProblem(w http.ResponseWriter, err error) {
	if pd, ok := domain.IsProblemDetail(err); ok {
		pd.WriteJSON(w)
		return
	}
	domain.NewInternalError(err.Error()).WriteJSON(w)
}

func (h *CobHandler) CreateCob(w http.ResponseWriter, r *http.Request) {
	txid := chi.URLParam(r, "txid")

	var cob domain.Cobranca
	if err := json.NewDecoder(r.Body).Decode(&cob); err != nil {
		writeProblem(w, domain.FormatValidationError("payload inválido: %s", err.Error()))
		return
	}
	cob.TxID = txid

	result, err := h.cobService.CreateCob(r.Context(), &cob)
	if err != nil {
		writeProblem(w, err)
		return
	}

	w.Header().Set("Location", result.Location)
	writeJSON(w, http.StatusCreated, result)
}

func (h *CobHandler) UpdateCob(w http.ResponseWriter, r *http.Request) {
	txid := chi.URLParam(r, "txid")

	var req domain.Cobranca
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		domain.NewValidationError("payload inválido: " + err.Error()).WriteJSON(w)
		return
	}
	req.TxID = txid
	req.Sanitize()

	if err := req.Validate(); err != nil {
		if pd, ok := domain.IsProblemDetail(err); ok {
			pd.WriteJSON(w)
			return
		}
		domain.NewValidationError(err.Error()).WriteJSON(w)
		return
	}

	cob, err := h.cobService.UpdateCob(r.Context(), txid, &req)
	if err != nil {
		if pd, ok := domain.IsProblemDetail(err); ok {
			pd.WriteJSON(w)
			return
		}
		slog.ErrorContext(r.Context(), "erro ao atualizar cobrança", "error", err, "txid", txid)
		domain.NewInternalError("erro ao atualizar cobrança").WriteJSON(w)
		return
	}

	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(cob)
}

func (h *CobHandler) PatchCob(w http.ResponseWriter, r *http.Request) {
	txid := chi.URLParam(r, "txid")

	var patch domain.CobrancaPatch
	if err := json.NewDecoder(r.Body).Decode(&patch); err != nil {
		writeProblem(w, domain.FormatValidationError("payload inválido: %s", err.Error()))
		return
	}

	result, err := h.cobService.PatchCob(r.Context(), txid, &patch)
	if err != nil {
		writeProblem(w, err)
		return
	}

	writeJSON(w, http.StatusOK, result)
}

func (h *CobHandler) GetCob(w http.ResponseWriter, r *http.Request) {
	txid := chi.URLParam(r, "txid")

	cob, err := h.cobService.GetCob(r.Context(), txid)
	if err != nil {
		writeProblem(w, err)
		return
	}

	writeJSON(w, http.StatusOK, cob)
}

func (h *CobHandler) ListCobs(w http.ResponseWriter, r *http.Request) {
	filter := domain.CobFilter{
		Limit:  20,
		Offset: 0,
	}

	if inicio := r.URL.Query().Get("inicio"); inicio != "" {
		if t, err := time.Parse(time.RFC3339, inicio); err == nil {
			filter.Inicio = t
		}
	}
	if fim := r.URL.Query().Get("fim"); fim != "" {
		if t, err := time.Parse(time.RFC3339, fim); err == nil {
			filter.Fim = t
		}
	}

	cobs, total, err := h.cobService.ListCobs(r.Context(), filter)
	if err != nil {
		writeProblem(w, err)
		return
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"cobs":  cobs,
		"total": total,
	})
}
