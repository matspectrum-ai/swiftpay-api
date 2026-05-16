package handler

import (
	"encoding/json"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"

	"github.com/matspectrum/swiftpay-api/internal/domain"
	"github.com/matspectrum/swiftpay-api/internal/service"
)

type PixHandler struct {
	pixService *service.PixService
}

func NewPixHandler(pixService *service.PixService) *PixHandler {
	return &PixHandler{pixService: pixService}
}

func (h *PixHandler) GetPix(w http.ResponseWriter, r *http.Request) {
	e2eid := chi.URLParam(r, "e2eid")

	pix, err := h.pixService.GetPix(r.Context(), e2eid)
	if err != nil {
		writeProblem(w, err)
		return
	}

	writeJSON(w, http.StatusOK, pix)
}

func (h *PixHandler) ListPix(w http.ResponseWriter, r *http.Request) {
	filter := domain.PixFilter{
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
	filter.TxID = r.URL.Query().Get("txid")
	filter.Chave = r.URL.Query().Get("chave")

	pixs, total, err := h.pixService.ListPix(r.Context(), filter)
	if err != nil {
		writeProblem(w, err)
		return
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"pix":   pixs,
		"total": total,
	})
}

type createDevolucaoRequest struct {
	Valor string `json:"valor"`
}

func (h *PixHandler) CreateDevolucao(w http.ResponseWriter, r *http.Request) {
	e2eid := chi.URLParam(r, "e2eid")
	devID := chi.URLParam(r, "id")

	var req createDevolucaoRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeProblem(w, domain.FormatValidationError("payload inválido: %s", err.Error()))
		return
	}

	dev, err := h.pixService.CreateDevolucao(r.Context(), e2eid, devID, req.Valor)
	if err != nil {
		writeProblem(w, err)
		return
	}

	writeJSON(w, http.StatusOK, dev)
}
