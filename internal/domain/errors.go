// Package domain contém entidades e erros de domínio do Pix.
package domain

import (
	"encoding/json"
	"fmt"
	"net/http"
)

// ProblemDetail segue RFC 7807 (Problem Details for HTTP APIs).
type ProblemDetail struct {
	Type     string `json:"type"`
	Title    string `json:"title"`
	Status   int    `json:"status"`
	Detail   string `json:"detail"`
	Instance string `json:"instance,omitempty"`
}

// Error retorna a mensagem de detalhe.
func (p *ProblemDetail) Error() string {
	return p.Detail
}

// WriteJSON serializa o erro como JSON na resposta HTTP.
func (p *ProblemDetail) WriteJSON(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "application/problem+json; charset=utf-8")
	w.WriteHeader(p.Status)
	json.NewEncoder(w).Encode(p)
}

// Erros de domínio comuns (sentinel errors).
var (
	ErrCobrancaNaoEncontrada = &ProblemDetail{
		Type:   "https://pix.bcb.gov.br/api/v2/error/NaoEncontrado",
		Title:  "Não encontrado",
		Status: http.StatusNotFound,
		Detail: "Cobrança não encontrada",
	}
	ErrPixNaoEncontrado = &ProblemDetail{
		Type:   "https://pix.bcb.gov.br/api/v2/error/NaoEncontrado",
		Title:  "Não encontrado",
		Status: http.StatusNotFound,
		Detail: "Pix não encontrado",
	}
	ErrWebhookNaoEncontrado = &ProblemDetail{
		Type:   "https://pix.bcb.gov.br/api/v2/error/NaoEncontrado",
		Title:  "Não encontrado",
		Status: http.StatusNotFound,
		Detail: "Webhook não encontrado",
	}
	ErrRequisicaoInvalida = &ProblemDetail{
		Type:   "https://pix.bcb.gov.br/api/v2/error/RequisicaoInvalida",
		Title:  "Requisição inválida",
		Status: http.StatusBadRequest,
		Detail: "Requisição inválida",
	}
	ErrIdempotencyKeyDiverged = &ProblemDetail{
		Type:   "https://pix.bcb.gov.br/api/v2/error/RequestIdAlreadyUsed",
		Title:  "RequestId já utilizado",
		Status: http.StatusBadRequest,
		Detail: "Idempotency-Key já utilizada com payload diferente",
	}
	ErrCobrancaStatusInvalido = &ProblemDetail{
		Type:   "https://pix.bcb.gov.br/api/v2/error/RequisicaoInvalida",
		Title:  "Requisição inválida",
		Status: http.StatusBadRequest,
		Detail: "Status de cobrança inválido",
	}
	ErrTxIDInvalido = &ProblemDetail{
		Type:   "https://pix.bcb.gov.br/api/v2/error/RequisicaoInvalida",
		Title:  "Requisição inválida",
		Status: http.StatusBadRequest,
		Detail: "txid inválido (deve ter entre 26 e 35 caracteres)",
	}
)

// NewValidationError cria erro de validação com detalhe customizado.
func NewValidationError(detail string) *ProblemDetail {
	return &ProblemDetail{
		Type:   "https://pix.bcb.gov.br/api/v2/error/RequisicaoInvalida",
		Title:  "Requisição inválida",
		Status: http.StatusBadRequest,
		Detail: detail,
	}
}

// NewInternalError cria erro interno.
func NewInternalError(detail string) *ProblemDetail {
	return &ProblemDetail{
		Type:   "https://pix.bcb.gov.br/api/v2/error/InternalServerError",
		Title:  "Erro interno",
		Status: http.StatusInternalServerError,
		Detail: detail,
	}
}

// IsProblemDetail verifica se o erro é um ProblemDetail.
func IsProblemDetail(err error) (*ProblemDetail, bool) {
	pd, ok := err.(*ProblemDetail)
	return pd, ok
}

// FormatValidationError retorna detalhe formatado para erro de validação.
func FormatValidationError(msg string, args ...interface{}) *ProblemDetail {
	return NewValidationError(fmt.Sprintf(msg, args...))
}
