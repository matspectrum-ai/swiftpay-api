// Package psp define a interface agnóstica de PSP seguindo o padrão BACEN.
package psp

import (
	"context"

	"github.com/matspectrum/swiftpay-api/internal/domain"
)

// CobRequest representa o payload para criar/atualizar cobrança no PSP.
type CobRequest struct {
	Calendar   domain.Calendar `json:"calendario"`
	Devedor    domain.Devedor  `json:"devedor,omitempty"`
	Valor      domain.Valor    `json:"valor"`
	Chave      string          `json:"chave"`
	SolPagador string          `json:"solicitacaoPagador,omitempty"`
}

// CobResponse representa a resposta do PSP ao criar cobrança.
type CobResponse struct {
	TxID          string           `json:"txid"`
	Revisao       int              `json:"revisao"`
	Calendar      domain.Calendar  `json:"calendario"`
	Devedor       domain.Devedor   `json:"devedor,omitempty"`
	Valor         domain.Valor     `json:"valor"`
	Chave         string           `json:"chave"`
	SolPagador    string           `json:"solicitacaoPagador,omitempty"`
	Status        domain.CobStatus `json:"status"`
	Location      string           `json:"location"`
	PixCopiaECola string           `json:"pixCopiaECola"`
}

// PixResponse representa a resposta do PSP para consulta de Pix.
type PixResponse struct {
	E2EID             string `json:"e2eid"`
	TxID              string `json:"txid,omitempty"`
	Valor             string `json:"valor"`
	HorarioLiquidacao string `json:"horario"`
	PagadorNome       string `json:"pagadorNome,omitempty"`
	PagadorCPF        string `json:"pagadorCpf,omitempty"`
	PagadorCNPJ       string `json:"pagadorCnpj,omitempty"`
	InfoPagador       string `json:"infoPagador,omitempty"`
}

// DevolucaoResponse representa a resposta do PSP para solicitação de devolução.
type DevolucaoResponse struct {
	ID      string `json:"id"`
	E2EID   string `json:"e2eid"`
	Valor   string `json:"valor"`
	Horario string `json:"horario"`
	Status  string `json:"status"`
}

// PSPClient é a interface que todo PSP deve implementar (padrão BACEN).
type PSPClient interface {
	// Cobranças
	CreateCob(ctx context.Context, txid string, req CobRequest) (*CobResponse, error)
	UpdateCob(ctx context.Context, txid string, req CobRequest) (*CobResponse, error)
	GetCob(ctx context.Context, txid string) (*CobResponse, error)
	ListCobs(ctx context.Context, inicio, fim string, limit, offset int) ([]CobResponse, int, error)

	// Pix
	GetPix(ctx context.Context, e2eid string) (*PixResponse, error)
	ListPix(ctx context.Context, inicio, fim string, limit, offset int) ([]PixResponse, int, error)
	CreateDevolucao(ctx context.Context, e2eid, id, valor string) (*DevolucaoResponse, error)

	// Webhook
	ConfigureWebhook(ctx context.Context, chave, url string) error
	GetWebhook(ctx context.Context, chave string) (*domain.WebhookConfig, error)
	DeleteWebhook(ctx context.Context, chave string) error
}
