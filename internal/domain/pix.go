package domain

import (
	"time"
)

// PixRecebido representa um pagamento Pix liquidado.
type PixRecebido struct {
	E2EID             string         `json:"e2eid"`
	TxID              string         `json:"txid,omitempty"`
	Chave             string         `json:"chave"`
	ValorCentavos     ValorCentavos  `json:"valor"`
	HorarioLiquidacao time.Time      `json:"horario"`
	PagadorNome       string    `json:"pagadorNome,omitempty"`
	PagadorCPF        string    `json:"pagadorCpf,omitempty"`
	PagadorCNPJ       string    `json:"pagadorCnpj,omitempty"`
	InfoPagador       string    `json:"infoPagador,omitempty"`
	CreatedAt         time.Time `json:"-"`
}

// PixFilter representa filtros para listagem de Pix.
type PixFilter struct {
	Inicio time.Time
	Fim    time.Time
	Limit  int
	Offset int
	TxID   string
	Chave  string
}

// Devolucao representa uma solicitação de devolução parcial/total.
type Devolucao struct {
	ID        string        `json:"id"`
	E2EID     string        `json:"e2eid"`
	Valor     ValorCentavos `json:"valor"`
	Horario   time.Time     `json:"horario"`
	Status    string        `json:"status"`
	CreatedAt time.Time     `json:"-"`
}

// WebhookPayload representa o payload enviado pelo PSP no callback.
type WebhookPayload struct {
	E2EID             string    `json:"e2eid"`
	TxID              string    `json:"txid"`
	Chave             string    `json:"chave"`
	Valor             string    `json:"valor"`
	HorarioLiquidacao time.Time `json:"horario"`
	PagadorNome       string    `json:"pagadorNome,omitempty"`
	PagadorCPF        string    `json:"pagadorCpf,omitempty"`
	PagadorCNPJ       string    `json:"pagadorCnpj,omitempty"`
	InfoPagador       string    `json:"infoPagador,omitempty"`
}

// ToPixRecebido converte webhook payload para PixRecebido.
func (p *WebhookPayload) ToPixRecebido() *PixRecebido {
	var v ValorCentavos
	v.UnmarshalJSON([]byte(`"` + p.Valor + `"`))
	return &PixRecebido{
		E2EID:             p.E2EID,
		TxID:              p.TxID,
		Chave:             p.Chave,
		ValorCentavos:     v,
		HorarioLiquidacao: p.HorarioLiquidacao,
		PagadorNome:       p.PagadorNome,
		PagadorCPF:        p.PagadorCPF,
		PagadorCNPJ:       p.PagadorCNPJ,
		InfoPagador:       p.InfoPagador,
	}
}

// WebhookConfig representa configuração de webhook.
type WebhookConfig struct {
	Chave      string    `json:"chave"`
	WebhookURL string    `json:"webhookUrl"`
	Status     string    `json:"status"`
	CreatedAt  time.Time `json:"-"`
	UpdatedAt  time.Time `json:"-"`
}
