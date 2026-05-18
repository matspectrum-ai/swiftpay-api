package domain

import (
	"fmt"
	"time"
)

// PixRecebido representa um pagamento Pix liquidado.
type PixRecebido struct {
	EndToEndID             string         `json:"endToEndId"`
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
	EndToEndID     string        `json:"endToEndId"`
	Valor     ValorCentavos `json:"valor"`
	Horario   time.Time     `json:"horario"`
	Status    string        `json:"status"`
	CreatedAt time.Time     `json:"-"`
}

// WebhookPayload representa o payload enviado pelo PSP no callback.
type WebhookPayload struct {
	EndToEndID             string    `json:"e2eid"`
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
// Retorna erro se o valor for inválido (ex: malformado, negativo, zero).
func (p *WebhookPayload) ToPixRecebido() (*PixRecebido, error) {
	var v ValorCentavos
	if err := v.UnmarshalJSON([]byte(`"` + p.Valor + `"`)); err != nil {
		return nil, fmt.Errorf("valor inválido no payload webhook: %s — %w", p.Valor, err)
	}
	if v <= 0 {
		return nil, fmt.Errorf("valor do pix deve ser positivo: %s", p.Valor)
	}
	return &PixRecebido{
		EndToEndID:             p.EndToEndID,
		TxID:              p.TxID,
		Chave:             p.Chave,
		ValorCentavos:     v,
		HorarioLiquidacao: p.HorarioLiquidacao,
		PagadorNome:       p.PagadorNome,
		PagadorCPF:        p.PagadorCPF,
		PagadorCNPJ:       p.PagadorCNPJ,
		InfoPagador:       p.InfoPagador,
	}, nil
}

// WebhookConfig representa configuração de webhook.
type WebhookConfig struct {
	Chave      string    `json:"chave"`
	WebhookURL string    `json:"webhookUrl"`
	Status     string    `json:"status"`
	CreatedAt  time.Time `json:"-"`
	UpdatedAt  time.Time `json:"-"`
}
