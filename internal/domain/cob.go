package domain

import (
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"
)

// CobStatus representa o status de uma cobrança imediata.
type CobStatus string

const (
	CobStatusAtiva               CobStatus = "ATIVA"
	CobStatusConcluida           CobStatus = "CONCLUIDA"
	CobStatusRemovidaPeloUsuario CobStatus = "REMOVIDA_PELO_USUARIO_RECEBEDOR"
	CobStatusRemovidaPeloPSP     CobStatus = "REMOVIDA_PELO_PSP"
)

// CobStatusTransitions define transições válidas de status.
var CobStatusTransitions = map[CobStatus][]CobStatus{
	CobStatusAtiva:               {CobStatusConcluida, CobStatusRemovidaPeloUsuario, CobStatusRemovidaPeloPSP},
	CobStatusConcluida:           {},
	CobStatusRemovidaPeloUsuario: {},
	CobStatusRemovidaPeloPSP:     {},
}

// CanTransitionTo verifica se uma transição de status é válida.
func (s CobStatus) CanTransitionTo(target CobStatus) bool {
	for _, valid := range CobStatusTransitions[s] {
		if valid == target {
			return true
		}
	}
	return false
}

// Calendar representa o calendário de expiração da cobrança.
type Calendar struct {
	Criacao   time.Time `json:"criacao"`
	Expiracao int       `json:"expiracao"` // segundos desde criacao (default 86400)
}

// Devedor representa o pagador.
type Devedor struct {
	Nome string `json:"nome,omitempty"`
	CPF  string `json:"cpf,omitempty"`
	CNPJ string `json:"cnpj,omitempty"`
}

// ValorCentavos representa um valor monetário em centavos (int64).
// R$ 123.45 = 12345 centavos.
// Serializa como string decimal no JSON (formato BACEN).
type ValorCentavos int64

func (v ValorCentavos) MarshalJSON() ([]byte, error) {
	return json.Marshal(fmt.Sprintf("%.2f", float64(v)/100.0))
}

func (v *ValorCentavos) UnmarshalJSON(data []byte) error {
	var s string
	if err := json.Unmarshal(data, &s); err != nil {
		return err
	}
	f, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return fmt.Errorf("valor inválido: %s", s)
	}
	if f < 0 {
		return fmt.Errorf("valor não pode ser negativo: %s", s)
	}
	if f > 999999999.99 {
		return fmt.Errorf("valor excede limite máximo: %s", s)
	}
	*v = ValorCentavos(int64(math.Round(f * 100.0)))
	return nil
}

// Valor representa o valor monetário.
type Valor struct {
	Original ValorCentavos `json:"original"`
}

// Cobranca representa uma cobrança imediata Pix.
type Cobranca struct {
	TxID               string    `json:"txid"`
	Revisao            int       `json:"revisao"`
	Chave              string    `json:"chave"`
	SolicitacaoPagador string    `json:"solicitacaoPagador,omitempty"`
	Calendar           Calendar  `json:"calendario"`
	Devedor            Devedor   `json:"devedor,omitempty"`
	Valor              Valor     `json:"valor"`
	Status             CobStatus `json:"status"`
	Location           string    `json:"location,omitempty"`
	PixCopiaECola      string    `json:"pixCopiaECola,omitempty"`
	CreatedAt          time.Time `json:"-"`
	UpdatedAt          time.Time `json:"-"`
}

// Validate verifica se a cobrança é válida.
func (c *Cobranca) Validate() error {
	if len(c.TxID) < 26 || len(c.TxID) > 35 {
		return ErrTxIDInvalido
	}
	if c.Chave == "" {
		return FormatValidationError("chave é obrigatória")
	}
	if c.Valor.Original <= 0 {
		return FormatValidationError("valor.original deve ser maior que zero")
	}
	if c.Devedor.CPF == "" && c.Devedor.CNPJ == "" {
		return FormatValidationError("devedor deve ter CPF ou CNPJ")
	}
	if c.Calendar.Expiracao <= 0 {
		c.Calendar.Expiracao = 86400 // 24 horas default
	}
	return nil
}

// Sanitize remove espaços e padroniza campos.
func (c *Cobranca) Sanitize() {
	c.TxID = strings.TrimSpace(c.TxID)
	c.Chave = strings.TrimSpace(c.Chave)
	c.Devedor.CPF = strings.TrimSpace(c.Devedor.CPF)
	c.Devedor.CNPJ = strings.TrimSpace(c.Devedor.CNPJ)
	c.Devedor.Nome = strings.TrimSpace(c.Devedor.Nome)
	c.SolicitacaoPagador = strings.TrimSpace(c.SolicitacaoPagador)
}

// CobrancaPatch representa alteração parcial de cobrança.
type CobrancaPatch struct {
	Status CobStatus `json:"status"`
}

// Validate verifica se o patch é válido.
func (p *CobrancaPatch) Validate() error {
	switch p.Status {
	case CobStatusRemovidaPeloUsuario:
		return nil
	case CobStatusAtiva, CobStatusConcluida, CobStatusRemovidaPeloPSP:
		return FormatValidationError("não é permitido alterar status para %s via PATCH", p.Status)
	default:
		return FormatValidationError("status desconhecido: %s", p.Status)
	}
}

// MarshalJSON customizado para lowercase nas chaves JSON.
func (c Cobranca) MarshalJSON() ([]byte, error) {
	type Alias Cobranca
	return json.Marshal(&struct {
		*Alias
	}{
		Alias: (*Alias)(&c),
	})
}

// CobFilter representa filtros para listagem de cobranças.
type CobFilter struct {
	Inicio time.Time
	Fim    time.Time
	Limit  int
	Offset int
}
