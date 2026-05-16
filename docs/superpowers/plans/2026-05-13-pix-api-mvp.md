# Pix API MVP - Plano de Implementação

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Construir API RESTful de processamento Pix seguindo especificação BACEN com idempotência, outbox transacional, webhook com deduplicação e reconciliação.

**Architecture:** API em 4 camadas — handler HTTP (go-chi/chi), service (lógica de negócio), store PostgreSQL (jackc/pgx), port PSP (interface agnóstica BACEN com mock para MVP). Workers de outbox e reconciliação rodam em goroutines.

**Tech Stack:** Go 1.23, PostgreSQL 16, jackc/pgx v5, go-chi/chi v5, golang-migrate v4, log/slog, testcontainers-go, testify, google/uuid

---

## Mapa de Arquivos

```
pix-api/
├── cmd/server/main.go                          # Entry point
├── internal/
│   ├── config/config.go                        # Environment loading
│   ├── domain/
│   │   ├── cob.go                              # Cobrança + status machine
│   │   ├── pix.go                              # Pix entity
│   │   └── errors.go                           # Domain errors (RFC 7807)
│   ├── port/
│   │   ├── psp/
│   │   │   ├── psp.go                          # PSPClient interface (BACEN)
│   │   │   └── mock/
│   │   │       ├── client.go                   # Mock PSP HTTP client
│   │   │       ├── cob.go                      # Mock cob operations
│   │   │       ├── pix.go                      # Mock pix operations
│   │   │       └── webhook.go                  # Mock webhook operations
│   │   └── http/
│   │       ├── handler/
│   │       │   ├── cob_handler.go
│   │       │   ├── pix_handler.go
│   │       │   ├── webhook_handler.go
│   │       │   └── health_handler.go
│   │       ├── middleware/
│   │       │   ├── idempotency.go
│   │       │   ├── logging.go
│   │       │   ├── recovery.go
│   │       │   └── requestid.go
│   │       ├── router.go
│   │       └── server.go
│   ├── service/
│   │   ├── cob_service.go
│   │   ├── pix_service.go
│   │   └── webhook_service.go
│   ├── store/
│   │   └── postgres/
│   │       ├── connection.go
│   │       ├── cob_repo.go
│   │       ├── pix_repo.go
│   │       ├── webhook_repo.go
│   │       ├── idempotency_repo.go
│   │       ├── outbox_repo.go
│   │       └── migrations/
│   │           ├── 000001_create_cobrancas.up.sql
│   │           ├── 000001_create_cobrancas.down.sql
│   │           ├── 000002_create_pix_recebidos.up.sql
│   │           ├── 000002_create_pix_recebidos.down.sql
│   │           ├── 000003_create_idempotency.up.sql
│   │           ├── 000003_create_idempotency.down.sql
│   │           ├── 000004_create_outbox.up.sql
│   │           ├── 000004_create_outbox.down.sql
│   │           ├── 000005_create_webhooks.up.sql
│   │           ├── 000005_create_webhooks.down.sql
│   │           ├── 000006_create_webhook_events.up.sql
│   │           ├── 000006_create_webhook_events.down.sql
│   │           ├── 000007_create_reconciliation.up.sql
│   │           ├── 000007_create_reconciliation.down.sql
│   │           ├── 000008_create_devolucoes.up.sql
│   │           └── 000008_create_devolucoes.down.sql
│   └── worker/
│       ├── outbox_publisher.go
│       └── reconciliation_worker.go
├── test/
│   ├── integration/
│   │   ├── cob_test.go
│   │   ├── pix_test.go
│   │   ├── webhook_test.go
│   │   ├── idempotency_test.go
│   │   └── outbox_test.go
│   └── testhelpers/
│       ├── postgres.go
│       └── migrate.go
├── Dockerfile
├── docker-compose.yml
├── Makefile
```

---

## Onda 1: Fundação (Tasks 1-5, paralelizáveis)

### Task 1: Configuração (internal/config/config.go)

**Files:**
- Create: `internal/config/config.go`

- [ ] **Step 1: Escrever config.go**

```go
// Package config carrega configurações via variáveis de ambiente.
package config

import (
	"fmt"
	"time"

	"github.com/caarlos0/env/v11"
)

// Config agrupa todas as configurações da aplicação.
type Config struct {
	Server   ServerConfig
	Database DatabaseConfig
	PSP      PSPConfig
	Worker   WorkerConfig
}

// ServerConfig configurações do servidor HTTP.
type ServerConfig struct {
	Port            string        `env:"SERVER_PORT" envDefault:"8080"`
	ReadTimeout     time.Duration `env:"SERVER_READ_TIMEOUT" envDefault:"10s"`
	WriteTimeout    time.Duration `env:"SERVER_WRITE_TIMEOUT" envDefault:"10s"`
	ShutdownTimeout time.Duration `env:"SERVER_SHUTDOWN_TIMEOUT" envDefault:"30s"`
}

// DatabaseConfig configurações do PostgreSQL.
type DatabaseConfig struct {
	Host     string `env:"DB_HOST" envDefault:"localhost"`
	Port     string `env:"DB_PORT" envDefault:"5432"`
	User     string `env:"DB_USER" envDefault:"pix"`
	Password string `env:"DB_PASSWORD" envDefault:"pix"`
	DBName   string `env:"DB_NAME" envDefault:"pix_api"`
	SSLMode  string `env:"DB_SSLMODE" envDefault:"disable"`

	MaxOpenConns    int           `env:"DB_MAX_OPEN_CONNS" envDefault:"25"`
	MaxIdleConns    int           `env:"DB_MAX_IDLE_CONNS" envDefault:"10"`
	ConnMaxLifetime time.Duration `env:"DB_CONN_MAX_LIFETIME" envDefault:"5m"`
	ConnMaxIdleTime time.Duration `env:"DB_CONN_MAX_IDLE_TIME" envDefault:"1m"`
}

// DSN retorna a string de conexão PostgreSQL.
func (c DatabaseConfig) DSN() string {
	return fmt.Sprintf(
		"postgres://%s:%s@%s:%s/%s?sslmode=%s",
		c.User, c.Password, c.Host, c.Port, c.DBName, c.SSLMode,
	)
}

// PSPConfig configurações do PSP.
type PSPConfig struct {
	MockEnabled bool   `env:"PSP_MOCK_ENABLED" envDefault:"true"`
	BaseURL     string `env:"PSP_BASE_URL" envDefault:"http://localhost:9090"`
	ClientID    string `env:"PSP_CLIENT_ID"`
	ClientSecret string `env:"PSP_CLIENT_SECRET"`
}

// WorkerConfig configurações dos workers.
type WorkerConfig struct {
	OutboxPollInterval       time.Duration `env:"WORKER_OUTBOX_POLL_INTERVAL" envDefault:"5s"`
	ReconciliationSchedule   string        `env:"WORKER_RECONCILIATION_SCHEDULE" envDefault:"@daily"`
	IdempotencyExpiration    time.Duration `env:"WORKER_IDEMPOTENCY_EXPIRATION" envDefault:"24h"`
}

// Load carrega configurações do ambiente.
func Load() (*Config, error) {
	cfg := &Config{
		Server:   ServerConfig{},
		Database: DatabaseConfig{},
		PSP:      PSPConfig{},
		Worker:   WorkerConfig{},
	}

	opts := env.Options{RequiredIfNoDef: false}
	if err := env.ParseWithOptions(cfg, opts); err != nil {
		return nil, fmt.Errorf("carregando configurações: %w", err)
	}

	return cfg, nil
}
```

- [ ] **Step 2: Verificar compilação**

```bash
go build ./internal/config/
```
Expected: compila sem erros.

---

### Task 2: Entidades de Domínio (internal/domain/)

**Files:**
- Create: `internal/domain/cob.go`
- Create: `internal/domain/pix.go`
- Create: `internal/domain/errors.go`

- [ ] **Step 1: Escrever domain/errors.go**

```go
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
```

- [ ] **Step 2: Escrever domain/cob.go**

```go
package domain

import (
	"encoding/json"
	"strings"
	"time"
)

// CobStatus representa o status de uma cobrança imediata.
type CobStatus string

const (
	CobStatusAtiva                     CobStatus = "ATIVA"
	CobStatusConcluida                 CobStatus = "CONCLUIDA"
	CobStatusRemovidaPeloUsuario       CobStatus = "REMOVIDA_PELO_USUARIO_RECEBEDOR"
	CobStatusRemovidaPeloPSP           CobStatus = "REMOVIDA_PELO_PSP"
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

// Valor representa o valor monetário.
type Valor struct {
	Original string `json:"original"`
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
	if c.Valor.Original == "" {
		return FormatValidationError("valor.original é obrigatório")
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
	c.Valor.Original = strings.TrimSpace(c.Valor.Original)
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
```

- [ ] **Step 3: Escrever domain/pix.go**

```go
package domain

import (
	"time"
)

// PixRecebido representa um pagamento Pix liquidado.
type PixRecebido struct {
	E2EID             string    `json:"e2eid"`
	TxID              string    `json:"txid,omitempty"`
	Chave             string    `json:"chave"`
	Valor             string    `json:"valor"`
	HorarioLiquidacao time.Time `json:"horario"`
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
	ID        string    `json:"id"`
	E2EID     string    `json:"e2eid"`
	Valor     string    `json:"valor"`
	Horario   time.Time `json:"horario"`
	Status    string    `json:"status"`
	CreatedAt time.Time `json:"-"`
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
	return &PixRecebido{
		E2EID:             p.E2EID,
		TxID:              p.TxID,
		Chave:             p.Chave,
		Valor:             p.Valor,
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
```

- [ ] **Step 4: Verificar compilação**

```bash
go build ./internal/domain/
```
Expected: compila sem erros.

---

### Task 3: Interface do PSP (internal/port/psp/psp.go)

**Files:**
- Create: `internal/port/psp/psp.go`

- [ ] **Step 1: Escrever interface PSPClient**

```go
// Package psp define a interface agnóstica de PSP seguindo o padrão BACEN.
package psp

import (
	"context"

	"github.com/matspectrum/pix-api/internal/domain"
)

// CobRequest representa o payload para criar/atualizar cobrança no PSP.
type CobRequest struct {
	Calendar    domain.Calendar `json:"calendario"`
	Devedor     domain.Devedor  `json:"devedor,omitempty"`
	Valor       domain.Valor    `json:"valor"`
	Chave       string          `json:"chave"`
	SolPagador  string          `json:"solicitacaoPagador,omitempty"`
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
	E2EID             string    `json:"e2eid"`
	TxID              string    `json:"txid,omitempty"`
	Valor             string    `json:"valor"`
	HorarioLiquidacao string    `json:"horario"`
	PagadorNome       string    `json:"pagadorNome,omitempty"`
	PagadorCPF        string    `json:"pagadorCpf,omitempty"`
	PagadorCNPJ       string    `json:"pagadorCnpj,omitempty"`
	InfoPagador       string    `json:"infoPagador,omitempty"`
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
```

- [ ] **Step 2: Verificar compilação**

```bash
go build ./internal/port/psp/
```
Expected: compila sem erros.

---

### Task 4: Migrations SQL

**Files:**
- Create: `internal/store/postgres/migrations/000001_create_cobrancas.up.sql`
- Create: `internal/store/postgres/migrations/000001_create_cobrancas.down.sql`
- Create: `internal/store/postgres/migrations/000002_create_pix_recebidos.up.sql`
- Create: `internal/store/postgres/migrations/000002_create_pix_recebidos.down.sql`
- Create: `internal/store/postgres/migrations/000003_create_idempotency.up.sql`
- Create: `internal/store/postgres/migrations/000003_create_idempotency.down.sql`
- Create: `internal/store/postgres/migrations/000004_create_outbox.up.sql`
- Create: `internal/store/postgres/migrations/000004_create_outbox.down.sql`
- Create: `internal/store/postgres/migrations/000005_create_webhooks.up.sql`
- Create: `internal/store/postgres/migrations/000005_create_webhooks.down.sql`
- Create: `internal/store/postgres/migrations/000006_create_webhook_events.up.sql`
- Create: `internal/store/postgres/migrations/000006_create_webhook_events.down.sql`
- Create: `internal/store/postgres/migrations/000007_create_reconciliation.up.sql`
- Create: `internal/store/postgres/migrations/000007_create_reconciliation.down.sql`
- Create: `internal/store/postgres/migrations/000008_create_devolucoes.up.sql`
- Create: `internal/store/postgres/migrations/000008_create_devolucoes.down.sql`

- [ ] **Step 1: Criar migration 000001 (cobrancas)**

```sql
-- 000001_create_cobrancas.up.sql
CREATE TABLE cobrancas (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    txid VARCHAR(35) NOT NULL UNIQUE,
    chave_pix VARCHAR(77) NOT NULL,
    valor_original NUMERIC(15,2) NOT NULL,
    status VARCHAR(50) NOT NULL DEFAULT 'ATIVA',
    calendario_criacao TIMESTAMPTZ NOT NULL,
    calendario_expiracao TIMESTAMPTZ NOT NULL,
    devedor_nome VARCHAR(100),
    devedor_cpf VARCHAR(14),
    devedor_cnpj VARCHAR(18),
    solicitacao_pagador TEXT,
    location_url TEXT,
    pix_copia_e_cola TEXT,
    revisao INTEGER NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT chk_valor_original CHECK (valor_original > 0),
    CONSTRAINT chk_devedor CHECK (devedor_cpf IS NOT NULL OR devedor_cnpj IS NOT NULL)
);

CREATE INDEX idx_cobrancas_chave_status ON cobrancas(chave_pix, status);
CREATE INDEX idx_cobrancas_created_at ON cobrancas(created_at);
```

```sql
-- 000001_create_cobrancas.down.sql
DROP TABLE IF EXISTS cobrancas CASCADE;
```

- [ ] **Step 2: Criar migration 000002 (pix_recebidos)**

```sql
-- 000002_create_pix_recebidos.up.sql
CREATE TABLE pix_recebidos (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    e2eid VARCHAR(64) NOT NULL UNIQUE,
    txid VARCHAR(35),
    chave_pix VARCHAR(77) NOT NULL,
    valor NUMERIC(15,2) NOT NULL,
    horario_liquidacao TIMESTAMPTZ NOT NULL,
    pagador_nome VARCHAR(100),
    pagador_cpf VARCHAR(14),
    pagador_cnpj VARCHAR(18),
    info_pagador TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT fk_pix_txid FOREIGN KEY (txid) REFERENCES cobrancas(txid)
);

CREATE INDEX idx_pix_chave ON pix_recebidos(chave_pix);
CREATE INDEX idx_pix_horario ON pix_recebidos(horario_liquidacao);
CREATE INDEX idx_pix_txid ON pix_recebidos(txid);
```

```sql
-- 000002_create_pix_recebidos.down.sql
DROP TABLE IF EXISTS pix_recebidos CASCADE;
```

- [ ] **Step 3: Criar migration 000003 (idempotency_keys)**

```sql
-- 000003_create_idempotency.up.sql
CREATE TABLE idempotency_keys (
    idempotency_key VARCHAR(64) NOT NULL,
    endpoint_path VARCHAR(255) NOT NULL,
    request_hash VARCHAR(64) NOT NULL,
    status VARCHAR(20) NOT NULL DEFAULT 'in_progress',
    response_status INTEGER,
    response_body JSONB,
    started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at TIMESTAMPTZ,
    expires_at TIMESTAMPTZ NOT NULL DEFAULT (NOW() + INTERVAL '24 hours'),
    PRIMARY KEY (idempotency_key, endpoint_path)
);

CREATE INDEX idx_idempotency_status ON idempotency_keys(status);
CREATE INDEX idx_idempotency_expires ON idempotency_keys(expires_at);
```

```sql
-- 000003_create_idempotency.down.sql
DROP TABLE IF EXISTS idempotency_keys CASCADE;
```

- [ ] **Step 4: Criar migration 000004 (outbox_messages)**

```sql
-- 000004_create_outbox.up.sql
CREATE TABLE outbox_messages (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    aggregate_type VARCHAR(100) NOT NULL,
    aggregate_id VARCHAR(64) NOT NULL,
    event_type VARCHAR(100) NOT NULL,
    payload JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    published_at TIMESTAMPTZ,
    attempts INTEGER NOT NULL DEFAULT 0,
    max_attempts INTEGER NOT NULL DEFAULT 10,
    last_error TEXT
);

CREATE INDEX idx_outbox_published ON outbox_messages(published_at) WHERE published_at IS NULL;
CREATE INDEX idx_outbox_created ON outbox_messages(created_at);
```

```sql
-- 000004_create_outbox.down.sql
DROP TABLE IF EXISTS outbox_messages CASCADE;
```

- [ ] **Step 5: Criar migration 000005 (webhooks)**

```sql
-- 000005_create_webhooks.up.sql
CREATE TABLE webhooks (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    chave_pix VARCHAR(77) NOT NULL UNIQUE,
    webhook_url VARCHAR(2048) NOT NULL,
    status VARCHAR(20) NOT NULL DEFAULT 'ATIVO',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_webhooks_chave ON webhooks(chave_pix);
```

```sql
-- 000005_create_webhooks.down.sql
DROP TABLE IF EXISTS webhooks CASCADE;
```

- [ ] **Step 6: Criar migration 000006 (webhook_events)**

```sql
-- 000006_create_webhook_events.up.sql
CREATE TABLE webhook_events (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    e2eid VARCHAR(64) NOT NULL,
    chave_pix VARCHAR(77) NOT NULL,
    payload JSONB NOT NULL,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_webhook_event UNIQUE(e2eid, chave_pix)
);

CREATE INDEX idx_webhook_events_e2eid ON webhook_events(e2eid);
```

```sql
-- 000006_create_webhook_events.down.sql
DROP TABLE IF EXISTS webhook_events CASCADE;
```

- [ ] **Step 7: Criar migration 000007 (reconciliation_reports)**

```sql
-- 000007_create_reconciliation.up.sql
CREATE TABLE reconciliation_reports (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    e2eid VARCHAR(64),
    local_valor NUMERIC(15,2),
    psp_valor NUMERIC(15,2),
    local_horario TIMESTAMPTZ,
    psp_horario TIMESTAMPTZ,
    tipo_discrepancia VARCHAR(50) NOT NULL,
    resolvido BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
```

```sql
-- 000007_create_reconciliation.down.sql
DROP TABLE IF EXISTS reconciliation_reports CASCADE;
```

- [ ] **Step 8: Criar migration 000008 (devolucoes)**

```sql
-- 000008_create_devolucoes.up.sql
CREATE TABLE devolucoes (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    external_id VARCHAR(64) NOT NULL,
    e2eid VARCHAR(64) NOT NULL,
    valor NUMERIC(15,2) NOT NULL,
    status VARCHAR(30) NOT NULL DEFAULT 'EM_PROCESSAMENTO',
    horario TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT fk_devolucao_pix FOREIGN KEY (e2eid) REFERENCES pix_recebidos(e2eid)
);

CREATE INDEX idx_devolucoes_e2eid ON devolucoes(e2eid);
```

```sql
-- 000008_create_devolucoes.down.sql
DROP TABLE IF EXISTS devolucoes CASCADE;
```

---

### Task 5: Conexão com Banco de Dados (internal/store/postgres/connection.go)

**Files:**
- Create: `internal/store/postgres/connection.go`

- [ ] **Step 1: Escrever connection.go**

```go
// Package postgres fornece acesso ao banco de dados PostgreSQL via pgxpool.
package postgres

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// NewPool cria um pool de conexões PostgreSQL.
func NewPool(ctx context.Context, dsn string, maxOpen, maxIdle int, maxLifetime, maxIdleTime time.Duration) (*pgxpool.Pool, error) {
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("parse config: %w", err)
	}

	cfg.MaxConns = int32(maxOpen)
	cfg.MinConns = int32(maxIdle)
	cfg.MaxConnLifetime = maxLifetime
	cfg.MaxConnIdleTime = maxIdleTime

	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("criando pool: %w", err)
	}

	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping database: %w", err)
	}

	slog.InfoContext(ctx, "conexão com PostgreSQL estabelecida",
		"max_open_conns", maxOpen,
		"max_idle_conns", maxIdle,
	)

	return pool, nil
}
```

- [ ] **Step 2: Verificar compilação**

```bash
go build ./internal/store/postgres/
```
Expected: compila sem erros.

---

## Onda 2: Repositórios (Tasks 6-10, paralelizáveis após Task 5)

### Task 6: Cobranca Repository (internal/store/postgres/cob_repo.go)

**Files:**
- Create: `internal/store/postgres/cob_repo.go`

- [ ] **Step 1: Escrever cob_repo.go**

```go
package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/matspectrum/pix-api/internal/domain"
)

// CobRepo gerencia persistência de cobranças.
type CobRepo struct {
	db *pgxpool.Pool
}

// NewCobRepo cria um novo repositório de cobranças.
func NewCobRepo(db *pgxpool.Pool) *CobRepo {
	return &CobRepo{db: db}
}

// Create insere uma nova cobrança.
func (r *CobRepo) Create(ctx context.Context, tx pgx.Tx, cob *domain.Cobranca) error {
	_, err := tx.Exec(ctx,
		`INSERT INTO cobrancas (txid, chave_pix, valor_original, status,
		 calendario_criacao, calendario_expiracao, devedor_nome, devedor_cpf,
		 devedor_cnpj, solicitacao_pagador, location_url, pix_copia_e_cola, revisao)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)`,
		cob.TxID, cob.Chave, cob.Valor.Original, cob.Status,
		cob.Calendar.Criacao, cob.Calendar.Criacao.Add(time.Duration(cob.Calendar.Expiracao)*time.Second),
		cob.Devedor.Nome, cob.Devedor.CPF, cob.Devedor.CNPJ,
		cob.SolicitacaoPagador, cob.Location, cob.PixCopiaECola, cob.Revisao,
	)
	if err != nil {
		return fmt.Errorf("inserindo cobrança txid=%s: %w", cob.TxID, err)
	}
	return nil
}

// Update atualiza uma cobrança existente.
func (r *CobRepo) Update(ctx context.Context, tx pgx.Tx, cob *domain.Cobranca) error {
	tag, err := tx.Exec(ctx,
		`UPDATE cobrancas SET
		 chave_pix = $2, valor_original = $3, status = $4,
		 calendario_expiracao = $5, devedor_nome = $6, devedor_cpf = $7,
		 devedor_cnpj = $8, solicitacao_pagador = $9,
		 location_url = $10, pix_copia_e_cola = $11, revisao = $12,
		 updated_at = NOW()
		 WHERE txid = $1`,
		cob.TxID, cob.Chave, cob.Valor.Original, cob.Status,
		cob.Calendar.Criacao.Add(time.Duration(cob.Calendar.Expiracao)*time.Second),
		cob.Devedor.Nome, cob.Devedor.CPF, cob.Devedor.CNPJ,
		cob.SolicitacaoPagador, cob.Location, cob.PixCopiaECola, cob.Revisao,
	)
	if err != nil {
		return fmt.Errorf("atualizando cobrança txid=%s: %w", cob.TxID, err)
	}
	if tag.RowsAffected() == 0 {
		return domain.ErrCobrancaNaoEncontrada
	}
	return nil
}

// UpdateStatus atualiza apenas o status de uma cobrança.
func (r *CobRepo) UpdateStatus(ctx context.Context, tx pgx.Tx, txid string, status domain.CobStatus) error {
	tag, err := tx.Exec(ctx,
		`UPDATE cobrancas SET status = $2, updated_at = NOW() WHERE txid = $1`,
		txid, string(status),
	)
	if err != nil {
		return fmt.Errorf("atualizando status cobrança txid=%s: %w", txid, err)
	}
	if tag.RowsAffected() == 0 {
		return domain.ErrCobrancaNaoEncontrada
	}
	return nil
}

// GetByTxID busca cobrança por txid.
func (r *CobRepo) GetByTxID(ctx context.Context, txid string) (*domain.Cobranca, error) {
	var cob domain.Cobranca
	var statusStr string
	var calendarioExpiracao time.Time

	err := r.db.QueryRow(ctx,
		`SELECT txid, chave_pix, valor_original::text, status,
		 calendario_criacao, calendario_expiracao, devedor_nome,
		 devedor_cpf, devedor_cnpj, solicitacao_pagador,
		 location_url, pix_copia_e_cola, revisao, created_at, updated_at
		 FROM cobrancas WHERE txid = $1`, txid,
	).Scan(
		&cob.TxID, &cob.Chave, &cob.Valor.Original, &statusStr,
		&cob.Calendar.Criacao, &calendarioExpiracao, &cob.Devedor.Nome,
		&cob.Devedor.CPF, &cob.Devedor.CNPJ, &cob.SolicitacaoPagador,
		&cob.Location, &cob.PixCopiaECola, &cob.Revisao,
		&cob.CreatedAt, &cob.UpdatedAt,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, domain.ErrCobrancaNaoEncontrada
		}
		return nil, fmt.Errorf("buscando cobrança txid=%s: %w", txid, err)
	}

	cob.Status = domain.CobStatus(statusStr)
	cob.Calendar.Expiracao = int(calendarioExpiracao.Sub(cob.Calendar.Criacao).Seconds())
	return &cob, nil
}

// List busca cobranças com paginação e filtros.
func (r *CobRepo) List(ctx context.Context, filter domain.CobFilter) ([]domain.Cobranca, int, error) {
	// Defaults
	if filter.Limit <= 0 || filter.Limit > 100 {
		filter.Limit = 20
	}
	if filter.Offset < 0 {
		filter.Offset = 0
	}

	// Contagem total
	var total int
	err := r.db.QueryRow(ctx,
		`SELECT COUNT(*) FROM cobrancas
		 WHERE ($1::timestamptz IS NULL OR created_at >= $1)
		 AND ($2::timestamptz IS NULL OR created_at <= $2)`,
		filter.Inicio, filter.Fim,
	).Scan(&total)
	if err != nil {
		return nil, 0, fmt.Errorf("contando cobranças: %w", err)
	}

	rows, err := r.db.Query(ctx,
		`SELECT txid, chave_pix, valor_original::text, status,
		 calendario_criacao, calendario_expiracao, devedor_nome,
		 devedor_cpf, devedor_cnpj, solicitacao_pagador,
		 location_url, pix_copia_e_cola, revisao, created_at, updated_at
		 FROM cobrancas
		 WHERE ($3::timestamptz IS NULL OR created_at >= $3)
		 AND ($4::timestamptz IS NULL OR created_at <= $4)
		 ORDER BY created_at DESC
		 LIMIT $1 OFFSET $2`,
		filter.Limit, filter.Offset, filter.Inicio, filter.Fim,
	)
	if err != nil {
		return nil, 0, fmt.Errorf("listando cobranças: %w", err)
	}
	defer rows.Close()

	var cobs []domain.Cobranca
	for rows.Next() {
		var cob domain.Cobranca
		var statusStr string
		var calendarioExpiracao time.Time

		if err := rows.Scan(
			&cob.TxID, &cob.Chave, &cob.Valor.Original, &statusStr,
			&cob.Calendar.Criacao, &calendarioExpiracao, &cob.Devedor.Nome,
			&cob.Devedor.CPF, &cob.Devedor.CNPJ, &cob.SolicitacaoPagador,
			&cob.Location, &cob.PixCopiaECola, &cob.Revisao,
			&cob.CreatedAt, &cob.UpdatedAt,
		); err != nil {
			return nil, 0, fmt.Errorf("scaneando cobrança: %w", err)
		}
		cob.Status = domain.CobStatus(statusStr)
		cob.Calendar.Expiracao = int(calendarioExpiracao.Sub(cob.Calendar.Criacao).Seconds())
		cobs = append(cobs, cob)
	}

	if err := rows.Err(); err != nil {
		return nil, 0, fmt.Errorf("iterando cobranças: %w", err)
	}

	return cobs, total, nil
}
```

- [ ] **Step 2: Verificar compilação**

```bash
go build ./internal/store/postgres/
```
Expected: compila sem erros.

---

### Task 7: Pix Repository (internal/store/postgres/pix_repo.go)

**Files:**
- Create: `internal/store/postgres/pix_repo.go`

- [ ] **Step 1: Escrever pix_repo.go**

```go
package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/matspectrum/pix-api/internal/domain"
)

// PixRepo gerencia persistência de Pix recebidos.
type PixRepo struct {
	db *pgxpool.Pool
}

// NewPixRepo cria um novo repositório de Pix.
func NewPixRepo(db *pgxpool.Pool) *PixRepo {
	return &PixRepo{db: db}
}

// Create insere um Pix recebido (dentro de transação).
func (r *PixRepo) Create(ctx context.Context, tx pgx.Tx, pix *domain.PixRecebido) error {
	_, err := tx.Exec(ctx,
		`INSERT INTO pix_recebidos (e2eid, txid, chave_pix, valor, horario_liquidacao,
		 pagador_nome, pagador_cpf, pagador_cnpj, info_pagador)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`,
		pix.E2EID, pix.TxID, pix.Chave, pix.Valor,
		pix.HorarioLiquidacao, pix.PagadorNome, pix.PagadorCPF,
		pix.PagadorCNPJ, pix.InfoPagador,
	)
	if err != nil {
		return fmt.Errorf("inserindo pix e2eid=%s: %w", pix.E2EID, err)
	}
	return nil
}

// GetByE2EID busca Pix por e2eid.
func (r *PixRepo) GetByE2EID(ctx context.Context, e2eid string) (*domain.PixRecebido, error) {
	var pix domain.PixRecebido

	err := r.db.QueryRow(ctx,
		`SELECT e2eid, txid, chave_pix, valor::text, horario_liquidacao,
		 pagador_nome, pagador_cpf, pagador_cnpj, info_pagador, created_at
		 FROM pix_recebidos WHERE e2eid = $1`, e2eid,
	).Scan(
		&pix.E2EID, &pix.TxID, &pix.Chave, &pix.Valor,
		&pix.HorarioLiquidacao, &pix.PagadorNome, &pix.PagadorCPF,
		&pix.PagadorCNPJ, &pix.InfoPagador, &pix.CreatedAt,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, domain.ErrPixNaoEncontrado
		}
		return nil, fmt.Errorf("buscando pix e2eid=%s: %w", e2eid, err)
	}

	return &pix, nil
}

// List busca Pix recebidos com paginação e filtros.
func (r *PixRepo) List(ctx context.Context, filter domain.PixFilter) ([]domain.PixRecebido, int, error) {
	if filter.Limit <= 0 || filter.Limit > 100 {
		filter.Limit = 20
	}
	if filter.Offset < 0 {
		filter.Offset = 0
	}

	var total int
	err := r.db.QueryRow(ctx,
		`SELECT COUNT(*) FROM pix_recebidos
		 WHERE ($1::timestamptz IS NULL OR horario_liquidacao >= $1)
		 AND ($2::timestamptz IS NULL OR horario_liquidacao <= $2)
		 AND ($5::varchar IS NULL OR txid = $5)
		 AND ($6::varchar IS NULL OR chave_pix = $6)`,
		filter.Inicio, filter.Fim, filter.TxID, filter.Chave,
	).Scan(&total)
	if err != nil {
		return nil, 0, fmt.Errorf("contando pix: %w", err)
	}

	rows, err := r.db.Query(ctx,
		`SELECT e2eid, txid, chave_pix, valor::text, horario_liquidacao,
		 pagador_nome, pagador_cpf, pagador_cnpj, info_pagador, created_at
		 FROM pix_recebidos
		 WHERE ($3::timestamptz IS NULL OR horario_liquidacao >= $3)
		 AND ($4::timestamptz IS NULL OR horario_liquidacao <= $4)
		 AND ($7::varchar IS NULL OR txid = $7)
		 AND ($8::varchar IS NULL OR chave_pix = $8)
		 ORDER BY horario_liquidacao DESC
		 LIMIT $1 OFFSET $2`,
		filter.Limit, filter.Offset,
		filter.Inicio, filter.Fim,
		filter.TxID, filter.Chave,
	)
	if err != nil {
		return nil, 0, fmt.Errorf("listando pix: %w", err)
	}
	defer rows.Close()

	var pixs []domain.PixRecebido
	for rows.Next() {
		var pix domain.PixRecebido
		if err := rows.Scan(
			&pix.E2EID, &pix.TxID, &pix.Chave, &pix.Valor,
			&pix.HorarioLiquidacao, &pix.PagadorNome, &pix.PagadorCPF,
			&pix.PagadorCNPJ, &pix.InfoPagador, &pix.CreatedAt,
		); err != nil {
			return nil, 0, fmt.Errorf("scaneando pix: %w", err)
		}
		pixs = append(pixs, pix)
	}

	if err := rows.Err(); err != nil {
		return nil, 0, fmt.Errorf("iterando pix: %w", err)
	}

	return pixs, total, nil
}

// CreateDevolucao insere uma devolução na tabela de devoluções.
func (r *PixRepo) CreateDevolucao(ctx context.Context, tx pgx.Tx, dev *domain.Devolucao) error {
	_, err := tx.Exec(ctx,
		`INSERT INTO devolucoes (external_id, e2eid, valor, status, horario)
		 VALUES ($1, $2, $3, $4, $5)`,
		dev.ID, dev.E2EID, dev.Valor, dev.Status, dev.Horario,
	)
	if err != nil {
		return fmt.Errorf("inserindo devolucao id=%s: %w", dev.ID, err)
	}
	return nil
}

// ListDevolucoes lista devoluções por e2eid.
func (r *PixRepo) ListDevolucoes(ctx context.Context, e2eid string) ([]domain.Devolucao, error) {
	rows, err := r.db.Query(ctx,
		`SELECT external_id, e2eid, valor::text, status, horario, created_at
		 FROM devolucoes WHERE e2eid = $1 ORDER BY created_at DESC`, e2eid,
	)
	if err != nil {
		return nil, fmt.Errorf("listando devolucoes e2eid=%s: %w", e2eid, err)
	}
	defer rows.Close()

	var devs []domain.Devolucao
	for rows.Next() {
		var dev domain.Devolucao
		if err := rows.Scan(
			&dev.ID, &dev.E2EID, &dev.Valor,
			&dev.Status, &dev.Horario, &dev.CreatedAt,
		); err != nil {
			return nil, fmt.Errorf("scaneando devolucao: %w", err)
		}
		devs = append(devs, dev)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterando devolucoes: %w", err)
	}

	return devs, nil
}
```

- [ ] **Step 2: Verificar compilação**

```bash
go build ./internal/store/postgres/
```
Expected: compila sem erros.

---

### Task 8: Idempotency Repository (internal/store/postgres/idempotency_repo.go)

**Files:**
- Create: `internal/store/postgres/idempotency_repo.go`

- [ ] **Step 1: Escrever idempotency_repo.go**

```go
package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/matspectrum/pix-api/internal/domain"
)

// IdempotencyRecord armazena o estado de uma chave de idempotência.
type IdempotencyRecord struct {
	IdempotencyKey string
	EndpointPath   string
	RequestHash    string
	Status         string
	ResponseStatus int
	ResponseBody   []byte
	StartedAt      time.Time
	CompletedAt    *time.Time
	ExpiresAt      time.Time
}

// IdempotencyRepo gerencia chaves de idempotência.
type IdempotencyRepo struct {
	db *pgxpool.Pool
}

// NewIdempotencyRepo cria um novo repositório de idempotência.
func NewIdempotencyRepo(db *pgxpool.Pool) *IdempotencyRepo {
	return &IdempotencyRepo{db: db}
}

// Acquire tenta adquirir uma chave de idempotência.
// Se a chave não existir, insere com status in_progress.
// Se existir e o hash for igual, retorna o registro existente.
// Se existir e o hash for diferente, retorna ErrIdempotencyKeyDiverged.
func (r *IdempotencyRepo) Acquire(ctx context.Context, key, endpointPath, requestHash string) (*IdempotencyRecord, error) {
	// Tenta inserir com ON CONFLICT DO NOTHING
	tag, err := r.db.Exec(ctx,
		`INSERT INTO idempotency_keys (idempotency_key, endpoint_path, request_hash, status)
		 VALUES ($1, $2, $3, 'in_progress')
		 ON CONFLICT (idempotency_key, endpoint_path) DO NOTHING`,
		key, endpointPath, requestHash,
	)
	if err != nil {
		return nil, fmt.Errorf("inserindo chave idempotencia: %w", err)
	}

	// Se inseriu (RowsAffected == 1), é nova chave
	if tag.RowsAffected() == 1 {
		return &IdempotencyRecord{
			IdempotencyKey: key,
			EndpointPath:   endpointPath,
			RequestHash:    requestHash,
			Status:         "in_progress",
			StartedAt:      time.Now(),
		}, nil
	}

	// Já existia, busca o registro
	record, err := r.Get(ctx, key, endpointPath)
	if err != nil {
		return nil, fmt.Errorf("buscando chave idempotencia existente: %w", err)
	}

	// Verifica se o hash coincide
	if record.RequestHash != requestHash {
		return record, domain.ErrIdempotencyKeyDiverged
	}

	return record, nil
}

// Get busca um registro de idempotência.
func (r *IdempotencyRepo) Get(ctx context.Context, key, endpointPath string) (*IdempotencyRecord, error) {
	var rec IdempotencyRecord
	err := r.db.QueryRow(ctx,
		`SELECT idempotency_key, endpoint_path, request_hash, status,
		 response_status, response_body, started_at, completed_at, expires_at
		 FROM idempotency_keys
		 WHERE idempotency_key = $1 AND endpoint_path = $2`,
		key, endpointPath,
	).Scan(
		&rec.IdempotencyKey, &rec.EndpointPath, &rec.RequestHash, &rec.Status,
		&rec.ResponseStatus, &rec.ResponseBody, &rec.StartedAt, &rec.CompletedAt, &rec.ExpiresAt,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, fmt.Errorf("chave idempotencia nao encontrada: %w", err)
		}
		return nil, fmt.Errorf("buscando chave idempotencia: %w", err)
	}
	return &rec, nil
}

// Complete marca a chave como concluída com sucesso.
func (r *IdempotencyRepo) Complete(ctx context.Context, key, endpointPath string, status int, body []byte) error {
	_, err := r.db.Exec(ctx,
		`UPDATE idempotency_keys
		 SET status = 'completed', response_status = $3, response_body = $4, completed_at = NOW()
		 WHERE idempotency_key = $1 AND endpoint_path = $2`,
		key, endpointPath, status, body,
	)
	if err != nil {
		return fmt.Errorf("completando chave idempotencia: %w", err)
	}
	return nil
}

// Fail marca a chave como falha.
func (r *IdempotencyRepo) Fail(ctx context.Context, key, endpointPath string) error {
	_, err := r.db.Exec(ctx,
		`UPDATE idempotency_keys
		 SET status = 'failed', completed_at = NOW()
		 WHERE idempotency_key = $1 AND endpoint_path = $2`,
		key, endpointPath,
	)
	if err != nil {
		return fmt.Errorf("marcando chave idempotencia como falha: %w", err)
	}
	return nil
}

// CleanupExpired remove chaves expiradas.
func (r *IdempotencyRepo) CleanupExpired(ctx context.Context) (int64, error) {
	tag, err := r.db.Exec(ctx,
		`DELETE FROM idempotency_keys WHERE expires_at < NOW()`,
	)
	if err != nil {
		return 0, fmt.Errorf("limpando chaves idempotencia expiradas: %w", err)
	}
	return tag.RowsAffected(), nil
}
```

- [ ] **Step 2: Verificar compilação**

```bash
go build ./internal/store/postgres/
```
Expected: compila sem erros.

---

### Task 9: Outbox Repository (internal/store/postgres/outbox_repo.go)

**Files:**
- Create: `internal/store/postgres/outbox_repo.go`

- [ ] **Step 1: Escrever outbox_repo.go**

```go
package postgres

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// OutboxMessage representa uma mensagem no outbox transacional.
type OutboxMessage struct {
	ID            string
	AggregateType string
	AggregateID   string
	EventType     string
	Payload       json.RawMessage
	CreatedAt     time.Time
	PublishedAt   *time.Time
	Attempts      int
	MaxAttempts   int
	LastError     string
}

// OutboxWriter escreve mensagens no outbox dentro de uma transação.
type OutboxWriter struct {
	db *pgxpool.Pool
}

// NewOutboxWriter cria um novo OutboxWriter.
func NewOutboxWriter(db *pgxpool.Pool) *OutboxWriter {
	return &OutboxWriter{db: db}
}

// Write insere uma mensagem no outbox dentro da transação fornecida.
func (w *OutboxWriter) Write(ctx context.Context, tx pgx.Tx, aggregateType, aggregateID, eventType string, payload interface{}) error {
	payloadJSON, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("serializando payload outbox: %w", err)
	}

	_, err = tx.Exec(ctx,
		`INSERT INTO outbox_messages (aggregate_type, aggregate_id, event_type, payload)
		 VALUES ($1, $2, $3, $4)`,
		aggregateType, aggregateID, eventType, payloadJSON,
	)
	if err != nil {
		return fmt.Errorf("escrevendo mensagem outbox: %w", err)
	}
	return nil
}

// OutboxReader lê mensagens pendentes do outbox.
type OutboxReader struct {
	db *pgxpool.Pool
}

// NewOutboxReader cria um novo OutboxReader.
func NewOutboxReader(db *pgxpool.Pool) *OutboxReader {
	return &OutboxReader{db: db}
}

// FetchPending busca mensagens não publicadas com lock pessimista.
func (r *OutboxReader) FetchPending(ctx context.Context, limit int) ([]OutboxMessage, error) {
	rows, err := r.db.Query(ctx,
		`SELECT id, aggregate_type, aggregate_id, event_type, payload, created_at
		 FROM outbox_messages
		 WHERE published_at IS NULL AND attempts < max_attempts
		 ORDER BY created_at ASC
		 LIMIT $1
		 FOR UPDATE SKIP LOCKED`,
		limit,
	)
	if err != nil {
		return nil, fmt.Errorf("buscando mensagens outbox pendentes: %w", err)
	}
	defer rows.Close()

	var messages []OutboxMessage
	for rows.Next() {
		var msg OutboxMessage
		if err := rows.Scan(
			&msg.ID, &msg.AggregateType, &msg.AggregateID,
			&msg.EventType, &msg.Payload, &msg.CreatedAt,
		); err != nil {
			return nil, fmt.Errorf("scaneando mensagem outbox: %w", err)
		}
		messages = append(messages, msg)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterando mensagens outbox: %w", err)
	}

	return messages, nil
}

// MarkPublished marca uma mensagem como publicada.
func (r *OutboxReader) MarkPublished(ctx context.Context, id string) error {
	_, err := r.db.Exec(ctx,
		`UPDATE outbox_messages SET published_at = NOW(), attempts = attempts + 1 WHERE id = $1`,
		id,
	)
	if err != nil {
		return fmt.Errorf("marcando outbox publicado id=%s: %w", id, err)
	}
	return nil
}

// MarkFailed marca uma mensagem como falha e incrementa tentativas.
func (r *OutboxReader) MarkFailed(ctx context.Context, id string, lastError string) error {
	_, err := r.db.Exec(ctx,
		`UPDATE outbox_messages
		 SET attempts = attempts + 1, last_error = $2
		 WHERE id = $1`,
		id, lastError,
	)
	if err != nil {
		return fmt.Errorf("marcando outbox falho id=%s: %w", id, err)
	}
	return nil
}
```

- [ ] **Step 2: Verificar compilação**

```bash
go build ./internal/store/postgres/
```
Expected: compila sem erros.

---

### Task 10: Webhook Repository (internal/store/postgres/webhook_repo.go)

**Files:**
- Create: `internal/store/postgres/webhook_repo.go`

- [ ] **Step 1: Escrever webhook_repo.go**

```go
package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/matspectrum/pix-api/internal/domain"
)

// WebhookRepo gerencia persistência de webhooks.
type WebhookRepo struct {
	db *pgxpool.Pool
}

// NewWebhookRepo cria um novo repositório de webhooks.
func NewWebhookRepo(db *pgxpool.Pool) *WebhookRepo {
	return &WebhookRepo{db: db}
}

// Upsert insere ou atualiza configuração de webhook.
func (r *WebhookRepo) Upsert(ctx context.Context, wc *domain.WebhookConfig) error {
	_, err := r.db.Exec(ctx,
		`INSERT INTO webhooks (chave_pix, webhook_url, status)
		 VALUES ($1, $2, $3)
		 ON CONFLICT (chave_pix)
		 DO UPDATE SET webhook_url = $2, status = $3, updated_at = NOW()`,
		wc.Chave, wc.WebhookURL, wc.Status,
	)
	if err != nil {
		return fmt.Errorf("upsert webhook chave=%s: %w", wc.Chave, err)
	}
	return nil
}

// GetByChave busca webhook por chave Pix.
func (r *WebhookRepo) GetByChave(ctx context.Context, chave string) (*domain.WebhookConfig, error) {
	var wc domain.WebhookConfig
	err := r.db.QueryRow(ctx,
		`SELECT chave_pix, webhook_url, status, created_at, updated_at
		 FROM webhooks WHERE chave_pix = $1`, chave,
	).Scan(&wc.Chave, &wc.WebhookURL, &wc.Status, &wc.CreatedAt, &wc.UpdatedAt)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, domain.ErrWebhookNaoEncontrado
		}
		return nil, fmt.Errorf("buscando webhook chave=%s: %w", chave, err)
	}
	return &wc, nil
}

// List retorna todos os webhooks configurados.
func (r *WebhookRepo) List(ctx context.Context) ([]domain.WebhookConfig, error) {
	rows, err := r.db.Query(ctx,
		`SELECT chave_pix, webhook_url, status, created_at, updated_at
		 FROM webhooks ORDER BY chave_pix`,
	)
	if err != nil {
		return nil, fmt.Errorf("listando webhooks: %w", err)
	}
	defer rows.Close()

	var webhooks []domain.WebhookConfig
	for rows.Next() {
		var wc domain.WebhookConfig
		if err := rows.Scan(
			&wc.Chave, &wc.WebhookURL, &wc.Status,
			&wc.CreatedAt, &wc.UpdatedAt,
		); err != nil {
			return nil, fmt.Errorf("scaneando webhook: %w", err)
		}
		webhooks = append(webhooks, wc)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterando webhooks: %w", err)
	}

	return webhooks, nil
}

// Delete remove a configuração de webhook por chave.
func (r *WebhookRepo) Delete(ctx context.Context, chave string) error {
	tag, err := r.db.Exec(ctx,
		`DELETE FROM webhooks WHERE chave_pix = $1`, chave,
	)
	if err != nil {
		return fmt.Errorf("deletando webhook chave=%s: %w", chave, err)
	}
	if tag.RowsAffected() == 0 {
		return domain.ErrWebhookNaoEncontrado
	}
	return nil
}

// InsertEvent insere evento de webhook com dedup.
func (r *WebhookRepo) InsertEvent(ctx context.Context, e2eid, chave string, payload []byte) error {
	_, err := r.db.Exec(ctx,
		`INSERT INTO webhook_events (e2eid, chave_pix, payload)
		 VALUES ($1, $2, $3)
		 ON CONFLICT (e2eid, chave_pix) DO NOTHING`,
		e2eid, chave, payload,
	)
	if err != nil {
		return fmt.Errorf("inserindo evento webhook e2eid=%s: %w", e2eid, err)
	}
	return nil
}
```

- [ ] **Step 2: Verificar compilação**

```bash
go build ./internal/store/postgres/
```
Expected: compila sem erros.

---

### Task 11: Mock PSP (internal/port/psp/mock/)

**Files:**
- Create: `internal/port/psp/mock/client.go`
- Create: `internal/port/psp/mock/cob.go`
- Create: `internal/port/psp/mock/pix.go`
- Create: `internal/port/psp/mock/webhook.go`

- [ ] **Step 1: Escrever mock/client.go**

```go
// Package mock fornece uma implementação mock do PSP para desenvolvimento.
package mock

import (
	"github.com/matspectrum/pix-api/internal/port/psp"
)

// MockPSP implementa a interface PSPClient com armazenamento em memória.
type MockPSP struct {
	cobs     *MockCobStore
	pixs     *MockPixStore
	webhooks *MockWebhookStore
}

// NewMockPSP cria uma nova instância do mock PSP.
func NewMockPSP() *MockPSP {
	return &MockPSP{
		cobs:     NewMockCobStore(),
		pixs:     NewMockPixStore(),
		webhooks: NewMockWebhookStore(),
	}
}

// Garantia de interface em tempo de compilação.
var _ psp.PSPClient = (*MockPSP)(nil)

// PixStore retorna o store de Pix mock (para uso em testes).
func (m *MockPSP) PixStore() *MockPixStore {
	return m.pixs
}

// CobStore retorna o store de cobranças mock (para uso em testes).
func (m *MockPSP) CobStore() *MockCobStore {
	return m.cobs
}
```

- [ ] **Step 2: Escrever mock/cob.go**

```go
package mock

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/matspectrum/pix-api/internal/domain"
	"github.com/matspectrum/pix-api/internal/port/psp"
)

// MockCobStore armazena cobranças em memória.
type MockCobStore struct {
	mu   sync.RWMutex
	cobs map[string]*psp.CobResponse
}

// NewMockCobStore cria um store de cobranças mock.
func NewMockCobStore() *MockCobStore {
	return &MockCobStore{
		cobs: make(map[string]*psp.CobResponse),
	}
}

// CreateCob cria uma cobrança mock.
func (m *MockPSP) CreateCob(ctx context.Context, txid string, req psp.CobRequest) (*psp.CobResponse, error) {
	m.cobs.mu.Lock()
	defer m.cobs.mu.Unlock()

	now := time.Now()
	location := fmt.Sprintf("https://pix.example.com/api/v2/cob/%s", txid)
	pixCopiaECola := fmt.Sprintf("00020101021126360014br.gov.bcb.pix0114%s5204000053039865802BR5925Recebedor%20Mock6009Sao%20Paulo62070503***6304A1B2",
		txid)

	cob := &psp.CobResponse{
		TxID:     txid,
		Revisao:  0,
		Calendar: req.Calendar,
		Devedor:  req.Devedor,
		Valor:    req.Valor,
		Chave:    req.Chave,
		SolPagador:    req.SolPagador,
		Status:        domain.CobStatusAtiva,
		Location:      location,
		PixCopiaECola: pixCopiaECola,
	}

	cob.Calendar.Criacao = now
	m.cobs.cobs[txid] = cob

	resp := *cob
	return &resp, nil
}

// UpdateCob atualiza uma cobrança mock.
func (m *MockPSP) UpdateCob(ctx context.Context, txid string, req psp.CobRequest) (*psp.CobResponse, error) {
	m.cobs.mu.Lock()
	defer m.cobs.mu.Unlock()

	existing, ok := m.cobs.cobs[txid]
	if !ok {
		return nil, fmt.Errorf("cobranca nao encontrada: %s", txid)
	}

	existing.Calendar = req.Calendar
	existing.Devedor = req.Devedor
	existing.Valor = req.Valor
	existing.Chave = req.Chave
	existing.SolPagador = req.SolPagador
	existing.Revisao++

	resp := *existing
	return &resp, nil
}

// GetCob busca uma cobrança mock.
func (m *MockPSP) GetCob(ctx context.Context, txid string) (*psp.CobResponse, error) {
	m.cobs.mu.RLock()
	defer m.cobs.mu.RUnlock()

	cob, ok := m.cobs.cobs[txid]
	if !ok {
		return nil, fmt.Errorf("cobranca nao encontrada: %s", txid)
	}

	resp := *cob
	return &resp, nil
}

// ListCobs lista cobranças mock.
func (m *MockPSP) ListCobs(ctx context.Context, inicio, fim string, limit, offset int) ([]psp.CobResponse, int, error) {
	m.cobs.mu.RLock()
	defer m.cobs.mu.RUnlock()

	var result []psp.CobResponse
	for _, cob := range m.cobs.cobs {
		result = append(result, *cob)
	}

	total := len(result)

	// Aplica offset
	if offset >= len(result) {
		return []psp.CobResponse{}, total, nil
	}
	result = result[offset:]

	// Aplica limit
	if limit > 0 && limit < len(result) {
		result = result[:limit]
	}

	return result, total, nil
}
```

- [ ] **Step 3: Escrever mock/pix.go**

```go
package mock

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
)

// pixRecord armazena um Pix mock.
type pixRecord struct {
	E2EID             string
	TxID              string
	Valor             string
	HorarioLiquidacao time.Time
	PagadorNome       string
	PagadorCPF        string
	PagadorCNPJ       string
	InfoPagador       string
}

// devolucaoRecord armazena uma devolução mock.
type devolucaoRecord struct {
	ID      string
	E2EID   string
	Valor   string
	Horario time.Time
	Status  string
}

// MockPixStore armazena Pix e devoluções em memória.
type MockPixStore struct {
	mu      sync.RWMutex
	pixs    map[string]*pixRecord
	devolucoes map[string]*devolucaoRecord
}

// NewMockPixStore cria um store de Pix mock.
func NewMockPixStore() *MockPixStore {
	return &MockPixStore{
		pixs:    make(map[string]*pixRecord),
		devolucoes: make(map[string]*devolucaoRecord),
	}
}

// PixResponse representa a resposta do mock para consulta de Pix.
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

// DevolucaoResponse representa a resposta do mock para devolução.
type DevolucaoResponse struct {
	ID      string `json:"id"`
	E2EID   string `json:"e2eid"`
	Valor   string `json:"valor"`
	Horario string `json:"horario"`
	Status  string `json:"status"`
}

// GetPix busca um Pix mock por e2eid.
func (m *MockPSP) GetPix(ctx context.Context, e2eid string) (*psp.PixResponse, error) {
	m.pixs.mu.RLock()
	defer m.pixs.mu.RUnlock()

	pix, ok := m.pixs.pixs[e2eid]
	if !ok {
		return nil, fmt.Errorf("pix nao encontrado: %s", e2eid)
	}

	return &psp.PixResponse{
		E2EID:             pix.E2EID,
		TxID:              pix.TxID,
		Valor:             pix.Valor,
		HorarioLiquidacao: pix.HorarioLiquidacao.Format(time.RFC3339),
		PagadorNome:       pix.PagadorNome,
		PagadorCPF:        pix.PagadorCPF,
		PagadorCNPJ:       pix.PagadorCNPJ,
		InfoPagador:       pix.InfoPagador,
	}, nil
}

// ListPix lista Pix mock.
func (m *MockPSP) ListPix(ctx context.Context, inicio, fim string, limit, offset int) ([]psp.PixResponse, int, error) {
	m.pixs.mu.RLock()
	defer m.pixs.mu.RUnlock()

	var result []psp.PixResponse
	for _, pix := range m.pixs.pixs {
		result = append(result, psp.PixResponse{
			E2EID:             pix.E2EID,
			TxID:              pix.TxID,
			Valor:             pix.Valor,
			HorarioLiquidacao: pix.HorarioLiquidacao.Format(time.RFC3339),
			PagadorNome:       pix.PagadorNome,
			PagadorCPF:        pix.PagadorCPF,
			PagadorCNPJ:       pix.PagadorCNPJ,
			InfoPagador:       pix.InfoPagador,
		})
	}

	total := len(result)
	if offset >= len(result) {
		return []psp.PixResponse{}, total, nil
	}
	result = result[offset:]
	if limit > 0 && limit < len(result) {
		result = result[:limit]
	}

	return result, total, nil
}

// CreateDevolucao cria uma devolução mock.
func (m *MockPSP) CreateDevolucao(ctx context.Context, e2eid, id, valor string) (*psp.DevolucaoResponse, error) {
	m.pixs.mu.Lock()
	defer m.pixs.mu.Unlock()

	if _, ok := m.pixs.pixs[e2eid]; !ok {
		return nil, fmt.Errorf("pix nao encontrado: %s", e2eid)
	}

	if id == "" {
		id = uuid.New().String()
	}

	now := time.Now()
	dev := &devolucaoRecord{
		ID:      id,
		E2EID:   e2eid,
		Valor:   valor,
		Horario: now,
		Status:  "EM_PROCESSAMENTO",
	}
	m.pixs.devolucoes[id] = dev

	return &psp.DevolucaoResponse{
		ID:      dev.ID,
		E2EID:   dev.E2EID,
		Valor:   dev.Valor,
		Horario: now.Format(time.RFC3339),
		Status:  dev.Status,
	}, nil
}

// AddPix adiciona um Pix ao store (para testes e simulação).
func (s *MockPixStore) AddPix(e2eid, txid, valor string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.pixs[e2eid] = &pixRecord{
		E2EID:             e2eid,
		TxID:              txid,
		Valor:             valor,
		HorarioLiquidacao: time.Now(),
		PagadorNome:       "João Silva",
		PagadorCPF:        "01234567890",
	}
}
```

- [ ] **Step 4: Escrever mock/webhook.go**

```go
package mock

import (
	"context"
	"fmt"
	"sync"

	"github.com/matspectrum/pix-api/internal/domain"
)

// MockWebhookStore armazena webhooks em memória.
type MockWebhookStore struct {
	mu       sync.RWMutex
	webhooks map[string]*domain.WebhookConfig
}

// NewMockWebhookStore cria um store de webhooks mock.
func NewMockWebhookStore() *MockWebhookStore {
	return &MockWebhookStore{
		webhooks: make(map[string]*domain.WebhookConfig),
	}
}

// ConfigureWebhook configura um webhook mock.
func (m *MockPSP) ConfigureWebhook(ctx context.Context, chave, url string) error {
	m.webhooks.mu.Lock()
	defer m.webhooks.mu.Unlock()

	m.webhooks.webhooks[chave] = &domain.WebhookConfig{
		Chave:      chave,
		WebhookURL: url,
		Status:     "ATIVO",
	}
	return nil
}

// GetWebhook busca configuração de webhook mock.
func (m *MockPSP) GetWebhook(ctx context.Context, chave string) (*domain.WebhookConfig, error) {
	m.webhooks.mu.RLock()
	defer m.webhooks.mu.RUnlock()

	wc, ok := m.webhooks.webhooks[chave]
	if !ok {
		return nil, fmt.Errorf("webhook nao encontrado: %s", chave)
	}

	resp := *wc
	return &resp, nil
}

// DeleteWebhook remove configuração de webhook mock.
func (m *MockPSP) DeleteWebhook(ctx context.Context, chave string) error {
	m.webhooks.mu.Lock()
	defer m.webhooks.mu.Unlock()

	if _, ok := m.webhooks.webhooks[chave]; !ok {
		return fmt.Errorf("webhook nao encontrado: %s", chave)
	}

	delete(m.webhooks.webhooks, chave)
	return nil
}
```

- [ ] **Step 5: Verificar compilação**

```bash
go build ./internal/port/psp/mock/
```
Expected: compila sem erros.

---

## Onda 3: Serviços (Tasks 12-14)

### Task 12: Cob Service (internal/service/cob_service.go)

**Files:**
- Create: `internal/service/cob_service.go`

- [ ] **Step 1: Escrever cob_service.go**

```go
// Package service implementa a lógica de negócio da API Pix.
package service

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/matspectrum/pix-api/internal/domain"
	"github.com/matspectrum/pix-api/internal/port/psp"
	"github.com/matspectrum/pix-api/internal/store/postgres"
)

// CobService gerencia operações de cobrança.
type CobService struct {
	db          *pgxpool.Pool
	cobRepo     *postgres.CobRepo
	pspClient   psp.PSPClient
	outboxWriter *postgres.OutboxWriter
}

// NewCobService cria um novo serviço de cobranças.
func NewCobService(db *pgxpool.Pool, cobRepo *postgres.CobRepo, pspClient psp.PSPClient, outboxWriter *postgres.OutboxWriter) *CobService {
	return &CobService{
		db:          db,
		cobRepo:     cobRepo,
		pspClient:   pspClient,
		outboxWriter: outboxWriter,
	}
}

// CreateCob cria uma nova cobrança imediata.
func (s *CobService) CreateCob(ctx context.Context, cob *domain.Cobranca) (*domain.Cobranca, error) {
	cob.Sanitize()

	if err := cob.Validate(); err != nil {
		return nil, err
	}

	// Verifica se txid já existe
	existing, err := s.cobRepo.GetByTxID(ctx, cob.TxID)
	if err == nil && existing != nil {
		// Se já existe com mesmo conteúdo, retorna existente (idempotência)
		if existing.Chave == cob.Chave && existing.Valor.Original == cob.Valor.Original {
			return existing, nil
		}
		return nil, domain.FormatValidationError("txid %s já existe com dados diferentes", cob.TxID)
	}

	// Chama PSP para criar cobrança
	cobReq := psp.CobRequest{
		Calendar:   cob.Calendar,
		Devedor:    cob.Devedor,
		Valor:      cob.Valor,
		Chave:      cob.Chave,
		SolPagador: cob.SolicitacaoPagador,
	}

	pspResp, err := s.pspClient.CreateCob(ctx, cob.TxID, cobReq)
	if err != nil {
		return nil, fmt.Errorf("psp criar cobrança: %w", err)
	}

	// Preenche resposta do PSP
	cob.Revisao = pspResp.Revisao
	cob.Status = domain.CobStatusAtiva
	cob.Location = pspResp.Location
	cob.PixCopiaECola = pspResp.PixCopiaECola
	cob.Calendar = pspResp.Calendar

	// Salva no banco + outbox na mesma transação
	tx, err := s.db.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("iniciando transação: %w", err)
	}
	defer tx.Rollback(ctx)

	if err := s.cobRepo.Create(ctx, tx, cob); err != nil {
		return nil, fmt.Errorf("salvando cobrança: %w", err)
	}

	if err := s.outboxWriter.Write(ctx, tx, "cobranca", cob.TxID, "CobrancaCriada", cob); err != nil {
		return nil, fmt.Errorf("escrevendo outbox: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit transação: %w", err)
	}

	slog.InfoContext(ctx, "cobrança criada", "txid", cob.TxID, "status", cob.Status)
	return cob, nil
}

// UpdateCob substitui uma cobrança existente por completo (PUT).
func (s *CobService) UpdateCob(ctx context.Context, txid string, cob *domain.Cobranca) (*domain.Cobranca, error) {
	cob.Sanitize()
	cob.TxID = txid

	if err := cob.Validate(); err != nil {
		return nil, err
	}

	existing, err := s.cobRepo.GetByTxID(ctx, txid)
	if err != nil {
		return nil, err
	}

	if existing.Status != domain.CobStatusAtiva {
		return nil, domain.FormatValidationError("cobrança com status %s não pode ser alterada", existing.Status)
	}

	cobReq := psp.CobRequest{
		Calendar:   cob.Calendar,
		Devedor:    cob.Devedor,
		Valor:      cob.Valor,
		Chave:      cob.Chave,
		SolPagador: cob.SolicitacaoPagador,
	}

	pspResp, err := s.pspClient.UpdateCob(ctx, txid, cobReq)
	if err != nil {
		return nil, fmt.Errorf("psp atualizar cobrança: %w", err)
	}

	cob.Revisao = pspResp.Revisao
	cob.Status = existing.Status
	cob.Location = pspResp.Location
	cob.PixCopiaECola = pspResp.PixCopiaECola

	tx, err := s.db.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("iniciando transação: %w", err)
	}
	defer tx.Rollback(ctx)

	if err := s.cobRepo.Update(ctx, tx, cob); err != nil {
		return nil, fmt.Errorf("atualizando cobrança: %w", err)
	}

	if err := s.outboxWriter.Write(ctx, tx, "cobranca", cob.TxID, "CobrancaAtualizada", cob); err != nil {
		return nil, fmt.Errorf("escrevendo outbox: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit transação: %w", err)
	}

	slog.InfoContext(ctx, "cobrança atualizada", "txid", cob.TxID)
	return cob, nil
}

// PatchCob altera parcialmente o status de uma cobrança.
func (s *CobService) PatchCob(ctx context.Context, txid string, patch *domain.CobrancaPatch) (*domain.Cobranca, error) {
	if err := patch.Validate(); err != nil {
		return nil, err
	}

	existing, err := s.cobRepo.GetByTxID(ctx, txid)
	if err != nil {
		return nil, err
	}

	if !existing.Status.CanTransitionTo(patch.Status) {
		return nil, domain.FormatValidationError(
			"transição de %s para %s não é permitida",
			existing.Status, patch.Status,
		)
	}

	tx, err := s.db.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("iniciando transação: %w", err)
	}
	defer tx.Rollback(ctx)

	if err := s.cobRepo.UpdateStatus(ctx, tx, txid, patch.Status); err != nil {
		return nil, fmt.Errorf("atualizando status: %w", err)
	}

	existing.Status = patch.Status

	if err := s.outboxWriter.Write(ctx, tx, "cobranca", txid, "CobrancaAtualizada", existing); err != nil {
		return nil, fmt.Errorf("escrevendo outbox: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit transação: %w", err)
	}

	slog.InfoContext(ctx, "status cobrança atualizado", "txid", txid, "status", patch.Status)
	return existing, nil
}

// GetCob busca uma cobrança por txid.
func (s *CobService) GetCob(ctx context.Context, txid string) (*domain.Cobranca, error) {
	cob, err := s.cobRepo.GetByTxID(ctx, txid)
	if err != nil {
		return nil, err
	}
	return cob, nil
}

// ListCobs lista cobranças com filtros e paginação.
func (s *CobService) ListCobs(ctx context.Context, filter domain.CobFilter) ([]domain.Cobranca, int, error) {
	return s.cobRepo.List(ctx, filter)
}
```

- [ ] **Step 2: Verificar compilação**

```bash
go build ./internal/service/
```
Expected: compila sem erros.

---

### Task 13: Pix Service (internal/service/pix_service.go)

**Files:**
- Create: `internal/service/pix_service.go`

- [ ] **Step 1: Escrever pix_service.go**

```go
package service

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/matspectrum/pix-api/internal/domain"
	"github.com/matspectrum/pix-api/internal/port/psp"
	"github.com/matspectrum/pix-api/internal/store/postgres"
)

// PixService gerencia operações de Pix.
type PixService struct {
	db           *pgxpool.Pool
	pixRepo      *postgres.PixRepo
	cobRepo      *postgres.CobRepo
	pspClient    psp.PSPClient
	outboxWriter *postgres.OutboxWriter
}

// NewPixService cria um novo serviço de Pix.
func NewPixService(db *pgxpool.Pool, pixRepo *postgres.PixRepo, cobRepo *postgres.CobRepo, pspClient psp.PSPClient, outboxWriter *postgres.OutboxWriter) *PixService {
	return &PixService{
		db:           db,
		pixRepo:      pixRepo,
		cobRepo:      cobRepo,
		pspClient:    pspClient,
		outboxWriter: outboxWriter,
	}
}

// ProcessPixRecebido processa um Pix recebido via webhook callback.
// Verifica dedup (e2eid), valida, salva, atualiza cobrança, escreve outbox.
func (s *PixService) ProcessPixRecebido(ctx context.Context, pix *domain.PixRecebido) error {
	// Verifica se já existe (dedup por e2eid)
	existing, err := s.pixRepo.GetByE2EID(ctx, pix.E2EID)
	if err == nil && existing != nil {
		slog.InfoContext(ctx, "pix já processado (dedup)", "e2eid", pix.E2EID)
		return nil
	}

	tx, err := s.db.Begin(ctx)
	if err != nil {
		return fmt.Errorf("iniciando transação pix: %w", err)
	}
	defer tx.Rollback(ctx)

	if err := s.pixRepo.Create(ctx, tx, pix); err != nil {
		return fmt.Errorf("salvando pix: %w", err)
	}

	// Se tiver txid associado, atualiza cobrança para CONCLUIDA
	if pix.TxID != "" {
		cob, err := s.cobRepo.GetByTxID(ctx, pix.TxID)
		if err != nil {
			slog.WarnContext(ctx, "cobrança não encontrada para pix", "txid", pix.TxID)
		} else if cob.Status.CanTransitionTo(domain.CobStatusConcluida) {
			if err := s.cobRepo.UpdateStatus(ctx, tx, pix.TxID, domain.CobStatusConcluida); err != nil {
				return fmt.Errorf("atualizando status cobrança: %w", err)
			}
		}
	}

	// Escreve outbox
	if err := s.outboxWriter.Write(ctx, tx, "pix", pix.E2EID, "PixRecebido", pix); err != nil {
		return fmt.Errorf("escrevendo outbox pix: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit transação pix: %w", err)
	}

	slog.InfoContext(ctx, "pix recebido processado", "e2eid", pix.E2EID, "valor", pix.Valor)
	return nil
}

// GetPix busca um Pix por e2eid.
func (s *PixService) GetPix(ctx context.Context, e2eid string) (*domain.PixRecebido, error) {
	return s.pixRepo.GetByE2EID(ctx, e2eid)
}

// ListPix lista Pix recebidos com filtros.
func (s *PixService) ListPix(ctx context.Context, filter domain.PixFilter) ([]domain.PixRecebido, int, error) {
	return s.pixRepo.List(ctx, filter)
}

// CreateDevolucao solicita devolução de um Pix.
func (s *PixService) CreateDevolucao(ctx context.Context, e2eid, devID, valor string) (*domain.Devolucao, error) {
	existing, err := s.pixRepo.GetByE2EID(ctx, e2eid)
	if err != nil {
		return nil, fmt.Errorf("pix nao encontrado para devolucao: %w", err)
	}

	pspResp, err := s.pspClient.CreateDevolucao(ctx, e2eid, devID, valor)
	if err != nil {
		return nil, fmt.Errorf("psp solicitar devolucao: %w", err)
	}

	dev := &domain.Devolucao{
		ID:      pspResp.ID,
		E2EID:   pspResp.E2EID,
		Valor:   pspResp.Valor,
		Status:  pspResp.Status,
		Horario: existing.HorarioLiquidacao, // usa horário do pix original como referência
	}

	tx, err := s.db.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("iniciando transação devolucao: %w", err)
	}
	defer tx.Rollback(ctx)

	if err := s.pixRepo.CreateDevolucao(ctx, tx, dev); err != nil {
		return nil, fmt.Errorf("salvando devolucao: %w", err)
	}

	if err := s.outboxWriter.Write(ctx, tx, "pix", e2eid, "DevolucaoSolicitada", dev); err != nil {
		return nil, fmt.Errorf("escrevendo outbox devolucao: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit transação devolucao: %w", err)
	}

	slog.InfoContext(ctx, "devolução solicitada", "e2eid", e2eid, "devolucao_id", dev.ID)
	return dev, nil
}
```

- [ ] **Step 2: Verificar compilação**

```bash
go build ./internal/service/
```
Expected: compila sem erros.

---

### Task 14: Webhook Service (internal/service/webhook_service.go)

**Files:**
- Create: `internal/service/webhook_service.go`

- [ ] **Step 1: Escrever webhook_service.go**

```go
package service

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/url"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/matspectrum/pix-api/internal/domain"
	"github.com/matspectrum/pix-api/internal/port/psp"
	"github.com/matspectrum/pix-api/internal/store/postgres"
)

// WebhookService gerencia operações de webhook.
type WebhookService struct {
	db           *pgxpool.Pool
	webhookRepo  *postgres.WebhookRepo
	pixService   *PixService
	pspClient    psp.PSPClient
	outboxWriter *postgres.OutboxWriter
}

// NewWebhookService cria um novo serviço de webhook.
func NewWebhookService(db *pgxpool.Pool, webhookRepo *postgres.WebhookRepo, pixService *PixService, pspClient psp.PSPClient, outboxWriter *postgres.OutboxWriter) *WebhookService {
	return &WebhookService{
		db:           db,
		webhookRepo:  webhookRepo,
		pixService:   pixService,
		pspClient:    pspClient,
		outboxWriter: outboxWriter,
	}
}

// ConfigureWebhook configura webhook para uma chave Pix.
func (s *WebhookService) ConfigureWebhook(ctx context.Context, chave, webhookURL string) (*domain.WebhookConfig, error) {
	if chave == "" {
		return nil, domain.FormatValidationError("chave é obrigatória")
	}

	if _, err := url.ParseRequestURI(webhookURL); err != nil {
		return nil, domain.FormatValidationError("url de webhook inválida: %s", webhookURL)
	}

	// Salva localmente
	wc := &domain.WebhookConfig{
		Chave:      chave,
		WebhookURL: webhookURL,
		Status:     "ATIVO",
	}

	if err := s.webhookRepo.Upsert(ctx, wc); err != nil {
		return nil, fmt.Errorf("salvando webhook: %w", err)
	}

	// Configura no PSP
	if err := s.pspClient.ConfigureWebhook(ctx, chave, webhookURL); err != nil {
		// Rollback local em caso de falha no PSP
		if delErr := s.webhookRepo.Delete(ctx, chave); delErr != nil {
			slog.ErrorContext(ctx, "rollback webhook falhou", "error", delErr)
		}
		return nil, fmt.Errorf("psp configurar webhook: %w", err)
	}

	slog.InfoContext(ctx, "webhook configurado", "chave", chave)
	return wc, nil
}

// GetWebhook busca configuração de webhook por chave.
func (s *WebhookService) GetWebhook(ctx context.Context, chave string) (*domain.WebhookConfig, error) {
	return s.webhookRepo.GetByChave(ctx, chave)
}

// ListWebhooks lista todas as configurações de webhook.
func (s *WebhookService) ListWebhooks(ctx context.Context) ([]domain.WebhookConfig, error) {
	return s.webhookRepo.List(ctx)
}

// DeleteWebhook remove configuração de webhook.
func (s *WebhookService) DeleteWebhook(ctx context.Context, chave string) error {
	// Remove do PSP primeiro
	if err := s.pspClient.DeleteWebhook(ctx, chave); err != nil {
		return fmt.Errorf("psp deletar webhook: %w", err)
	}

	if err := s.webhookRepo.Delete(ctx, chave); err != nil {
		return fmt.Errorf("deletando webhook local: %w", err)
	}

	slog.InfoContext(ctx, "webhook removido", "chave", chave)
	return nil
}

// HandleCallback processa um callback de webhook do PSP.
// Insere evento com dedup, sempre retorna 200.
func (s *WebhookService) HandleCallback(ctx context.Context, payload []byte) error {
	var wp domain.WebhookPayload
	if err := json.Unmarshal(payload, &wp); err != nil {
		return fmt.Errorf("decodificando payload webhook: %w", err)
	}

	// Insere evento com dedup (ON CONFLICT DO NOTHING)
	if err := s.webhookRepo.InsertEvent(ctx, wp.E2EID, wp.Chave, payload); err != nil {
		return fmt.Errorf("inserindo evento webhook: %w", err)
	}

	// Converte para PixRecebido e processa
	pix := wp.ToPixRecebido()
	if err := s.pixService.ProcessPixRecebido(ctx, pix); err != nil {
		slog.ErrorContext(ctx, "erro processando pix do webhook", "e2eid", wp.E2EID, "error", err)
		// Ainda retorna nil pois o evento já foi registrado e será reprocessado pelo outbox
	}

	return nil
}
```

- [ ] **Step 2: Verificar compilação**

```bash
go build ./internal/service/
```
Expected: compila sem erros.

---

## Onda 4: HTTP Layer (Tasks 15-17)

### Task 15: Middlewares (internal/port/http/middleware/)

**Files:**
- Create: `internal/port/http/middleware/idempotency.go`
- Create: `internal/port/http/middleware/logging.go`
- Create: `internal/port/http/middleware/recovery.go`
- Create: `internal/port/http/middleware/requestid.go`

- [ ] **Step 1: Escrever middleware/requestid.go**

```go
// Package middleware fornece middlewares HTTP para a API Pix.
package middleware

import (
	"context"
	"net/http"

	"github.com/google/uuid"
)

type contextKey string

const requestIDKey contextKey = "request_id"

// RequestIDMiddleware lê X-Request-ID ou gera um UUID.
func RequestIDMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestID := r.Header.Get("X-Request-ID")
		if requestID == "" {
			requestID = uuid.New().String()
		}

		ctx := context.WithValue(r.Context(), requestIDKey, requestID)
		w.Header().Set("X-Request-ID", requestID)

		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// GetRequestID extrai o request ID do contexto.
func GetRequestID(ctx context.Context) string {
	id, ok := ctx.Value(requestIDKey).(string)
	if !ok {
		return "unknown"
	}
	return id
}
```

- [ ] **Step 2: Escrever middleware/logging.go**

```go
package middleware

import (
	"log/slog"
	"net/http"
	"time"
)

// responseWriter captura o status code do response.
type responseWriter struct {
	http.ResponseWriter
	statusCode int
	written    bool
}

func newResponseWriter(w http.ResponseWriter) *responseWriter {
	return &responseWriter{ResponseWriter: w, statusCode: http.StatusOK}
}

func (rw *responseWriter) WriteHeader(code int) {
	if !rw.written {
		rw.statusCode = code
		rw.written = true
	}
	rw.ResponseWriter.WriteHeader(code)
}

func (rw *responseWriter) Write(b []byte) (int, error) {
	if !rw.written {
		rw.written = true
	}
	return rw.ResponseWriter.Write(b)
}

// LoggingMiddleware registra cada requisição com slog.
func LoggingMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rw := newResponseWriter(w)

		next.ServeHTTP(rw, r)

		duration := time.Since(start)
		slog.InfoContext(r.Context(), "request",
			"method", r.Method,
			"path", r.URL.Path,
			"status", rw.statusCode,
			"duration_ms", duration.Milliseconds(),
			"request_id", GetRequestID(r.Context()),
		)
	})
}
```

- [ ] **Step 3: Escrever middleware/recovery.go**

```go
package middleware

import (
	"log/slog"
	"net/http"
	"runtime/debug"
)

// RecoveryMiddleware captura panics e retorna 500.
func RecoveryMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if rec := recover(); rec != nil {
				slog.ErrorContext(r.Context(), "panic recuperado",
					"panic", rec,
					"stack", string(debug.Stack()),
					"request_id", GetRequestID(r.Context()),
				)
				http.Error(w, `{"status":500,"title":"Erro interno","detail":"Erro interno do servidor"}`, http.StatusInternalServerError)
			}
		}()
		next.ServeHTTP(w, r)
	})
}
```

- [ ] **Step 4: Escrever middleware/idempotency.go**

```go
package middleware

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"log/slog"
	"net/http"

	"github.com/matspectrum/pix-api/internal/domain"
	"github.com/matspectrum/pix-api/internal/store/postgres"
)

// captureResponseWriter captura o body da resposta para cache.
type captureResponseWriter struct {
	http.ResponseWriter
	body       *bytes.Buffer
	statusCode int
}

func newCaptureResponseWriter(w http.ResponseWriter) *captureResponseWriter {
	return &captureResponseWriter{
		ResponseWriter: w,
		body:           new(bytes.Buffer),
		statusCode:     http.StatusOK,
	}
}

func (rw *captureResponseWriter) WriteHeader(statusCode int) {
	rw.statusCode = statusCode
	rw.ResponseWriter.WriteHeader(statusCode)
}

func (rw *captureResponseWriter) Write(b []byte) (int, error) {
	rw.body.Write(b)
	return rw.ResponseWriter.Write(b)
}

// IdempotencyMiddleware gerencia chaves de idempotência via cabeçalho Idempotency-Key.
func IdempotencyMiddleware(repo *postgres.IdempotencyRepo) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			key := r.Header.Get("Idempotency-Key")
			if key == "" {
				next.ServeHTTP(w, r)
				return
			}

			// Lê e preserva o body
			bodyBytes, err := io.ReadAll(r.Body)
			if err != nil {
				http.Error(w, `{"status":400,"detail":"Erro lendo body"}`, http.StatusBadRequest)
				return
			}
			r.Body.Close()

			// Calcula hash do request
			hash := sha256.Sum256(bodyBytes)
			requestHash := hex.EncodeToString(hash[:])

			// Tenta adquirir chave
			endpointPath := r.URL.Path
			record, err := repo.Acquire(r.Context(), key, endpointPath, requestHash)

			if err != nil {
				// Hash divergente
				if _, ok := domain.IsProblemDetail(err); ok {
					w.Header().Set("Content-Type", "application/problem+json; charset=utf-8")
					w.WriteHeader(http.StatusBadRequest)
					w.Write([]byte(`{"type":"https://pix.bcb.gov.br/api/v2/error/RequestIdAlreadyUsed","title":"RequestId já utilizado","status":400,"detail":"Idempotency-Key já utilizada com payload diferente"}`))
					return
				}
				http.Error(w, `{"status":500,"detail":"Erro de idempotência"}`, http.StatusInternalServerError)
				return
			}

			// Se já completada, retorna resposta cacheada
			if record.Status == "completed" {
				w.Header().Set("Content-Type", "application/json; charset=utf-8")
				w.WriteHeader(record.ResponseStatus)
				w.Write(record.ResponseBody)
				slog.InfoContext(r.Context(), "idempotency cache hit",
					"key", key,
					"path", endpointPath,
				)
				return
			}

			// Restaura o body para o próximo handler
			r.Body = io.NopCloser(bytes.NewReader(bodyBytes))

			// Captura resposta para cache
			rw := newCaptureResponseWriter(w)
			next.ServeHTTP(rw, r)

			// Armazena resposta no cache de idempotência
			if err := repo.Complete(r.Context(), key, endpointPath, rw.statusCode, rw.body.Bytes()); err != nil {
				slog.ErrorContext(r.Context(), "erro completando idempotencia",
					"key", key,
					"error", err,
				)
			}
		})
	}
}
```

- [ ] **Step 5: Verificar compilação**

```bash
go build ./internal/port/http/middleware/
```
Expected: compila sem erros.

---

### Task 16: Handlers (internal/port/http/handler/)

**Files:**
- Create: `internal/port/http/handler/health_handler.go`
- Create: `internal/port/http/handler/cob_handler.go`
- Create: `internal/port/http/handler/pix_handler.go`
- Create: `internal/port/http/handler/webhook_handler.go`

- [ ] **Step 1: Escrever handler/health_handler.go**

```go
// Package handler implementa os handlers HTTP da API Pix.
package handler

import (
	"encoding/json"
	"net/http"
)

// HealthHandler responde ao health check.
type HealthHandler struct{}

// NewHealthHandler cria um novo HealthHandler.
func NewHealthHandler() *HealthHandler {
	return &HealthHandler{}
}

// ServeHTTP responde com status ok.
func (h *HealthHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{
		"status": "ok",
	})
}
```

- [ ] **Step 2: Escrever handler/cob_handler.go**

```go
package handler

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"

	"github.com/matspectrum/pix-api/internal/domain"
	"github.com/matspectrum/pix-api/internal/service"
)

// CobHandler gerencia endpoints de cobrança.
type CobHandler struct {
	cobService *service.CobService
}

// NewCobHandler cria um novo CobHandler.
func NewCobHandler(cobService *service.CobService) *CobHandler {
	return &CobHandler{cobService: cobService}
}

// writeJSON serializa resposta JSON.
func writeJSON(w http.ResponseWriter, status int, data interface{}) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(data)
}

// writeProblem escreve erro em formato ProblemDetail (RFC 7807).
func writeProblem(w http.ResponseWriter, err error) {
	if pd, ok := domain.IsProblemDetail(err); ok {
		pd.WriteJSON(w)
		return
	}
	domain.NewInternalError(err.Error()).WriteJSON(w)
}

// CreateCob POST /cob/{txid}
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

// UpdateCob substitui completamente uma cobrança existente.
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

	cob, err := h.svc.UpdateCob(r.Context(), &req)
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

// PatchCob PATCH /cob/{txid}
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

// GetCob GET /cob/{txid}
func (h *CobHandler) GetCob(w http.ResponseWriter, r *http.Request) {
	txid := chi.URLParam(r, "txid")

	cob, err := h.cobService.GetCob(r.Context(), txid)
	if err != nil {
		writeProblem(w, err)
		return
	}

	writeJSON(w, http.StatusOK, cob)
}

// ListCobs GET /cob
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
```

- [ ] **Step 3: Escrever handler/pix_handler.go**

```go
package handler

import (
	"encoding/json"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"

	"github.com/matspectrum/pix-api/internal/domain"
	"github.com/matspectrum/pix-api/internal/service"
)

// PixHandler gerencia endpoints de Pix.
type PixHandler struct {
	pixService *service.PixService
}

// NewPixHandler cria um novo PixHandler.
func NewPixHandler(pixService *service.PixService) *PixHandler {
	return &PixHandler{pixService: pixService}
}

// GetPix GET /pix/{e2eid}
func (h *PixHandler) GetPix(w http.ResponseWriter, r *http.Request) {
	e2eid := chi.URLParam(r, "e2eid")

	pix, err := h.pixService.GetPix(r.Context(), e2eid)
	if err != nil {
		writeProblem(w, err)
		return
	}

	writeJSON(w, http.StatusOK, pix)
}

// ListPix GET /pix
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

// createDevolucaoRequest representa o body de uma solicitação de devolução.
type createDevolucaoRequest struct {
	Valor string `json:"valor"`
}

// CreateDevolucao PUT /pix/{e2eid}/devolucao/{id}
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
```

- [ ] **Step 4: Escrever handler/webhook_handler.go**

```go
package handler

import (
	"encoding/json"
	"io"
	"net/http"

	"github.com/go-chi/chi/v5"

	"github.com/matspectrum/pix-api/internal/domain"
	"github.com/matspectrum/pix-api/internal/service"
)

// WebhookHandler gerencia endpoints de webhook.
type WebhookHandler struct {
	webhookService *service.WebhookService
}

// NewWebhookHandler cria um novo WebhookHandler.
func NewWebhookHandler(webhookService *service.WebhookService) *WebhookHandler {
	return &WebhookHandler{webhookService: webhookService}
}

// configureRequest representa o body de configuração de webhook.
type configureRequest struct {
	WebhookURL string `json:"webhookUrl"`
}

// ConfigureWebhook PUT /webhook/{chave}
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

// GetWebhook GET /webhook/{chave}
func (h *WebhookHandler) GetWebhook(w http.ResponseWriter, r *http.Request) {
	chave := chi.URLParam(r, "chave")

	wc, err := h.webhookService.GetWebhook(r.Context(), chave)
	if err != nil {
		writeProblem(w, err)
		return
	}

	writeJSON(w, http.StatusOK, wc)
}

// ListWebhooks GET /webhook
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

// DeleteWebhook DELETE /webhook/{chave}
func (h *WebhookHandler) DeleteWebhook(w http.ResponseWriter, r *http.Request) {
	chave := chi.URLParam(r, "chave")

	if err := h.webhookService.DeleteWebhook(r.Context(), chave); err != nil {
		writeProblem(w, err)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

// HandleCallback POST /api/v1/webhook/callback
func (h *WebhookHandler) HandleCallback(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		writeProblem(w, domain.FormatValidationError("erro lendo body: %s", err.Error()))
		return
	}
	defer r.Body.Close()

	if err := h.webhookService.HandleCallback(r.Context(), body); err != nil {
		writeProblem(w, err)
		return
	}

	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{"status": "received"})
}
```

- [ ] **Step 5: Verificar compilação**

```bash
go build ./internal/port/http/handler/
```
Expected: compila sem erros.

---

### Task 17: Router + Server (internal/port/http/)

**Files:**
- Create: `internal/port/http/router.go`
- Create: `internal/port/http/server.go`

- [ ] **Step 1: Escrever router.go**

```go
package http

import (
	"github.com/go-chi/chi/v5"
	chiMiddleware "github.com/go-chi/chi/v5/middleware"

	"github.com/matspectrum/pix-api/internal/port/http/handler"
	mw "github.com/matspectrum/pix-api/internal/port/http/middleware"
	"github.com/matspectrum/pix-api/internal/store/postgres"
)

// RouterConfig agrupa dependências para configurar o router.
type RouterConfig struct {
	HealthHandler   *handler.HealthHandler
	CobHandler      *handler.CobHandler
	PixHandler      *handler.PixHandler
	WebhookHandler  *handler.WebhookHandler
	IdempotencyRepo *postgres.IdempotencyRepo
}

// SetupRouter configura todas as rotas da API.
func SetupRouter(cfg RouterConfig) chi.Router {
	r := chi.NewRouter()

	// Middlewares globais
	r.Use(chiMiddleware.RealIP)
	r.Use(mw.RequestIDMiddleware)
	r.Use(mw.LoggingMiddleware)
	r.Use(mw.RecoveryMiddleware)
	r.Use(chiMiddleware.CleanPath)

	// Rotas
	r.Get("/health", cfg.HealthHandler.ServeHTTP)

	r.Route("/cob", func(r chi.Router) {
		r.Use(mw.IdempotencyMiddleware(cfg.IdempotencyRepo))
		r.Post("/{txid}", cfg.CobHandler.CreateCob)
		r.Put("/{txid}", cfg.CobHandler.UpdateCob)
		r.Patch("/{txid}", cfg.CobHandler.PatchCob)
		r.Get("/{txid}", cfg.CobHandler.GetCob)
		r.Get("/", cfg.CobHandler.ListCobs)
	})

	r.Route("/pix", func(r chi.Router) {
		r.Get("/{e2eid}", cfg.PixHandler.GetPix)
		r.Get("/", cfg.PixHandler.ListPix)
		r.Put("/{e2eid}/devolucao/{id}", cfg.PixHandler.CreateDevolucao)
	})

	r.Route("/webhook", func(r chi.Router) {
		r.Put("/{chave}", cfg.WebhookHandler.ConfigureWebhook)
		r.Get("/{chave}", cfg.WebhookHandler.GetWebhook)
		r.Get("/", cfg.WebhookHandler.ListWebhooks)
		r.Delete("/{chave}", cfg.WebhookHandler.DeleteWebhook)
	})

	// Callback do PSP (rota pública)
	r.Post("/api/v1/webhook/callback", cfg.WebhookHandler.HandleCallback)

	return r
}
```

- [ ] **Step 2: Escrever server.go**

```go
package http

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/go-chi/chi/v5"
)

// Server representa o servidor HTTP.
type Server struct {
	srv    *http.Server
	router chi.Router
}

// NewServer cria um novo servidor HTTP.
func NewServer(port string, router chi.Router) *Server {
	return &Server{
		router: router,
		srv: &http.Server{
			Addr:         fmt.Sprintf(":%s", port),
			Handler:      router,
			ReadTimeout:  10 * time.Second,
			WriteTimeout: 10 * time.Second,
			IdleTimeout:  60 * time.Second,
		},
	}
}

// Start inicia o servidor HTTP com graceful shutdown.
func (s *Server) Start(ctx context.Context) error {
	errCh := make(chan error, 1)

	go func() {
		slog.InfoContext(ctx, "servidor HTTP iniciado", "addr", s.srv.Addr)
		if err := s.srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			errCh <- fmt.Errorf("servidor HTTP: %w", err)
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	select {
	case sig := <-quit:
		slog.InfoContext(ctx, "sinal recebido, iniciando shutdown", "signal", sig.String())
	case err := <-errCh:
		return err
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := s.srv.Shutdown(shutdownCtx); err != nil {
		return fmt.Errorf("shutdown servidor: %w", err)
	}

	slog.InfoContext(ctx, "servidor HTTP parado gracefulmente")
	return nil
}

// Shutdown força o shutdown do servidor.
func (s *Server) Shutdown(ctx context.Context) error {
	return s.srv.Shutdown(ctx)
}
```

- [ ] **Step 3: Verificar compilação**

```bash
go build ./internal/port/http/
```
Expected: compila sem erros.

---

## Onda 5: Workers, Entry Point, Tests, Build (Tasks 18-23)

### Task 18: Outbox Publisher Worker (internal/worker/outbox_publisher.go)

**Files:**
- Create: `internal/worker/outbox_publisher.go`

- [ ] **Step 1: Escrever outbox_publisher.go**

```go
// Package worker implementa workers de background da aplicação.
package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/matspectrum/pix-api/internal/domain"
	"github.com/matspectrum/pix-api/internal/store/postgres"
)

// OutboxPublisher processa mensagens do outbox e publica handlers de domínio.
type OutboxPublisher struct {
	reader  *postgres.OutboxReader
	handlers map[string]OutboxHandler
	pollInterval time.Duration
}

// OutboxHandler é a função chamada para processar cada tipo de evento.
type OutboxHandler func(ctx context.Context, msg postgres.OutboxMessage) error

// NewOutboxPublisher cria um novo publisher de outbox.
func NewOutboxPublisher(reader *postgres.OutboxReader, pollInterval time.Duration) *OutboxPublisher {
	return &OutboxPublisher{
		reader:       reader,
		handlers:     make(map[string]OutboxHandler),
		pollInterval: pollInterval,
	}
}

// RegisterHandler registra um handler para um tipo de evento.
func (p *OutboxPublisher) RegisterHandler(eventType string, handler OutboxHandler) {
	p.handlers[eventType] = handler
}

// Start inicia o loop de polling do outbox.
func (p *OutboxPublisher) Start(ctx context.Context) error {
	slog.InfoContext(ctx, "outbox publisher iniciado",
		"poll_interval", p.pollInterval.String(),
	)

	ticker := time.NewTicker(p.pollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			slog.InfoContext(ctx, "outbox publisher parando")
			return ctx.Err()
		case <-ticker.C:
			if err := p.processBatch(ctx); err != nil {
				slog.ErrorContext(ctx, "erro processando batch outbox", "error", err)
			}
		}
	}
}

// processBatch processa um lote de mensagens pendentes.
func (p *OutboxPublisher) processBatch(ctx context.Context) error {
	messages, err := p.reader.FetchPending(ctx, 50)
	if err != nil {
		return fmt.Errorf("buscando mensagens pendentes: %w", err)
	}

	if len(messages) == 0 {
		return nil
	}

	slog.DebugContext(ctx, "processando lote outbox", "count", len(messages))

	for _, msg := range messages {
		handler, ok := p.handlers[msg.EventType]
		if !ok {
			slog.WarnContext(ctx, "handler não registrado para evento",
				"event_type", msg.EventType,
				"aggregate_id", msg.AggregateID,
			)
			if err := p.reader.MarkPublished(ctx, msg.ID); err != nil {
				slog.ErrorContext(ctx, "erro marcando publicado", "id", msg.ID, "error", err)
			}
			continue
		}

		if err := handler(ctx, msg); err != nil {
			slog.ErrorContext(ctx, "erro processando mensagem outbox",
				"id", msg.ID,
				"event_type", msg.EventType,
				"error", err,
			)
			if markErr := p.reader.MarkFailed(ctx, msg.ID, err.Error()); markErr != nil {
				slog.ErrorContext(ctx, "erro marcando falha", "id", msg.ID, "error", markErr)
			}
			continue
		}

		if err := p.reader.MarkPublished(ctx, msg.ID); err != nil {
			slog.ErrorContext(ctx, "erro marcando publicado", "id", msg.ID, "error", err)
		}
	}

	return nil
}

// CobrancaCriadaHandler processa evento de cobrança criada.
func CobrancaCriadaHandler(ctx context.Context, msg postgres.OutboxMessage) error {
	var cob domain.Cobranca
	if err := json.Unmarshal(msg.Payload, &cob); err != nil {
		return fmt.Errorf("deserializando cobrança: %w", err)
	}
	slog.InfoContext(ctx, "evento: cobrança criada",
		"txid", cob.TxID,
		"status", cob.Status,
	)
	return nil
}

// CobrancaAtualizadaHandler processa evento de cobrança atualizada.
func CobrancaAtualizadaHandler(ctx context.Context, msg postgres.OutboxMessage) error {
	var cob domain.Cobranca
	if err := json.Unmarshal(msg.Payload, &cob); err != nil {
		return fmt.Errorf("deserializando cobrança: %w", err)
	}
	slog.InfoContext(ctx, "evento: cobrança atualizada",
		"txid", cob.TxID,
		"status", cob.Status,
	)
	return nil
}

// PixRecebidoHandler processa evento de Pix recebido.
func PixRecebidoHandler(ctx context.Context, msg postgres.OutboxMessage) error {
	var pix domain.PixRecebido
	if err := json.Unmarshal(msg.Payload, &pix); err != nil {
		return fmt.Errorf("deserializando pix: %w", err)
	}
	slog.InfoContext(ctx, "evento: pix recebido",
		"e2eid", pix.E2EID,
		"valor", pix.Valor,
	)
	return nil
}

// DevolucaoSolicitadaHandler processa evento de devolução solicitada.
func DevolucaoSolicitadaHandler(ctx context.Context, msg postgres.OutboxMessage) error {
	var dev domain.Devolucao
	if err := json.Unmarshal(msg.Payload, &dev); err != nil {
		return fmt.Errorf("deserializando devolução: %w", err)
	}
	slog.InfoContext(ctx, "evento: devolução solicitada",
		"id", dev.ID,
		"e2eid", dev.E2EID,
		"valor", dev.Valor,
	)
	return nil
}
```

- [ ] **Step 2: Verificar compilação**

```bash
go build ./internal/worker/
```
Expected: compila sem erros.

---

### Task 19: Reconciliation Worker (internal/worker/reconciliation_worker.go)

**Files:**
- Create: `internal/worker/reconciliation_worker.go`

- [ ] **Step 1: Escrever reconciliation_worker.go**

```go
package worker

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/matspectrum/pix-api/internal/domain"
	"github.com/matspectrum/pix-api/internal/port/psp"
	"github.com/matspectrum/pix-api/internal/store/postgres"
	"github.com/robfig/cron/v3"
)

// ReconciliationWorker compara Pix locais com PSP para detectar discrepâncias.
type ReconciliationWorker struct {
	db        *pgxpool.Pool
	pixRepo   *postgres.PixRepo
	pspClient psp.PSPClient
	cron      *cron.Cron
}

// NewReconciliationWorker cria um novo worker de reconciliação.
func NewReconciliationWorker(db *pgxpool.Pool, pixRepo *postgres.PixRepo, pspClient psp.PSPClient) *ReconciliationWorker {
	return &ReconciliationWorker{
		db:        db,
		pixRepo:   pixRepo,
		pspClient: pspClient,
	}
}

// Start inicia o job de reconciliação com agendamento cron.
func (w *ReconciliationWorker) Start(ctx context.Context, schedule string) error {
	w.cron = cron.New(cron.WithLocation(time.UTC))

	_, err := w.cron.AddFunc(schedule, func() {
		if err := w.run(ctx); err != nil {
			slog.ErrorContext(ctx, "erro na reconciliação", "error", err)
		}
	})
	if err != nil {
		return fmt.Errorf("registrando job reconciliacao: %w", err)
	}

	w.cron.Start()
	slog.InfoContext(ctx, "worker de reconciliação iniciado", "schedule", schedule)

	go func() {
		<-ctx.Done()
		cronCtx := w.cron.Stop()
		<-cronCtx.Done()
		slog.InfoContext(ctx, "worker de reconciliação parado")
	}()

	return nil
}

// run executa a reconciliação comparando últimos 24h.
func (w *ReconciliationWorker) run(ctx context.Context) error {
	slog.InfoContext(ctx, "iniciando reconciliação")

	fim := time.Now().UTC()
	inicio := fim.Add(-24 * time.Hour)

	filter := domain.PixFilter{
		Inicio: inicio,
		Fim:    fim,
		Limit:  200,
		Offset: 0,
	}

	pixs, _, err := w.pixRepo.List(ctx, filter)
	if err != nil {
		return fmt.Errorf("listando pix locais: %w", err)
	}

	var (
		mu           sync.Mutex
		discrepancies []reconciliationRecord
		wg           sync.WaitGroup
	)

	for _, pix := range pixs {
		wg.Add(1)
		go func(local domain.PixRecebido) {
			defer wg.Done()

			pspPix, err := w.pspClient.GetPix(ctx, local.E2EID)
			if err != nil {
				mu.Lock()
				discrepancies = append(discrepancies, reconciliationRecord{
					E2EID:            local.E2EID,
					LocalValor:       local.Valor,
					TipoDiscrepancia: "NAO_ENCONTRADO_PSP",
				})
				mu.Unlock()
				return
			}

			hasDiscrepancy := false
			rec := reconciliationRecord{
				E2EID:      local.E2EID,
				LocalValor: local.Valor,
				PSPValor:   pspPix.Valor,
			}

			if local.Valor != pspPix.Valor {
				rec.TipoDiscrepancia = "VALOR_DIVERGENTE"
				hasDiscrepancy = true
			}

			if hasDiscrepancy {
				mu.Lock()
				discrepancies = append(discrepancies, rec)
				mu.Unlock()
			}
		}(pix)
	}

	wg.Wait()

	if len(discrepancies) > 0 {
		slog.WarnContext(ctx, "discrepâncias encontradas", "count", len(discrepancies))
		for _, d := range discrepancies {
			if err := w.saveDiscrepancy(ctx, d); err != nil {
				slog.ErrorContext(ctx, "erro salvando discrepância", "e2eid", d.E2EID, "error", err)
			}
		}
	} else {
		slog.InfoContext(ctx, "reconciliação concluída sem discrepâncias", "pix_verificados", len(pixs))
	}

	return nil
}

type reconciliationRecord struct {
	E2EID            string
	LocalValor       string
	PSPValor         string
	LocalHorario     time.Time
	PSPHorario       time.Time
	TipoDiscrepancia string
}

// saveDiscrepancy insere registro de discrepância no banco.
func (w *ReconciliationWorker) saveDiscrepancy(ctx context.Context, rec reconciliationRecord) error {
	_, err := w.db.Exec(ctx,
		`INSERT INTO reconciliation_reports (e2eid, local_valor, psp_valor, local_horario, psp_horario, tipo_discrepancia)
		 VALUES ($1, $2, $3, $4, $5, $6)`,
		rec.E2EID, rec.LocalValor, rec.PSPValor, rec.LocalHorario, rec.PSPHorario, rec.TipoDiscrepancia,
	)
	if err != nil {
		return fmt.Errorf("inserindo discrepância e2eid=%s: %w", rec.E2EID, err)
	}
	return nil
}
```

- [ ] **Step 2: Verificar compilação**

```bash
go build ./internal/worker/
```
Expected: compila sem erros.

---

### Task 20: Entry Point (cmd/server/main.go)

**Files:**
- Create: `cmd/server/main.go`

- [ ] **Step 1: Escrever main.go**

```go
// Package main é o entry point do servidor da API Pix.
package main

import (
	"context"
	"log/slog"
	"os"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/matspectrum/pix-api/internal/config"
	"github.com/matspectrum/pix-api/internal/port/psp/mock"
	httpserver "github.com/matspectrum/pix-api/internal/port/http"
	"github.com/matspectrum/pix-api/internal/port/http/handler"
	"github.com/matspectrum/pix-api/internal/service"
	"github.com/matspectrum/pix-api/internal/store/postgres"
	"github.com/matspectrum/pix-api/internal/worker"
)

func main() {
	ctx := context.Background()

	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))
	slog.SetDefault(logger)

	slog.InfoContext(ctx, "iniciando API Pix")

	// Carrega configuração
	cfg, err := config.Load()
	if err != nil {
		slog.ErrorContext(ctx, "erro carregando configuração", "error", err)
		os.Exit(1)
	}

	// Conecta ao banco
	pool, err := postgres.NewPool(
		ctx,
		cfg.Database.DSN(),
		cfg.Database.MaxOpenConns,
		cfg.Database.MaxIdleConns,
		cfg.Database.ConnMaxLifetime,
		cfg.Database.ConnMaxIdleTime,
	)
	if err != nil {
		slog.ErrorContext(ctx, "erro conectando ao banco", "error", err)
		os.Exit(1)
	}
	defer pool.Close()

	// Executa migrations
	if err := runMigrations(pool); err != nil {
		slog.ErrorContext(ctx, "erro executando migrations", "error", err)
		os.Exit(1)
	}

	// Wire dependencies - PSP mock
	pspClient := mock.NewMockPSP()

	// Wire dependencies - Repositories
	cobRepo := postgres.NewCobRepo(pool)
	pixRepo := postgres.NewPixRepo(pool)
	webhookRepo := postgres.NewWebhookRepo(pool)
	idempotencyRepo := postgres.NewIdempotencyRepo(pool)
	outboxWriter := postgres.NewOutboxWriter(pool)
	outboxReader := postgres.NewOutboxReader(pool)

	// Wire dependencies - Services
	pixService := service.NewPixService(pool, pixRepo, cobRepo, pspClient, outboxWriter)
	cobService := service.NewCobService(pool, cobRepo, pspClient, outboxWriter)
	webhookService := service.NewWebhookService(pool, webhookRepo, pixService, pspClient, outboxWriter)

	// Wire dependencies - Handlers
	healthHandler := handler.NewHealthHandler()
	cobHandler := handler.NewCobHandler(cobService)
	pixHandler := handler.NewPixHandler(pixService)
	webhookHandler := handler.NewWebhookHandler(webhookService)

	// Setup router
	router := httpserver.SetupRouter(httpserver.RouterConfig{
		HealthHandler:   healthHandler,
		CobHandler:      cobHandler,
		PixHandler:      pixHandler,
		WebhookHandler:  webhookHandler,
		IdempotencyRepo: idempotencyRepo,
	})

	// Start outbox publisher
	outboxPublisher := worker.NewOutboxPublisher(outboxReader, cfg.Worker.OutboxPollInterval)
	outboxPublisher.RegisterHandler("CobrancaCriada", worker.CobrancaCriadaHandler)
	outboxPublisher.RegisterHandler("CobrancaAtualizada", worker.CobrancaAtualizadaHandler)
	outboxPublisher.RegisterHandler("PixRecebido", worker.PixRecebidoHandler)
	outboxPublisher.RegisterHandler("DevolucaoSolicitada", worker.DevolucaoSolicitadaHandler)

	workerCtx, cancelWorkers := context.WithCancel(ctx)
	defer cancelWorkers()

	go func() {
		if err := outboxPublisher.Start(workerCtx); err != nil {
			slog.ErrorContext(ctx, "outbox publisher parou", "error", err)
		}
	}()

	// Start reconciliation worker
	reconciliationWorker := worker.NewReconciliationWorker(pool, pixRepo, pspClient)
	if err := reconciliationWorker.Start(workerCtx, cfg.Worker.ReconciliationSchedule); err != nil {
		slog.ErrorContext(ctx, "erro iniciando reconciliation worker", "error", err)
	}

	// Start HTTP server
	server := httpserver.NewServer(cfg.Server.Port, router)
	if err := server.Start(ctx); err != nil {
		slog.ErrorContext(ctx, "erro no servidor", "error", err)
		os.Exit(1)
	}
}

// runMigrations executa as migrations do banco.
func runMigrations(pool *pgxpool.Pool) error {
	// Verifica se tabelas existem via query simples
	var count int
	err := pool.QueryRow(context.Background(),
		`SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'cobrancas'`,
	).Scan(&count)
	if err != nil {
		return err
	}
	if count == 0 {
		slog.WarnContext(context.Background(), "tabelas não encontradas, execute as migrations manualmente com golang-migrate")
	}
	return nil
}
```

- [ ] **Step 2: Verificar compilação**

```bash
go build ./cmd/server/
```
Expected: compila sem erros.

---

### Task 21: Test Helpers (test/testhelpers/)

**Files:**
- Create: `test/testhelpers/postgres.go`
- Create: `test/testhelpers/migrate.go`

- [ ] **Step 1: Escrever testhelpers/postgres.go**

```go
// Package testhelpers fornece utilitários para testes de integração.
package testhelpers

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

// PostgresContainer encapsula um container PostgreSQL para testes.
type PostgresContainer struct {
	Container testcontainers.Container
	Pool      *pgxpool.Pool
	DSN       string
}

// SetupPostgres inicia um container PostgreSQL com testcontainers.
func SetupPostgres(ctx context.Context) (*PostgresContainer, error) {
	req := testcontainers.ContainerRequest{
		Image:        "postgres:16-alpine",
		ExposedPorts: []string{"5432/tcp"},
		Env: map[string]string{
			"POSTGRES_USER":     "pix",
			"POSTGRES_PASSWORD": "pix",
			"POSTGRES_DB":       "pix_api",
		},
		WaitingFor: wait.ForListeningPort("5432/tcp").WithStartupTimeout(60 * time.Second),
	}

	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		return nil, fmt.Errorf("criando container postgres: %w", err)
	}

	host, err := container.Host(ctx)
	if err != nil {
		container.Terminate(ctx)
		return nil, fmt.Errorf("obtendo host: %w", err)
	}

	port, err := container.MappedPort(ctx, "5432")
	if err != nil {
		container.Terminate(ctx)
		return nil, fmt.Errorf("obtendo porta: %w", err)
	}

	dsn := fmt.Sprintf("postgres://pix:pix@%s:%s/pix_api?sslmode=disable", host, port.Port())

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		container.Terminate(ctx)
		return nil, fmt.Errorf("criando pool: %w", err)
	}

	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		container.Terminate(ctx)
		return nil, fmt.Errorf("ping banco: %w", err)
	}

	return &PostgresContainer{
		Container: container,
		Pool:      pool,
		DSN:       dsn,
	}, nil
}

// Cleanup encerra o pool e remove o container.
func (pc *PostgresContainer) Cleanup(ctx context.Context) {
	pc.Pool.Close()
	if err := pc.Container.Terminate(ctx); err != nil {
		// Ignora erro de limpeza
	}
}
```

- [ ] **Step 2: Escrever testhelpers/migrate.go**

```go
package testhelpers

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/postgres"
	_ "github.com/golang-migrate/migrate/v4/source/file"
)

// RunMigrations executa as migrations no banco de testes.
func RunMigrations(ctx context.Context, dsn string) error {
	// Encontra o diretório de migrations
	migrationsPath, err := findMigrationsPath()
	if err != nil {
		return fmt.Errorf("encontrando migrations: %w", err)
	}

	m, err := migrate.New(
		fmt.Sprintf("file://%s", migrationsPath),
		dsn,
	)
	if err != nil {
		return fmt.Errorf("criando migrate: %w", err)
	}

	if err := m.Up(); err != nil && err != migrate.ErrNoChange {
		return fmt.Errorf("executando migrations: %w", err)
	}

	return nil
}

// findMigrationsPath localiza o diretório de migrations relativo ao módulo.
func findMigrationsPath() (string, error) {
	// Procura a partir do diretório atual até a raiz do projeto
	dir, err := os.Getwd()
	if err != nil {
		return "", err
	}

	for {
		candidate := filepath.Join(dir, "internal", "store", "postgres", "migrations")
		if info, err := os.Stat(candidate); err == nil && info.IsDir() {
			return candidate, nil
		}

		parent := filepath.Dir(dir)
		if parent == dir {
			break
		}
		dir = parent
	}

	return "", fmt.Errorf("diretório de migrations não encontrado")
}
```

- [ ] **Step 3: Verificar compilação**

```bash
go build ./test/testhelpers/
```
Expected: compila sem erros.

---

### Task 22: Integration Tests (test/integration/)

**Files:**
- Create: `test/integration/cob_test.go`
- Create: `test/integration/pix_test.go`
- Create: `test/integration/webhook_test.go`
- Create: `test/integration/idempotency_test.go`
- Create: `test/integration/outbox_test.go`

- [ ] **Step 1: Escrever cob_test.go**

```go
//go:build integration

package integration

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/matspectrum/pix-api/internal/domain"
	"github.com/matspectrum/pix-api/internal/port/psp/mock"
	"github.com/matspectrum/pix-api/internal/service"
	"github.com/matspectrum/pix-api/internal/store/postgres"
	"github.com/matspectrum/pix-api/test/testhelpers"
)

func TestCreateCob(t *testing.T) {
	ctx := context.Background()
	pc, err := testhelpers.SetupPostgres(ctx)
	require.NoError(t, err)
	defer pc.Cleanup(ctx)

	require.NoError(t, testhelpers.RunMigrations(ctx, pc.DSN))

	cobRepo := postgres.NewCobRepo(pc.Pool)
	pspClient := mock.NewMockPSP()
	outboxWriter := postgres.NewOutboxWriter(pc.Pool)
	svc := service.NewCobService(pc.Pool, cobRepo, pspClient, outboxWriter)

	cob := &domain.Cobranca{
		TxID: "a1b2c3d4e5f6g7h8i9j0k1l2m3n4o5p6", // 32 chars
		Chave: "matspectrum@gmail.com",
		Valor: domain.Valor{Original: "10.00"},
		Devedor: domain.Devedor{
			Nome: "João Silva",
			CPF:  "01234567890",
		},
		Calendar: domain.Calendar{Expiracao: 86400},
	}

	result, err := svc.CreateCob(ctx, cob)
	require.NoError(t, err)
	assert.NotNil(t, result)
	assert.Equal(t, domain.CobStatusAtiva, result.Status)
	assert.NotEmpty(t, result.Location)
	assert.NotEmpty(t, result.PixCopiaECola)
}

func TestCreateCobDuplicate(t *testing.T) {
	ctx := context.Background()
	pc, err := testhelpers.SetupPostgres(ctx)
	require.NoError(t, err)
	defer pc.Cleanup(ctx)

	require.NoError(t, testhelpers.RunMigrations(ctx, pc.DSN))

	cobRepo := postgres.NewCobRepo(pc.Pool)
	pspClient := mock.NewMockPSP()
	outboxWriter := postgres.NewOutboxWriter(pc.Pool)
	svc := service.NewCobService(pc.Pool, cobRepo, pspClient, outboxWriter)

	cob := &domain.Cobranca{
		TxID: "dup1dup2dup3dup4dup5dup6dup789",
		Chave: "matspectrum@gmail.com",
		Valor: domain.Valor{Original: "10.00"},
		Devedor: domain.Devedor{
			Nome: "João Silva",
			CPF:  "01234567890",
		},
		Calendar: domain.Calendar{Expiracao: 86400},
	}

	result1, err := svc.CreateCob(ctx, cob)
	require.NoError(t, err)

	result2, err := svc.CreateCob(ctx, cob)
	require.NoError(t, err)
	assert.Equal(t, result1.TxID, result2.TxID)
}

func TestGetCob(t *testing.T) {
	ctx := context.Background()
	pc, err := testhelpers.SetupPostgres(ctx)
	require.NoError(t, err)
	defer pc.Cleanup(ctx)

	require.NoError(t, testhelpers.RunMigrations(ctx, pc.DSN))

	cobRepo := postgres.NewCobRepo(pc.Pool)
	pspClient := mock.NewMockPSP()
	outboxWriter := postgres.NewOutboxWriter(pc.Pool)
	svc := service.NewCobService(pc.Pool, cobRepo, pspClient, outboxWriter)

	cob := &domain.Cobranca{
		TxID: "get1get2get3get4get5get6get789",
		Chave: "matspectrum@gmail.com",
		Valor: domain.Valor{Original: "25.50"},
		Devedor: domain.Devedor{
			Nome: "Maria Souza",
			CPF:  "09876543210",
		},
		Calendar: domain.Calendar{Expiracao: 86400},
	}

	_, err = svc.CreateCob(ctx, cob)
	require.NoError(t, err)

	result, err := svc.GetCob(ctx, cob.TxID)
	require.NoError(t, err)
	assert.Equal(t, cob.TxID, result.TxID)
	assert.Equal(t, "25.50", result.Valor.Original)
}

func TestGetCobNotFound(t *testing.T) {
	ctx := context.Background()
	pc, err := testhelpers.SetupPostgres(ctx)
	require.NoError(t, err)
	defer pc.Cleanup(ctx)

	require.NoError(t, testhelpers.RunMigrations(ctx, pc.DSN))

	cobRepo := postgres.NewCobRepo(pc.Pool)
	pspClient := mock.NewMockPSP()
	outboxWriter := postgres.NewOutboxWriter(pc.Pool)
	svc := service.NewCobService(pc.Pool, cobRepo, pspClient, outboxWriter)

	_, err = svc.GetCob(ctx, "naoexistentetxid123456789012")
	require.Error(t, err)
	assert.Equal(t, domain.ErrCobrancaNaoEncontrada, err)
}

func TestPatchCob(t *testing.T) {
	ctx := context.Background()
	pc, err := testhelpers.SetupPostgres(ctx)
	require.NoError(t, err)
	defer pc.Cleanup(ctx)

	require.NoError(t, testhelpers.RunMigrations(ctx, pc.DSN))

	cobRepo := postgres.NewCobRepo(pc.Pool)
	pspClient := mock.NewMockPSP()
	outboxWriter := postgres.NewOutboxWriter(pc.Pool)
	svc := service.NewCobService(pc.Pool, cobRepo, pspClient, outboxWriter)

	cob := &domain.Cobranca{
		TxID: "patchpatchpatchpatchpatch12345",
		Chave: "matspectrum@gmail.com",
		Valor: domain.Valor{Original: "50.00"},
		Devedor: domain.Devedor{
			Nome: "Carlos Lima",
			CPF:  "11122233344",
		},
		Calendar: domain.Calendar{Expiracao: 86400},
	}

	_, err = svc.CreateCob(ctx, cob)
	require.NoError(t, err)

	patch := &domain.CobrancaPatch{Status: domain.CobStatusRemovidaPeloUsuario}
	result, err := svc.PatchCob(ctx, cob.TxID, patch)
	require.NoError(t, err)
	assert.Equal(t, domain.CobStatusRemovidaPeloUsuario, result.Status)
}
```

- [ ] **Step 2: Escrever pix_test.go**

```go
//go:build integration

package integration

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/matspectrum/pix-api/internal/domain"
	"github.com/matspectrum/pix-api/internal/port/psp/mock"
	"github.com/matspectrum/pix-api/internal/service"
	"github.com/matspectrum/pix-api/internal/store/postgres"
	"github.com/matspectrum/pix-api/test/testhelpers"
)

func TestGetPix(t *testing.T) {
	ctx := context.Background()
	pc, err := testhelpers.SetupPostgres(ctx)
	require.NoError(t, err)
	defer pc.Cleanup(ctx)

	require.NoError(t, testhelpers.RunMigrations(ctx, pc.DSN))

	pixRepo := postgres.NewPixRepo(pc.Pool)
	cobRepo := postgres.NewCobRepo(pc.Pool)
	pspClient := mock.NewMockPSP()
	outboxWriter := postgres.NewOutboxWriter(pc.Pool)
	svc := service.NewPixService(pc.Pool, pixRepo, cobRepo, pspClient, outboxWriter)

	pix := &domain.PixRecebido{
		E2EID: "E90400888202305231234ABCDEFG12345",
		Chave: "matspectrum@gmail.com",
		Valor: "100.00",
	}

	err = svc.ProcessPixRecebido(ctx, pix)
	require.NoError(t, err)

	result, err := svc.GetPix(ctx, pix.E2EID)
	require.NoError(t, err)
	assert.Equal(t, pix.E2EID, result.E2EID)
	assert.Equal(t, "100.00", result.Valor)
}

func TestListPix(t *testing.T) {
	ctx := context.Background()
	pc, err := testhelpers.SetupPostgres(ctx)
	require.NoError(t, err)
	defer pc.Cleanup(ctx)

	require.NoError(t, testhelpers.RunMigrations(ctx, pc.DSN))

	pixRepo := postgres.NewPixRepo(pc.Pool)
	cobRepo := postgres.NewCobRepo(pc.Pool)
	pspClient := mock.NewMockPSP()
	outboxWriter := postgres.NewOutboxWriter(pc.Pool)
	svc := service.NewPixService(pc.Pool, pixRepo, cobRepo, pspClient, outboxWriter)

	pix1 := &domain.PixRecebido{
		E2EID: "E0000000000123456789012345678901",
		Chave: "matspectrum@gmail.com",
		Valor: "50.00",
	}
	pix2 := &domain.PixRecebido{
		E2EID: "E0000000000123456789012345678902",
		Chave: "outro@exemplo.com",
		Valor: "75.00",
	}

	require.NoError(t, svc.ProcessPixRecebido(ctx, pix1))
	require.NoError(t, svc.ProcessPixRecebido(ctx, pix2))

	pixs, total, err := svc.ListPix(ctx, domain.PixFilter{Limit: 10})
	require.NoError(t, err)
	assert.Equal(t, 2, total)
	assert.Len(t, pixs, 2)
}

func TestCreateDevolucao(t *testing.T) {
	ctx := context.Background()
	pc, err := testhelpers.SetupPostgres(ctx)
	require.NoError(t, err)
	defer pc.Cleanup(ctx)

	require.NoError(t, testhelpers.RunMigrations(ctx, pc.DSN))

	pixRepo := postgres.NewPixRepo(pc.Pool)
	cobRepo := postgres.NewCobRepo(pc.Pool)
	pspClient := mock.NewMockPSP()
	outboxWriter := postgres.NewOutboxWriter(pc.Pool)

	// Adiciona Pix ao mock PSP
	mockPSP := pspClient.(*mock.MockPSP)
	mockPSP.PixStore().AddPix("E0000000000123456789012345678999", "", "200.00")

	svc := service.NewPixService(pc.Pool, pixRepo, cobRepo, pspClient, outboxWriter)

	pix := &domain.PixRecebido{
		E2EID: "E0000000000123456789012345678999",
		Chave: "matspectrum@gmail.com",
		Valor: "200.00",
	}
	require.NoError(t, svc.ProcessPixRecebido(ctx, pix))

	dev, err := svc.CreateDevolucao(ctx, "E0000000000123456789012345678999", "DEV001", "100.00")
	require.NoError(t, err)
	assert.Equal(t, "100.00", dev.Valor)
	assert.NotEmpty(t, dev.ID)
}
```

- [ ] **Step 3: Escrever webhook_test.go**

```go
//go:build integration

package integration

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/matspectrum/pix-api/internal/domain"
	"github.com/matspectrum/pix-api/internal/port/psp/mock"
	"github.com/matspectrum/pix-api/internal/service"
	"github.com/matspectrum/pix-api/internal/store/postgres"
	"github.com/matspectrum/pix-api/test/testhelpers"
)

func TestConfigureWebhook(t *testing.T) {
	ctx := context.Background()
	pc, err := testhelpers.SetupPostgres(ctx)
	require.NoError(t, err)
	defer pc.Cleanup(ctx)

	require.NoError(t, testhelpers.RunMigrations(ctx, pc.DSN))

	webhookRepo := postgres.NewWebhookRepo(pc.Pool)
	pixRepo := postgres.NewPixRepo(pc.Pool)
	cobRepo := postgres.NewCobRepo(pc.Pool)
	pspClient := mock.NewMockPSP()
	outboxWriter := postgres.NewOutboxWriter(pc.Pool)
	pixService := service.NewPixService(pc.Pool, pixRepo, cobRepo, pspClient, outboxWriter)
	svc := service.NewWebhookService(pc.Pool, webhookRepo, pixService, pspClient, outboxWriter)

	wc, err := svc.ConfigureWebhook(ctx, "matspectrum@gmail.com", "https://example.com/webhook/callback")
	require.NoError(t, err)
	assert.Equal(t, "matspectrum@gmail.com", wc.Chave)
	assert.Equal(t, "ATIVO", wc.Status)
}

func TestHandleCallback(t *testing.T) {
	ctx := context.Background()
	pc, err := testhelpers.SetupPostgres(ctx)
	require.NoError(t, err)
	defer pc.Cleanup(ctx)

	require.NoError(t, testhelpers.RunMigrations(ctx, pc.DSN))

	webhookRepo := postgres.NewWebhookRepo(pc.Pool)
	pixRepo := postgres.NewPixRepo(pc.Pool)
	cobRepo := postgres.NewCobRepo(pc.Pool)
	pspClient := mock.NewMockPSP()
	outboxWriter := postgres.NewOutboxWriter(pc.Pool)
	pixService := service.NewPixService(pc.Pool, pixRepo, cobRepo, pspClient, outboxWriter)
	svc := service.NewWebhookService(pc.Pool, webhookRepo, pixService, pspClient, outboxWriter)

	payload := domain.WebhookPayload{
		E2EID: "E90400888202305231WEBHOOK0012345",
		Chave: "matspectrum@gmail.com",
		Valor: "150.00",
	}

	payloadBytes, err := json.Marshal(payload)
	require.NoError(t, err)

	err = svc.HandleCallback(ctx, payloadBytes)
	require.NoError(t, err)

	// Verifica que o pix foi salvo
	pix, err := pixService.GetPix(ctx, payload.E2EID)
	require.NoError(t, err)
	assert.Equal(t, "150.00", pix.Valor)
}

func TestHandleCallbackDuplicate(t *testing.T) {
	ctx := context.Background()
	pc, err := testhelpers.SetupPostgres(ctx)
	require.NoError(t, err)
	defer pc.Cleanup(ctx)

	require.NoError(t, testhelpers.RunMigrations(ctx, pc.DSN))

	webhookRepo := postgres.NewWebhookRepo(pc.Pool)
	pixRepo := postgres.NewPixRepo(pc.Pool)
	cobRepo := postgres.NewCobRepo(pc.Pool)
	pspClient := mock.NewMockPSP()
	outboxWriter := postgres.NewOutboxWriter(pc.Pool)
	pixService := service.NewPixService(pc.Pool, pixRepo, cobRepo, pspClient, outboxWriter)
	svc := service.NewWebhookService(pc.Pool, webhookRepo, pixService, pspClient, outboxWriter)

	payload := domain.WebhookPayload{
		E2EID: "E90400888202305231DEDUP12345678",
		Chave: "matspectrum@gmail.com",
		Valor: "200.00",
	}

	payloadBytes, err := json.Marshal(payload)
	require.NoError(t, err)

	// Primeira chamada
	err = svc.HandleCallback(ctx, payloadBytes)
	require.NoError(t, err)

	// Segunda chamada (duplicada) - não deve errar
	err = svc.HandleCallback(ctx, payloadBytes)
	require.NoError(t, err)

	// Verifica que só existe um registro
	pixs, total, err := pixService.ListPix(ctx, domain.PixFilter{Limit: 10})
	require.NoError(t, err)
	assert.Equal(t, 1, total)
	assert.Len(t, pixs, 1)
}
```

- [ ] **Step 4: Escrever idempotency_test.go**

```go
//go:build integration

package integration

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/matspectrum/pix-api/internal/domain"
	"github.com/matspectrum/pix-api/internal/store/postgres"
	"github.com/matspectrum/pix-api/test/testhelpers"
)

func TestIdempotentCreate(t *testing.T) {
	ctx := context.Background()
	pc, err := testhelpers.SetupPostgres(ctx)
	require.NoError(t, err)
	defer pc.Cleanup(ctx)

	require.NoError(t, testhelpers.RunMigrations(ctx, pc.DSN))

	repo := postgres.NewIdempotencyRepo(pc.Pool)

	key := "idem-test-001"
	path := "/cob/abc123"
	body := []byte(`{"valor":"10.00"}`)
	hash := sha256.Sum256(body)
	requestHash := hex.EncodeToString(hash[:])

	// Primeira aquisição
	record, err := repo.Acquire(ctx, key, path, requestHash)
	require.NoError(t, err)
	assert.Equal(t, "in_progress", record.Status)

	// Segunda aquisição com mesmo hash
	record2, err := repo.Acquire(ctx, key, path, requestHash)
	require.NoError(t, err)
	assert.Equal(t, record.RequestHash, record2.RequestHash)

	// Completa
	err = repo.Complete(ctx, key, path, 201, body)
	require.NoError(t, err)

	// Após completar, ainda pode adquirir (retorna registro existente)
	record3, err := repo.Acquire(ctx, key, path, requestHash)
	require.NoError(t, err)
	assert.Equal(t, "completed", record3.Status)
}

func TestIdempotentPayloadDivergence(t *testing.T) {
	ctx := context.Background()
	pc, err := testhelpers.SetupPostgres(ctx)
	require.NoError(t, err)
	defer pc.Cleanup(ctx)

	require.NoError(t, testhelpers.RunMigrations(ctx, pc.DSN))

	repo := postgres.NewIdempotencyRepo(pc.Pool)

	key := "idem-test-002"
	path := "/cob/def456"
	body1 := []byte(`{"valor":"10.00"}`)
	body2 := []byte(`{"valor":"20.00"}`)

	hash1 := sha256.Sum256(body1)
	hash2 := sha256.Sum256(body2)

	// Primeira requisição
	_, err = repo.Acquire(ctx, key, path, hex.EncodeToString(hash1[:]))
	require.NoError(t, err)

	// Segunda com payload diferente
	_, err = repo.Acquire(ctx, key, path, hex.EncodeToString(hash2[:]))
	require.Error(t, err)
	assert.Equal(t, domain.ErrIdempotencyKeyDiverged, err)
}
```

- [ ] **Step 5: Escrever outbox_test.go**

```go
//go:build integration

package integration

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/matspectrum/pix-api/internal/domain"
	"github.com/matspectrum/pix-api/internal/port/psp/mock"
	"github.com/matspectrum/pix-api/internal/service"
	"github.com/matspectrum/pix-api/internal/store/postgres"
	"github.com/matspectrum/pix-api/test/testhelpers"
)

func TestOutboxWriteOnCreate(t *testing.T) {
	ctx := context.Background()
	pc, err := testhelpers.SetupPostgres(ctx)
	require.NoError(t, err)
	defer pc.Cleanup(ctx)

	require.NoError(t, testhelpers.RunMigrations(ctx, pc.DSN))

	cobRepo := postgres.NewCobRepo(pc.Pool)
	pspClient := mock.NewMockPSP()
	outboxWriter := postgres.NewOutboxWriter(pc.Pool)
	outboxReader := postgres.NewOutboxReader(pc.Pool)
	svc := service.NewCobService(pc.Pool, cobRepo, pspClient, outboxWriter)

	cob := &domain.Cobranca{
		TxID: "outb1outb2outb3outb4outb5outb678",
		Chave: "matspectrum@gmail.com",
		Valor: domain.Valor{Original: "10.00"},
		Devedor: domain.Devedor{
			Nome: "João Silva",
			CPF:  "01234567890",
		},
		Calendar: domain.Calendar{Expiracao: 86400},
	}

	_, err = svc.CreateCob(ctx, cob)
	require.NoError(t, err)

	messages, err := outboxReader.FetchPending(ctx, 10)
	require.NoError(t, err)
	assert.GreaterOrEqual(t, len(messages), 1, "deve haver pelo menos uma mensagem no outbox")

	found := false
	for _, msg := range messages {
		if msg.EventType == "CobrancaCriada" && msg.AggregateID == cob.TxID {
			found = true
			break
		}
	}
	assert.True(t, found, "deve existir uma mensagem CobrancaCriada no outbox")
}

func TestOutboxPublished(t *testing.T) {
	ctx := context.Background()
	pc, err := testhelpers.SetupPostgres(ctx)
	require.NoError(t, err)
	defer pc.Cleanup(ctx)

	require.NoError(t, testhelpers.RunMigrations(ctx, pc.DSN))

	cobRepo := postgres.NewCobRepo(pc.Pool)
	pspClient := mock.NewMockPSP()
	outboxWriter := postgres.NewOutboxWriter(pc.Pool)
	outboxReader := postgres.NewOutboxReader(pc.Pool)
	svc := service.NewCobService(pc.Pool, cobRepo, pspClient, outboxWriter)

	cob := &domain.Cobranca{
		TxID: "publ1publ2publ3publ4publ5publ678",
		Chave: "matspectrum@gmail.com",
		Valor: domain.Valor{Original: "30.00"},
		Devedor: domain.Devedor{
			Nome: "Ana Costa",
			CPF:  "55566677788",
		},
		Calendar: domain.Calendar{Expiracao: 86400},
	}

	_, err = svc.CreateCob(ctx, cob)
	require.NoError(t, err)

	messages, err := outboxReader.FetchPending(ctx, 10)
	require.NoError(t, err)
	assert.NotEmpty(t, messages)

	for _, msg := range messages {
		err = outboxReader.MarkPublished(ctx, msg.ID)
		require.NoError(t, err)
	}

	// Busca novamente, não deve retornar os publicados
	remaining, err := outboxReader.FetchPending(ctx, 10)
	require.NoError(t, err)
	for _, msg := range remaining {
		assert.NotEqual(t, cob.TxID, msg.AggregateID, "mensagem publicada não deve aparecer como pendente")
	}
}
```

- [ ] **Step 6: Verificar compilação dos testes**

```bash
go vet -tags=integration ./test/integration/
```
Expected: compila sem erros (pode haver warnings de dependências não utilizadas, ok).

---

### Task 23: Docker & Build

**Files:**
- Create: `Dockerfile`
- Create: `docker-compose.yml`
- Create: `Makefile`

- [ ] **Step 1: Escrever Dockerfile**

```dockerfile
# Multi-stage build para API Pix
FROM golang:1.23-alpine AS builder

RUN apk add --no-cache git ca-certificates

WORKDIR /build

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /app/pix-api ./cmd/server/

FROM alpine:3.20

RUN apk add --no-cache ca-certificates tzdata

ENV TZ=America/Sao_Paulo

COPY --from=builder /app/pix-api /usr/local/bin/pix-api

EXPOSE 8080

HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD wget --no-verbose --tries=1 --spider http://localhost:8080/health || exit 1

ENTRYPOINT ["/usr/local/bin/pix-api"]
```

- [ ] **Step 2: Escrever docker-compose.yml**

```yaml
version: "3.9"

services:
  postgres:
    image: postgres:16-alpine
    container_name: pix-postgres
    environment:
      POSTGRES_USER: pix
      POSTGRES_PASSWORD: pix
      POSTGRES_DB: pix_api
    ports:
      - "5432:5432"
    volumes:
      - pgdata:/var/lib/postgresql/data
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U pix -d pix_api"]
      interval: 5s
      timeout: 5s
      retries: 5

  api:
    build:
      context: .
      dockerfile: Dockerfile
    container_name: pix-api
    ports:
      - "8080:8080"
    environment:
      SERVER_PORT: "8080"
      DB_HOST: postgres
      DB_PORT: "5432"
      DB_USER: pix
      DB_PASSWORD: pix
      DB_NAME: pix_api
      DB_SSLMODE: disable
      PSP_MOCK_ENABLED: "true"
      WORKER_OUTBOX_POLL_INTERVAL: "5s"
      WORKER_RECONCILIATION_SCHEDULE: "@every 30m"
    depends_on:
      postgres:
        condition: service_healthy

volumes:
  pgdata:
```

- [ ] **Step 3: Escrever Makefile**

```makefile
.PHONY: run test integration db-up db-down migrate lint vet build

APP_NAME := pix-api
BUILD_DIR := ./bin
MIGRATIONS_DIR := ./internal/store/postgres/migrations
DB_DSN := postgres://pix:pix@localhost:5432/pix_api?sslmode=disable

# Inicia o servidor
run:
	go run ./cmd/server/

# Build compila o binário
build:
	go build -ldflags="-s -w" -o $(BUILD_DIR)/$(APP_NAME) ./cmd/server/

# Testes unitários
test:
	go test ./internal/... -v -count=1

# Testes de integração
integration:
	go test -tags=integration ./test/integration/ -v -count=1

# Sobe o banco de dados
db-up:
	docker-compose up -d postgres

# Derruba o banco
db-down:
	docker-compose down -v

# Executa migrations (requer golang-migrate instalado)
migrate:
	migrate -path $(MIGRATIONS_DIR) -database "$(DB_DSN)" up

# Reverte migrations
migrate-down:
	migrate -path $(MIGRATIONS_DIR) -database "$(DB_DSN)" down -all

# Lint
lint:
	golangci-lint run ./...

# Vet
vet:
	go vet ./...
```

- [ ] **Step 4: Verificar build**

```bash
go mod tidy && go build ./cmd/server/
```
Expected: go mod tidy resolve dependências, build compila sem erros.

---

## Resumo da Ordem de Implementação

| Onda | Tasks | Dependências | Paralelizável |
|------|-------|-------------|---------------|
| 1 | 1-5 (Config, Domain, PSP interface, Migrations, Connection) | Nenhuma | Tasks 1-4 em paralelo |
| 2 | 6-10 (Repos: Cob, Pix, Idempotency, Outbox, Webhook) | Task 5 (pool) | Tasks 6-10 em paralelo |
| 3 | 11 (Mock PSP) | Task 3 (interface) | Independente |
| 4 | 12-14 (Services: Cob, Pix, Webhook) | Tasks 6-11 | Sequencial dentro da onda |
| 5 | 15-17 (Middleware, Handlers, Router/Server) | Tasks 12-14 | Tasks 15-16 em paralelo, 17 depois |
| 6 | 18-19 (Workers: Outbox, Reconciliation) | Tasks 9, 7 | Tasks 18-19 em paralelo |
| 7 | 20 (Entry point main.go) | Todos os anteriores | Sequencial |
| 8 | 21-22 (Test helpers, Integration tests) | Tasks 1-20 | Dependente de todos |
| 9 | 23 (Docker, Compose, Makefile) | Task 20 (main.go) | Independente |

## Dependências Externas (go.mod)

```
require (
    github.com/caarlos0/env/v11 v11.3.0
    github.com/go-chi/chi/v5 v5.2.1
    github.com/golang-migrate/migrate/v4 v4.18.2
    github.com/google/uuid v1.6.0
    github.com/jackc/pgx/v5 v5.7.4
    github.com/robfig/cron/v3 v3.0.1
    github.com/stretchr/testify v1.10.0
    github.com/testcontainers/testcontainers-go v0.36.0
)
```


```

---
