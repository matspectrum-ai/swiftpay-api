# API SwiftPay

![Go](https://img.shields.io/badge/Go-1.23-00ADD8?logo=go)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-16-4169E1?logo=postgresql)
![Docker](https://img.shields.io/badge/Docker-ready-2496ED?logo=docker)
![License](https://img.shields.io/badge/license-MIT-green)

API RESTful para processamento de pagamentos Pix seguindo a especificação do Banco Central do Brasil (BACEN). Integrada ao provedor **MagicPay**.

---

## Sumário

- [Visão Geral](#visão-geral)
- [Funcionalidades](#funcionalidades)
- [Tecnologias](#tecnologias)
- [Arquitetura](#arquitetura)
- [Requisitos](#requisitos)
- [Instalação](#instalação)
- [Endpoints](#endpoints)
- [Configuração](#configuração)
- [Banco de Dados](#banco-de-dados)
- [Padrões Implementados](#padrões-implementados)
- [Observabilidade](#observabilidade)
- [Segurança](#segurança)
- [Resiliência](#resiliência)
- [Testes](#testes)
- [Deploy](#deploy)
- [Contribuição](#contribuição)

---

## Visão Geral

A **API SwiftPay** é uma solução completa para orquestração de pagamentos Pix. Desenvolvida em Go, implementa todos os padrões exigidos pelo BACEN para sistemas financeiros de pagamento instantâneo.

### Fluxo de Pagamento

```
Recebedor → POST /cob/{txid} → API SwiftPay → MagicPay (PSP) → QR Code
                                                      ↓
Pagador → Escaneia QR → Paga → MagicPay → Webhook → API SwiftPay
                                                      ↓
                                            Pix persistido + Cobrança CONCLUIDA
```

---

## Funcionalidades

- **Cobranças Imediatas (cob)** — Criar, consultar, alterar e cancelar cobranças Pix
- **Pix Recebidos (pix)** — Consultar Pix liquidados e solicitar devoluções
- **Webhooks** — Configurar callbacks e receber notificações de liquidação
- **Idempotência** — Garantia de operação única por chave de idempotência
- **Outbox Transacional** — Publicação confiável de eventos com lease-based claiming
- **Reconciliação** — Job diário comparando registros locais com PSP
- **Ledger Imutável** — Registro append-only de todos os eventos financeiros
- **Liveness/Readiness** — Health checks para orquestração de containers

---

## Tecnologias

| Componente | Tecnologia |
|------------|------------|
| Linguagem | Go 1.23 |
| Banco de Dados | PostgreSQL 16 |
| Driver SQL | jackc/pgx v5 |
| Migrations | golang-migrate v4 |
| Roteador HTTP | go-chi/chi v5 |
| Logging | log/slog (stdlib) |
| Métricas | Prometheus client_golang |
| Tracing | OpenTelemetry |
| Testes | testcontainers-go + testify |
| Container | Docker + docker-compose |
| PSP | MagicPay (produção) / Mock (desenvolvimento) |

---

## Arquitetura

```
cmd/server/main.go                      # Entry point
internal/
├── config/                              # Configuração via variáveis de ambiente
├── domain/                              # Entidades e regras de negócio
│   ├── cob.go                           # Cobrança + FSM de status
│   ├── pix.go                           # Pix recebido
│   ├── errors.go                        # Erros RFC 7807
│   └── context.go                       # Chaves de contexto
├── observability/                       # Métricas e tracing
│   ├── metrics.go                       # 8 métricas Prometheus
│   └── tracing.go                       # OpenTelemetry spans
├── security/                            # Segurança
│   ├── webhook.go                       # Verificação HMAC-SHA256
│   └── rate_limiter.go                  # Token bucket rate limiter
├── port/
│   ├── http/                            # Camada HTTP
│   │   ├── handler/                     # Handlers REST
│   │   ├── middleware/                  # Middlewares (idempotência, logging, rate limit)
│   │   ├── router.go                    # Rotas chi
│   │   └── server.go                    # Servidor HTTP + graceful shutdown
│   └── psp/                             # Provedores de Serviço de Pagamento
│       ├── psp.go                       # Interface PSPClient (padrão BACEN)
│       ├── mock/                        # Mock para desenvolvimento
│       └── magicpay/                    # Cliente MagicPay (produção)
├── service/                             # Lógica de aplicação
│   ├── cob_service.go                   # Serviço de cobranças
│   ├── pix_service.go                   # Serviço de Pix
│   └── webhook_service.go              # Serviço de webhooks
├── store/
│   └── postgres/                        # Camada de persistência
│       ├── connection.go                # Pool pgx + slow query logging
│       ├── *_repo.go                    # Repositórios (cob, pix, webhook, idempotency, outbox, ledger)
│       └── migrations/                  # 11 migrations SQL
└── worker/                              # Workers assíncronos
    ├── outbox_publisher.go              # Publisher com lease-based claiming
    ├── reconciliation_worker.go         # Worker de reconciliação
    ├── retry.go                         # Engine de retry (exponential backoff)
    ├── circuit_breaker.go               # Circuit breaker
    └── leader_election.go              # Liderança via advisory lock
```

---

## Requisitos

- Go 1.23+
- PostgreSQL 16+
- Docker (para testes de integração)
- Token de API MagicPay (para produção)

---

## Instalação

### Desenvolvimento Local

```bash
# Clone o repositório
git clone https://github.com/matspectrum/swiftpay-api.git
cd swiftpay-api

# Configure o ambiente
cp .env.example .env
# Edite .env com suas configurações

# Inicie o PostgreSQL
make db-up

# Execute as migrations
make migrate

# Inicie a API
make run
```

### Docker

```bash
# Build e execução completa (API + PostgreSQL)
docker-compose up -d

# Verificar health
curl http://localhost:8080/health/live

# Métricas
curl http://localhost:8080/metrics
```

---

## Endpoints

### Health

| Método | Rota | Descrição |
|--------|------|-----------|
| `GET` | `/health` | Status básico |
| `GET` | `/health/live` | Liveness probe |
| `GET` | `/health/ready` | Readiness probe (DB + PSP) |
| `GET` | `/metrics` | Métricas Prometheus |

### Cobranças (`/cob`)

| Método | Rota | Descrição |
|--------|------|-----------|
| `POST` | `/cob/{txid}` | Criar cobrança imediata |
| `PUT` | `/cob/{txid}` | Substituir cobrança |
| `PATCH` | `/cob/{txid}` | Alterar status |
| `GET` | `/cob/{txid}` | Consultar cobrança |
| `GET` | `/cob` | Listar cobranças |

### Pix (`/pix`)

| Método | Rota | Descrição |
|--------|------|-----------|
| `GET` | `/pix/{e2eid}` | Consultar Pix |
| `GET` | `/pix` | Listar Pix recebidos |
| `PUT` | `/pix/{e2eid}/devolucao/{id}` | Solicitar devolução |

### Webhooks (`/webhook`)

| Método | Rota | Descrição |
|--------|------|-----------|
| `PUT` | `/webhook/{chave}` | Configurar webhook |
| `GET` | `/webhook/{chave}` | Consultar webhook |
| `GET` | `/webhook` | Listar webhooks |
| `DELETE` | `/webhook/{chave}` | Remover webhook |
| `POST` | `/api/v1/webhook/callback` | Callback do PSP |

---

## Configuração

### Variáveis de Ambiente

| Variável | Padrão | Descrição |
|----------|--------|-----------|
| `SERVER_PORT` | `8080` | Porta do servidor HTTP |
| `DB_HOST` | `localhost` | Host PostgreSQL |
| `DB_PORT` | `5432` | Porta PostgreSQL |
| `DB_USER` | `swiftpay` | Usuário do banco |
| `DB_PASSWORD` | `swiftpay` | Senha do banco |
| `DB_NAME` | `swiftpay` | Nome do banco |
| `DB_MAX_OPEN_CONNS` | `25` | Conexões máximas |
| `DB_MAX_IDLE_CONNS` | `10` | Conexões ociosas |
| `PSP_MOCK_ENABLED` | `true` | Usar PSP mock (dev) |
| `PSP_BASE_URL` | `https://api.sistema-magicpay.com` | URL base do PSP |
| `PSP_CLIENT_SECRET` | — | Token de API do MagicPay |
| `WORKER_OUTBOX_POLL_INTERVAL` | `5s` | Intervalo de polling do outbox |
| `WORKER_RECONCILIATION_SCHEDULE` | `@daily` | Cron de reconciliação |

---

## Banco de Dados

### Tabelas (11)

| Tabela | Descrição |
|--------|-----------|
| `cobrancas` | Cobranças Pix imediatas |
| `pix_recebidos` | Pix liquidados |
| `idempotency_keys` | Chaves de idempotência |
| `outbox_messages` | Mensagens do outbox transacional |
| `outbox_dead_letter` | Mensagens envenenadas (poison) |
| `webhooks` | Configurações de webhook |
| `webhook_events` | Eventos de webhook (dedup) |
| `reconciliation_reports` | Relatórios de reconciliação |
| `devolucoes` | Devoluções de Pix |
| `ledger_events` | Eventos financeiros imutáveis |

### Migrations

```bash
# Executar migrations
make migrate

# Criar nova migration
migrate create -ext sql -dir internal/store/postgres/migrations -seq nome_da_migration
```

---

## Padrões Implementados

### Idempotência

Toda requisição `POST`/`PUT`/`PATCH` pode incluir o header `Idempotency-Key`. O middleware:

1. Calcula hash SHA256 do corpo da requisição
2. Insere chave no banco com `ON CONFLICT DO NOTHING`
3. Se chave já existe: verifica divergência de payload (400) ou faz replay (200)
4. Se chave nova: processa e completa **dentro da mesma transação de negócio** (atomicidade)

### Outbox Transacional

- **Write**: Mensagem inserida na mesma transação `pgx.Tx` que a operação de negócio
- **Claim**: `ClaimAndFetch` com `FOR UPDATE SKIP LOCKED` dentro de transação
- **Process**: Processamento **fora** da transação de claim
- **Ack/Nack**: Confirmação ou retry após processamento
- **DeadLetter**: Mensagens que excedem `max_attempts` vão para `outbox_dead_letter`
- **Lease Recovery**: Claims expirados (`claimed_at + lease_timeout < NOW`) são automaticamente revividos

### FSM (Máquina de Estados)

```
ATIVA → CONCLUIDA         (pagamento recebido)
ATIVA → REMOVIDA_PELO_USUARIO_RECEBEDOR   (cancelada pelo recebedor)
ATIVA → REMOVIDA_PELO_PSP                  (cancelada pelo PSP)

CONCLUIDA, REMOVIDA_* → (terminal, sem transições)
```

Todas as transições são validadas via `CanTransitionTo()` e protegidas por **optimistic locking** (`WHERE revisao = $3`).

### Reconciliação

- Job diário via cron
- Paginação completa (todas as páginas)
- Semáforo de concorrência (10 chamadas simultâneas ao PSP)
- Apenas **lê** dados (não altera estado)
- Registra discrepâncias em `reconciliation_reports`
- Leader election via `pg_try_advisory_lock` (única instância ativa)

---

## Observabilidade

### Métricas Prometheus

| Métrica | Tipo | Descrição |
|---------|------|-----------|
| `swiftpay_webhook_processed_total` | CounterVec | Webhooks processados por outcome |
| `swiftpay_outbox_lag_seconds` | Gauge | Idade da mensagem mais antiga |
| `swiftpay_reconciliation_duration_seconds` | Histogram | Duração da reconciliação |
| `swiftpay_psp_latency_seconds` | HistogramVec | Latência do PSP por operação |
| `swiftpay_idempotency_hits_total` | Counter | Replays de idempotência |
| `swiftpay_idempotency_misses_total` | Counter | Novas requisições |
| `swiftpay_retry_total` | CounterVec | Retries por componente |
| `swiftpay_worker_errors_total` | CounterVec | Erros de workers |

### Tracing

Spans OpenTelemetry em todos os níveis: HTTP, DB, PSP, Workers.

### Logs

JSON estruturado via `log/slog` com `request_id`, `correlation_id`, `txid`, `e2eid`.

---

## Segurança

- **Webhook Signature**: HMAC-SHA256 via header `X-Signature`
- **Rate Limiting**: Token bucket por chave de idempotência
- **Config Validation**: Bloqueia startup com configuração inválida
- **mTLS Ready**: Estrutura para certificados TLS mútuos ICP-Brasil

---

## Resiliência

- **Circuit Breaker**: Protege chamadas ao PSP (Closed → HalfOpen → Open)
- **Retry Engine**: Exponential backoff (100ms → 30s) com ±25% jitter
- **Graceful Degradation**: Sistema continua processando localmente com PSP indisponível
- **Leader Election**: PostgreSQL advisory lock para jobs únicos
- **Liveness/Readiness**: Health probes para Kubernetes/Docker

---

## Testes

### Testes de Integração

```bash
# Requer Docker rodando
make integration
```

### Evals Automatizadas

```bash
cd pix-breaker-harness
go test -race -v ./...
```

**28 evals** cobrindo: concorrência, replay, outbox, webhook, recovery, transações, consistência, PSP failure.

### Chaos Testing

```bash
cd pix-breaker-harness
go run . --mode memory --concurrency 1000 --replays 100 --webhook-duplicates 50
```

---

## Deploy

### Docker Compose

```bash
docker-compose up -d
```

### Kubernetes (breve)

Helm chart em desenvolvimento.

---

## Contribuição

1. Fork o repositório
2. Crie sua branch (`git checkout -b feat/nova-funcionalidade`)
3. Commit suas mudanças (`git commit -m 'feat: adiciona nova funcionalidade'`)
4. Push para a branch (`git push origin feat/nova-funcionalidade`)
5. Abra um Pull Request

### Convenções de Commit

Seguimos [Conventional Commits](https://www.conventionalcommits.org/):

- `feat:` Nova funcionalidade
- `fix:` Correção de bug
- `docs:` Documentação
- `test:` Testes
- `refactor:` Refatoração
- `perf:` Performance
- `chore:` Tarefas de build/config

---

## Licença

MIT © 2026 SwiftPay

---

## Contato

- **Autor**: Matspectrum AI
- **GitHub**: [github.com/matspectrum/swiftpay-api](https://github.com/matspectrum/swiftpay-api)
