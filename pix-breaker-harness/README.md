# PIX Breaker Harness

Harness Go executável para auditoria adversarial de sistemas Pix.

## Modo memória

```bash
go run . --mode memory --concurrency 500 --replays 50 --webhook-duplicates 20 --demo-bugs
```

## Modo HTTP

```bash
go run . --mode http --base-url https://sua-api.exemplo --concurrency 500
```

## Saída

Gera um `report.json` com:
- invariantes
- ataques executados
- falhas críticas
- score de resiliência
- veredito final

## Endpoints esperados no modo HTTP

- `POST /payments`
- `GET /payments/{id}`
- `POST /webhooks/pix`
- `POST /payments/{id}/reconcile`
