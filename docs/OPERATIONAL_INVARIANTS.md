# SwiftPay — Operational Invariants

## I-1: Webhook Durability
Acknowledged webhook events must survive process crash.
- ACK returned only after PostgreSQL commit.
- Duplicate delivery is safe (at-least-once).
- Deduplication via UNIQUE(e2eid, chave_pix).

## I-2: Replay Determinism
Idempotency replay must return byte-equivalent responses.
- Same Idempotency-Key + same payload → same HTTP body.
- Stored response is the canonical pre-PSP version.
- PSP enrichment is non-critical (location, pixCopiaECola).

## I-3: Leader Monotonicity
Only one reconciliation leader may mutate state at any time.
- Fencing token (epoch) is monotonic via leader_leases.
- Stale leaders rejected via expires_at < NOW().
- Heartbeat renewal every 10s during execution.
- Lease expiration → other pod takes over after 30s.

## I-4: Outbox Exactly-Once Observation
Each outbox message must be processed at most once.
- ClaimAndFetch uses CTE + FOR UPDATE SKIP LOCKED.
- AckPublished validates claimed_by ownership.
- MoveToDeadLetter validates claimed_by ownership.
- NackFailed releases claim for retry.

## I-5: Retry Boundedness
Retries must be bounded and not cause amplification.
- Max 5 attempts per message.
- RetryBudget: 100 retries/minute sliding window.
- Overload errors (429/503) use half MaxDelay.
- Permanent errors (4xx) never retried.

## I-6: Optimistic Locking
Stale revisions must never overwrite newer state.
- cobRepo.Update: WHERE revisao = $13.
- cobRepo.UpdateStatus: WHERE revisao = $3.
- RowsAffected == 0 → conflict detected.

## I-7: Financial Immutability
Ledger events are append-only and never mutated.
- ledger_events: INSERT only, no UPDATE/DELETE.
- settlement_snapshots: ON CONFLICT DO UPDATE (idempotent).
- Reconciliation reads, never mutates financial state.

## I-8: PSP Isolation
PSP failures must not corrupt local state.
- Local-first persistence (transaction before PSP call).
- PSP enrichment is best-effort (location, pixCopiaECola).
- Reconciliation detects and reports PSP divergence.
