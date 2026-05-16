# Fix Reconciliation + Webhook — P0/P1 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development

**Goal:** Add PSP rate limiter, full pagination to reconciliation worker, and make ConfigureWebhook transactional.

**Architecture:** Semaphore-based concurrency control (channel buffered) for PSP calls. Pagination loop replaces single query. Webhook config flow inverted: PSP first, then local persistence.

**Tech Stack:** Go 1.23, stdlib channels

---

### Task 1: Rate Limiter + Pagination on Reconciliation Worker

**Files:**
- Modify: `internal/worker/reconciliation_worker.go`
- Modify: `cmd/server/main.go`

#### Step 1: Add semaphore rate limiter

In `ReconciliationWorker` struct, add `maxPSPConcurrency int` field. In `NewReconciliationWorker`, accept `maxPSPConcurrency int` param.

#### Step 2: Add pagination loop

Replace the single query at `reconciliation_worker.go:58-74` with a paginated loop.

#### Step 3: Update main.go

In `cmd/server/main.go`, update the `NewReconciliationWorker` call to pass `maxPSPConcurrency`.

---

### Task 2: ConfigureWebhook Transacional

**Files:**
- Modify: `internal/service/webhook_service.go`

#### Step 1: Invert order - PSP first, then local Upsert

Replace `ConfigureWebhook` method (lines 39-67) with PSP-first approach. If PSP succeeds but local persistence fails, log critical error requiring manual intervention. No rollback attempt.
