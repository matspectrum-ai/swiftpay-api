package main

import "time"

type PaymentStatus string

const (
	StatusCreated           PaymentStatus = "CREATED"
	StatusSubmissionPending PaymentStatus = "SUBMISSION_PENDING"
	StatusSubmitted         PaymentStatus = "SUBMITTED"
	StatusConfirmed         PaymentStatus = "CONFIRMED"
	StatusSettled           PaymentStatus = "SETTLED"
	StatusRejected          PaymentStatus = "REJECTED"
	StatusFailed            PaymentStatus = "FAILED"
	StatusCanceled          PaymentStatus = "CANCELED"
)

func (s PaymentStatus) Terminal() bool {
	switch s {
	case StatusConfirmed, StatusSettled, StatusRejected, StatusFailed, StatusCanceled:
		return true
	default:
		return false
	}
}

type CreatePaymentRequest struct {
	MerchantID     string `json:"merchant_id"`
	IdempotencyKey string `json:"idempotency_key"`
	AmountCents    int64  `json:"amount_cents"`
	PixKey         string `json:"pix_key"`
	Description    string `json:"description,omitempty"`
	CorrelationID  string `json:"correlation_id,omitempty"`
}

type CreatePaymentResponse struct {
	PaymentID      string       `json:"payment_id"`
	Status         PaymentStatus `json:"status"`
	ProviderRef    string       `json:"provider_reference,omitempty"`
	IdempotencyKey string       `json:"idempotency_key"`
}

type Payment struct {
	ID             string       `json:"payment_id"`
	MerchantID     string       `json:"merchant_id"`
	IdempotencyKey string       `json:"idempotency_key"`
	IdempotencyHash string       `json:"idempotency_hash"`
	AmountCents    int64        `json:"amount_cents"`
	PixKey         string       `json:"pix_key"`
	Description    string       `json:"description,omitempty"`
	Status         PaymentStatus `json:"status"`
	ProviderRef    string       `json:"provider_reference,omitempty"`
	Version        int64        `json:"version"`
	CreatedAt      time.Time    `json:"created_at"`
	UpdatedAt      time.Time    `json:"updated_at"`
}

type WebhookEvent struct {
	EventID       string        `json:"event_id"`
	ProviderRef   string        `json:"provider_reference"`
	PaymentStatus PaymentStatus `json:"payment_status"`
	OccurredAt    time.Time     `json:"occurred_at"`
	RawPayload    map[string]any `json:"raw_payload,omitempty"`
}

type Report struct {
	Summary        Summary        `json:"summary"`
	SystemAnalysis SystemAnalysis `json:"system_analysis"`
	Invariants     []Invariant    `json:"invariants"`
	AttackResults  []AttackResult `json:"attack_results"`
	CriticalBreaks []CriticalBreak `json:"critical_breaks"`
	MajorIssues    []Issue        `json:"major_issues"`
	MinorIssues    []Issue        `json:"minor_issues"`
	Confidence     Confidence    `json:"confidence"`
	FinalVerdict   FinalVerdict   `json:"final_verdict"`
}

type Summary struct {
	SystemName             string   `json:"system_name"`
	Version                string   `json:"version"`
	TotalScenariosExecuted int      `json:"total_scenarios_executed"`
	MaxConcurrencyLevel   int      `json:"max_concurrency_level"`
	FaultsInjected         []string `json:"faults_injected"`
	TerminationReason      string   `json:"termination_reason"`
}

type SystemAnalysis struct {
	ArchitectureType    string `json:"architecture_type"`
	FSMValid            bool   `json:"fsm_valid"`
	IdempotencyStrength string `json:"idempotency_strength"`
	ReplaySafety        string `json:"replay_safety"`
	OutboxCorrectness   string `json:"outbox_correctness"`
	PSPIsolation        string `json:"psp_isolation"`
	OptimisticLocking   string `json:"optimistic_locking"`
}

type Invariant struct {
	Name      string `json:"name"`
	Status    string `json:"status"`
	Evidence  string `json:"evidence"`
	RiskLevel string `json:"risk_level"`
}

type AttackResult struct {
	Category         string `json:"category"`
	Scenario         string `json:"scenario"`
	ExpectedBehavior string `json:"expected_behavior"`
	ObservedBehavior string `json:"observed_behavior"`
	Result           string `json:"result"`
	Notes            string `json:"notes"`
}

type CriticalBreak struct {
	Title             string   `json:"title"`
	Severity          string   `json:"severity"`
	Scenario          string   `json:"scenario"`
	RootCause         string   `json:"root_cause"`
	FinancialImpact   string   `json:"financial_impact"`
	ReproductionSteps []string `json:"reproduction_steps"`
	WhyCritical       string   `json:"why_critical"`
}

type Issue struct {
	Title       string `json:"title"`
	Description string `json:"description"`
	Impact      string `json:"impact,omitempty"`
	Likelihood  string `json:"likelihood,omitempty"`
}

type Confidence struct {
	EvidenceQuality          string `json:"evidence_quality"`
	Reproducibility          string `json:"reproducibility"`
	CoverageOfChaosScenarios string `json:"coverage_of_chaos_scenarios"`
}

type FinalVerdict struct {
	Status          string `json:"status"`
	ResilienceScore int    `json:"resilience_score"`
	Conclusion      string `json:"conclusion"`
}
