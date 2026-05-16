package observability

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	WebhookProcessed = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "swiftpay_webhook_processed_total",
		Help: "Total webhooks processed by outcome",
	}, []string{"outcome"})

	OutboxLag = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "swiftpay_outbox_lag_seconds",
		Help: "Seconds since oldest unpublished outbox message",
	})

	ReconciliationDuration = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "swiftpay_reconciliation_duration_seconds",
		Help:    "Duration of reconciliation runs",
		Buckets: prometheus.DefBuckets,
	})

	PSPLatency = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "swiftpay_psp_latency_seconds",
		Help:    "PSP call latency by operation",
		Buckets: prometheus.DefBuckets,
	}, []string{"operation"})

	IdempotencyHits = promauto.NewCounter(prometheus.CounterOpts{
		Name: "swiftpay_idempotency_hits_total",
		Help: "Idempotency cache hits (replay responses)",
	})

	IdempotencyMisses = promauto.NewCounter(prometheus.CounterOpts{
		Name: "swiftpay_idempotency_misses_total",
		Help: "Idempotency misses (new requests)",
	})

	RetryCount = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "swiftpay_retry_total",
		Help: "Retry attempts by component",
	}, []string{"component", "outcome"})

	WorkerErrors = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "swiftpay_worker_errors_total",
		Help: "Worker errors by worker type",
	}, []string{"worker"})
)
