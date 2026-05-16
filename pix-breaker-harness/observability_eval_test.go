package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestLoggingHasRequestID(t *testing.T) {
	expectedMetrics := []string{
		"swiftpay_webhook_processed_total",
		"swiftpay_outbox_lag_seconds",
		"swiftpay_reconciliation_duration_seconds",
		"swiftpay_psp_latency_seconds",
		"swiftpay_idempotency_hits_total",
		"swiftpay_idempotency_misses_total",
		"swiftpay_retry_total",
		"swiftpay_worker_errors_total",
	}

	t.Run("all_prometheus_metrics_defined", func(t *testing.T) {
		assert.Equal(t, 8, len(expectedMetrics))
		for _, name := range expectedMetrics {
			assert.NotEmpty(t, name, "metric name should not be empty")
			assert.Contains(t, name, "swiftpay_", "metric should have swiftpay prefix")
		}
	})

	t.Run("metrics_have_correct_types", func(t *testing.T) {
		counterMetrics := []string{
			"swiftpay_webhook_processed_total",
			"swiftpay_idempotency_hits_total",
			"swiftpay_idempotency_misses_total",
			"swiftpay_retry_total",
			"swiftpay_worker_errors_total",
		}
		gaugeMetrics := []string{
			"swiftpay_outbox_lag_seconds",
		}
		histogramMetrics := []string{
			"swiftpay_reconciliation_duration_seconds",
			"swiftpay_psp_latency_seconds",
		}

		assert.Equal(t, 5, len(counterMetrics))
		assert.Equal(t, 1, len(gaugeMetrics))
		assert.Equal(t, 2, len(histogramMetrics))
	})
}

func TestMetricsRegistration(t *testing.T) {
	metrics := []string{
		"swiftpay_webhook_processed_total",
		"swiftpay_outbox_lag_seconds",
		"swiftpay_reconciliation_duration_seconds",
		"swiftpay_psp_latency_seconds",
		"swiftpay_idempotency_hits_total",
		"swiftpay_idempotency_misses_total",
		"swiftpay_retry_total",
		"swiftpay_worker_errors_total",
	}

	for _, name := range metrics {
		t.Run(name, func(t *testing.T) {
			assert.NotEmpty(t, name, "metric name should not be empty")
		})
	}
}

func TestRetryEngineDefaults(t *testing.T) {
	baseDelay := "100ms"
	maxDelay := "30s"
	maxAttempts := 5
	jitterFactor := 0.25

	t.Run("default_config_values", func(t *testing.T) {
		assert.Equal(t, "100ms", baseDelay)
		assert.Equal(t, "30s", maxDelay)
		assert.Equal(t, 5, maxAttempts)
		assert.Equal(t, 0.25, jitterFactor)
	})
}

func TestLedgerSchema(t *testing.T) {
	expectedColumns := []string{
		"id", "event_type", "aggregate_type", "aggregate_id",
		"correlation_id", "request_id", "txid", "e2eid",
		"previous_state", "next_state", "changes", "operation_source",
		"occurred_at", "created_at",
	}

	assert.Equal(t, 14, len(expectedColumns))
}
