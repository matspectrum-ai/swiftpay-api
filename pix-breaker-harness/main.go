package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"time"
)

func main() {
	var (
		mode              = flag.String("mode", "memory", "memory|http")
		baseURL           = flag.String("base-url", "", "HTTP target base URL")
		outPath           = flag.String("out", "report.json", "output report path")
		concurrency       = flag.Int("concurrency", 100, "number of concurrent create requests")
		replays           = flag.Int("replays", 25, "number of webhook replay rounds")
		seed              = flag.Int64("seed", time.Now().UnixNano(), "random seed")
		merchantID        = flag.String("merchant-id", "merchant-001", "merchant id")
		idempotencyKey    = flag.String("idempotency-key", "idem-001", "idempotency key")
		amountCents       = flag.Int64("amount-cents", 1234, "payment amount in cents")
		webhookDuplicates = flag.Int("webhook-duplicates", 10, "duplicate webhook deliveries per round")
		demoBugs          = flag.Bool("demo-bugs", false, "enable fault injection in memory target")
	)
	flag.Parse()

	ctx := context.Background()

	var target Target
	switch *mode {
	case "memory":
		target = NewMemoryTarget(MemoryTargetConfig{DemoBugs: *demoBugs, Seed: *seed})
	case "http":
		if *baseURL == "" {
			fmt.Fprintln(os.Stderr, "base-url is required in http mode")
			os.Exit(2)
		}
		target = NewHTTPTarget(*baseURL)
	default:
		fmt.Fprintf(os.Stderr, "invalid mode: %s\n", *mode)
		os.Exit(2)
	}

	h := NewHarness(target, HarnessConfig{
		Seed:              *seed,
		Concurrency:       *concurrency,
		Replays:           *replays,
		WebhookDuplicates: *webhookDuplicates,
		MerchantID:        *merchantID,
		IdempotencyKey:    *idempotencyKey,
		AmountCents:       *amountCents,
	})

	report, err := h.Run(ctx)
	if err != nil {
		report.FinalVerdict.Status = "UNSAFE"
		report.FinalVerdict.Conclusion = err.Error()
	}

	data, marshalErr := json.MarshalIndent(report, "", "  ")
	if marshalErr != nil {
		fmt.Fprintf(os.Stderr, "marshal report: %v\n", marshalErr)
		os.Exit(1)
	}

	if writeErr := os.WriteFile(*outPath, data, 0o644); writeErr != nil {
		fmt.Fprintf(os.Stderr, "write report: %v\n", writeErr)
		os.Exit(1)
	}

	fmt.Printf("report written to %s\n", *outPath)
	if report.FinalVerdict.Status == "UNSAFE" || report.FinalVerdict.Status == "BROKEN" {
		os.Exit(1)
	}
}
