package main

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"time"
)

type ChaosScenario struct {
	Name        string
	Concurrency int
	Duration    time.Duration
	Run         func(ctx context.Context, target Target) error
}

var ChaosScenarios = []ChaosScenario{
	{
		Name:        "duplicate_webhook_flood",
		Concurrency: 50,
		Duration:    30 * time.Second,
		Run: func(ctx context.Context, target Target) error {
			req := CreatePaymentRequest{MerchantID: "chaos", IdempotencyKey: "chaos-flood-1", AmountCents: 999, PixKey: "chaos@pix.com"}
			p, err := target.CreatePayment(ctx, req)
			if err != nil {
				return err
			}

			ev := WebhookEvent{EventID: fmt.Sprintf("chaos-ev-%d", rand.Int63()), ProviderRef: p.ProviderRef, PaymentStatus: StatusSettled, OccurredAt: time.Now()}

			var wg sync.WaitGroup
			errors := make(chan error, 1000)

			for i := 0; i < 1000; i++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					if err := target.HandleWebhook(ctx, ev); err != nil {
						select {
						case errors <- err:
						default:
						}
					}
				}()
			}
			wg.Wait()
			close(errors)

			p2, err := target.GetPayment(ctx, p.ID)
			if err != nil {
				return err
			}
			if p2.Status != StatusSettled {
				return fmt.Errorf("expected SETTLED, got %s", p2.Status)
			}
			return nil
		},
	},
	{
		Name:        "idempotency_collision_storm",
		Concurrency: 1000,
		Duration:    15 * time.Second,
		Run: func(ctx context.Context, target Target) error {
			req := CreatePaymentRequest{MerchantID: "chaos", IdempotencyKey: "chaos-collision", AmountCents: 500, PixKey: "collision@pix.com"}

			var ids sync.Map
			var wg sync.WaitGroup

			for i := 0; i < 500; i++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					p, err := target.CreatePayment(ctx, req)
					if err != nil {
						return
					}
					ids.Store(p.ID, true)
				}()
			}
			wg.Wait()

			count := 0
			ids.Range(func(_, _ interface{}) bool {
				count++
				return true
			})
			if count > 1 {
				return fmt.Errorf("duplicate payments: %d", count)
			}
			return nil
		},
	},
	{
		Name:        "reconciliation_backlog",
		Concurrency: 10,
		Duration:    60 * time.Second,
		Run: func(ctx context.Context, target Target) error {
			for i := 0; i < 50; i++ {
				req := CreatePaymentRequest{MerchantID: "chaos", IdempotencyKey: fmt.Sprintf("chaos-backlog-%d", i), AmountCents: int64(100 + i), PixKey: fmt.Sprintf("backlog%d@pix.com", i)}
				p, err := target.CreatePayment(ctx, req)
				if err != nil {
					return err
				}
				if err := target.Reconcile(ctx, p.ID); err != nil {
					return err
				}
			}
			return nil
		},
	},
}

func RunChaosSuite(ctx context.Context, target Target) []AttackResult {
	var results []AttackResult

	for _, scenario := range ChaosScenarios {
		ctx, cancel := context.WithTimeout(ctx, scenario.Duration)

		start := time.Now()
		err := scenario.Run(ctx, target)
		elapsed := time.Since(start)

		result := AttackResult{
			Category:         "CHAOS",
			Scenario:         scenario.Name,
			ExpectedBehavior: "system survives without corruption",
			Notes:            fmt.Sprintf("duration=%v concurrency=%d", elapsed, scenario.Concurrency),
		}

		if err != nil {
			result.Result = "FAIL"
			result.ObservedBehavior = err.Error()
		} else {
			result.Result = "PASS"
			result.ObservedBehavior = fmt.Sprintf("completed in %v", elapsed)
		}

		results = append(results, result)
		cancel()
	}

	return results
}
