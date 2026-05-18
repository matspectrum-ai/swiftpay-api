package worker

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/robfig/cron/v3"

	"github.com/matspectrum/swiftpay-api/internal/domain"
	"github.com/matspectrum/swiftpay-api/internal/observability"
	"github.com/matspectrum/swiftpay-api/internal/port/psp"
	"github.com/matspectrum/swiftpay-api/internal/store/postgres"
)

type ReconciliationWorker struct {
	db                *pgxpool.Pool
	pixRepo           *postgres.PixRepo
	pspClient         psp.PSPClient
	cron              *cron.Cron
	leaderElection    *LeaderElection
	maxPSPConcurrency int
}

func NewReconciliationWorker(db *pgxpool.Pool, pixRepo *postgres.PixRepo, pspClient psp.PSPClient, leaderElection *LeaderElection, maxPSPConcurrency int) *ReconciliationWorker {
	return &ReconciliationWorker{
		db:                db,
		pixRepo:           pixRepo,
		pspClient:         pspClient,
		leaderElection:    leaderElection,
		maxPSPConcurrency: maxPSPConcurrency,
	}
}

func (w *ReconciliationWorker) Start(ctx context.Context, schedule string) error {
	w.cron = cron.New(cron.WithLocation(time.UTC))

	_, err := w.cron.AddFunc(schedule, func() {
		epoch, err := w.leaderElection.TryAcquire(ctx)
		if err != nil {
			slog.ErrorContext(ctx, "erro tentando adquirir liderança", "error", err)
			return
		}
		if epoch == nil {
			slog.DebugContext(ctx, "outra instância é líder, pulando reconciliação")
			return
		}
		defer func() {
			if relErr := w.leaderElection.Release(ctx); relErr != nil {
				slog.ErrorContext(ctx, "erro liberando liderança", "error", relErr)
			}
		}()

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

func (w *ReconciliationWorker) run(ctx context.Context) error {
	startTime := time.Now().UTC()
	defer func() {
		observability.ReconciliationDuration.Observe(time.Since(startTime).Seconds())
	}()

	slog.InfoContext(ctx, "iniciando reconciliação")

	fim := time.Now().UTC()
	inicio := fim.Add(-24 * time.Hour)

	const pageSize = 200
	var (
		mu            sync.Mutex
		discrepancies []reconciliationRecord
		wg            sync.WaitGroup
		sem           = make(chan struct{}, w.maxPSPConcurrency)
		totalChecked  int
	)

	for offset := 0; ; offset += pageSize {
		filter := domain.PixFilter{
			Inicio: inicio,
			Fim:    fim,
			Limit:  pageSize,
			Offset: offset,
		}

		pixs, _, err := w.pixRepo.List(ctx, filter)
		if err != nil {
			return fmt.Errorf("listando pix locais: %w", err)
		}

		if len(pixs) == 0 {
			break
		}

		for _, pix := range pixs {
			wg.Add(1)
			sem <- struct{}{}
			go func(local domain.PixRecebido) {
				defer wg.Done()
				defer func() { <-sem }()

				pspPix, err := w.pspClient.GetPix(ctx, local.EndToEndID)
				if err != nil {
					mu.Lock()
					discrepancies = append(discrepancies, reconciliationRecord{
						EndToEndID:            local.EndToEndID,
						LocalValor:       fmt.Sprintf("%.2f", float64(local.ValorCentavos)/100.0),
						TipoDiscrepancia: "NAO_ENCONTRADO_PSP",
					})
					mu.Unlock()
					return
				}

				hasDiscrepancy := false
				var pspValor domain.ValorCentavos
				if err := pspValor.UnmarshalJSON([]byte(`"` + pspPix.Valor + `"`)); err != nil {
					slog.WarnContext(ctx, "erro convertendo valor PSP", "e2eid", local.EndToEndID, "psp_valor", pspPix.Valor, "error", err)
					return
				}

				rec := reconciliationRecord{
					EndToEndID:      local.EndToEndID,
					LocalValor: fmt.Sprintf("%.2f", float64(local.ValorCentavos)/100.0),
					PSPValor:   pspPix.Valor,
				}

				if local.ValorCentavos != pspValor {
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

		totalChecked += len(pixs)

		if len(pixs) < pageSize {
			break
		}
	}

	wg.Wait()

	if len(discrepancies) > 0 {
		slog.WarnContext(ctx, "discrepâncias encontradas", "count", len(discrepancies))
		for _, d := range discrepancies {
			if err := w.saveDiscrepancy(ctx, d); err != nil {
				slog.ErrorContext(ctx, "erro salvando discrepância", "e2eid", d.EndToEndID, "error", err)
			}
		}
	} else {
		slog.InfoContext(ctx, "reconciliação concluída sem discrepâncias", "pix_verificados", totalChecked)
	}

	_, snapErr := w.db.Exec(ctx,
		`INSERT INTO settlement_snapshots (snapshot_date, total_pix_received, total_amount_cents, total_discrepancies, reconciliation_completed, snapshot_data)
		 VALUES ($1, $2, $3, $4, $5, $6::jsonb)
		 ON CONFLICT (snapshot_date) DO UPDATE SET
		   total_pix_received = $2, total_amount_cents = $3, total_discrepancies = $4,
		   reconciliation_completed = $5, snapshot_data = $6::jsonb, created_at = NOW()`,
		fim.Truncate(24*time.Hour),
		totalChecked,
		0,
		len(discrepancies),
		len(discrepancies) == 0,
		"{}",
	)
	if snapErr != nil {
		slog.ErrorContext(ctx, "erro salvando snapshot de reconciliação", "error", snapErr)
	}

	return nil
}

type reconciliationRecord struct {
	EndToEndID            string
	LocalValor       string
	PSPValor         string
	LocalHorario     time.Time
	PSPHorario       time.Time
	TipoDiscrepancia string
}

func (w *ReconciliationWorker) saveDiscrepancy(ctx context.Context, rec reconciliationRecord) error {
	_, err := w.db.Exec(ctx,
		`INSERT INTO reconciliation_reports (e2eid, local_valor, psp_valor, local_horario, psp_horario, tipo_discrepancia)
		 VALUES ($1, $2, $3, $4, $5, $6)`,
		rec.EndToEndID, rec.LocalValor, rec.PSPValor, rec.LocalHorario, rec.PSPHorario, rec.TipoDiscrepancia,
	)
	if err != nil {
		return fmt.Errorf("inserindo discrepância e2eid=%s: %w", rec.EndToEndID, err)
	}
	return nil
}
