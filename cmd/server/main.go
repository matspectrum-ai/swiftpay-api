package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/postgres"
	_ "github.com/golang-migrate/migrate/v4/source/file"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/matspectrum/swiftpay-api/internal/config"
	"github.com/matspectrum/swiftpay-api/internal/domain"
	"github.com/matspectrum/swiftpay-api/internal/port/psp"
	"github.com/matspectrum/swiftpay-api/internal/port/psp/magicpay"
	"github.com/matspectrum/swiftpay-api/internal/port/psp/mock"
	httpserver "github.com/matspectrum/swiftpay-api/internal/port/http"
	"github.com/matspectrum/swiftpay-api/internal/port/http/handler"
	"github.com/matspectrum/swiftpay-api/internal/security"
	"github.com/matspectrum/swiftpay-api/internal/service"
	"github.com/matspectrum/swiftpay-api/internal/store/postgres"
	"github.com/matspectrum/swiftpay-api/internal/worker"
)

func main() {
	ctx := context.Background()

	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))
	slog.SetDefault(logger)

	slog.InfoContext(ctx, "iniciando API Pix")

	cfg, err := config.Load()
	if err != nil {
		slog.ErrorContext(ctx, "erro carregando configuração", "error", err)
		os.Exit(1)
	}

	if err := cfg.Validate(); err != nil {
		slog.ErrorContext(ctx, "configuração inválida", "error", err)
		os.Exit(1)
	}

	pool, err := postgres.NewPool(
		ctx,
		cfg.Database.DSN(),
		cfg.Database.MaxOpenConns,
		cfg.Database.MaxIdleConns,
		cfg.Database.ConnMaxLifetime,
		cfg.Database.ConnMaxIdleTime,
	)
	if err != nil {
		slog.ErrorContext(ctx, "erro conectando ao banco", "error", err)
		os.Exit(1)
	}
	defer pool.Close()

	if err := runMigrations(pool); err != nil {
		slog.ErrorContext(ctx, "erro executando migrations", "error", err)
		os.Exit(1)
	}

	var pspClient psp.PSPClient
	if cfg.PSP.MockEnabled {
		pspClient = mock.NewMockPSP()
	} else {
		pspClient = magicpay.NewClient(cfg.PSP.BaseURL, cfg.PSP.ClientSecret)
	}

	cobRepo := postgres.NewCobRepo(pool)
	pixRepo := postgres.NewPixRepo(pool)
	webhookRepo := postgres.NewWebhookRepo(pool)
	idempotencyRepo := postgres.NewIdempotencyRepo(pool)
	outboxWriter := postgres.NewOutboxWriter(pool)
	outboxReader := postgres.NewOutboxReader(pool)
	ledgerRepo := postgres.NewLedgerRepo(pool)

	pixService := service.NewPixService(pool, pixRepo, cobRepo, pspClient, outboxWriter)
	cobService := service.NewCobService(pool, cobRepo, pspClient, outboxWriter, idempotencyRepo, ledgerRepo)
	webhookService := service.NewWebhookService(pool, webhookRepo, pixRepo, cobRepo, pixService, pspClient, outboxWriter, ledgerRepo)

	healthHandler := handler.NewHealthHandler(pool, pspClient)
	cobHandler := handler.NewCobHandler(cobService)
	pixHandler := handler.NewPixHandler(pixService)
	webhookHandler := handler.NewWebhookHandler(webhookService)

	rateLimiter := security.NewRateLimiter(10, 20)
	go rateLimiter.Cleanup(ctx, 5*time.Minute)

	router := httpserver.SetupRouter(httpserver.RouterConfig{
		HealthHandler:   healthHandler,
		CobHandler:      cobHandler,
		PixHandler:      pixHandler,
		WebhookHandler:  webhookHandler,
		IdempotencyRepo: idempotencyRepo,
		RateLimiter:     rateLimiter,
	})

	outboxPublisher := worker.NewOutboxPublisher(pool, outboxReader, cfg.Worker.OutboxPollInterval)
	outboxPublisher.RegisterHandler("CobrancaCriada", worker.CobrancaCriadaHandler)
	outboxPublisher.RegisterHandler("CobrancaAtualizada", worker.CobrancaAtualizadaHandler)
	outboxPublisher.RegisterHandler("PixRecebido", worker.PixRecebidoHandler)
	outboxPublisher.RegisterHandler("DevolucaoSolicitada", func(ctx context.Context, msg postgres.OutboxMessage) error {
		var dev domain.Devolucao
		if err := json.Unmarshal(msg.Payload, &dev); err != nil {
			return fmt.Errorf("deserializando devolução: %w", err)
		}
		_, err := pspClient.CreateDevolucao(ctx, dev.EndToEndID, dev.ID, fmt.Sprintf("%.2f", float64(dev.Valor)/100.0))
		if err != nil {
			return fmt.Errorf("psp devolução: %w", err)
		}
		return nil
	})
	outboxPublisher.RegisterHandler("WebhookConfigurado", func(ctx context.Context, msg postgres.OutboxMessage) error {
		var wc domain.WebhookConfig
		if err := json.Unmarshal(msg.Payload, &wc); err != nil {
			return fmt.Errorf("deserializando webhook: %w", err)
		}
		if err := pspClient.ConfigureWebhook(ctx, wc.Chave, wc.WebhookURL); err != nil {
			return fmt.Errorf("psp configure webhook: %w", err)
		}
		return nil
	})

	instanceID := os.Getenv("INSTANCE_ID")
	if instanceID == "" {
		instanceID = fmt.Sprintf("swiftpay-%s", uuid.New().String()[:8])
	}
	leaderElection := worker.NewLeaderElection(pool, instanceID)

	workerCtx, cancelWorkers := context.WithCancel(ctx)
	defer cancelWorkers()

	var workerWg sync.WaitGroup

	workerWg.Add(1)
	go func() {
		defer workerWg.Done()
		if err := outboxPublisher.Start(workerCtx); err != nil && err != context.Canceled {
			slog.ErrorContext(ctx, "outbox publisher parou com erro", "error", err)
		}
	}()

	workerWg.Add(1)
	go func() {
		defer workerWg.Done()
		cleanupWorker := worker.NewCleanupWorker(pool, outboxReader, idempotencyRepo, 1*time.Hour, 7)
		if err := cleanupWorker.Start(workerCtx); err != nil && err != context.Canceled {
			slog.ErrorContext(ctx, "cleanup worker parou com erro", "error", err)
		}
	}()

	reconciliationWorker := worker.NewReconciliationWorker(pool, pixRepo, pspClient, leaderElection, 10)
	if err := reconciliationWorker.Start(workerCtx, cfg.Worker.ReconciliationSchedule); err != nil {
		slog.ErrorContext(ctx, "erro iniciando reconciliation worker", "error", err)
	}

	server := httpserver.NewServer(cfg.Server.Port, router)
	if err := server.Start(ctx); err != nil {
		slog.ErrorContext(ctx, "erro no servidor", "error", err)
		os.Exit(1)
	}

	slog.InfoContext(ctx, "iniciando shutdown de workers")
	cancelWorkers()

	done := make(chan struct{})
	go func() {
		workerWg.Wait()
		close(done)
	}()

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer shutdownCancel()

	select {
	case <-done:
		slog.InfoContext(ctx, "todos os workers pararam gracefulmente")
	case <-shutdownCtx.Done():
		slog.ErrorContext(ctx, "shutdown forçado por timeout — workers não finalizaram a tempo")
	}
}

func runMigrations(pool *pgxpool.Pool) error {
	migrationsPath, err := findMigrationsPath()
	if err != nil {
		return err
	}

	conn := pool.Config().ConnConfig
	dsn := "postgres://" + conn.User + ":" + conn.Password + "@" + conn.Host + ":" + strconv.FormatUint(uint64(conn.Port), 10) + "/" + conn.Database + "?sslmode=disable"

	m, err := migrate.New(
		"file://"+migrationsPath,
		dsn,
	)
	if err != nil {
		return err
	}

	if err := m.Up(); err != nil && err != migrate.ErrNoChange {
		return err
	}

	slog.InfoContext(context.Background(), "migrations executadas com sucesso")
	return nil
}

func findMigrationsPath() (string, error) {
	dir, err := os.Getwd()
	if err != nil {
		return "", err
	}

	for {
		candidate := filepath.Join(dir, "internal", "store", "postgres", "migrations")
		if info, err := os.Stat(candidate); err == nil && info.IsDir() {
			return candidate, nil
		}

		parent := filepath.Dir(dir)
		if parent == dir {
			break
		}
		dir = parent
	}

	return "", errors.New("diretório de migrations não encontrado")
}
