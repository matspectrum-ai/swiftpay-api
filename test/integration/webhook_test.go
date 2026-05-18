//go:build integration

package integration

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/matspectrum/swiftpay-api/internal/domain"
	"github.com/matspectrum/swiftpay-api/internal/port/psp/mock"
	"github.com/matspectrum/swiftpay-api/internal/service"
	"github.com/matspectrum/swiftpay-api/internal/store/postgres"
	"github.com/matspectrum/swiftpay-api/test/testhelpers"
)

func TestConfigureWebhook(t *testing.T) {
	ctx := context.Background()
	pc, err := testhelpers.SetupPostgres(ctx)
	require.NoError(t, err)
	defer pc.Cleanup(ctx)

	require.NoError(t, testhelpers.RunMigrations(ctx, pc.DSN))

	webhookRepo := postgres.NewWebhookRepo(pc.Pool)
	pixRepo := postgres.NewPixRepo(pc.Pool)
	cobRepo := postgres.NewCobRepo(pc.Pool)
	pspClient := mock.NewMockPSP()
	outboxWriter := postgres.NewOutboxWriter(pc.Pool)
	pixService := service.NewPixService(pc.Pool, pixRepo, cobRepo, pspClient, outboxWriter)
	svc := service.NewWebhookService(pc.Pool, webhookRepo, pixRepo, cobRepo, pixService, pspClient, outboxWriter, nil)

	wc, err := svc.ConfigureWebhook(ctx, "matspectrum@gmail.com", "https://example.com/webhook/callback")
	require.NoError(t, err)
	assert.Equal(t, "matspectrum@gmail.com", wc.Chave)
	assert.Equal(t, "ATIVO", wc.Status)
}

func TestHandleCallback(t *testing.T) {
	ctx := context.Background()
	pc, err := testhelpers.SetupPostgres(ctx)
	require.NoError(t, err)
	defer pc.Cleanup(ctx)

	require.NoError(t, testhelpers.RunMigrations(ctx, pc.DSN))

	webhookRepo := postgres.NewWebhookRepo(pc.Pool)
	pixRepo := postgres.NewPixRepo(pc.Pool)
	cobRepo := postgres.NewCobRepo(pc.Pool)
	pspClient := mock.NewMockPSP()
	outboxWriter := postgres.NewOutboxWriter(pc.Pool)
	pixService := service.NewPixService(pc.Pool, pixRepo, cobRepo, pspClient, outboxWriter)
	svc := service.NewWebhookService(pc.Pool, webhookRepo, pixRepo, cobRepo, pixService, pspClient, outboxWriter, nil)

	payload := domain.WebhookPayload{
		EndToEndID: "E90400888202305231WEBHOOK0012345",
		Chave: "matspectrum@gmail.com",
		Valor: "150.00",
	}

	payloadBytes, err := json.Marshal(payload)
	require.NoError(t, err)

	err = svc.HandleCallback(ctx, payloadBytes)
	require.NoError(t, err)

	pix, err := pixService.GetPix(ctx, payload.EndToEndID)
	require.NoError(t, err)
	assert.Equal(t, domain.ValorCentavos(15000), pix.ValorCentavos)
}

func TestHandleCallbackDuplicate(t *testing.T) {
	ctx := context.Background()
	pc, err := testhelpers.SetupPostgres(ctx)
	require.NoError(t, err)
	defer pc.Cleanup(ctx)

	require.NoError(t, testhelpers.RunMigrations(ctx, pc.DSN))

	webhookRepo := postgres.NewWebhookRepo(pc.Pool)
	pixRepo := postgres.NewPixRepo(pc.Pool)
	cobRepo := postgres.NewCobRepo(pc.Pool)
	pspClient := mock.NewMockPSP()
	outboxWriter := postgres.NewOutboxWriter(pc.Pool)
	pixService := service.NewPixService(pc.Pool, pixRepo, cobRepo, pspClient, outboxWriter)
	svc := service.NewWebhookService(pc.Pool, webhookRepo, pixRepo, cobRepo, pixService, pspClient, outboxWriter, nil)

	payload := domain.WebhookPayload{
		EndToEndID: "E90400888202305231DEDUP12345678",
		Chave: "matspectrum@gmail.com",
		Valor: "200.00",
	}

	payloadBytes, err := json.Marshal(payload)
	require.NoError(t, err)

	err = svc.HandleCallback(ctx, payloadBytes)
	require.NoError(t, err)

	err = svc.HandleCallback(ctx, payloadBytes)
	require.NoError(t, err)

	pixs, total, err := pixService.ListPix(ctx, domain.PixFilter{Limit: 10})
	require.NoError(t, err)
	assert.Equal(t, 1, total)
	assert.Len(t, pixs, 1)
}
