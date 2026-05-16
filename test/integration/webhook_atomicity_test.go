//go:build integration

package integration

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/matspectrum/swiftpay-api/internal/port/psp/mock"
	"github.com/matspectrum/swiftpay-api/internal/service"
	"github.com/matspectrum/swiftpay-api/internal/store/postgres"
	"github.com/matspectrum/swiftpay-api/test/testhelpers"
)

func TestWebhookCallbackAtomicity(t *testing.T) {
	ctx := context.Background()
	pc, err := testhelpers.SetupPostgres(ctx)
	require.NoError(t, err)
	defer pc.Cleanup(ctx)
	require.NoError(t, testhelpers.RunMigrations(ctx, pc.DSN))

	cobRepo := postgres.NewCobRepo(pc.Pool)
	pixRepo := postgres.NewPixRepo(pc.Pool)
	webhookRepo := postgres.NewWebhookRepo(pc.Pool)
	outboxWriter := postgres.NewOutboxWriter(pc.Pool)
	pspClient := mock.NewMockPSP()

	pixService := service.NewPixService(pc.Pool, pixRepo, cobRepo, pspClient, outboxWriter)
	webhookService := service.NewWebhookService(pc.Pool, webhookRepo, pixRepo, cobRepo, pixService, pspClient, outboxWriter, nil)

	payload := []byte(`{"e2eid":"E1234567890123456789012345678901","txid":"test12345678901234567890123456","chave":"test@email.com","valor":"10.00","horario":"2024-01-01T00:00:00Z"}`)

	err = webhookService.HandleCallback(ctx, payload)
	require.NoError(t, err)

	pix, err := pixRepo.GetByE2EID(ctx, "E1234567890123456789012345678901")
	require.NoError(t, err)
	assert.NotNil(t, pix)
	assert.Equal(t, "10.00", pix.Valor)

	var eventCount int
	err = pc.Pool.QueryRow(ctx,
		`SELECT COUNT(*) FROM webhook_events WHERE e2eid = $1`,
		"E1234567890123456789012345678901",
	).Scan(&eventCount)
	require.NoError(t, err)
	assert.Equal(t, 1, eventCount, "webhook_events deve ter exatamente 1 entrada")
}
