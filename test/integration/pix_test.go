//go:build integration

package integration

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/matspectrum/swiftpay-api/internal/domain"
	"github.com/matspectrum/swiftpay-api/internal/port/psp/mock"
	"github.com/matspectrum/swiftpay-api/internal/service"
	"github.com/matspectrum/swiftpay-api/internal/store/postgres"
	"github.com/matspectrum/swiftpay-api/test/testhelpers"
)

func TestGetPix(t *testing.T) {
	ctx := context.Background()
	pc, err := testhelpers.SetupPostgres(ctx)
	require.NoError(t, err)
	defer pc.Cleanup(ctx)

	require.NoError(t, testhelpers.RunMigrations(ctx, pc.DSN))

	pixRepo := postgres.NewPixRepo(pc.Pool)
	cobRepo := postgres.NewCobRepo(pc.Pool)
	pspClient := mock.NewMockPSP()
	outboxWriter := postgres.NewOutboxWriter(pc.Pool)
	svc := service.NewPixService(pc.Pool, pixRepo, cobRepo, pspClient, outboxWriter)

	pix := &domain.PixRecebido{
		E2EID: "E90400888202305231234ABCDEFG12345",
		Chave: "matspectrum@gmail.com",
		Valor: "100.00",
	}

	err = svc.ProcessPixRecebido(ctx, pix)
	require.NoError(t, err)

	result, err := svc.GetPix(ctx, pix.E2EID)
	require.NoError(t, err)
	assert.Equal(t, pix.E2EID, result.E2EID)
	assert.Equal(t, "100.00", result.Valor)
}

func TestListPix(t *testing.T) {
	ctx := context.Background()
	pc, err := testhelpers.SetupPostgres(ctx)
	require.NoError(t, err)
	defer pc.Cleanup(ctx)

	require.NoError(t, testhelpers.RunMigrations(ctx, pc.DSN))

	pixRepo := postgres.NewPixRepo(pc.Pool)
	cobRepo := postgres.NewCobRepo(pc.Pool)
	pspClient := mock.NewMockPSP()
	outboxWriter := postgres.NewOutboxWriter(pc.Pool)
	svc := service.NewPixService(pc.Pool, pixRepo, cobRepo, pspClient, outboxWriter)

	pix1 := &domain.PixRecebido{
		E2EID: "E0000000000123456789012345678901",
		Chave: "matspectrum@gmail.com",
		Valor: "50.00",
	}
	pix2 := &domain.PixRecebido{
		E2EID: "E0000000000123456789012345678902",
		Chave: "outro@exemplo.com",
		Valor: "75.00",
	}

	require.NoError(t, svc.ProcessPixRecebido(ctx, pix1))
	require.NoError(t, svc.ProcessPixRecebido(ctx, pix2))

	pixs, total, err := svc.ListPix(ctx, domain.PixFilter{Limit: 10})
	require.NoError(t, err)
	assert.Equal(t, 2, total)
	assert.Len(t, pixs, 2)
}

func TestCreateDevolucao(t *testing.T) {
	ctx := context.Background()
	pc, err := testhelpers.SetupPostgres(ctx)
	require.NoError(t, err)
	defer pc.Cleanup(ctx)

	require.NoError(t, testhelpers.RunMigrations(ctx, pc.DSN))

	pixRepo := postgres.NewPixRepo(pc.Pool)
	cobRepo := postgres.NewCobRepo(pc.Pool)
	pspClient := mock.NewMockPSP()
	outboxWriter := postgres.NewOutboxWriter(pc.Pool)

	pspClient.PixStore().AddPix("E0000000000123456789012345678999", "", "200.00")

	svc := service.NewPixService(pc.Pool, pixRepo, cobRepo, pspClient, outboxWriter)

	pix := &domain.PixRecebido{
		E2EID: "E0000000000123456789012345678999",
		Chave: "matspectrum@gmail.com",
		Valor: "200.00",
	}
	require.NoError(t, svc.ProcessPixRecebido(ctx, pix))

	dev, err := svc.CreateDevolucao(ctx, "E0000000000123456789012345678999", "DEV001", "100.00")
	require.NoError(t, err)
	assert.Equal(t, "100.00", dev.Valor)
	assert.NotEmpty(t, dev.ID)
}
