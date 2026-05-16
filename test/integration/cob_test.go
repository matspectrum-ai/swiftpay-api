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

func TestCreateCob(t *testing.T) {
	ctx := context.Background()
	pc, err := testhelpers.SetupPostgres(ctx)
	require.NoError(t, err)
	defer pc.Cleanup(ctx)

	require.NoError(t, testhelpers.RunMigrations(ctx, pc.DSN))

	cobRepo := postgres.NewCobRepo(pc.Pool)
	pspClient := mock.NewMockPSP()
	outboxWriter := postgres.NewOutboxWriter(pc.Pool)
	idempotencyRepo := postgres.NewIdempotencyRepo(pc.Pool)
	svc := service.NewCobService(pc.Pool, cobRepo, pspClient, outboxWriter, idempotencyRepo, nil)

	cob := &domain.Cobranca{
		TxID:  "a1b2c3d4e5f6g7h8i9j0k1l2m3n4o5p6",
		Chave: "matspectrum@gmail.com",
		Valor: domain.Valor{Original: "10.00"},
		Devedor: domain.Devedor{
			Nome: "João Silva",
			CPF:  "01234567890",
		},
		Calendar: domain.Calendar{Expiracao: 86400},
	}

	result, err := svc.CreateCob(ctx, cob)
	require.NoError(t, err)
	assert.NotNil(t, result)
	assert.Equal(t, domain.CobStatusAtiva, result.Status)
	assert.NotEmpty(t, result.Location)
	assert.NotEmpty(t, result.PixCopiaECola)
}

func TestCreateCobDuplicate(t *testing.T) {
	ctx := context.Background()
	pc, err := testhelpers.SetupPostgres(ctx)
	require.NoError(t, err)
	defer pc.Cleanup(ctx)

	require.NoError(t, testhelpers.RunMigrations(ctx, pc.DSN))

	cobRepo := postgres.NewCobRepo(pc.Pool)
	pspClient := mock.NewMockPSP()
	outboxWriter := postgres.NewOutboxWriter(pc.Pool)
	idempotencyRepo := postgres.NewIdempotencyRepo(pc.Pool)
	svc := service.NewCobService(pc.Pool, cobRepo, pspClient, outboxWriter, idempotencyRepo, nil)

	cob := &domain.Cobranca{
		TxID:  "dup1dup2dup3dup4dup5dup6dup789",
		Chave: "matspectrum@gmail.com",
		Valor: domain.Valor{Original: "10.00"},
		Devedor: domain.Devedor{
			Nome: "João Silva",
			CPF:  "01234567890",
		},
		Calendar: domain.Calendar{Expiracao: 86400},
	}

	result1, err := svc.CreateCob(ctx, cob)
	require.NoError(t, err)

	result2, err := svc.CreateCob(ctx, cob)
	require.NoError(t, err)
	assert.Equal(t, result1.TxID, result2.TxID)
}

func TestGetCob(t *testing.T) {
	ctx := context.Background()
	pc, err := testhelpers.SetupPostgres(ctx)
	require.NoError(t, err)
	defer pc.Cleanup(ctx)

	require.NoError(t, testhelpers.RunMigrations(ctx, pc.DSN))

	cobRepo := postgres.NewCobRepo(pc.Pool)
	pspClient := mock.NewMockPSP()
	outboxWriter := postgres.NewOutboxWriter(pc.Pool)
	idempotencyRepo := postgres.NewIdempotencyRepo(pc.Pool)
	svc := service.NewCobService(pc.Pool, cobRepo, pspClient, outboxWriter, idempotencyRepo, nil)

	cob := &domain.Cobranca{
		TxID:  "get1get2get3get4get5get6get789",
		Chave: "matspectrum@gmail.com",
		Valor: domain.Valor{Original: "25.50"},
		Devedor: domain.Devedor{
			Nome: "Maria Souza",
			CPF:  "09876543210",
		},
		Calendar: domain.Calendar{Expiracao: 86400},
	}

	_, err = svc.CreateCob(ctx, cob)
	require.NoError(t, err)

	result, err := svc.GetCob(ctx, cob.TxID)
	require.NoError(t, err)
	assert.Equal(t, cob.TxID, result.TxID)
	assert.Equal(t, "25.50", result.Valor.Original)
}

func TestGetCobNotFound(t *testing.T) {
	ctx := context.Background()
	pc, err := testhelpers.SetupPostgres(ctx)
	require.NoError(t, err)
	defer pc.Cleanup(ctx)

	require.NoError(t, testhelpers.RunMigrations(ctx, pc.DSN))

	cobRepo := postgres.NewCobRepo(pc.Pool)
	pspClient := mock.NewMockPSP()
	outboxWriter := postgres.NewOutboxWriter(pc.Pool)
	idempotencyRepo := postgres.NewIdempotencyRepo(pc.Pool)
	svc := service.NewCobService(pc.Pool, cobRepo, pspClient, outboxWriter, idempotencyRepo, nil)

	_, err = svc.GetCob(ctx, "naoexistentetxid123456789012")
	require.Error(t, err)
	assert.Equal(t, domain.ErrCobrancaNaoEncontrada, err)
}

func TestPatchCob(t *testing.T) {
	ctx := context.Background()
	pc, err := testhelpers.SetupPostgres(ctx)
	require.NoError(t, err)
	defer pc.Cleanup(ctx)

	require.NoError(t, testhelpers.RunMigrations(ctx, pc.DSN))

	cobRepo := postgres.NewCobRepo(pc.Pool)
	pspClient := mock.NewMockPSP()
	outboxWriter := postgres.NewOutboxWriter(pc.Pool)
	idempotencyRepo := postgres.NewIdempotencyRepo(pc.Pool)
	svc := service.NewCobService(pc.Pool, cobRepo, pspClient, outboxWriter, idempotencyRepo, nil)

	cob := &domain.Cobranca{
		TxID:  "patchpatchpatchpatchpatch12345",
		Chave: "matspectrum@gmail.com",
		Valor: domain.Valor{Original: "50.00"},
		Devedor: domain.Devedor{
			Nome: "Carlos Lima",
			CPF:  "11122233344",
		},
		Calendar: domain.Calendar{Expiracao: 86400},
	}

	_, err = svc.CreateCob(ctx, cob)
	require.NoError(t, err)

	patch := &domain.CobrancaPatch{Status: domain.CobStatusRemovidaPeloUsuario}
	result, err := svc.PatchCob(ctx, cob.TxID, patch)
	require.NoError(t, err)
	assert.Equal(t, domain.CobStatusRemovidaPeloUsuario, result.Status)
}
