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

func TestOutboxWriteOnCreate(t *testing.T) {
	ctx := context.Background()
	pc, err := testhelpers.SetupPostgres(ctx)
	require.NoError(t, err)
	defer pc.Cleanup(ctx)

	require.NoError(t, testhelpers.RunMigrations(ctx, pc.DSN))

	cobRepo := postgres.NewCobRepo(pc.Pool)
	pspClient := mock.NewMockPSP()
	outboxWriter := postgres.NewOutboxWriter(pc.Pool)
	idempotencyRepo := postgres.NewIdempotencyRepo(pc.Pool)
	outboxReader := postgres.NewOutboxReader(pc.Pool)
	svc := service.NewCobService(pc.Pool, cobRepo, pspClient, outboxWriter, idempotencyRepo, nil)

	cob := &domain.Cobranca{
		TxID:  "outb1outb2outb3outb4outb5outb678",
		Chave: "matspectrum@gmail.com",
		Valor: domain.Valor{Original: 1000},
		Devedor: domain.Devedor{
			Nome: "João Silva",
			CPF:  "01234567890",
		},
		Calendar: domain.Calendar{Expiracao: 86400},
	}

	_, err = svc.CreateCob(ctx, cob)
	require.NoError(t, err)

	messages, err := outboxReader.FetchPending(ctx, 10)
	require.NoError(t, err)
	assert.GreaterOrEqual(t, len(messages), 1, "deve haver pelo menos uma mensagem no outbox")

	found := false
	for _, msg := range messages {
		if msg.EventType == "CobrancaCriada" && msg.AggregateID == cob.TxID {
			found = true
			break
		}
	}
	assert.True(t, found, "deve existir uma mensagem CobrancaCriada no outbox")
}

func TestOutboxPublished(t *testing.T) {
	ctx := context.Background()
	pc, err := testhelpers.SetupPostgres(ctx)
	require.NoError(t, err)
	defer pc.Cleanup(ctx)

	require.NoError(t, testhelpers.RunMigrations(ctx, pc.DSN))

	cobRepo := postgres.NewCobRepo(pc.Pool)
	pspClient := mock.NewMockPSP()
	outboxWriter := postgres.NewOutboxWriter(pc.Pool)
	idempotencyRepo := postgres.NewIdempotencyRepo(pc.Pool)
	outboxReader := postgres.NewOutboxReader(pc.Pool)
	svc := service.NewCobService(pc.Pool, cobRepo, pspClient, outboxWriter, idempotencyRepo, nil)

	cob := &domain.Cobranca{
		TxID:  "publ1publ2publ3publ4publ5publ678",
		Chave: "matspectrum@gmail.com",
		Valor: domain.Valor{Original: 3000},
		Devedor: domain.Devedor{
			Nome: "Ana Costa",
			CPF:  "55566677788",
		},
		Calendar: domain.Calendar{Expiracao: 86400},
	}

	_, err = svc.CreateCob(ctx, cob)
	require.NoError(t, err)

	messages, err := outboxReader.FetchPending(ctx, 10)
	require.NoError(t, err)
	assert.NotEmpty(t, messages)

	for _, msg := range messages {
		err = outboxReader.MarkPublished(ctx, msg.ID)
		require.NoError(t, err)
	}

	remaining, err := outboxReader.FetchPending(ctx, 10)
	require.NoError(t, err)
	for _, msg := range remaining {
		assert.NotEqual(t, cob.TxID, msg.AggregateID, "mensagem publicada não deve aparecer como pendente")
	}
}
