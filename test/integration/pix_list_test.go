//go:build integration

package integration

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/matspectrum/swiftpay-api/internal/domain"
	"github.com/matspectrum/swiftpay-api/internal/store/postgres"
	"github.com/matspectrum/swiftpay-api/test/testhelpers"
)

func TestPixRepoList(t *testing.T) {
	ctx := context.Background()
	pc, err := testhelpers.SetupPostgres(ctx)
	require.NoError(t, err)
	defer pc.Cleanup(ctx)
	require.NoError(t, testhelpers.RunMigrations(ctx, pc.DSN))

	repo := postgres.NewPixRepo(pc.Pool)

	_, total, err := repo.List(ctx, domain.PixFilter{Limit: 10})
	require.NoError(t, err)
	assert.Equal(t, 0, total)

	_, total, err = repo.List(ctx, domain.PixFilter{
		Inicio: time.Now().Add(-24 * time.Hour),
		Fim:    time.Now(),
		Limit:  10,
		TxID:   "test12345678901234567890123456",
		Chave:  "test@email.com",
	})
	require.NoError(t, err)
}
