//go:build integration

package integration

import (
	"context"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/matspectrum/swiftpay-api/internal/store/postgres"
	"github.com/matspectrum/swiftpay-api/test/testhelpers"
)

func TestOutboxExclusiveClaiming(t *testing.T) {
	ctx := context.Background()
	pc, err := testhelpers.SetupPostgres(ctx)
	require.NoError(t, err)
	defer pc.Cleanup(ctx)
	require.NoError(t, testhelpers.RunMigrations(ctx, pc.DSN))

	writer := postgres.NewOutboxWriter(pc.Pool)

	tx, err := pc.Pool.Begin(ctx)
	require.NoError(t, err)
	err = writer.Write(ctx, tx, "pix", "E1234567890123456789012345678901", "PixRecebido", map[string]string{"e2eid": "E1234567890123456789012345678901"})
	require.NoError(t, err)
	err = tx.Commit(ctx)
	require.NoError(t, err)

	reader := postgres.NewOutboxReader(pc.Pool)

	var mu sync.Mutex
	var claimed int
	var wg sync.WaitGroup

	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			tx, err := pc.Pool.Begin(ctx)
			if err != nil {
				return
			}
			defer tx.Rollback(ctx)

			msgs, err := reader.FetchPendingTx(ctx, tx, 1)
			if err != nil {
				return
			}
			if len(msgs) > 0 {
				mu.Lock()
				claimed++
				mu.Unlock()
			}
		}()
	}

	wg.Wait()

	assert.Equal(t, 1, claimed, "apenas 1 worker deve receber a mensagem com FOR UPDATE SKIP LOCKED")
}
