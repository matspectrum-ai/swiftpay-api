//go:build integration

package integration

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/matspectrum/swiftpay-api/internal/domain"
	"github.com/matspectrum/swiftpay-api/internal/store/postgres"
	"github.com/matspectrum/swiftpay-api/test/testhelpers"
)

func TestIdempotentCreate(t *testing.T) {
	ctx := context.Background()
	pc, err := testhelpers.SetupPostgres(ctx)
	require.NoError(t, err)
	defer pc.Cleanup(ctx)

	require.NoError(t, testhelpers.RunMigrations(ctx, pc.DSN))

	repo := postgres.NewIdempotencyRepo(pc.Pool)

	key := "idem-test-001"
	path := "/cob/abc123"
	body := []byte(`{"valor":"10.00"}`)
	hash := sha256.Sum256(body)
	requestHash := hex.EncodeToString(hash[:])

	record, err := repo.Acquire(ctx, key, path, requestHash)
	require.NoError(t, err)
	assert.Equal(t, "in_progress", record.Status)

	record2, err := repo.Acquire(ctx, key, path, requestHash)
	require.NoError(t, err)
	assert.Equal(t, record.RequestHash, record2.RequestHash)

	err = repo.Complete(ctx, key, path, 201, body)
	require.NoError(t, err)

	record3, err := repo.Acquire(ctx, key, path, requestHash)
	require.NoError(t, err)
	assert.Equal(t, "completed", record3.Status)
}

func TestIdempotentPayloadDivergence(t *testing.T) {
	ctx := context.Background()
	pc, err := testhelpers.SetupPostgres(ctx)
	require.NoError(t, err)
	defer pc.Cleanup(ctx)

	require.NoError(t, testhelpers.RunMigrations(ctx, pc.DSN))

	repo := postgres.NewIdempotencyRepo(pc.Pool)

	key := "idem-test-002"
	path := "/cob/def456"
	body1 := []byte(`{"valor":"10.00"}`)
	body2 := []byte(`{"valor":"20.00"}`)

	hash1 := sha256.Sum256(body1)
	hash2 := sha256.Sum256(body2)

	_, err = repo.Acquire(ctx, key, path, hex.EncodeToString(hash1[:]))
	require.NoError(t, err)

	_, err = repo.Acquire(ctx, key, path, hex.EncodeToString(hash2[:]))
	require.Error(t, err)
	assert.Equal(t, domain.ErrIdempotencyKeyDiverged, err)
}
