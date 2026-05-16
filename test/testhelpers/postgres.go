package testhelpers

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

type PostgresContainer struct {
	Container testcontainers.Container
	Pool      *pgxpool.Pool
	DSN       string
}

func SetupPostgres(ctx context.Context) (*PostgresContainer, error) {
	req := testcontainers.ContainerRequest{
		Image:        "postgres:16-alpine",
		ExposedPorts: []string{"5432/tcp"},
		Env: map[string]string{
			"POSTGRES_USER":     "pix",
			"POSTGRES_PASSWORD": "pix",
			"POSTGRES_DB":       "pix_api",
		},
		WaitingFor: wait.ForListeningPort("5432/tcp").WithStartupTimeout(60 * time.Second),
	}

	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		return nil, fmt.Errorf("criando container postgres: %w", err)
	}

	host, err := container.Host(ctx)
	if err != nil {
		container.Terminate(ctx)
		return nil, fmt.Errorf("obtendo host: %w", err)
	}

	port, err := container.MappedPort(ctx, "5432")
	if err != nil {
		container.Terminate(ctx)
		return nil, fmt.Errorf("obtendo porta: %w", err)
	}

	dsn := fmt.Sprintf("postgres://pix:pix@%s:%s/pix_api?sslmode=disable", host, port.Port())

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		container.Terminate(ctx)
		return nil, fmt.Errorf("criando pool: %w", err)
	}

	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		container.Terminate(ctx)
		return nil, fmt.Errorf("ping banco: %w", err)
	}

	return &PostgresContainer{
		Container: container,
		Pool:      pool,
		DSN:       dsn,
	}, nil
}

func (pc *PostgresContainer) Cleanup(ctx context.Context) {
	pc.Pool.Close()
	if err := pc.Container.Terminate(ctx); err != nil {
	}
}
