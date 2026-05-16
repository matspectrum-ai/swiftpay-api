// Package postgres fornece acesso ao banco de dados PostgreSQL via pgxpool.
package postgres

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/tracelog"
)

// NewPool cria um pool de conexões PostgreSQL.
func NewPool(ctx context.Context, dsn string, maxOpen, maxIdle int, maxLifetime, maxIdleTime time.Duration) (*pgxpool.Pool, error) {
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("parse config: %w", err)
	}

	cfg.MaxConns = int32(maxOpen)
	cfg.MinConns = int32(maxIdle)
	cfg.MaxConnLifetime = maxLifetime
	cfg.MaxConnIdleTime = maxIdleTime

	cfg.ConnConfig.Tracer = &tracelog.TraceLog{
		Logger:   &slogAdapter{logger: slog.Default()},
		LogLevel: tracelog.LogLevelWarn,
	}

	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("criando pool: %w", err)
	}

	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping database: %w", err)
	}

	slog.InfoContext(ctx, "conexão com PostgreSQL estabelecida",
		"max_open_conns", maxOpen,
		"max_idle_conns", maxIdle,
	)

	return pool, nil
}

type slogAdapter struct {
	logger *slog.Logger
}

func (a *slogAdapter) Log(ctx context.Context, level tracelog.LogLevel, msg string, data map[string]interface{}) {
	attrs := make([]slog.Attr, 0, len(data))
	for k, v := range data {
		attrs = append(attrs, slog.Any(k, v))
	}
	switch level {
	case tracelog.LogLevelError:
		a.logger.LogAttrs(ctx, slog.LevelError, msg, attrs...)
	case tracelog.LogLevelWarn:
		a.logger.LogAttrs(ctx, slog.LevelWarn, msg, attrs...)
	case tracelog.LogLevelInfo:
		a.logger.LogAttrs(ctx, slog.LevelInfo, msg, attrs...)
	default:
		a.logger.LogAttrs(ctx, slog.LevelDebug, msg, attrs...)
	}
}
