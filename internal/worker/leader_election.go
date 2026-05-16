package worker

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

const leaderLockID = 0x5377696674506179

type LeaderElection struct {
	db       *pgxpool.Pool
	acquired bool
}

func NewLeaderElection(db *pgxpool.Pool) *LeaderElection {
	return &LeaderElection{db: db}
}

func (l *LeaderElection) TryAcquire(ctx context.Context) (bool, error) {
	var acquired bool
	err := l.db.QueryRow(ctx,
		`SELECT pg_try_advisory_lock($1)`, leaderLockID,
	).Scan(&acquired)
	if err != nil {
		return false, fmt.Errorf("acquiring leader lock: %w", err)
	}
	l.acquired = acquired
	if acquired {
		slog.InfoContext(ctx, "liderança adquirida")
	}
	return acquired, nil
}

func (l *LeaderElection) Release(ctx context.Context) error {
	if !l.acquired {
		return nil
	}
	_, err := l.db.Exec(ctx, `SELECT pg_advisory_unlock($1)`, leaderLockID)
	if err != nil {
		return fmt.Errorf("releasing leader lock: %w", err)
	}
	l.acquired = false
	slog.InfoContext(ctx, "liderança liberada")
	return nil
}

func (l *LeaderElection) RunAsLeader(ctx context.Context, heartbeat time.Duration, work func(context.Context) error) error {
	for {
		acquired, err := l.TryAcquire(ctx)
		if err != nil {
			return err
		}
		if acquired {
			defer l.Release(ctx)
			return work(ctx)
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(heartbeat):
		}
	}
}
