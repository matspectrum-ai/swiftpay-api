package worker

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

const leaderLockID = 0x5377696674506179

type LeaderEpoch struct {
	InstanceID string
	Epoch      int64
	AcquiredAt time.Time
	ExpiresAt  time.Time
}

type LeaderElection struct {
	db         *pgxpool.Pool
	instanceID string
	current    *LeaderEpoch
}

func NewLeaderElection(db *pgxpool.Pool, instanceID string) *LeaderElection {
	return &LeaderElection{db: db, instanceID: instanceID}
}

func (l *LeaderElection) TryAcquire(ctx context.Context) (*LeaderEpoch, error) {
	var epoch LeaderEpoch
	err := l.db.QueryRow(ctx,
		`INSERT INTO leader_leases (lock_id, instance_id, epoch, acquired_at, expires_at)
		 VALUES ($1, $2, COALESCE((SELECT MAX(epoch) FROM leader_leases WHERE lock_id = $1), 0) + 1, NOW(), NOW() + INTERVAL '30 seconds')
		 ON CONFLICT (lock_id) DO UPDATE
		 SET instance_id = $2, epoch = leader_leases.epoch + 1, acquired_at = NOW(), expires_at = NOW() + INTERVAL '30 seconds'
		 WHERE leader_leases.expires_at < NOW()
		 RETURNING instance_id, epoch, acquired_at, expires_at`,
		leaderLockID, l.instanceID,
	).Scan(&epoch.InstanceID, &epoch.Epoch, &epoch.AcquiredAt, &epoch.ExpiresAt)
	if err != nil {
		return nil, fmt.Errorf("acquiring leader lock: %w", err)
	}
	l.current = &epoch
	slog.InfoContext(ctx, "liderança adquirida",
		"instance_id", epoch.InstanceID,
		"epoch", epoch.Epoch,
	)
	return &epoch, nil
}

func (l *LeaderElection) RenewHeartbeat(ctx context.Context) error {
	if l.current == nil {
		return fmt.Errorf("not leader")
	}
	_, err := l.db.Exec(ctx,
		`UPDATE leader_leases SET expires_at = NOW() + INTERVAL '30 seconds'
		 WHERE lock_id = $1 AND instance_id = $2 AND epoch = $3`,
		leaderLockID, l.instanceID, l.current.Epoch,
	)
	if err != nil {
		return fmt.Errorf("renewing leader heartbeat: %w", err)
	}
	return nil
}

func (l *LeaderElection) Release(ctx context.Context) error {
	if l.current == nil {
		return nil
	}
	_, err := l.db.Exec(ctx,
		`DELETE FROM leader_leases WHERE lock_id = $1 AND instance_id = $2 AND epoch = $3`,
		leaderLockID, l.instanceID, l.current.Epoch,
	)
	if err != nil {
		return fmt.Errorf("releasing leader lock: %w", err)
	}
	l.current = nil
	slog.InfoContext(ctx, "liderança liberada", "instance_id", l.instanceID)
	return nil
}

func (l *LeaderElection) IsCurrentLeader(ctx context.Context) bool {
	if l.current == nil {
		return false
	}
	var instanceID string
	err := l.db.QueryRow(ctx,
		`SELECT instance_id FROM leader_leases
		 WHERE lock_id = $1 AND epoch = $2 AND expires_at > NOW()`,
		leaderLockID, l.current.Epoch,
	).Scan(&instanceID)
	return err == nil && instanceID == l.instanceID
}

func (l *LeaderElection) RunAsLeader(ctx context.Context, heartbeat time.Duration, work func(context.Context) error) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		epoch, err := l.TryAcquire(ctx)
		if err != nil || epoch == nil {
			time.Sleep(heartbeat)
			continue
		}
		defer l.Release(ctx)

		ctx2, cancel := context.WithCancel(ctx)
		defer cancel()
		go func() {
			ticker := time.NewTicker(heartbeat)
			defer ticker.Stop()
			for {
				select {
				case <-ctx2.Done():
					return
				case <-ticker.C:
					if err := l.RenewHeartbeat(ctx2); err != nil {
						slog.ErrorContext(ctx2, "falha ao renovar heartbeat", "error", err)
						cancel()
						return
					}
				}
			}
		}()

		err = work(ctx2)
		if err != nil {
			slog.ErrorContext(ctx, "work falhou", "error", err)
		}
		return err
	}
}
