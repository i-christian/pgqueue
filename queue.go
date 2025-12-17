package pgqueue

import (
	"context"
	"database/sql"
	_ "embed"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/lib/pq"
	"github.com/robfig/cron/v3"
)

//go:embed migrations/*
var schemaSQL string

// NewQueue initializes the queue, runs migrations, and starts internal maintenance.
func NewQueue(db *sql.DB, connString string, logger *slog.Logger, opts ...QueueOption) (*Queue, *Metrics, error) {
	cfg := defaultQueueConfig()
	for _, opt := range opts {
		opt(&cfg)
	}

	ctx, cancel := context.WithCancel(context.Background())

	q := &Queue{
		db:         db,
		connString: connString,
		logger:     logger,
		scheduler:  cron.New(cron.WithChain(cron.Recover(cron.DefaultLogger))),
		ctx:        ctx,
		cancel:     cancel,
		config:     cfg,
	}

	migrateCtx, migrateCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer migrateCancel()
	if err := q.migrate(migrateCtx); err != nil {
		cancel()
		return nil, nil, err
	}

	q.scheduler.Start()

	q.wg.Add(1)
	go q.runMaintenanceLoop()

	return q, NewMetrics(), nil
}

func (q *Queue) migrate(ctx context.Context) error {
	_, err := q.db.ExecContext(ctx, schemaSQL)
	return err
}

// runMaintenanceLoop handles all background system jobs
func (q *Queue) runMaintenanceLoop() {
	defer q.wg.Done()

	var rescueTicker *time.Ticker
	var cleanupTicker *time.Ticker

	if q.config.rescueEnabled {
		rescueTicker = time.NewTicker(q.config.rescueInterval)
		q.logger.Info("Internal Rescue started", "interval", q.config.rescueInterval.Minutes())
	} else {
		rescueTicker = time.NewTicker(24 * time.Hour)
		rescueTicker.Stop()
	}

	if q.config.cleanupEnabled {
		cleanupTicker = time.NewTicker(q.config.cleanupInterval)
		q.logger.Info("Internal Cleanup started", "interval", q.config.cleanupInterval.Hours(), "strategy", q.config.cleanupStrategy)
	} else {
		cleanupTicker = time.NewTicker(24 * time.Hour)
		cleanupTicker.Stop()
	}

	defer func() {
		if rescueTicker != nil {
			rescueTicker.Stop()
		}
		if cleanupTicker != nil {
			cleanupTicker.Stop()
		}
	}()

	for {
		select {
		case <-q.ctx.Done():
			return

		case <-rescueTicker.C:
			count, err := q.RescueStuckTasks(q.ctx, q.config.rescueVisibility)
			if err != nil {
				q.logger.Error("Rescue failed", "error", err)
			} else if count > 0 {
				q.logger.Info("Rescued stuck tasks", "count", count)
			}

		case <-cleanupTicker.C:
			if err := q.runCleanup(q.ctx); err != nil {
				q.logger.Error("Cleanup failed", "error", err)
			}
		}
	}
}

// Enqueue adds a job to the queue
func (q *Queue) Enqueue(ctx context.Context, task TaskType, payload any, opts ...EnqueueOption) error {
	cfg := enqueueConfig{
		processAt:  nil,
		dedupKey:   nil,
		priority:   DefaultPriority,
		maxRetries: 5,
	}
	for _, opt := range opts {
		opt(&cfg)
	}

	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("failed to marshal payload: %w", err)
	}

	query := `INSERT INTO tasks (task_type, priority, payload, next_run_at, deduplication_key) VALUES ($1, $2, $3, $4, $5)`

	var args []any
	args = append(args, task, cfg.priority, payloadBytes)

	var nextRunAt sql.NullTime
	if cfg.processAt != nil {
		nextRunAt = sql.NullTime{Time: *cfg.processAt, Valid: true}
	} else {
		nextRunAt = sql.NullTime{Time: time.Now(), Valid: true}
	}

	var dedupKey sql.NullString
	if cfg.dedupKey != nil {
		dedupKey = sql.NullString{String: *cfg.dedupKey, Valid: true}
	}

	args = append(args, nextRunAt, dedupKey)

	_, err = q.db.ExecContext(ctx, query, args...)
	if err != nil {
		if pqErr, ok := err.(*pq.Error); ok && pqErr.Code == "23505" {
			return nil
		}
		return err
	}
	return nil
}

// RescueStuckTasks finds tasks that have been 'processing' for too long
// and resets them to 'pending' so they can be picked up again.
func (q *Queue) RescueStuckTasks(ctx context.Context, timeout time.Duration) (int64, error) {
	query := `
        UPDATE tasks
        SET status = 'pending',
            updated_at = NOW(),
            next_run_at = NOW(),
            attempts = attempts + 1,
            last_error = 'detected stuck task; resetting'
        WHERE status = 'processing'
          AND updated_at < NOW() - ($1 * INTERVAL '1 seconds')
          AND attempts < max_retries
    `
	res, err := q.db.ExecContext(ctx, query, timeout.Seconds())
	if err != nil {
		return 0, err
	}

	return res.RowsAffected()
}
