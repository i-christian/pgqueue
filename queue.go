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

// NewQueue initializes the queue, a scheduler and runs migrations automatically
func NewQueue(db *sql.DB, connString string, logger *slog.Logger) (*Queue, *Metrics, error) {
	ctx, cancel := context.WithCancel(context.Background())

	queue := &Queue{
		db:         db,
		connString: connString,
		scheduler:  cron.New(cron.WithChain(cron.Recover(cron.DefaultLogger))),
		ctx:        ctx,
		cancel:     cancel,
	}

	migrateCtx, migrateCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer migrateCancel()

	if err := queue.migrate(migrateCtx); err != nil {
		return nil, nil, err
	}

	queue.scheduler.Start()

	return queue, NewMetrics(), nil
}

func (q *Queue) migrate(ctx context.Context) error {
	_, err := q.db.ExecContext(ctx, schemaSQL)
	return err
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
