// Package pgqueue provides a lightweight, PostgreSQL-backed job queue for Go.
//
// It enables asynchronous background processing using PostgreSQL while offering safe concurrency, retries with backoff, delayed jobs, and cron scheduling.
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

// NewQueue returns a Queue and a Metric instance given:
//   - a postgres connection,
//   - connection string,
//   - slog.logger pointer.
//
// The parameter opts is optional, defaults will be used if opts is set to nil
func NewQueue(db *sql.DB, connString string, logger *slog.Logger, opts ...QueueOption) (*Queue, *Metrics, error) {
	cfg := defaultQueueConfig()
	for _, opt := range opts {
		opt(&cfg)
	}

	ctx, cancel := context.WithCancel(context.Background())

	var scheduler *cron.Cron
	if cfg.cronEnabled {
		scheduler = cron.New(
			cron.WithChain(cron.Recover(cron.DefaultLogger)),
		)
		scheduler.Start()
	}

	q := &Queue{
		db:         db,
		connString: connString,
		logger:     logger,
		scheduler:  scheduler,
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

	q.wg.Add(1)
	go q.runMaintenanceLoop()

	return q, NewMetrics(), nil
}

func (q *Queue) migrate(ctx context.Context) error {
	_, err := q.db.ExecContext(ctx, schemaSQL)
	return err
}

// Enqueue adds a task to the queue
//
// Enqueue returns nil error if the task is enqueued successfully, otherwise returns a non-nil error.
//
// The argument opts specifies the behavior of task processing.
// By default, max retry is set to 5 and priority is set to DefaultPriority.
//
// If no WithDelay option is provided, the task will be pending immediately.
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
