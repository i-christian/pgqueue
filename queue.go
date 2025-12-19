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
	"os"
	"time"

	"github.com/lib/pq"
	"github.com/robfig/cron/v3"
)

//go:embed migrations/*
var schemaSQL string

// NewClient returns a Queue, a logger and a Metric instance given a postgres connection.
//
// The parameter opts is optional, defaults will be used if opts is set to nil
func NewClient(db *sql.DB, opts ...QueueOption) (client *Client, err error) {
	cfg := defaultQueueConfig()
	for _, opt := range opts {
		opt(&cfg)
	}

	ctx, cancel := context.WithCancel(context.Background())
	logger := slog.New(
		slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
			Level: slog.LevelInfo,
		}),
	)

	var scheduler *cron.Cron
	if cfg.cronEnabled {
		scheduler = cron.New(
			cron.WithChain(cron.Recover(cron.DefaultLogger)),
		)
		scheduler.Start()
	}

	q := &Queue{
		logger:    logger,
		scheduler: scheduler,
		ctx:       ctx,
		cancel:    cancel,
		config:    cfg,
	}

	migrateCtx, migrateCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer migrateCancel()
	if err := migrate(migrateCtx, db); err != nil {
		cancel()
		return nil, err
	}

	q.wg.Add(1)
	go q.runMaintenanceLoop(db)

	return &Client{
		db:      db,
		Queue:   q,
		Metrics: NewMetrics(),
		Logger:  logger,
	}, nil
}

func migrate(ctx context.Context, db *sql.DB) error {
	_, err := db.ExecContext(ctx, schemaSQL)
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
func (q *Client) Enqueue(ctx context.Context, task TaskType, payload any, opts ...EnqueueOption) error {
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

func (q *Queue) Shutdown(ctx context.Context) error {
	q.logger.Info("pgqueue: shutting down")

	q.cancel()

	var cronDone <-chan struct{}
	if q.scheduler != nil {
		cronCtx := q.scheduler.Stop()
		cronDone = cronCtx.Done()
	} else {
		closed := make(chan struct{})
		close(closed)
		cronDone = closed
	}

	workersDone := make(chan struct{})
	go func() {
		q.wg.Wait()
		close(workersDone)
	}()

	select {
	case <-workersDone:
		<-cronDone
		q.logger.Info("pgqueue: shutdown complete")
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
