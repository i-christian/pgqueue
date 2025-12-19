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

// NewClient returns a Queue's Client.
func NewClient(db *sql.DB, opts ...QueueOption) (client *Client, err error) {
	cfg := defaultQueueConfig()
	for _, opt := range opts {
		opt(&cfg)
	}

	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))

	ctx, cancel := context.WithCancel(context.Background())

	q := &Queue{
		config: cfg,
		logger: logger,
		ctx:    ctx,
		cancel: cancel,
	}

	if cfg.cronEnabled {
		q.scheduler = cron.New(cron.WithChain(cron.Recover(cron.DefaultLogger)))
		q.scheduler.Start()
	}

	migrateCtx, migrateCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer migrateCancel()
	if err := migrate(migrateCtx, db); err != nil {
		cancel()
		return nil, err
	}

	client = &Client{
		db:      db,
		queue:   q,
		Metrics: NewMetrics(),
		Logger:  logger,
	}

	q.wg.Add(1)
	go q.runMaintenanceLoop(db)

	return client, nil
}

// Close shuts down the Client's background maintenance routines and Cron scheduler.
func (c *Client) Close() error {
	return c.queue.shutdown()
}

func migrate(ctx context.Context, db *sql.DB) error {
	_, err := db.ExecContext(ctx, schemaSQL)
	return err
}

// Enqueue adds a task to the queue
func (c *Client) Enqueue(ctx context.Context, task TaskType, payload any, opts ...EnqueueOption) error {
	cfg := enqueueConfig{
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

	nextRunAt := sql.NullTime{Time: time.Now(), Valid: true}
	if cfg.processAt != nil {
		nextRunAt = sql.NullTime{Time: *cfg.processAt, Valid: true}
	}

	var dedupKey sql.NullString
	if cfg.dedupKey != nil {
		dedupKey = sql.NullString{String: *cfg.dedupKey, Valid: true}
	}

	args = append(args, nextRunAt, dedupKey)

	_, err = c.db.ExecContext(ctx, query, args...)
	if err != nil {
		if pqErr, ok := err.(*pq.Error); ok && pqErr.Code == "23505" {
			return nil // Deduplication hit
		}
		return err
	}
	return nil
}

func (q *Queue) shutdown() error {
	q.logger.Info("pgqueue: client shutting down")
	q.cancel()

	if q.scheduler != nil {
		ctx := q.scheduler.Stop()
		<-ctx.Done()
	}

	q.wg.Wait()
	q.logger.Info("pgqueue: client shutdown complete")
	return nil
}
