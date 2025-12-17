package pgqueue

import (
	"context"
	"database/sql"
	_ "embed"
	"encoding/json"
	"fmt"
	"time"

	"github.com/lib/pq"
	"github.com/robfig/cron/v3"
)

//go:embed migrations/*
var schemaSQL string

// NewQueue initializes the queue, a scheduler and runs migrations automatically
func NewQueue(db *sql.DB, connString string) (*Queue, error) {
	q := &Queue{
		db:         db,
		connString: connString,
		scheduler: cron.New(cron.WithChain(
			cron.Recover(cron.DefaultLogger),
		)),
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := q.migrate(ctx); err != nil {
		return nil, fmt.Errorf("migration failed: %w", err)
	}

	q.scheduler.Start()

	return q, nil
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
