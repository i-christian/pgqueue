package pgqueue

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"log/slog"
	"math/rand/v2"
	"os"
	"time"

	"github.com/lib/pq"
)

// NewServer initializes a pgqueue worker server.
//
// A Server manages a pool of background workers that:
//
//   - Listen for task notifications via LISTEN / NOTIFY
//   - Fetch tasks safely using SELECT ... FOR UPDATE SKIP LOCKED
//   - Process tasks concurrently with bounded parallelism
//
// It requires a shared *sql.DB connection, a connection string for the LISTEN/NOTIFY listener,
// the desired number of concurrent worker goroutines, and a handler to process the tasks.
//
// The server is safe to run across multiple processes or machines
// connected to the same PostgreSQL database.
func NewServer(db *sql.DB, connString string, concurrency int, handler WorkerHandler, opts ...ServerOption) *Server {
	s := &Server{
		connString:  connString,
		db:          db,
		handler:     handler,
		batchSize:   10,
		concurrency: concurrency,
	}

	for _, opt := range opts {
		opt(s)
	}

	return s
}

// Start launches the worker pool and PostgreSQL LISTEN loop.
//
// Start is strictly non-blocking: it initializes background goroutines
// and returns immediately if startup is successful.
//
// Calling Start on an already-running server returns an error.
// Shutdown(ctx) must be called to gracefully stop workers.
func (s *Server) Start() error {
	if !s.running.CompareAndSwap(false, true) {
		return errors.New("server already running")
	}

	s.ctx, s.cancel = context.WithCancel(context.Background())
	s.shutdownDone = make(chan struct{})

	if err := s.db.Ping(); err != nil {
		return fmt.Errorf("database unreachable: %w", err)
	}

	listener := pq.NewListener(
		s.connString,
		10*time.Second,
		time.Minute,
		func(ev pq.ListenerEventType, err error) {
			if err != nil {
				slog.Error("pgqueue listener error", "error", err)
			}
		},
	)

	if err := listener.Listen("new_task"); err != nil {
		return fmt.Errorf("failed to listen on channel: %w", err)
	}

	wakeUp := make(chan struct{}, 1)

	s.wg.Go(func() {
		defer listener.Close()

		for {
			select {
			case <-s.ctx.Done():
				return
			case <-listener.Notify:
				// Non-blocking send to wake up workers
				select {
				case wakeUp <- struct{}{}:
				default:
				}
			}
		}
	})

	slog.Info("pgqueue: starting workers", "count", s.concurrency)

	for i := 0; i < s.concurrency; i++ {
		s.wg.Add(1)
		go func(id int) {
			defer s.wg.Done()
			s.workerLoop(s.ctx, wakeUp)
		}(i)
	}

	return nil
}

// Shutdown gracefully stops the worker server.
//
// It cancels the internal context and waits for all workers
// to finish processing their current tasks or until ctx expires.
func (s *Server) Shutdown(ctx context.Context) error {
	if !s.running.Load() {
		return nil
	}

	slog.Info("pgqueue: stopping workers")
	s.cancel()

	done := make(chan struct{})
	go func() {
		s.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		slog.Info("pgqueue: all workers stopped")
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// workerLoop runs the main execution loop for a single worker.
//
// Each worker:
//
//   - Applies startup jitter to avoid synchronized DB spikes
//   - Continuously drains available work when tasks exist
//   - Sleeps when idle using LISTEN / NOTIFY and a safety ticker
//   - Applies exponential backoff on repeated database errors
func (s *Server) workerLoop(ctx context.Context, wakeUp <-chan struct{}) {
	time.Sleep(time.Duration(rand.N(2000)) * time.Millisecond)

	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	var errorCount int

	for {
		count, err := s.processBatch(ctx, s.handler)
		if err != nil {
			errorCount++

			if errors.Is(err, context.Canceled) {
				return
			}

			if errorCount == 1 || errorCount%10 == 0 {
				slog.Error("pgqueue worker error", "err", err, "attempt", errorCount)
			}

			sleep := time.Duration(100*(1<<min(errorCount, 5))) * time.Millisecond

			select {
			case <-ctx.Done():
				return
			case <-time.After(sleep):
				continue
			}
		}

		errorCount = 0

		if count > 0 {
			continue
		}

		select {
		case <-ctx.Done():
			return
		case <-wakeUp:
			// Signaled via PostgreSQL NOTIFY
		case <-ticker.C:
			// Periodic safety poll.
		}
	}
}

// processBatch fetches and processes a batch of tasks.
//
// It returns the number of tasks processed and an error if the batch
// could not be fetched or committed.
func (s *Server) processBatch(ctx context.Context, handler WorkerHandler) (int, error) {
	if ctx.Err() != nil {
		return 0, ctx.Err()
	}

	tasks, err := s.fetchBatch(ctx, s.batchSize)
	if err != nil {
		return 0, err
	}

	if len(tasks) == 0 {
		return 0, nil
	}

	for _, task := range tasks {
		if jobErr := handler.ProcessTask(ctx, &task); jobErr != nil {
			s.handleFailure(ctx, task, jobErr)
		} else {
			s.markDone(ctx, task)
		}
	}

	return len(tasks), nil
}

// fetchBatch atomically selects and claims a batch of pending tasks.
//
// Tasks are claimed using UPDATE ... FOR UPDATE SKIP LOCKED to ensure
// that no two workers can process the same task concurrently.
func (s *Server) fetchBatch(ctx context.Context, limit uint16) ([]Task, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback()

	rows, err := tx.QueryContext(ctx, `
		UPDATE pgqueue.tasks
		SET status = 'processing',
		    attempts = attempts + 1,
		    updated_at = NOW()
		WHERE (task_id, created_at) IN (
			SELECT task_id, created_at
			FROM tasks
			WHERE status = 'pending'
			  AND next_run_at <= NOW()
			ORDER BY priority DESC, next_run_at ASC
			FOR UPDATE SKIP LOCKED
			LIMIT $1
		)
		RETURNING task_id, created_at, task_type, payload, attempts, max_retries, priority
	`, limit)
	if err != nil {
		return nil, fmt.Errorf("query fetch: %w", err)
	}
	defer rows.Close()

	var tasks []Task
	for rows.Next() {
		var t Task
		var payload []byte
		if err := rows.Scan(
			&t.ID,
			&t.CreatedAt,
			&t.Type,
			&payload,
			&t.Attempts,
			&t.MaxRetries,
			&t.Priority,
		); err != nil {
			return nil, fmt.Errorf("scan task: %w", err)
		}
		t.Payload = payload
		tasks = append(tasks, t)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("rows iteration: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("commit: %w", err)
	}

	return tasks, nil
}

// markDone marks a task as successfully completed.
func (s *Server) markDone(ctx context.Context, task Task) {
	_, err := s.db.ExecContext(ctx, `
		UPDATE pgqueue.tasks
		SET status = $3,
		    updated_at = NOW()
		WHERE task_id = $1 AND created_at = $2
	`, task.ID, task.CreatedAt, TaskDone)
	if err != nil {
		log.Printf("pgqueue: failed to mark task %s as done: %v", task.ID, err)
	}
}

// handleFailure records a task failure and schedules retries if applicable.
//
// Tasks exceeding their retry limit are marked as permanently failed.
// Otherwise, exponential backoff with jitter is applied.
func (s *Server) handleFailure(ctx context.Context, task Task, jobErr error) {
	if task.Attempts >= task.MaxRetries {
		s.db.ExecContext(ctx, `
			UPDATE pgqueue.tasks
			SET status = $4, last_error = $1
			WHERE task_id = $2 AND created_at = $3
		`, jobErr.Error(), task.ID, task.CreatedAt, TaskFailed)
		return
	}

	backoff := time.Duration(1<<task.Attempts) * time.Second
	jitter := rand.N(backoff)
	totalWait := (backoff / 2) + jitter
	isTest := os.Getenv("GO_ENV") == "test"

	_, err := s.db.ExecContext(ctx, `
		UPDATE pgqueue.tasks
		SET status = $5,
		    next_run_at = NOW() + (
		        $1 * CASE
		            WHEN $6 = true THEN INTERVAL '1 millisecond'
		            ELSE INTERVAL '1 second'
		        END
		    ),
		    last_error = $2
		WHERE task_id = $3 AND created_at = $4
	`, totalWait.Seconds(), jobErr.Error(), task.ID, task.CreatedAt, TaskPending, isTest)
	if err != nil {
		slog.Error("pgqueue: failed to reschedule task", "taskID", task.ID, "error", err)
	}
}
