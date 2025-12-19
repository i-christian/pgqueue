package pgqueue

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"log/slog"
	"math/rand/v2"
	"time"

	"github.com/google/uuid"
	"github.com/lib/pq"
)

// NewServer initializes the worker pool settings.
func NewServer(db *sql.DB, connString string, concurrency int, handler WorkerHandler) *Server {
	return &Server{
		connString:  connString,
		db:          db,
		handler:     handler,
		concurrency: concurrency,
	}
}

// Start launches the background workers and listener in a separate goroutine.
//
// Strictly non-blocking: It returns nil immediately if startup is successful.
// You must call Shutdown(ctx) to stop the server and wait for workers to finish.
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
				slog.Error("Listener error", "error", err)
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

	slog.Info("Starting workers...", "count", s.concurrency)

	for i := 0; i < s.concurrency; i++ {
		s.wg.Add(1)
		go func(id int) {
			defer s.wg.Done()
			s.workerLoop(s.ctx, wakeUp)
		}(i)
	}

	return nil
}

// Shutdown gracefully stops the server.
// It cancels the internal context and waits for all workers to finish.
func (s *Server) Shutdown(ctx context.Context) error {
	if !s.running.Load() {
		return nil
	}

	slog.Info("pgqueue: Stopping workers...")
	s.cancel()

	done := make(chan struct{})
	go func() {
		s.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		slog.Info("pgqueue: All workers stopped.")
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *Server) workerLoop(ctx context.Context, wakeUp <-chan struct{}) {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-wakeUp:
			s.processBatch(ctx, s.handler)
		case <-ticker.C:
			s.processBatch(ctx, s.handler)
		}
	}
}

func (s *Server) processBatch(ctx context.Context, handler WorkerHandler) {
	for {
		if ctx.Err() != nil {
			return
		}
		processed, err := s.processOne(ctx, handler)
		if err != nil {
			slog.Error("Processing error", "error", err)
			time.Sleep(1 * time.Second)
			return
		}
		if !processed {
			return
		}
	}
}

func (s *Server) processOne(ctx context.Context, handler WorkerHandler) (bool, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return false, err
	}
	defer tx.Rollback()

	var task Task
	var payloadBytes []byte

	row := tx.QueryRowContext(ctx, `
		SELECT task_id, task_type, payload, attempts, max_retries, priority
		FROM tasks 
		WHERE status = 'pending' AND next_run_at <= NOW()
		ORDER BY priority DESC, next_run_at ASC 
		FOR UPDATE SKIP LOCKED 
		LIMIT 1
	`)

	err = row.Scan(&task.ID, &task.Type, &payloadBytes, &task.Attempts, &task.MaxRetries, &task.Priority)
	if err == sql.ErrNoRows {
		return false, nil
	}
	if err != nil {
		return false, err
	}

	_, err = tx.ExecContext(ctx, `
		UPDATE tasks
			SET status = 'processing',
			attempts = attempts + 1,
			updated_at = NOW()
		WHERE task_id = $1
	`, task.ID)
	if err != nil {
		return false, err
	}

	if err := tx.Commit(); err != nil {
		return false, err
	}

	task.Payload = payloadBytes

	jobErr := handler.ProcessTask(ctx, &task)

	if jobErr != nil {
		s.handleFailure(ctx, task, jobErr)
	} else {
		s.markDone(ctx, task.ID)
	}

	return true, nil
}

func (s *Server) markDone(ctx context.Context, id uuid.UUID) {
	_, err := s.db.ExecContext(ctx, `
		UPDATE tasks 
		SET status = $2, 
		    updated_at = NOW() 
		WHERE task_id = $1`,
		id, TaskDone,
	)
	if err != nil {
		log.Printf("Internal Error: Failed to mark task %s as done: %v", id, err)
	}
}

func (s *Server) handleFailure(ctx context.Context, task Task, jobErr error) {
	newAttempts := task.Attempts + 1
	if newAttempts >= task.MaxRetries {
		s.db.ExecContext(ctx, `
			UPDATE tasks
				SET status = $3,
				last_error = $1
			WHERE task_id = $2`, jobErr.Error(), task.ID, TaskFailed)
		return
	}

	// Exponential backoff with Jitter to prevent "Thundering Herd"
	backoff := time.Duration(1<<newAttempts) * time.Second
	jitter := rand.N(backoff)
	totalWait := (backoff / 2) + jitter

	_, err := s.db.ExecContext(ctx, `
		UPDATE tasks
			SET status = $5,
			attempts = $1,
			next_run_at = NOW() + ($2 * INTERVAL '1 second'),
			last_error = $3 
		WHERE task_id = $4`, newAttempts, totalWait.Seconds(), jobErr.Error(), task.ID, TaskPending)
	if err != nil {
		log.Printf("an error occured %v\n", err)
	}
}
