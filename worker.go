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

	"github.com/google/uuid"
	"github.com/lib/pq"
)

// NewServer initializes the worker pool settings.
func NewServer(db *sql.DB, connString string, concurrency int, handler WorkerHandler) *Server {
	return &Server{
		connString:  connString,
		db:          db,
		handler:     handler,
		batchSize:   10,
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
				slog.Error("Worker encountered error", "err", err, "attempt", errorCount)
			}

			sleepDuration := time.Duration(100*(1<<min(errorCount, 5))) * time.Millisecond
			
			select {
			case <-ctx.Done():
				return
			case <-time.After(sleepDuration):
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
			// Listener says new task arrived. Loop around to fetch it.
		case <-ticker.C:
			// Periodic safety check.
		}
	}
}

func (s *Server) processBatch(ctx context.Context, handler WorkerHandler) (int, error) {
	if ctx.Err() != nil {
		return 0, ctx.Err()
	}

	tasks, err := s.fetchBatch(ctx, s.batchSize)
	if err != nil {
		return 0, err
	}

	count := len(tasks)
	if count == 0 {
		return 0, nil
	}

	for _, task := range tasks {
		jobErr := handler.ProcessTask(ctx, &task)
		if jobErr != nil {
			s.handleFailure(ctx, task, jobErr)
		} else {
			s.markDone(ctx, task.ID)
		}
	}

	return count, nil
}

func (s *Server) fetchBatch(ctx context.Context, limit uint16) ([]Task, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback() 

	rows, err := tx.QueryContext(ctx, `
		UPDATE tasks
		SET status = 'processing',
		    attempts = attempts + 1,
		    updated_at = NOW()
		WHERE task_id IN (
			SELECT task_id
			FROM tasks 
			WHERE status = 'pending' AND next_run_at <= NOW()
			ORDER BY priority DESC, next_run_at ASC 
			FOR UPDATE SKIP LOCKED 
			LIMIT $1
		)
		RETURNING task_id, task_type, payload, attempts, max_retries, priority
	`, limit)
	if err != nil {
		return nil, fmt.Errorf("query fetch: %w", err)
	}
	defer rows.Close()

	var tasks []Task
	for rows.Next() {
		var t Task
		var payload []byte
		if err := rows.Scan(&t.ID, &t.Type, &payload, &t.Attempts, &t.MaxRetries, &t.Priority); err != nil {
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
	if task.Attempts >= task.MaxRetries {
		s.db.ExecContext(ctx, `
			UPDATE tasks
				SET status = $3,
				last_error = $1
			WHERE task_id = $2`, jobErr.Error(), task.ID, TaskFailed)
		return
	}

	// Exponential backoff with Jitter to prevent "Thundering Herd"
	backoff := time.Duration(1<<task.Attempts) * time.Second
	jitter := rand.N(backoff)
	totalWait := (backoff / 2) + jitter
	isTest := os.Getenv("GO_ENV") == "test"

	query := `
        UPDATE tasks
        SET status = $4,
            next_run_at = NOW() + (
                $1 * CASE WHEN $5 = true THEN INTERVAL '1 millisecond' 
                          ELSE INTERVAL '1 second' END
            ),
            last_error = $2 
        WHERE task_id = $3`

	_, err := s.db.ExecContext(ctx, query, totalWait.Seconds(), jobErr.Error(), task.ID, TaskPending, isTest)
	if err != nil {
		log.Printf("an error occured %v\n", err)
	}
}
