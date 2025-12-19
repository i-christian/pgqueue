package pgqueue

import (
	"context"
	"database/sql"
	"log"
	"log/slog"
	"math/rand/v2"
	"time"

	"github.com/google/uuid"
	"github.com/lib/pq"
)

// NewServer starts the worker pool, by wrapping the task's consumer.
//
// It runs the tasks consumers in a separate gorutine
func NewServer(queue *Queue, db *sql.DB, connString string, concurrency int, handler WorkerHandler) *Server {
	srv := &Server{
		connString:  connString,
		db:          db,
		queue:       queue,
		concurrency: concurrency,
	}

	go srv.startConsumer(handler)
	return srv
}

// startConsumer starts a worker pool
func (s *Server) startConsumer(handler WorkerHandler) {
	listener := pq.NewListener(
		s.connString,
		10*time.Second,
		time.Minute,
		func(ev pq.ListenerEventType, err error) {
			if err != nil {
				log.Println("Listener error:", err)
			}
		},
	)

	if err := listener.Listen("new_task"); err != nil {
		slog.Error("Failed to listen ", "details", err)
	}

	wakeUp := make(chan struct{}, 1)

	go func() {
		for {
			select {
			case <-s.queue.ctx.Done():
				return
			case <-listener.Notify:
				select {
				case wakeUp <- struct{}{}:
				default:
				}
			}
		}
	}()

	log.Printf("Starting %d workers...", s.concurrency)

	for i := range s.concurrency {
		s.queue.wg.Add(1)
		go func(id int) {
			defer s.queue.wg.Done()
			s.workerLoop(s.queue.ctx, id, handler, wakeUp)
		}(i)
	}

	go func() {
		<-s.queue.ctx.Done()
		log.Println("Stopping workers...")
		listener.Close()
	}()
}

func (s *Server) workerLoop(ctx context.Context, _ int, handler WorkerHandler, wakeUp <-chan struct{}) {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-wakeUp:
			s.processBatch(ctx, handler)
		case <-ticker.C:
			s.processBatch(ctx, handler)
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
			log.Printf("Processing error: %v", err)
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

// markDone updates the task status to 'done'.
func (q *Server) markDone(ctx context.Context, id uuid.UUID) {
	_, err := q.db.ExecContext(ctx, `
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

func (q *Server) handleFailure(ctx context.Context, task Task, jobErr error) {
	newAttempts := task.Attempts + 1
	if newAttempts >= task.MaxRetries {
		q.db.ExecContext(ctx, `
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

	_, err := q.db.ExecContext(ctx, `
		UPDATE tasks
			SET status = $5,
			attempts = $1,
			next_run_at = NOW() + ($2 * INTERVAL '1 seconds'),
			last_error = $3 
		WHERE task_id = $4`, newAttempts, totalWait.Seconds(), jobErr.Error(), task.ID, TaskPending)
	if err != nil {
		log.Printf("an error occured %v\n", err)
	}
}
