package pgqueue

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"math/rand"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/lib/pq"
)

// StartConsumer starts a worker pool
func (q *Queue) StartConsumer(ctx context.Context, concurrency int, handler WorkerHandler) {
	var wg sync.WaitGroup

	listener := pq.NewListener(q.connString, 10*time.Second, time.Minute, func(ev pq.ListenerEventType, err error) {
		if err != nil {
			log.Println("Listener error:", err)
		}
	})
	if err := listener.Listen("new_task"); err != nil {
		log.Printf("Failed to listen: %v", err)
	}

	wakeUp := make(chan struct{}, 1)

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-listener.Notify:
				select {
				case wakeUp <- struct{}{}:
				default:
				}
			}
		}
	}()

	log.Printf("Starting %d workers...", concurrency)
	for i := range concurrency {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			q.workerLoop(ctx, workerID, handler, wakeUp)
		}(i)
	}

	<-ctx.Done()
	log.Println("Shutting down workers...")
	listener.Close()
	wg.Wait()
	q.scheduler.Stop()
}

func (q *Queue) workerLoop(ctx context.Context, _ int, handler WorkerHandler, wakeUp <-chan struct{}) {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-wakeUp:
			q.processBatch(ctx, handler)
		case <-ticker.C:
			q.processBatch(ctx, handler)
		}
	}
}

func (q *Queue) processBatch(ctx context.Context, handler WorkerHandler) {
	for {
		if ctx.Err() != nil {
			return
		}
		processed, err := q.processOne(ctx, handler)
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

func (q *Queue) processOne(ctx context.Context, handler WorkerHandler) (bool, error) {
	tx, err := q.db.BeginTx(ctx, nil)
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
		UPDATE tasks SET status = 'processing', updated_at = NOW() WHERE task_id = $1
	`, task.ID)
	if err != nil {
		return false, err
	}

	if err := tx.Commit(); err != nil {
		return false, err
	}

	task.Payload = payloadBytes

	runTask := func() (runErr error) {
		defer func() {
			if r := recover(); r != nil {
				runErr = fmt.Errorf("PANIC: %v", r)
			}
		}()
		return handler(ctx, task.Type, task)
	}

	jobErr := runTask()

	if jobErr != nil {
		q.handleFailure(ctx, task, jobErr)
	} else {
		q.markDone(ctx, task.ID)
	}

	return true, nil
}

// markDone updates the task status to 'done'.
func (q *Queue) markDone(ctx context.Context, id uuid.UUID) {
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

func (q *Queue) handleFailure(ctx context.Context, task Task, jobErr error) {
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
	jitter := time.Duration(rand.Int63n(int64(backoff / 2)))
	totalWait := (backoff / 2) + jitter

	q.db.ExecContext(ctx, `
		UPDATE tasks
			SET status = $5,
			attempts = $1,
			next_run_at = NOW() + $2,
			last_error = $3 
		WHERE task_id = $4`, newAttempts, totalWait, jobErr.Error(), task.ID, TaskPending)
}
