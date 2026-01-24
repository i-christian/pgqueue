package pgqueue

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	_ "github.com/lib/pq"
)

// setupTestDB cleans the database and returns a connection
func setupTestDB(t *testing.T) (db *sql.DB, connString string) {
	dsn := os.Getenv("TEST_DB_DSN")
	if dsn == "" {
		dsn = "postgres://user:pass@localhost:5432/task_queue_test?sslmode=disable"
	}

	db, err := sql.Open("postgres", dsn)
	if err != nil {
		t.Fatalf("Failed to open DB: %v", err)
	}

	if err := db.Ping(); err != nil {
		t.Skipf("Skipping tests: Database not reachable at %s", dsn)
	}

	_, err = db.Exec("TRUNCATE TABLE tasks, tasks_archive RESTART IDENTITY;")
	if err != nil {
		t.Logf("Truncate failed: %v", err)
	}

	return db, dsn
}

func TestEnqueueAndProcess(t *testing.T) {
	db, dsn := setupTestDB(t)
	defer db.Close()

	client, err := NewClient(db)
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()

	done := make(chan struct{})
	mux := NewServeMux()
	mux.HandleFunc("test:success", func(ctx context.Context, task *Task) error {
		var p map[string]string
		if err := json.Unmarshal(task.Payload, &p); err != nil {
			return err
		}
		if p["msg"] != "hello" {
			return fmt.Errorf("unexpected payload: %v", p)
		}
		close(done)
		return nil
	})

	server := NewServer(db, dsn, 1, mux)
	if err := server.Start(); err != nil {
		t.Fatalf("Failed to start server: %v", err)
	}
	defer server.Shutdown(context.Background())

	err = client.Enqueue(context.Background(), "test:success", map[string]string{"msg": "hello"})
	if err != nil {
		t.Fatalf("Enqueue failed: %v", err)
	}

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Timeout waiting for task processing")
	}

	var status string
	err = db.QueryRow("SELECT status FROM tasks WHERE task_type = $1", "test:success").Scan(&status)
	if err != nil {
		t.Fatalf("Failed to query task: %v", err)
	}
	if status != TaskDone {
		t.Errorf("Expected status 'done', got '%s'", status)
	}
}

func TestRetryLogic(t *testing.T) {
	os.Setenv("GO_ENV", "test")
	defer os.Unsetenv("GO_ENV")

	db, dsn := setupTestDB(t)
	defer db.Close()

	client, _ := NewClient(db)
	defer client.Close()

	attemptsCh := make(chan int, 5)

	mux := NewServeMux()
	mux.HandleFunc("test:fail", func(ctx context.Context, task *Task) error {
		attemptsCh <- task.Attempts
		return fmt.Errorf("intentional failure")
	})

	server := NewServer(db, dsn, 1, mux)
	server.Start()
	defer server.Shutdown(context.Background())

	client.Enqueue(context.Background(), "test:fail", nil, WithMaxRetries(2))

	expectedAttempts := 2

	for i := range expectedAttempts {
		select {
		case <-attemptsCh:
		case <-time.After(5 * time.Second):
			t.Fatalf("Timeout waiting for attempt %d", i)
		}
	}

	time.Sleep(500 * time.Millisecond)

	var status string
	var attempts int
	err := db.QueryRow("SELECT status, attempts FROM tasks WHERE task_type = 'test:fail'").Scan(&status, &attempts)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	if status != TaskFailed {
		t.Errorf("Expected final status 'failed', got '%s'", status)
	}
}

func TestDeduplication(t *testing.T) {
	db, _ := setupTestDB(t)
	defer db.Close()
	client, _ := NewClient(db)
	defer client.Close()

	ctx := context.Background()
	key := "unique-order-123"

	err := client.Enqueue(ctx, "test:dedup", nil, WithDedup(key))
	if err != nil {
		t.Fatalf("First enqueue failed: %v", err)
	}

	err = client.Enqueue(ctx, "test:dedup", nil, WithDedup(key))
	if err != nil {
		t.Fatalf("Second enqueue should not return error: %v", err)
	}

	var count int
	db.QueryRow("SELECT count(*) FROM tasks").Scan(&count)

	if count != 1 {
		t.Errorf("Expected 1 task, found %d", count)
	}
}

func TestDelayedExecution(t *testing.T) {
	db, dsn := setupTestDB(t)
	defer db.Close()
	client, _ := NewClient(db)
	defer client.Close()

	processed := make(chan struct{})
	mux := NewServeMux()
	mux.HandleFunc("test:delay", func(ctx context.Context, t *Task) error {
		close(processed)
		return nil
	})

	server := NewServer(db, dsn, 1, mux)
	server.Start()
	defer server.Shutdown(context.Background())

	start := time.Now()
	client.Enqueue(context.Background(), "test:delay", nil, WithDelay(2*time.Second))

	select {
	case <-processed:
		elapsed := time.Since(start)
		if elapsed < 1900*time.Millisecond {
			t.Errorf("Task processed too early: %v", elapsed)
		}
	case <-time.After(4 * time.Second):
		t.Fatal("Task not processed within expected window")
	}
}

func TestRescueStuckTasks(t *testing.T) {
	db, _ := setupTestDB(t)
	defer db.Close()

	client, _ := NewClient(db, WithRescueConfig(100*time.Millisecond, 1*time.Second))
	defer client.Close()

	stuckID := uuid.New()
	_, err := db.Exec(`
		INSERT INTO tasks (task_id, task_type, status, priority, max_retries, payload, next_run_at, updated_at, created_at)
		VALUES ($1, 'test:rescue', 'processing', 3, 5, '{}', NOW(), NOW() - INTERVAL '1 hour', NOW())
	`, stuckID)
	if err != nil {
		t.Fatalf("Failed to insert stuck task: %v", err)
	}

	time.Sleep(2 * time.Second)

	var status string
	var lastError sql.NullString
	err = db.QueryRow("SELECT status, last_error FROM tasks WHERE task_id = $1", stuckID).Scan(&status, &lastError)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	if status != TaskPending {
		t.Errorf("Expected stuck task to be reset to 'pending', got '%s'", status)
	}

	if !lastError.Valid || lastError.String != "detected stuck task; resetting" {
		t.Errorf("Expected rescue message in last_error, got: %v", lastError)
	}
}

// BenchmarkEnqueue measures how fast we can push tasks into PostgreSQL
func BenchmarkEnqueue(b *testing.B) {
	db, _ := setupTestDB(nil)
	defer db.Close()
	client, _ := NewClient(db)
	defer client.Close()

	ctx := context.Background()
	payload := map[string]string{"data": "bench"}

	for b.Loop() {
		_ = client.Enqueue(ctx, "bench:task", payload)
	}
}

// BenchmarkWorkerThroughput measures how fast workers can drain the queue
func BenchmarkWorkerThroughput(b *testing.B) {
	db, dsn := setupTestDB(nil)
	defer db.Close()

	client, _ := NewClient(db)
	ctx := context.Background()
	for i := 0; b.Loop(); i++ {
		_ = client.Enqueue(ctx, "bench:process", i)
	}
	client.Close()

	var wg sync.WaitGroup
	wg.Add(b.N)
	mux := NewServeMux()
	mux.HandleFunc("bench:process", func(ctx context.Context, t *Task) error {
		wg.Done()
		return nil
	})

	server := NewServer(db, dsn, 5, mux)
	_ = server.Start()

	b.ResetTimer()
	wg.Wait()
	b.StopTimer()

	server.Shutdown(ctx)
}
