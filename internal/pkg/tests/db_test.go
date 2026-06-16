package tests

import (
	"context"
	"database/sql"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/i-christian/pgqueue/internal/pkg/migrations"
	"github.com/i-christian/pgqueue/internal/pkg/queries"
	_ "github.com/lib/pq"
)

// setupTestDB provisions a real PostgreSQL connection and minimal schema
// required to test the queries in isolation.
func setupTestDB(t *testing.T) *sql.DB {
	t.Helper()
	dsn := os.Getenv("TEST_DB_DSN")
	if dsn == "" {
		dsn = "postgres://user:pass@localhost:5432/task_queue_test?sslmode=disable"
	}

	db, err := sql.Open("postgres", dsn)
	if err != nil {
		t.Fatalf("Failed to open DB: %v", err)
	}

	if err := db.Ping(); err != nil {
		t.Skipf("Skipping query tests: Database not reachable at %s", dsn)
	}

	t.Cleanup(func() {
		db.Exec("DROP SCHEMA IF EXISTS pgqueue CASCADE;")
		db.Close()
	})

	ctx := context.Background()
	if err := migrations.Migrate(ctx, db); err != nil {
		t.Fatalf("Failed to apply schema: %v", err)
	}

	return db
}

// TestPreparedStatements ensures all raw SQL strings compile successfully
// against the Postgres engine.
func TestPreparedStatements(t *testing.T) {
	db := setupTestDB(t)
	ctx := context.Background()

	stmts, err := queries.NewPrepared(ctx, db)
	if err != nil {
		t.Fatalf("Failed to prepare statements: %v", err)
	}
	defer stmts.Close()

	if stmts.EnqueueTask == nil || stmts.FetchBatch == nil {
		t.Error("Expected statements to be initialized, got nil")
	}
}

func TestTaskQueries(t *testing.T) {
	db := setupTestDB(t)
	ctx := context.Background()

	stmts, err := queries.NewPrepared(ctx, db)
	if err != nil {
		t.Fatalf("Failed to prepare statements: %v", err)
	}
	defer stmts.Close()

	taskID := uuid.New()
	createdAt := time.Now().UTC()
	nextRunAt := createdAt
	payload := []byte(`{"hello":"world"}`)

	_, err = stmts.EnqueueTask.ExecContext(ctx,
		taskID, createdAt, "test:query", 1, 3, payload, nextRunAt, nil,
	)
	if err != nil {
		t.Fatalf("EnqueueTask failed: %v", err)
	}

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("Failed to begin tx: %v", err)
	}
	defer tx.Rollback()

	stmt := tx.StmtContext(ctx, stmts.FetchBatch)
	rows, err := stmt.QueryContext(ctx, "processing", "pending", 1)
	if err != nil {
		t.Fatalf("FetchBatch failed: %v", err)
	}
	defer rows.Close()

	var fetchedID uuid.UUID
	var fetchedType string
	var attempts int

	if !rows.Next() {
		t.Fatal("Expected to fetch 1 task, got none")
	}

	if err := rows.Scan(&fetchedID, &createdAt, &fetchedType, &payload, &attempts, new(int), new(int)); err != nil {
		t.Fatalf("Failed to scan fetched task: %v", err)
	}

	if fetchedID != taskID {
		t.Errorf("Expected fetched ID %s, got %s", taskID, fetchedID)
	}
	if attempts != 1 {
		t.Errorf("Expected attempts to increment to 1, got %d", attempts)
	}

	tx.Commit()

	_, err = stmts.MarkTaskDone.ExecContext(ctx, taskID, createdAt, "done")
	if err != nil {
		t.Fatalf("MarkTaskDone failed: %v", err)
	}

	var status string
	err = db.QueryRow("SELECT status FROM pgqueue.tasks WHERE task_id = $1", taskID).Scan(&status)
	if err != nil {
		t.Fatalf("Failed to query final status: %v", err)
	}
	if status != "done" {
		t.Errorf("Expected status 'done', got '%s'", status)
	}
}

// TestCronQueries validates the cron dashboarding and execution metadata logic
func TestCronQueries(t *testing.T) {
	db := setupTestDB(t)
	ctx := context.Background()

	stmts, err := queries.NewPrepared(ctx, db)
	if err != nil {
		t.Fatalf("Failed to prepare statements: %v", err)
	}
	defer stmts.Close()

	jobID := uuid.New()
	now := time.Now().UTC()
	nextRun := now.Add(time.Minute)

	var returnedID uuid.UUID
	err = stmts.UpsertCronJob.QueryRowContext(ctx,
		jobID, "daily_report", "0 0 * * *", nextRun, now,
	).Scan(&returnedID)
	if err != nil {
		t.Fatalf("UpsertCronJob failed: %v", err)
	}
	if returnedID != jobID {
		t.Errorf("Expected returning ID %s, got %s", jobID, returnedID)
	}

	newNextRun := now.Add(2 * time.Minute)
	err = stmts.UpsertCronJob.QueryRowContext(ctx,
		uuid.New(), "daily_report", "0 12 * * *", newNextRun, now,
	).Scan(&returnedID)
	if err != nil {
		t.Fatalf("UpsertCronJob conflict resolution failed: %v", err)
	}

	var expression string
	db.QueryRow("SELECT expression FROM pgqueue.cron_jobs WHERE job_id = $1", jobID).Scan(&expression)
	if expression != "0 12 * * *" {
		t.Errorf("Expected updated expression '0 12 * * *', got '%s'", expression)
	}

	_, err = stmts.DeleteCronJob.ExecContext(ctx, jobID)
	if err != nil {
		t.Fatalf("DeleteCronJob failed: %v", err)
	}
}
