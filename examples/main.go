package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/i-christian/pgqueue"
	_ "github.com/lib/pq"
)

// Task type constants.
// Use bounded, descriptive task categories.
const (
	TaskSendEmail   = "task:send:email"
	TaskCleanupBase = "task:cleanup:"
	TaskReportBase  = "task:report:"
)

type EmailPayload struct {
	Subject string `json:"subject"`
}

type CleanupPayload struct {
	Resource string `json:"resource"`
	DryRun   bool   `json:"dry_run"`
}

type ReportPayload struct {
	ReportName string `json:"report_name"`
	EmailTo    string `json:"email_to"`
}

func main() {
	// Database connection
	connStr := "postgres://myuser:mypass@localhost:5432/task_queue?sslmode=disable"
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// Structured logger
	logger := slog.New(
		slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
			Level: slog.LevelInfo,
		}),
	)

	// Initialize queue
	queue, metrics, err := pgqueue.NewQueue(db, connStr, logger)
	if err != nil {
		log.Fatalf("Failed to init queue: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// ---- Enqueue some example jobs ----

	queue.Enqueue(ctx,
		"task:cleanup:expired-sessions",
		CleanupPayload{Resource: "sessions"},
	)

	queue.Enqueue(ctx,
		"task:report:daily",
		ReportPayload{
			ReportName: "Daily Sales",
			EmailTo:    "operations@example.com",
		},
	)

	go func() {
		queue.Enqueue(ctx,
			TaskSendEmail,
			EmailPayload{Subject: "Welcome!"},
			pgqueue.WithPriority(pgqueue.HighPriority),
		)
	}()

	// ---- Worker setup ----

	mux := pgqueue.NewServeMux()

	// Middleware runs for every task
	mux.Use(pgqueue.SlogMiddleware(logger, metrics))

	// Register handlers
	mux.HandleFunc(TaskSendEmail, sendEmailHandler)
	mux.HandleFunc(TaskCleanupBase, cleanupHandler) // prefix match
	mux.HandleFunc(TaskReportBase, reportHandler)

	// Start workers
	go queue.StartConsumer(ctx, 3, mux)

	// Register Cron Jobs
	// "0 * * * * " means every hour on the hour.
	// "* * * * *" means every minute
	err = queue.ScheduleCron("* * * * *", "cleanup_report", TaskSendEmail, EmailPayload{Subject: "Minute Report"})
	if err != nil {
		log.Fatalf("Failed to schedule cron: %v", err)
	}

	// Monitor Stats Loop
	go func() {
		ticker := time.NewTicker(15 * time.Second)
		for range ticker.C {
			stats, _ := queue.Stats(ctx)
			fmt.Printf("--- Queue Stats ---\nPending: %d | Processing: %d | Failed: %d | Success: %d\n",
				stats.Pending, stats.Processing, stats.Failed, stats.Done)
		}
	}()

	// ---- Graceful shutdown ----

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	fmt.Println("Shutting down...")
	cancel()
}

func sendEmailHandler(ctx context.Context, t *pgqueue.Task) error {
	var p EmailPayload
	if err := json.Unmarshal(t.Payload, &p); err != nil {
		return err
	}
	return nil
}

func cleanupHandler(ctx context.Context, t *pgqueue.Task) error {
	var p CleanupPayload
	if err := json.Unmarshal(t.Payload, &p); err != nil {
		return err
	}

	time.Sleep(1 * time.Second)
	return nil
}

func reportHandler(ctx context.Context, t *pgqueue.Task) error {
	var p ReportPayload
	if err := json.Unmarshal(t.Payload, &p); err != nil {
		return err
	}

	time.Sleep(2 * time.Second)
	return nil
}
