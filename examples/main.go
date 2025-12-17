package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/i-christian/pgqueue"
	_ "github.com/lib/pq"
)

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
	// Example connection string
	connStr := "postgres://myuser:mypass@localhost:5432/task_queue?sslmode=disable"
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	queue, err := pgqueue.NewQueue(db, connStr)
	if err != nil {
		log.Fatalf("Failed to init queue: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	queue.Enqueue(ctx,
		"task:cleanup:expired-sessions",
		CleanupPayload{
			Resource: "sessions",
			DryRun:   false,
		},
	)

	queue.Enqueue(ctx,
		"task:report:daily",
		ReportPayload{
			ReportName: "Daily Sales",
			EmailTo:    "ops@example.com",
		},
	)

	// task with no handler test
	queue.Enqueue(ctx,
		"task:unknown",
		CleanupPayload{
			Resource: "sessions",
			DryRun:   false,
		},
	)

	// High Priority Job
	go func() {
		payload := EmailPayload{Subject: "RESET PASSWORD"}
		err := queue.Enqueue(ctx, TaskSendEmail, payload,
			pgqueue.WithPriority(pgqueue.HighPriority), // High Priority
			pgqueue.WithMaxRetries(10),                 // Retry many times
		)

		log.Print(err)
	}()

	// Low Priority Job
	go func() {
		payload := EmailPayload{Subject: "Weekly Newsletter"}
		err := queue.Enqueue(ctx, TaskSendEmail, payload,
			pgqueue.WithPriority(pgqueue.LowPriority),
		)
		log.Print(err)
	}()

	// Monitor Stats Loop
	go func() {
		ticker := time.NewTicker(15 * time.Second)
		for range ticker.C {
			stats, _ := queue.Stats(ctx)
			fmt.Printf("--- Queue Stats ---\nPending: %d | Processing: %d | Failed: %d\n",
				stats.Pending, stats.Processing, stats.Failed)
		}
	}()

	// Register Cron Jobs
	// "0 * * * *" means every hour on the hour.
	err = queue.ScheduleCron("0 * * * *", "cleanup_report", TaskSendEmail, EmailPayload{Subject: "Hourly Report"})
	if err != nil {
		log.Fatalf("Failed to schedule cron: %v", err)
	}

	// Start Workers in a background gorutine
	mux := pgqueue.NewServeMux()
	mux.HandleFunc(TaskSendEmail, processEmailSendTask)
	mux.HandleFunc(TaskCleanupBase, cleanupHandler)
	mux.HandleFunc(TaskReportBase, reportHandler)

	go queue.StartConsumer(ctx, 3, mux)

	go func() {
		time.Sleep(2 * time.Second)
		fmt.Println("Manually Enqueuing a delayed task...")

		payload := EmailPayload{Subject: "Welcome Email"}

		// Enqueue with options
		err := queue.Enqueue(ctx, TaskSendEmail, payload,
			pgqueue.WithDelay(5*time.Second),
		)
		if err != nil {
			log.Println("Enqueue failed:", err)
		}
	}()

	// Graceful Shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan
	fmt.Println("Stopping...")
}

func processEmailSendTask(ctx context.Context, t *pgqueue.Task) error {
	var p EmailPayload
	if err := json.Unmarshal(t.Payload, &p); err != nil {
		return fmt.Errorf("bad payload: %v", err)
	}
	log.Printf("Processing: %s (Attempts: %d)\n", p.Subject, t.Attempts)
	return nil
}

func cleanupHandler(ctx context.Context, t *pgqueue.Task) error {
	var p CleanupPayload
	if err := json.Unmarshal(t.Payload, &p); err != nil {
		return fmt.Errorf("cleanup: invalid payload: %w", err)
	}

	log.Printf(
		"[CLEANUP] task=%s resource=%s dryRun=%v attempts=%d",
		t.Type,
		p.Resource,
		p.DryRun,
		t.Attempts,
	)

	// Simulate work
	select {
	case <-time.After(1 * time.Second):
	case <-ctx.Done():
		return ctx.Err()
	}

	return nil
}

func reportHandler(ctx context.Context, t *pgqueue.Task) error {
	var p ReportPayload
	if err := json.Unmarshal(t.Payload, &p); err != nil {
		return fmt.Errorf("report: invalid payload: %w", err)
	}

	log.Printf(
		"[REPORT] task=%s report=%s sendTo=%s attempts=%d",
		t.Type,
		p.ReportName,
		p.EmailTo,
		t.Attempts,
	)

	// Simulate report generation
	select {
	case <-time.After(2 * time.Second):
	case <-ctx.Done():
		return ctx.Err()
	}

	return nil
}
