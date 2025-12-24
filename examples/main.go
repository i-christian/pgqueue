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

	client, err := pgqueue.NewClient(db,
		// Check every 15 min, consider stuck if running > 60 mins
		pgqueue.WithRescueConfig(15*time.Minute, 60*time.Minute),

		// Run hourly, Archive tasks older than 24 hours
		pgqueue.WithCleanupConfig(1*time.Hour, 24*time.Hour, pgqueue.ArchiveStrategy),
		// Enables cron job scheduling,
		pgqueue.WithCronEnabled(),
	)
	if err != nil {
		log.Fatalf("Failed to init queue: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// ---- Enqueue some example jobs ----

	err = client.Enqueue(ctx,
		"task:cleanup:expired-sessions",
		CleanupPayload{Resource: "sessions"},
	)
	if err != nil {
		log.Println(err)
	}

	err = client.Enqueue(ctx,
		"task:report:daily",
		ReportPayload{
			ReportName: "Daily Sales",
			EmailTo:    "operations@example.com",
		},
	)
	if err != nil {
		log.Println(err)
	}

	go func() {
		err := client.Enqueue(ctx,
			TaskSendEmail,
			EmailPayload{Subject: "Welcome!"},
			pgqueue.WithPriority(pgqueue.HighPriority),
			pgqueue.WithMaxRetries(1),
		)
		if err != nil {
			log.Println(err)
		}
	}()

	// ---- Worker setup ----

	mux := pgqueue.NewServeMux()

	// Middleware runs for every task
	mux.Use(pgqueue.SlogMiddleware(client.Logger, client.Metrics))

	// Register handlers
	mux.HandleFunc(TaskSendEmail, sendEmailHandler)
	mux.HandleFunc(TaskCleanupBase, cleanupHandler)
	mux.HandleFunc(TaskReportBase, reportHandler)

	// Start workers
	server := pgqueue.NewServer(db, connStr, 3, mux)

	// This spins up goroutines and returns immediately.
	if err := server.Start(); err != nil {
		log.Fatal(err)
	}
	log.Println("Worker server started...")

	// Register Cron Jobs
	// "0 * * * * " means every hour on the hour.
	// "* * * * *" means every minute
	cronID, err := client.ScheduleCron(
		"0 * * * *",
		"hourly-report",
		TaskReportBase+"hourly",
		ReportPayload{ReportName: "Hourly"},
	)
	if err != nil {
		log.Printf("failed to schedule cron job with error: %v\n", err)
	} else {
		log.Println("cron task scheduled successfully, id", cronID)
	}

	jobs, _ := client.ListCronJobs()
	for _, job := range jobs {
		fmt.Printf(
			"Cron %d → next: %s\n",
			job.ID,
			job.NextRun.Format(time.DateTime),
		)
	}

	// Monitor Stats Loop
	go func() {
		ticker := time.NewTicker(15 * time.Second)
		for range ticker.C {
			stats, _ := client.Stats(ctx)
			fmt.Printf("--- Queue Stats ---\nPending: %d | Processing: %d | Failed: %d | Success: %d\n",
				stats.Pending, stats.Processing, stats.Failed, stats.Done)
		}
	}()

	// ---- Graceful shutdown ----
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	// Shutdown Worker Server first (stop processing new tasks)
	if err := server.Shutdown(shutdownCtx); err != nil {
		log.Printf("pgqueue: Server shutdown error: %v", err)
	}

	// Close Client (stop cron/maintenance)
	if err := client.Close(); err != nil {
		log.Printf("pgqueue: client close error: %v", err)
	}
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
