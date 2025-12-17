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

const TaskSendEmail = "task:send:email"

type EmailPayload struct {
	Subject string `json:"subject"`
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
