# pgqueue

![GitHub go.mod Go version](https://img.shields.io/github/go-mod/go-version/i-christian/pgqueue)
[![License](https://img.shields.io/github/license/i-christian/pgqueue)](./LICENSE)
[![Project Status](https://img.shields.io/badge/status-learning--project-orange)](#)
[![Tests](https://github.com/i-christian/pgqueue/actions/workflows/ci.yml/badge.svg)](https://github.com/i-christian/pgqueue/actions/workflows/ci.yml)

**pgqueue** is a lightweight, asynchronous, durable, PostgreSQL-backed job queue for Go.

It is designed to be **simple**, **safe**, and **easy to reason about**, using only PostgreSQL and standard SQL.

---

## Features

* ✅ Distributed-safe workers
* ⏱ Delayed execution
* 🔁 Automatic retries with exponential backoff + jitter
* 🚦 Job priorities
* 🧠 Deduplication support
* ⏰ Cron jobs (run once across many servers)
* 📊 Queue metrics & stats
* 🪵 Structured logging (`slog` middleware)
* 💥 Crash-resilient, at-least-once delivery

---

## Why pgqueue?

If you already use PostgreSQL, you don’t need Redis, SQS, or Kafka **just to run background jobs**.

PostgreSQL is already:

* Durable
* Transactional
* Highly available
* Operationally familiar

`pgqueue` builds a background job queue using:

* `SELECT … FOR UPDATE SKIP LOCKED`
* Advisory locking semantics
* Transactions for correctness
* `LISTEN / NOTIFY` for fast wake-ups

---

## Architecture Overview
This diagram shows how producers, PostgreSQL, workers, and cron jobs interact inside **pgqueue**.
```mermaid
flowchart LR
    %% Nodes
    P["Producers<br/>queue.Enqueue()"]
    C["Cron Scheduler<br/>ScheduleCron()"]

    T["PostgreSQL<br/>tasks table"]
    A["tasks_archive"]
    N["LISTEN / NOTIFY"]

    W["Worker Pool<br/>StartConsumer(n)"]
    M["ServeMux"]
    H["Task Handlers"]
    R["Retry & Rescue"]

    %% Flows
    P --> T
    C --> T
    T --> N
    N --> W
    W --> M
    M --> H
    H -->|success| T
    H -->|failure| R
    R --> T
    T --> A

    %% Styles
    classDef producer fill:#E3F2FD,stroke:#1565C0,stroke-width:2px;
    classDef postgres fill:#E8F5E9,stroke:#2E7D32,stroke-width:2px;
    classDef worker fill:#FFF8E1,stroke:#EF6C00,stroke-width:2px;
    classDef handler fill:#F3E5F5,stroke:#6A1B9A,stroke-width:2px;

    class P,C producer;
    class T,A,N postgres;
    class W,M,R worker;
    class H handler;
````

---

## Installation

```bash
go get github.com/i-christian/pgqueue
```

---

## Initilise queue's client with options

```go
client, err := pgqueue.NewClient(
    db,
    pgqueue.WithRescueConfig(5*time.Minute, 30*time.Minute),
    pgqueue.WithCleanupConfig(1*time.Hour, 24*time.Hour, pgqueue.ArchiveStrategy),
    pgqueue.WithCronEnabled(),
)
if err != nil {
    log.Fatalf("Failed to init queue: %v", err)
}
```

## Enqueue a Job

```go
type EmailPayload struct {
    Subject string `json:"subject"`
}

client.Enqueue(
    ctx,
    "task:send:email",
    EmailPayload{Subject: "Welcome!"},
)
```

### Enqueue with Options

```go
client.Enqueue(
    ctx,
    "task:send:email",
    payload,
    pgqueue.WithPriority(pgqueue.HighPriority),
    pgqueue.WithDelay(5*time.Minute),
    pgqueue.WithMaxRetries(10),
    pgqueue.WithDedup("email:user:123"),
)
```

Supported options include:

* Priority
* Delayed execution
* Retry limits
* Deduplication keys

---

## Start Workers (ServeMux)

`pgqueue` uses a `ServeMux` to route tasks by type, similar to `http.ServeMux`.

```go
mux := pgqueue.NewServeMux()

// Middleware runs for every task
mux.Use(pgqueue.SlogMiddleware(client.Logger))

// Exact match
mux.HandleFunc("task:send:email", sendEmailHandler)

// Prefix match
mux.HandleFunc("task:cleanup:", cleanupHandler)
mux.HandleFunc("task:report:", reportHandler)

// Start worker pool
server := pgqueue.NewServer(db, connStr, 3, mux, pgqueue.WithBatchSize(20))
if err := server.Start(); err != nil {
    log.Fatal(err)
}
log.Println("Worker server started...")
```

---

## ⚠️ Bounded Task Types (Important)

Task types **must be bounded**.

### ✅ Good (bounded)

```
task:send:email
task:cleanup:expired-sessions
task:report:daily
```

### ❌ Bad (unbounded)

```
task:report:user:123
task:email:user:UUID
```

### Why this matters

* Routing is based on task type or prefix
* Metrics are keyed by task type
* Unbounded types can cause **unbounded memory growth**

**Rule of thumb:** Use task **categories**, not per-entity identifiers.

---

## Cron Jobs

Run scheduled jobs **once**, even when multiple workers or servers are running.

```go
cronID, err := client.ScheduleCron(
    "0 * * * *",
    "hourly-report",
    TaskReportBase+"hourly",
    ReportPayload{ReportName: "Hourly"},
)
if err != nil {
    log.Fatal(err)
}

jobs, _ := client.ListCronJobs()
for _, job := range jobs {
    fmt.Printf(
        "Cron %d → next: %s\n",
        job.ID,
        job.NextRun.Format(time.DateTime),
    )
}

// Optional cleanup
client.RemoveCron(cronID)
```

---

## Retries & Backoff

* At-least-once execution
* Automatic retries on failure
* Exponential backoff: `2^attempts`
* Jitter added to prevent thundering-herd effects
* Max retries configurable per job

---

## Queue Stats

```go
stats, _ := client.Stats(ctx)

fmt.Printf(
    "Pending: %d | Processing: %d | Failed: %d | Done: %d\n",
    stats.Pending,
    stats.Processing,
    stats.Failed,
    stats.Done,
)
```

---

## Examples

A complete, runnable example demonstrating:

* Worker pools
* ServeMux routing
* slog logging
* Priorities
* Retries
* Cron jobs

➡️ **See the full example here:**
👉 [Examples](https://github.com/i-christian/pgqueue/tree/main/examples)

---

## 🖥️ CLI Dashboard

**pgqueue-dash** is a high-performance Terminal User Interface (TUI) for monitoring your queue in real-time. It allows you to view processed tasks, inspect payloads, and retry failed tasks manually.

**Installation:**

```bash
go install github.com/i-christian/pgqueue/cmd/pgqueue-dash@latest
```

**Usage:**

```bash
pgqueue-dash --dsn="postgres://user:pass@localhost:5432/dbname"
```

👉 **[View Full Dashboard Documentation](https://github.com/i-christian/pgqueue/tree/main/cmd/pgqueue-dash/README.md)**

---

## Guarantees

pgqueue provides the following guarantees:

✔ **At-least-once execution**
✔ **No concurrent double-processing of the same task**
✔ **Safe concurrency across multiple workers and processes**
✔ **Crash resilience**

---

## When **Not** to Use pgqueue

pgqueue is not a replacement for high-throughput message brokers.

Avoid pgqueue if you need:

* Ultra-low latency (<1ms)
* Massive fan-out (millions of jobs per second)
* Cross-region replication
* Exactly-once semantics

---

## Testing

pgqueue uses PostgreSQL V18 for integration tests.

### Run tests locally (Docker required)

```bash
make test-full
```

This will:

* start a temporary PostgreSQL container
* run all tests with the race detector
* clean up automatically

### Run tests against an existing PostgreSQL instance

```bash
export TEST_DB_DSN="postgres://user:pass@localhost:5432/task_queue_test?sslmode=disable"
go test -v ./...
```

### Benchmarks

```bash
make bench
```

Benchmarks run against a fresh PostgreSQL container and are intended for local performance exploration only.

---

## Contributing

Contributions are welcome! Here’s how you can help:

* 🐛 **Report bugs** by opening issues
* 💡 **Suggest features** via GitHub discussions or issues
* ✍️ **Submit pull requests** with clear descriptions
* 📝 **Update documentation** and examples

Please follow standard Golang conventions and run `make test-full` before submitting any PR to ensure nothing breaks.
