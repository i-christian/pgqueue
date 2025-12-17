# pgqueue

## Description

A lightweight, asynchronous, durable, PostgreSQL-backed job queue for Go.

The motivation for this project was to understand how background job queues are built.
This is primarily a **learning project**, but aims to follow solid, production-style patterns.

`pgqueue` provides:

* Distributed-safe workers
* Priority jobs
* Delayed execution
* Automatic retries with backoff
* Cron jobs (run once across many servers)
* Deduplication

---

## Why pgqueue?

If you already use PostgreSQL, you don’t need Redis, SQS, or Kafka just to run background jobs.

**Postgres is already:**

* Durable
* Transactional
* Highly available

`pgqueue` uses standard SQL primitives to build a safe background job queue.

---

## Installation

```bash
go get github.com/i-christian/pgqueue
```

---

## Enqueue a Job

```go
type EmailPayload struct {
    Subject string `json:"subject"`
}

queue.Enqueue(ctx,
    "task:send:email",
    EmailPayload{Subject: "Welcome!"},
)
```

### With options

```go
queue.Enqueue(ctx,
    "task:send:email",
    payload,
    pgqueue.WithPriority(pgqueue.HighPriority),
    pgqueue.WithDelay(5*time.Minute),
    pgqueue.WithMaxRetries(10),
    pgqueue.WithDedup("email:user:123"),
)
```

---

## Start Workers (ServeMux + slog)

`pgqueue` uses a `ServeMux` to route tasks by type (similar to `http.ServeMux`).

```go
mux := pgqueue.NewServeMux()

mux.Use(pgqueue.SlogMiddleware(logger, metrics))

mux.HandleFunc("task:send:email", sendEmailHandler)
mux.HandleFunc("task:cleanup:", cleanupHandler) // prefix match
mux.HandleFunc("task:report:", reportHandler)

go queue.StartConsumer(ctx, 3, mux)
```

---

## ⚠️ NOTE: Bounded Task Types

Task types **must be bounded**.

✅ Good:

```
task:send:email
task:cleanup:expired-sessions
task:report:daily
```

❌ Bad (unbounded):

```
task:report:user:123
task:email:user:UUID
```

Why?

* Metrics and routing are keyed by task type or prefix
* Unbounded task types can cause **unbounded memory growth**

**Rule of thumb:**
Use task **categories**, not per-entity identifiers.

---

## Cron Jobs

```go
queue.ScheduleCron(
    "0 * * * *",
    "hourly-report",
    "task:report:hourly",
    ReportPayload{ReportName: "Hourly"},
)
```

---

## Retries & Backoff

* Exponential backoff: `2^attempts`
* Jitter added automatically
* Max retries configurable per job

---

## Queue Stats

```go
stats, _ := queue.Stats(ctx)
fmt.Println(stats.Pending, stats.Processing, stats.Failed)
```

---

## Examples

A complete, runnable example using:

* ServeMux
* slog logging
* priorities
* retries

➡️ **See the full example here:**
👉 [Examples](https://github.com/i-christian/pgqueue/tree/main/examples)

---

## Guarantees

✔ At-least-once execution
✔ No double-processing
✔ Safe concurrency
✔ Crash resilient

---

## When not to use this

* Ultra-low latency (<1ms)
* Massive fan-out (millions/sec)
* Cross-region replication
