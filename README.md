# pgqueue

## Description
A lightweight, durable, PostgreSQL-backed job queue for Go. The motivation for this project was just to figure out how background queues are implemented. So this is just a `learning project`.

`pgqueue` provides:

* Distributed-safe workers
* Priority jobs
* Delayed execution
* Automatic retries with backoff
* Cron jobs (run once across many servers)
* Deduplication

---

## Why pgqueue?

If you already have PostgreSQL, you don’t need Redis, SQS, or Kafka to increase your tech stack just to run background jobs.

**Postgres is already:**

* Durable
* Transactional
* Highly available

`pgqueue` uses standard SQL primitives to build a safe background jobs queue.

---

## Installation

```bash
go get github.com/i-christian/pgqueue
```

---

## Enqueue a Job

```go
queue.Enqueue(ctx, EmailPayload{
    Subject: "Welcome!",
})
```

### With options

```go
queue.Enqueue(ctx, payload,
    pgqueue.WithPriority(10),
    pgqueue.WithDelay(5*time.Minute),
    pgqueue.WithMaxRetries(10),
    pgqueue.WithDedup("email:user:123"),
)
```

---

## Start Workers

```go
queue.StartConsumer(ctx, 5, func(ctx context.Context, task pgqueue.Task) error {
    var p EmailPayload
    json.Unmarshal(task.Payload, &p)

    sendEmail(p.Subject)
    return nil
})
```

---

## Cron Jobs

```go
queue.ScheduleCron("0 * * * *", "hourly-report", ReportPayload{})
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

fmt.Println(stats.Pending, stats.Processing)
```

---

## Archiving

Move completed jobs out of the hot queue:

```go
queue.Archive(ctx)
```

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
