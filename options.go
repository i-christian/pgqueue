package pgqueue

import "time"

// WithCronEnabled enables cron jobs functionality.
//
// Cron jobs are disabled by default.
func WithCronEnabled() QueueOption {
	return func(c *queueConfig) {
		c.cronEnabled = true
	}
}

// WithRescueConfig configures the automatic stuck task rescue.
//
// params:
//   - interval: how often to check for stuck tasks.
//   - visibilityTimeout: how long a task can stay 'processing' before being reset.
func WithRescueConfig(interval, visibilityTimeout time.Duration) QueueOption {
	return func(c *queueConfig) {
		c.rescueEnabled = true
		c.rescueInterval = interval
		c.rescueVisibility = visibilityTimeout
	}
}

// WithCleanupConfig configures automatic removal of old data.
//
// params:
//   - retentionMonths: how many months of data to keep active in the queue.
//   - strategy: pgqueue.DeleteStrategy (drops) or pgqueue.ArchiveStrategy (detaches).
func WithCleanupConfig(retentionMonths int, strategy CleanupStrategy) QueueOption {
	return func(c *queueConfig) {
		c.cleanupEnabled = true
		c.cleanupRetentionMonths = retentionMonths
		c.cleanupStrategy = strategy
	}
}

// defaultQueueConfig provides sensible defaults.
func defaultQueueConfig() queueConfig {
	return queueConfig{
		cronEnabled: false,

		rescueEnabled:    true,
		rescueInterval:   5 * time.Minute,
		rescueVisibility: 20 * time.Minute,

		cleanupEnabled:         false,
		cleanupInterval:        24 * time.Hour,
		cleanupRetentionMonths: 2,
		cleanupStrategy:        DeleteStrategy,
	}
}

// WithPriority sets the priority
func WithPriority(p Priority) EnqueueOption {
	return func(c *enqueueConfig) {
		c.priority = p
	}
}

// WithMaxRetries overrides the default retry count (default is 5)
func WithMaxRetries(n int) EnqueueOption {
	return func(c *enqueueConfig) {
		c.maxRetries = n
	}
}

// WithDelay schedules the task to run in the future
func WithDelay(d time.Duration) EnqueueOption {
	return func(c *enqueueConfig) {
		t := time.Now().Add(d)
		c.processAt = &t
	}
}

// WithDedup ensures a task with this key is only enqueued once
func WithDedup(key string) EnqueueOption {
	return func(c *enqueueConfig) {
		c.dedupKey = &key
	}
}

// ServerOption configures a worker Server.
//
// Server options control how workers fetch and process tasks,
// such as batch size or concurrency-related behavior.
type ServerOption func(*Server)

// WithBatchSize configures how many tasks a worker fetches per database round-trip.
//
// A larger batch size increases throughput by reducing database transactions,
// but may reduce fairness between workers(starvation of goroutines) and increase the number of tasks
// locked by a single worker.
//
// Sensible values typically range from 5 to 20.
// The default batch size is 10.
func WithBatchSize(n uint16) ServerOption {
	return func(s *Server) {
		if n == 0 {
			return
		}
		s.batchSize = n
	}
}
