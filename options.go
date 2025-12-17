package pgqueue

import "time"

type enqueueConfig struct {
	processAt  *time.Time
	dedupKey   *string
	priority   Priority
	maxRetries int
}

// EnqueueOption allows configuring options like delays or deduplication
type EnqueueOption func(*enqueueConfig)

type CleanupStrategy int

const (
	// DeleteStrategy hard deletes old tasks.
	DeleteStrategy CleanupStrategy = iota
	// ArchiveStrategy moves old tasks to the tasks_archive table.
	ArchiveStrategy
)

type queueConfig struct {
	rescueEnabled    bool
	rescueInterval   time.Duration
	rescueVisibility time.Duration

	cleanupEnabled   bool
	cleanupStrategy  CleanupStrategy
	cleanupInterval  time.Duration
	cleanupRetention time.Duration
}

// QueueOption is a function that modifies the queue configuration.
type QueueOption func(*queueConfig)

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
//   - interval: how often to run the cleanup job.
//   - retention: how old a 'done'/'failed' task must be to be removed.
//   - strategy: either pgqueue.DeleteStrategy or pgqueue.ArchiveStrategy.
func WithCleanupConfig(interval, retention time.Duration, strategy CleanupStrategy) QueueOption {
	return func(c *queueConfig) {
		c.cleanupEnabled = true
		c.cleanupInterval = interval
		c.cleanupRetention = retention
		c.cleanupStrategy = strategy
	}
}

// defaultQueueConfig provides sensible defaults.
func defaultQueueConfig() queueConfig {
	return queueConfig{
		rescueEnabled:    true,
		rescueInterval:   1 * time.Minute,
		rescueVisibility: 10 * time.Minute,

		cleanupEnabled:   false,
		cleanupInterval:  1 * time.Hour,
		cleanupRetention: 24 * time.Hour,
		cleanupStrategy:  DeleteStrategy,
	}
}

// WithPriority sets the priority (e.g., 10 for Critical, 0 for Default)
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
