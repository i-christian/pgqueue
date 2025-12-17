package pgqueue

import "time"

// EnqueueOption allows configuring options like delays or deduplication
type EnqueueOption func(*enqueueConfig)

type enqueueConfig struct {
	processAt  *time.Time
	dedupKey   *string
	priority   Priority
	maxRetries int
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
