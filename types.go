package pgqueue

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"log/slog"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/robfig/cron/v3"
)

var ErrHandlerNotFound = errors.New("handler not found for task")

const (
	TaskDone       = "done"
	TaskPending    = "pending"
	TaskProcessing = "processing"
	TaskFailed     = "failed"
)

type (
	Priority int
	TaskType string
)

const (
	HighPriority    Priority = 6
	DefaultPriority Priority = 3
	LowPriority     Priority = 1
)

func (p Priority) String() string {
	switch p {
	case HighPriority:
		return "high"
	case DefaultPriority:
		return "default"
	case LowPriority:
		return "low"
	default:
		return "unknown"
	}
}

type Queue struct {
	db         *sql.DB
	connString string
	scheduler  *cron.Cron
	logger     *slog.Logger

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
	config queueConfig
}

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

func (c CleanupStrategy) String() string {
	switch c {
	case DeleteStrategy:
		return "Delete old tasks"
	case ArchiveStrategy:
		return "Archive old tasks"
	default:
		return "unknown"
	}
}

type queueConfig struct {
	cronEnabled bool

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

type Task struct {
	ID         uuid.UUID
	Type       TaskType
	Payload    json.RawMessage
	Attempts   int
	MaxRetries int
	Priority   Priority
	CreatedAt  time.Time
}

type QueueStats struct {
	Pending    int
	Processing int
	Failed     int
	Done       int
	Total      int
}

type (
	CronID      int
	CronJobInfo struct {
		ID      CronID
		NextRun time.Time
		PrevRun time.Time
	}
)

// WorkerHandler processes tasks.
//
// ProcessTask should return nil if the processing of a task is successful.
type WorkerHandler interface {
	ProcessTask(context.Context, *Task) error
}

// Middleware wraps a WorkerHandler with extra behavior.
type Middleware func(WorkerHandler) WorkerHandler

// The HandlerFunc type is an adapter to allow the use of
// ordinary functions as a Handler. If f is a function
// with the appropriate signature, HandlerFunc(f) is a
// Handler that calls f.
type HandlerFunc func(context.Context, *Task) error

// ProcessTask calls fn(ctx, task)
func (fn HandlerFunc) ProcessTask(ctx context.Context, task *Task) error {
	return fn(ctx, task)
}
