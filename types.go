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
