package pgqueue

import (
	"context"
	"database/sql"
	"encoding/json"
	"time"

	"github.com/google/uuid"
	"github.com/robfig/cron/v3"
)

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

type Queue struct {
	db         *sql.DB
	connString string
	scheduler  *cron.Cron
}

type Task struct {
	ID         uuid.UUID
	Type       TaskType
	Payload    json.RawMessage
	Attempts   int
	MaxRetries int
	Priority   int
	CreatedAt  time.Time
}

type QueueStats struct {
	Pending    int
	Processing int
	Failed     int
	Done       int
	Total      int
}

type WorkerHandler func(ctx context.Context, taskType TaskType, task Task) error
