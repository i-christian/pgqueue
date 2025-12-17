package pgqueue

import (
	"context"
	"fmt"
	"log/slog"
	"time"
)

func SlogMiddleware(logger *slog.Logger, metrics *Metrics) Middleware {
	return func(next WorkerHandler) WorkerHandler {
		return HandlerFunc(func(ctx context.Context, task *Task) error {
			start := time.Now()

			metrics.RecordStart(task.Priority, task.Type)

			logger.Info(
				"task started",
				slog.String("task_id", task.ID.String()),
				slog.String("task_type", string(task.Type)),
				slog.String("priority", task.Priority.String()),
				slog.Int("priority_value", int(task.Priority)),
				slog.Int("attempts", task.Attempts),
			)

			err := next.ProcessTask(ctx, task)

			elapsed := time.Since(start)

			if err != nil {
				metrics.RecordFailure(task.Priority, task.Type, elapsed)

				logger.Error(
					"task failed",
					slog.String("task_id", task.ID.String()),
					slog.String("task_type", string(task.Type)),
					slog.String("priority", task.Priority.String()),
					slog.Duration("duration", elapsed),
					slog.Any("error", err),
				)
				return err
			}

			metrics.RecordSuccess(task.Priority, task.Type, elapsed)

			logger.Info(
				"task completed",
				slog.String("task_id", task.ID.String()),
				slog.String("task_type", string(task.Type)),
				slog.String("priority", task.Priority.String()),
				slog.Duration("duration", elapsed),
			)

			return nil
		})
	}
}

func recoverMiddleware() Middleware {
	return func(next WorkerHandler) WorkerHandler {
		return HandlerFunc(func(ctx context.Context, task *Task) (err error) {
			defer func() {
				if r := recover(); r != nil {
					err = fmt.Errorf("panic in task %s: %v", task.Type, r)
				}
			}()
			return next.ProcessTask(ctx, task)
		})
	}
}
