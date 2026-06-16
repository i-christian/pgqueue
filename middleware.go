package pgqueue

import (
	"context"
	"fmt"
	"log/slog"
	"runtime/debug"
	"time"
)

// slogMiddleware logs task lifecycle events.
func slogMiddleware(logger *slog.Logger) Middleware {
	return func(next WorkerHandler) WorkerHandler {
		return HandlerFunc(func(ctx context.Context, task *Task) error {
			start := time.Now()

			logger.Info(
				"task started",
				slog.String("task_id", task.ID.String()),
				slog.String("task_type", string(task.Type)),
				slog.String("priority", task.Priority.String()),
				slog.Int("attempts", task.Attempts),
			)

			err := next.ProcessTask(ctx, task)

			elapsed := time.Since(start)

			if err != nil {
				logger.Error(
					"task failed",
					slog.String("task_id", task.ID.String()),
					slog.String("task_type", string(task.Type)),
					slog.Duration("duration", elapsed),
					slog.Any("error", err),
				)
				return err
			}

			logger.Info(
				"task completed",
				slog.String("task_id", task.ID.String()),
				slog.String("task_type", string(task.Type)),
				slog.Duration("duration", elapsed),
			)

			return nil
		})
	}
}

// recoverMiddleware catches panics in task handlers and logs full stack traces.
func recoverMiddleware() Middleware {
	return func(next WorkerHandler) WorkerHandler {
		return HandlerFunc(func(ctx context.Context, task *Task) (err error) {
			defer func() {
				if r := recover(); r != nil {
					stack := debug.Stack()

					slog.Default().Error(
						"panic recovered in task handler",
						slog.String("task_id", task.ID.String()),
						slog.String("task_type", string(task.Type)),
						slog.Any("panic", r),
						slog.String("stacktrace", string(stack)),
					)

					err = fmt.Errorf(
						"panic in task %s: %v",
						task.Type,
						r,
					)
				}
			}()

			return next.ProcessTask(ctx, task)
		})
	}
}
