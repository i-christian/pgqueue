package pgqueue

import (
	"context"
	"fmt"
	"log"
	"time"
)

func LoggingMiddleware(logger *log.Logger) Middleware {
	return func(next WorkerHandler) WorkerHandler {
		return HandlerFunc(func(ctx context.Context, task *Task) error {
			start := time.Now()

			logger.Printf(
				"[START] priority=%v, task=%s attempts=%d",
				task.Priority,
				task.Type,
				task.Attempts,
			)

			err := next.ProcessTask(ctx, task)

			elapsed := time.Since(start)

			if err != nil {
				logger.Printf(
					"[ERROR] task=%s duration=%s err=%v",
					task.Type,
					elapsed,
					err,
				)
			} else {
				logger.Printf(
					"[DONE] task=%s duration=%s",
					task.Type,
					elapsed,
				)
			}

			return err
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
