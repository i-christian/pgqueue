package pgqueue

import (
	"context"
	"fmt"
	"log"
	"time"
)

// ScheduleCron registers a recurring job.
func (q *queue) ScheduleCron(spec string, jobName string, task TaskType, payload any) error {
	_, err := q.scheduler.AddFunc(spec, func() {
		now := time.Now().Truncate(time.Minute)
		dedupKey := fmt.Sprintf("%s:%s", jobName, now.Format(time.RFC3339))

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		err := q.Enqueue(ctx, task, payload, WithDedup(dedupKey))
		if err != nil {
			log.Printf("Cron error enqueuing %s: %v", jobName, err)
		}
	})
	return err
}
