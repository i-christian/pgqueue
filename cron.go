package pgqueue

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/robfig/cron/v3"
)

// ScheduleCron registers a recurring job.
func (q *Queue) ScheduleCron(
	spec string,
	jobName string,
	task TaskType,
	payload any,
) (CronID, error) {
	if q.scheduler == nil {
		return 0, errors.New("cron is disabled")
	}

	id, err := q.scheduler.AddFunc(spec, func() {
		now := time.Now().Truncate(time.Minute)
		dedupKey := fmt.Sprintf("%s:%s", jobName, now.Format(time.RFC3339))

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		if err := q.Enqueue(ctx, task, payload, WithDedup(dedupKey)); err != nil {
			q.logger.Error("cron enqueue error:", "job", jobName, "error", err)
		}
	})
	if err != nil {
		return 0, err
	}

	return CronID(id), nil
}

// ListCronJobs returns a list of scheduled tasks
func (q *Queue) ListCronJobs() ([]CronJobInfo, error) {
	if q.scheduler == nil {
		return nil, errors.New("cron is disabled")
	}

	entries := q.scheduler.Entries()
	jobs := make([]CronJobInfo, 0, len(entries))

	for _, e := range entries {
		jobs = append(jobs, CronJobInfo{
			ID:      CronID(e.ID),
			NextRun: e.Next,
			PrevRun: e.Prev,
		})
	}

	return jobs, nil
}

// RemoveCron removes a scheduled task from cron
func (q *Queue) RemoveCron(id CronID) error {
	if q.scheduler == nil {
		return errors.New("cron is disabled")
	}

	q.scheduler.Remove(cron.EntryID(id))
	return nil
}
