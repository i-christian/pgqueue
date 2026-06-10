package pgqueue

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/i-christian/pgqueue/internal/pkg/stringutils"
	"github.com/robfig/cron/v3"
)

// ScheduleCron registers a recurring job.
func (c *Client) ScheduleCron(
	spec string,
	jobName string,
	task TaskType,
	payload any,
) (CronID, error) {
	if c.queue.scheduler == nil {
		return 0, errors.New("cron is disabled")
	}

	schedule, err := cron.ParseStandard(spec)
	if err != nil {
		return 0, fmt.Errorf("invalid cron spec: %w", err)
	}

	now := time.Now()
	nextRun := schedule.Next(now)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	newID, cTime, err := stringutils.NewUUIDv7()
	if err != nil {
		return 0, err
	}

	var jobID string
	err = c.db.QueryRowContext(ctx, `
		INSERT INTO pgqueue.cron_jobs (job_id, name, expression, next_run_at, created_at)
		VALUES ($1, $2, $3, $4, $5)
		ON CONFLICT (name) DO UPDATE
		SET expression = EXCLUDED.expression,
		    next_run_at = EXCLUDED.next_run_at
		RETURNING job_id
	`, newID, jobName, spec, nextRun, cTime).Scan(&jobID)
	if err != nil {
		return 0, fmt.Errorf("failed to persist cron job: %w", err)
	}

	entryID, err := c.queue.scheduler.AddFunc(spec, func() {
		runCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		now := time.Now().Truncate(time.Minute)
		dedupKey := fmt.Sprintf("%s:%s", jobID, now.Format(time.RFC3339))

		enqueueErr := c.Enqueue(
			runCtx,
			task,
			payload,
			WithDedup(dedupKey),
		)

		next := schedule.Next(time.Now())

		status := "success"
		var errMsg sql.NullString

		if enqueueErr != nil {
			status = "failed"
			errMsg = sql.NullString{
				String: enqueueErr.Error(),
				Valid:  true,
			}
			c.Logger.Error(
				"cron enqueue failed",
				"job", jobName,
				"error", errMsg,
				"status", status,
			)
		}

		_, dbErr := c.db.ExecContext(runCtx, `
			UPDATE pgqueue.cron_jobs
			SET last_run_at = NOW(),
			    next_run_at = $1
			WHERE job_id = $2
		`, next, jobID)

		if dbErr != nil {
			c.Logger.Error(
				"cron metadata sync failed",
				"job", jobName,
				"error", dbErr,
			)
		}
	})
	if err != nil {
		return 0, err
	}

	return CronID(entryID), nil
}

// ListCronJobs returns a list of scheduled tasks
func (c *Client) ListCronJobs() ([]CronJobInfo, error) {
	if c.queue.scheduler == nil {
		return nil, errors.New("cron is disabled")
	}

	entries := c.queue.scheduler.Entries()
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
func (c *Client) RemoveCron(id CronID, jobID string) error {
	if c.queue.scheduler == nil {
		return errors.New("cron is disabled")
	}

	c.queue.scheduler.Remove(cron.EntryID(id))
	_, err := c.db.Exec("DELETE FROM pgqueue.cron_jobs WHERE job_id = $1", jobID)

	return err
}
