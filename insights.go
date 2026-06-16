package pgqueue

import (
	"context"

	"github.com/i-christian/pgqueue/internal/pkg/queries"
)

// ListTasks returns a paginated list of tasks, optionally filtered by status.
func (c *Client) ListTasks(ctx context.Context, status *Status, page Pagination) ([]TaskInfo, error) {
	if page.Limit <= 0 {
		page.Limit = 50
	}

	var statusFilter *string
	if status != nil {
		s := string(*status)
		statusFilter = &s
	}

	rows, err := c.db.QueryContext(ctx, queries.ListTasks, statusFilter, page.Limit, page.Offset)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var tasks []TaskInfo
	for rows.Next() {
		var t TaskInfo
		var s string
		if err := rows.Scan(
			&t.ID, &t.CreatedAt, &t.Type, &s,
			&t.Attempts, &t.MaxRetries, &t.Priority,
			&t.NextRunAt, &t.LastError,
		); err != nil {
			return nil, err
		}
		t.Status = Status(s)
		tasks = append(tasks, t)
	}

	return tasks, rows.Err()
}

// ListCronJobs returns a paginated list of registered cron schedules.
func (c *Client) ListCronJobs(ctx context.Context, page Pagination) ([]CronJob, error) {
	if page.Limit <= 0 {
		page.Limit = 50
	}

	rows, err := c.db.QueryContext(ctx, queries.ListCronJobs, page.Limit, page.Offset)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var jobs []CronJob
	for rows.Next() {
		var j CronJob
		if err := rows.Scan(
			&j.ID, &j.Name, &j.Expression,
			&j.LastRunAt, &j.NextRunAt, &j.CreatedAt,
		); err != nil {
			return nil, err
		}
		jobs = append(jobs, j)
	}

	return jobs, rows.Err()
}

func (c *Client) Stats(ctx context.Context) (QueueStats, error) {
	rows, err := c.db.QueryContext(ctx, queries.GetQueueStats)
	if err != nil {
		return QueueStats{}, err
	}
	defer rows.Close()

	var s QueueStats
	for rows.Next() {
		var status string
		var count int
		if err := rows.Scan(&status, &count); err != nil {
			continue
		}
		
		s.Total += count
		
		switch Status(status) {
		case TaskPending:
			s.Pending = count
		case TaskProcessing:
			s.Processing = count
		case TaskFailed:
			s.Failed = count
		case TaskDone:
			s.Done = count
		}
	}
	return s, rows.Err()
}
