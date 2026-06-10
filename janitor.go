package pgqueue

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

// runMaintenanceLoop handles all background system jobs
func (q *Queue) runMaintenanceLoop(db *sql.DB) {
	defer q.wg.Done()

	var rescueTicker *time.Ticker
	var cleanupTicker *time.Ticker
	partitionTicker := time.NewTicker(24 * time.Hour)

	if q.config.rescueEnabled {
		rescueTicker = time.NewTicker(q.config.rescueInterval)
		q.logger.Info("Internal Rescue started")
	} else {
		rescueTicker = time.NewTicker(24 * time.Hour)
		rescueTicker.Stop()
	}

	if q.config.cleanupEnabled {
		cleanupTicker = time.NewTicker(q.config.cleanupInterval)
		q.logger.Info("Internal Cleanup started", "strategy", q.config.cleanupStrategy.String())
	} else {
		cleanupTicker = time.NewTicker(24 * time.Hour)
		cleanupTicker.Stop()
	}

	defer func() {
		if rescueTicker != nil {
			rescueTicker.Stop()
		}
		if cleanupTicker != nil {
			cleanupTicker.Stop()
		}
	}()

	for {
		select {
		case <-q.ctx.Done():
			return

		case <-partitionTicker.C:
			_, err := db.ExecContext(q.ctx, `
				SELECT pgqueue.ensure_partition('pgqueue.tasks', 0);
				SELECT pgqueue.ensure_partition('pgqueue.tasks', 1);
			`)
			if err != nil {
				q.logger.Error("Partition maintenance failed", "error", err)
			}

		case <-rescueTicker.C:
			count, err := q.rescueStuckTasks(q.ctx, q.config.rescueVisibility, db)
			if err != nil {
				q.logger.Error("Rescue failed", "error", err)
			} else if count > 0 {
				q.logger.Info("Rescued stuck tasks", "count", count)
			}

		case <-cleanupTicker.C:
			if err := q.runCleanup(q.ctx, db); err != nil {
				q.logger.Error("Cleanup failed", "error", err)
			}
		}
	}
}

// rescueStuckTasks finds tasks that have been 'processing' for too long
// and resets them to 'pending', or marks them failed if retries are exhausted.
func (q *Queue) rescueStuckTasks(ctx context.Context, timeout time.Duration, db *sql.DB) (int64, error) {
	query := `
		UPDATE pgqueue.tasks
		SET
			status = CASE
				WHEN attempts >= max_retries THEN 'failed'
				WHEN status = 'processing' THEN 'pending'
				ELSE status
			END,
			updated_at = NOW(),
			next_run_at = CASE
				WHEN status = 'processing' AND attempts < max_retries THEN NOW()
				ELSE next_run_at
			END,
			attempts = CASE
				WHEN status = 'processing' AND attempts < max_retries THEN attempts + 1
				ELSE attempts
			END,
			last_error = CASE
				WHEN status = 'processing' AND attempts < max_retries
				THEN 'detected stuck task; resetting'
				ELSE last_error
			END
		WHERE
			attempts >= max_retries
			OR (
				status = 'processing'
				AND attempts < max_retries
				AND updated_at < NOW() - ($1 * INTERVAL '1 seconds')
			);
	`

	res, err := db.ExecContext(ctx, query, timeout.Seconds())
	if err != nil {
		return 0, err
	}

	return res.RowsAffected()
}

// runCleanup executes the cleanup strategy defined in configuration
func (q *Queue) runCleanup(ctx context.Context, db *sql.DB) error {
	retentionMonths := max(q.config.cleanupRetentionMonths, 1)

	doDelete := q.config.cleanupStrategy == DeleteStrategy

	query := `SELECT pgqueue.manage_old_partitions('tasks', $1, $2);`

	var processedCount int
	err := db.QueryRowContext(ctx, query, retentionMonths, doDelete).Scan(&processedCount)
	if err != nil {
		return fmt.Errorf("failed to process old partitions: %w", err)
	}

	if processedCount > 0 {
		action := "Archived (detached)"
		if doDelete {
			action = "Deleted (dropped)"
		}
		q.logger.Info("Maintenance complete on old task partitions",
			"action", action,
			"count", processedCount,
			"retention_months", retentionMonths,
		)
	}

	return nil
}
