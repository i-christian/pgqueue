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
		UPDATE tasks
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
	retentionSeconds := q.config.cleanupRetention.Seconds()

	if q.config.cleanupStrategy == DeleteStrategy {
		query := `
			DELETE FROM tasks 
				WHERE status IN ('done', 'failed') 
				AND updated_at < NOW() - ($1 * INTERVAL '1 seconds')
		`
		res, err := db.ExecContext(ctx, query, retentionSeconds)
		if err != nil {
			return fmt.Errorf("failed to delete old tasks: %w", err)
		}
		rows, _ := res.RowsAffected()
		if rows > 0 {
			q.logger.Info("Pruned old tasks", "count", rows)
		}
		return nil
	}

	query := `
		WITH moved_rows AS (
			DELETE FROM tasks 
			WHERE status IN ('done', 'failed') 
			AND updated_at < NOW() - ($1 * INTERVAL '1 seconds')
			RETURNING *
		)
		INSERT INTO tasks_archive 
		SELECT * FROM moved_rows;
	`
	res, err := db.ExecContext(ctx, query, retentionSeconds)
	if err != nil {
		return fmt.Errorf("failed to archive tasks: %w", err)
	}

	rows, _ := res.RowsAffected()
	if rows > 0 {
		q.logger.Info("Archived old tasks", "count", rows)
	}

	return nil
}
