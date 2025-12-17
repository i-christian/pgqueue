package pgqueue

import (
	"context"
	"fmt"
	"time"
)

// runMaintenanceLoop handles all background system jobs
func (q *Queue) runMaintenanceLoop() {
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
		q.logger.Info("Internal Cleanup started", "strategy", q.config.cleanupStrategy)
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
			count, err := q.rescueStuckTasks(q.ctx, q.config.rescueVisibility)
			if err != nil {
				q.logger.Error("Rescue failed", "error", err)
			} else if count > 0 {
				q.logger.Info("Rescued stuck tasks", "count", count)
			}

		case <-cleanupTicker.C:
			if err := q.runCleanup(q.ctx); err != nil {
				q.logger.Error("Cleanup failed", "error", err)
			}
		}
	}
}

// rescueStuckTasks finds tasks that have been 'processing' for too long
// and resets them to 'pending' so they can be picked up again.
func (q *Queue) rescueStuckTasks(ctx context.Context, timeout time.Duration) (int64, error) {
	query := `
        UPDATE tasks
        SET status = 'pending',
            updated_at = NOW(),
            next_run_at = NOW(),
            attempts = attempts + 1,
            last_error = 'detected stuck task; resetting'
        WHERE status = 'processing'
          AND updated_at < NOW() - ($1 * INTERVAL '1 seconds')
          AND attempts < max_retries
    `
	res, err := q.db.ExecContext(ctx, query, timeout.Seconds())
	if err != nil {
		return 0, err
	}

	return res.RowsAffected()
}

// runCleanup executes the cleanup strategy defined in configuration
func (q *Queue) runCleanup(ctx context.Context) error {
	retentionSeconds := q.config.cleanupRetention.Seconds()

	if q.config.cleanupStrategy == DeleteStrategy {
		query := `
			DELETE FROM tasks 
				WHERE status IN ('done', 'failed') 
				AND updated_at < NOW() - ($1 * INTERVAL '1 seconds')
		`
		res, err := q.db.ExecContext(ctx, query, retentionSeconds)
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
	res, err := q.db.ExecContext(ctx, query, retentionSeconds)
	if err != nil {
		return fmt.Errorf("failed to archive tasks: %w", err)
	}

	rows, _ := res.RowsAffected()
	if rows > 0 {
		q.logger.Info("Archived old tasks", "count", rows)
	}

	return nil
}
