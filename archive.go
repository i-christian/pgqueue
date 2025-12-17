package pgqueue

import (
	"context"
	"fmt"
)

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
