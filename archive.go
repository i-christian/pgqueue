package pgqueue

import "context"

// Archive cleans up old data from tasks table
func (q *Queue) Archive(ctx context.Context) error {
	_, err := q.db.ExecContext(ctx, `
		WITH moved_rows AS (
			DELETE FROM tasks 
				WHERE status IN ('done', 'failed') 
				AND updated_at < NOW() - INTERVAL '1 hour'
			RETURNING *
		)
		INSERT INTO tasks_archive SELECT * FROM moved_rows;
	`)
	return err
}
