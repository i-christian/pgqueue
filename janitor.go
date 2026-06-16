package pgqueue

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/i-christian/pgqueue/internal/pkg/queries"
)

// runMaintenanceLoop handles all background system jobs
func (q *Queue) runMaintenanceLoop(db *sql.DB, stmts *queries.Prepared) {
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
			_, err := db.ExecContext(q.ctx, queries.EnsurePartitions)
			if err != nil {
				q.logger.Error("Partition maintenance failed", "error", err)
			}

		case <-rescueTicker.C:
			count, err := q.rescueStuckTasks(q.ctx, q.config.rescueVisibility, stmts)
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
func (q *Queue) rescueStuckTasks(ctx context.Context, timeout time.Duration, stmts *queries.Prepared) (int64, error) {
	res, err := stmts.RescueStuckTasks.ExecContext(ctx,
		timeout.Seconds(),
		TaskFailed,
		TaskProcessing,
		TaskPending,
	)
	if err != nil {
		return 0, err
	}

	return res.RowsAffected()
}

// runCleanup executes the cleanup strategy defined in configuration
func (q *Queue) runCleanup(ctx context.Context, db *sql.DB) error {
	retentionMonths := max(q.config.cleanupRetentionMonths, 1)

	doDelete := q.config.cleanupStrategy == DeleteStrategy

	var processedCount int
	err := db.QueryRowContext(ctx, queries.ManageOldPartitions, retentionMonths, doDelete).Scan(&processedCount)
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
