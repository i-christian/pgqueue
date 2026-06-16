package queries

import (
	"context"
	"database/sql"
	"fmt"
)

// Prepared holds the compiled SQL statements for high-frequency operations.
type Prepared struct {
	EnqueueTask          *sql.Stmt
	FetchBatch           *sql.Stmt
	MarkTaskDone         *sql.Stmt
	MarkTaskFailed       *sql.Stmt
	RescheduleTask       *sql.Stmt
	GetQueueStats        *sql.Stmt
	RescueStuckTasks     *sql.Stmt
	ListCronJobs         *sql.Stmt
	ListTasks            *sql.Stmt
	UpsertCronJob        *sql.Stmt
	UpdateCronJobRunMeta *sql.Stmt
	DeleteCronJob        *sql.Stmt
}

// NewPrepared compiles all high-frequency queries against the database pool.
func NewPrepared(ctx context.Context, db *sql.DB) (*Prepared, error) {
	var err error
	p := &Prepared{}

	prepare := func(query string) *sql.Stmt {
		if err != nil {
			return nil
		}

		stmt, e := db.PrepareContext(ctx, query)
		if e != nil {
			err = fmt.Errorf("failed to prepare query: %w", e)
			return nil
		}
		return stmt
	}

	p.EnqueueTask = prepare(EnqueueTask)
	p.FetchBatch = prepare(FetchBatch)
	p.MarkTaskDone = prepare(MarkTaskDone)
	p.MarkTaskFailed = prepare(MarkTaskFailed)
	p.RescheduleTask = prepare(RescheduleTask)
	p.GetQueueStats = prepare(GetQueueStats)
	p.RescueStuckTasks = prepare(RescueStuckTasks)
	p.ListCronJobs = prepare(ListCronJobs)
	p.ListTasks = prepare(ListTasks)
	p.UpsertCronJob = prepare(UpsertCronJob)
	p.UpdateCronJobRunMeta = prepare(UpdateCronJobRunMeta)
	p.DeleteCronJob = prepare(DeleteCronJob)

	if err != nil {
		_ = p.Close()
		return nil, err
	}

	return p, nil
}

// Close closes all prepared statements.
func (p *Prepared) Close() error {
	stmts := []*sql.Stmt{
		p.EnqueueTask,
		p.FetchBatch,
		p.MarkTaskDone,
		p.MarkTaskFailed,
		p.RescheduleTask,
		p.GetQueueStats,
		p.RescueStuckTasks,
		p.ListCronJobs,
		p.ListTasks,
		p.UpsertCronJob,
		p.UpdateCronJobRunMeta,
		p.DeleteCronJob,
	}

	var firstErr error
	for _, stmt := range stmts {
		if stmt != nil {
			if err := stmt.Close(); err != nil && firstErr == nil {
				firstErr = err
			}
		}
	}
	return firstErr
}
