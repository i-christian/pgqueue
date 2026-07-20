package queries

const (
	EnsurePartitions = `
		SELECT pgqueue.ensure_partition('pgqueue.tasks', 0);
		SELECT pgqueue.ensure_partition('pgqueue.tasks', 1);
	`

	EnqueueTask = `
		INSERT INTO pgqueue.tasks (
			task_id, created_at, task_type, priority, 
			max_retries, payload, next_run_at, deduplication_key
		) 
		SELECT $1, $2, $3, $4, $5, $6, $7, $8
		WHERE $8::text IS NULL OR NOT EXISTS (
			SELECT 1 FROM pgqueue.tasks 
			WHERE deduplication_key = $8
		)
	`

	FetchBatch = `
		UPDATE pgqueue.tasks
		SET status = $1,
		    attempts = attempts + 1,
		    updated_at = NOW()
		WHERE (task_id, created_at) IN (
			SELECT task_id, created_at
			FROM pgqueue.tasks
			WHERE status = $2
			  AND next_run_at <= NOW()
			ORDER BY priority DESC, next_run_at ASC
			FOR UPDATE SKIP LOCKED
			LIMIT $3
		)
		RETURNING task_id, created_at, task_type, payload, attempts, max_retries, priority
	`

	MarkTaskDone = `
		UPDATE pgqueue.tasks
		SET status = $3,
		    updated_at = NOW()
		WHERE task_id = $1 AND created_at = $2
	`

	MarkTaskFailed = `
		UPDATE pgqueue.tasks
		SET status = $4, last_error = $1
		WHERE task_id = $2 AND created_at = $3
	`

	RescheduleTask = `
		UPDATE pgqueue.tasks
		SET status = $5,
		    next_run_at = NOW() + (
		        $1 * CASE
		            WHEN $6 = true THEN INTERVAL '1 millisecond'
		            ELSE INTERVAL '1 second'
		        END
		    ),
		    last_error = $2
		WHERE task_id = $3 AND created_at = $4
	`

	RetryTask = `
		UPDATE pgqueue.tasks
		SET status = 'pending',
		    attempts = 0,
		    last_error = NULL,
		    next_run_at = NOW(),
		    updated_at = NOW()
		WHERE task_id = $1 AND created_at = $2
	`

	GetQueueStats = `SELECT status, count(*) FROM pgqueue.tasks GROUP BY status`

	UpsertCronJob = `
		INSERT INTO pgqueue.cron_jobs (job_id, name, expression, next_run_at, created_at)
		VALUES ($1, $2, $3, $4, $5)
		ON CONFLICT (name) DO UPDATE
			SET
				expression = EXCLUDED.expression,
				next_run_at = EXCLUDED.next_run_at
		RETURNING job_id
	`

	UpdateCronJobRunMeta = `
		UPDATE pgqueue.cron_jobs
		SET
			last_run_at = NOW(),
			next_run_at = $1
		WHERE job_id = $2
	`

	DeleteCronJob = `DELETE FROM pgqueue.cron_jobs WHERE job_id = $1`

	RescueStuckTasks = `
		UPDATE pgqueue.tasks
		SET
			status = CASE
				WHEN attempts >= max_retries THEN $2
				WHEN status = $3 THEN $4
				ELSE status
			END,
			updated_at = NOW(),
			next_run_at = CASE
				WHEN status = $3 AND attempts < max_retries THEN NOW()
				ELSE next_run_at
			END,
			attempts = CASE
				WHEN status = $3 AND attempts < max_retries THEN attempts + 1
				ELSE attempts
			END,
			last_error = CASE
				WHEN status = $3 AND attempts < max_retries
				THEN 'detected stuck task; resetting'
				ELSE last_error
			END
		WHERE
			attempts >= max_retries
			OR (
				status = $3
				AND attempts < max_retries
				AND updated_at < NOW() - ($1 * INTERVAL '1 seconds')
			);
	`

	ManageOldPartitions = `SELECT pgqueue.manage_old_partitions('pgqueue.tasks', $1, $2);`

	CountCronJobs = `SELECT COUNT(*) FROM pgqueue.cron_jobs`

	ListCronJobs = `
		SELECT job_id, name, expression, last_run_at, next_run_at, created_at 
		FROM pgqueue.cron_jobs 
		ORDER BY created_at DESC 
		LIMIT $1 OFFSET $2
	`

	ListTasks = `
		SELECT task_id, created_at, task_type, status, attempts, max_retries, priority, next_run_at, last_error, payload
		FROM pgqueue.tasks
		WHERE ($1::text IS NULL OR status = $1)
		ORDER BY created_at DESC
		LIMIT $2 OFFSET $3
	`
)
