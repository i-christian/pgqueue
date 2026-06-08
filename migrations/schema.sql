CREATE TABLE IF NOT EXISTS cron_jobs (
    job_id UUID PRIMARY KEY,
    name TEXT UNIQUE NOT NULL,
    expression TEXT NOT NULL,
    last_run_at TIMESTAMP WITH TIME ZONE,
    next_run_at TIMESTAMP WITH TIME ZONE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);


CREATE TABLE IF NOT EXISTS tasks (
    task_id UUID NOT NULL,
    task_type VARCHAR(255) NOT NULL,
    payload JSONB NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending',
    created_at TIMESTAMP WITH TIME ZONE NOT NULL,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    next_run_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    attempts INTEGER DEFAULT 0,
    last_error TEXT,
    priority INTEGER NOT NULL DEFAULT 3,
    max_retries INTEGER NOT NULL DEFAULT 5,
    deduplication_key TEXT,
    PRIMARY KEY (task_id, created_at),
    UNIQUE (deduplication_key, created_at)
) PARTITION BY RANGE (created_at);


CREATE INDEX IF NOT EXISTS idx_tasks_poll ON tasks (status, priority DESC, next_run_at ASC);
CREATE INDEX IF NOT EXISTS idx_tasks_archive ON tasks (status, updated_at);
CREATE INDEX IF NOT EXISTS idx_tasks_processing_stuck 
    ON tasks (updated_at) WHERE status = 'processing';
CREATE INDEX IF NOT EXISTS idx_tasks_search ON tasks USING GIN (
    to_tsvector('simple', coalesce(task_type,'') || ' ' || coalesce(last_error,''))
);

-- Archive Table
CREATE TABLE IF NOT EXISTS tasks_archive (LIKE tasks INCLUDING ALL);

CREATE OR REPLACE FUNCTION notify_new_task() RETURNS trigger AS $$
BEGIN
  PERFORM pg_notify('new_task', '1');
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Trigger (Drop first to ensure idempotency during migration)
DROP TRIGGER IF EXISTS task_enqueued ON tasks;
CREATE TRIGGER task_enqueued
    AFTER INSERT ON tasks
        FOR EACH ROW
            EXECUTE PROCEDURE notify_new_task();
