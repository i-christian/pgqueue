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

CREATE OR REPLACE FUNCTION ensure_partition(table_name TEXT, month_offset INTEGER DEFAULT 0)
RETURNS void AS $$
DECLARE
    target_month DATE := date_trunc('month', now() + (month_offset || ' month')::interval);
    target_schema TEXT := COALESCE(nullif(split_part(table_name, '.', 1), table_name), 'public');
    base_table TEXT := CASE WHEN table_name LIKE '%._%' THEN split_part(table_name, '.', 2) ELSE table_name END;
    
    partition_name TEXT := base_table || '_y' || to_char(target_month, 'YYYY') || '_m' || to_char(target_month, 'MM');
    start_date TEXT := to_char(target_month, 'YYYY-MM-DD');
    end_date TEXT := to_char(target_month + interval '1 month', 'YYYY-MM-DD');
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = target_schema AND c.relname = partition_name
    ) THEN
        EXECUTE format('CREATE TABLE IF NOT EXISTS %I.%I PARTITION OF %I.%I FOR VALUES FROM (%L) TO (%L)', 
                       target_schema, partition_name, target_schema, base_table, start_date, end_date);
    END IF;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION drop_old_partitions(target_table TEXT, retention_months INTEGER)
RETURNS INTEGER AS $$
DECLARE
    partition_record RECORD;
    cutoff_date DATE := date_trunc('month', now() - (retention_months || ' months')::interval);
    dropped_count INTEGER := 0;
    schema_name TEXT := COALESCE(nullif(split_part(target_table, '.', 1), target_table), 'public');
    base_table TEXT := CASE WHEN target_table LIKE '%._%' THEN split_part(target_table, '.', 2) ELSE target_table END;
BEGIN
    FOR partition_record IN
        SELECT child.relname AS partition_name
        FROM pg_inherits
        JOIN pg_class parent ON pg_inherits.inhparent = parent.oid
        JOIN pg_class child ON pg_inherits.inhrelid = child.oid
        JOIN pg_namespace n ON parent.relnamespace = n.oid
        WHERE n.nspname = schema_name 
          AND parent.relname = base_table
    LOOP
        IF partition_record.partition_name ~ '_y[0-9]{4}_m[0-9]{2}$' THEN
            DECLARE
                partition_date DATE := to_date(substring(partition_record.partition_name from '_y([0-9]{4}_m[0-9]{2})$'), 'YYYY_mMM');
            BEGIN
                IF partition_date < cutoff_date THEN
                    EXECUTE format('DROP TABLE %I.%I', schema_name, partition_record.partition_name);
                    dropped_count := dropped_count + 1;
                END IF;
            EXCEPTION WHEN OTHERS THEN
                -- Skip unparseable partitions
            END;
        END IF;
    END LOOP;
    RETURN dropped_count;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION notify_new_task() RETURNS trigger AS $$
BEGIN
  PERFORM pg_notify('new_task', '1');
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS task_enqueued ON tasks;
CREATE TRIGGER task_enqueued
    AFTER INSERT ON tasks
    FOR EACH ROW EXECUTE PROCEDURE notify_new_task();


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
