CREATE SCHEMA IF NOT EXISTS pgqueue;

CREATE TABLE IF NOT EXISTS pgqueue.cron_jobs (
    job_id UUID PRIMARY KEY,
    name TEXT UNIQUE NOT NULL,
    expression TEXT NOT NULL,
    last_run_at TIMESTAMP WITH TIME ZONE,
    next_run_at TIMESTAMP WITH TIME ZONE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);


CREATE TABLE IF NOT EXISTS pgqueue.tasks (
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


-- Indexes
CREATE INDEX IF NOT EXISTS idx_tasks_poll ON pgqueue.tasks (status, priority DESC, next_run_at ASC);
CREATE INDEX IF NOT EXISTS idx_tasks_archive ON pgqueue.tasks (status, updated_at);
CREATE INDEX IF NOT EXISTS idx_tasks_processing_stuck 
    ON pgqueue.tasks (updated_at) WHERE status = 'processing';
CREATE INDEX IF NOT EXISTS idx_tasks_search ON pgqueue.tasks USING GIN (
    to_tsvector('simple', coalesce(task_type,'') || ' ' || coalesce(last_error,''))
);

-- notify_new_task creates a new task insertion event which the application listens.
CREATE OR REPLACE FUNCTION pgqueue.notify_new_task() RETURNS trigger AS $$
BEGIN
  PERFORM pg_notify('new_task', '1');
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Trigger (Drop first to ensure idempotency during migration)
DROP TRIGGER IF EXISTS task_enqueued ON pgqueue.tasks;
CREATE TRIGGER task_enqueued
    AFTER INSERT ON pgqueue.tasks
    FOR EACH ROW EXECUTE PROCEDURE pgqueue.notify_new_task();

-- ensure_partition creates tasks table partitions a month offset of 0 means current month and 1 means next month 
CREATE OR REPLACE FUNCTION pgqueue.ensure_partition(table_name TEXT, month_offset INTEGER DEFAULT 0)
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

-- manage_old_partitions either deletes old partitions if the system is using delete strategy on older tasks
-- Or it just detaches old partitions if it is in archive strategy
CREATE OR REPLACE FUNCTION pgqueue.manage_old_partitions(target_table TEXT, retention_months INTEGER, do_delete BOOLEAN)
RETURNS INTEGER AS $$
DECLARE
    partition_record RECORD;
    cutoff_date DATE := date_trunc('month', now() - (retention_months || ' months')::interval);
    processed_count INTEGER := 0;
    schema_name TEXT := 'pgqueue';
    base_table TEXT := target_table;
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
                    IF do_delete THEN
                        EXECUTE format('DROP TABLE %I.%I', schema_name, partition_record.partition_name);
                    ELSE
                        EXECUTE format('ALTER TABLE %I.%I DETACH PARTITION %I.%I', schema_name, base_table, schema_name, partition_record.partition_name);
                    END IF;
                    processed_count := processed_count + 1;
                END IF;
            EXCEPTION WHEN OTHERS THEN
                -- Skip unparseable partitions safely
            END;
        END IF;
    END LOOP;

    RETURN processed_count;
END;
$$ LANGUAGE plpgsql;
