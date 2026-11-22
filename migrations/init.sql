CREATE SCHEMA IF NOT EXISTS jobs;

-- Postgres has no CREATE TYPE ... IF NOT EXISTS, so check the catalog first.
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_type t
        JOIN pg_namespace n ON n.oid = t.typnamespace
        WHERE t.typname = 'job_status' AND n.nspname = 'jobs'
    ) THEN
        CREATE TYPE jobs.job_status AS ENUM ('PENDING', 'IN_PROGRESS', 'COMPLETED', 'DEAD');
    END IF;
END
$$;

CREATE TABLE IF NOT EXISTS jobs.jobs (
    "id" UUID NOT NULL PRIMARY KEY DEFAULT gen_random_uuid(),
    "function_id" TEXT NOT NULL,
    "function_params" JSONB NOT NULL DEFAULT '{}',
    "status" jobs.job_status NOT NULL DEFAULT 'PENDING',
    "attempts" INT NOT NULL DEFAULT 0,
    "last_attempt_at" TIMESTAMPTZ,
    "pickup_at" TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    "locked_by" UUID,
    "locked_until" TIMESTAMPTZ,
    "created_at" TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    "updated_at" TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    "last_error" TEXT,
    "max_attempts" INT NOT NULL DEFAULT 3,
    "idempotency_key" TEXT,

    CONSTRAINT "unique_function_idempotency" UNIQUE ("function_id", "idempotency_key")
);
