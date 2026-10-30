CREATE SCHEMA jobs IF NOT EXISTS;

Create TYPE jobs.job_status as ENUM ('PENDING', 'IN_PROGRESS', 'COMPLETED', 'DEAD') IF NOT EXISTS;

CREATE TABLE jobs.jobs IF NOT EXISTS (
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

/*
indexes where I think should be applied
1. function_id, idempotency_key // already covered due to the constraint
2. status, pickup_at // partial index can be pretty good here
3. locked_by, status // not needed, already searching by id
4. status, locked_until,  // partial here too
*/

CREATE INDEX IF NOT EXISTS idx_jobs_pending_pickup 
ON jobs.jobs (pickup_at ASC) 
WHERE status = 'PENDING';

CREATE INDEX IF NOT EXISTS idx_jobs_reap_expired 
ON jobs.jobs (locked_until ASC) 
WHERE status = 'IN_PROGRESS';
