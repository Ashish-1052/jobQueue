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