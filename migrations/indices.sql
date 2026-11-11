/*
indexes where I think should be applied
1. function_id, idempotency_key // already covered due to the constraint
2. status, pickup_at // partial index can be pretty good here
3. locked_by, status // not needed, already searching by id
4. status, locked_until,  // partial here too
*/

CREATE INDEX IF NOT EXISTS idx_jobs_pending_pickup 
ON jobs.jobs (pickup_at ASC) 
WHERE status = 'PENDING'::jobs.job_status;

CREATE INDEX IF NOT EXISTS idx_jobs_reap_expired 
ON jobs.jobs (locked_until ASC) 
WHERE status = 'IN_PROGRESS'::jobs.job_status;