/*
  Wake idle workers as soon as new jobs are ready, instead of waiting for their next poll.

  - FOR EACH STATEMENT: a bulk INSERT of 10,000 rows sends ONE notification, not 10,000.
  - The NEW TABLE transition table lets us skip the notify when every inserted job is
    scheduled for later (pickup_at in the future); polling picks those up when they're due.
  - NOTIFY is transactional: it is delivered only when the inserting transaction commits,
    so a worker can never wake up before the new rows are visible to it.
  - Postgres also merges identical notifications within one transaction.
*/

CREATE OR REPLACE FUNCTION jobs.notify_new_jobs() RETURNS trigger AS $$
BEGIN
  IF EXISTS (SELECT 1 FROM new_jobs WHERE pickup_at <= now()) THEN
    PERFORM pg_notify('jobs_new', '');
  END IF;
  RETURN NULL;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS jobs_notify_insert ON jobs.jobs;

CREATE TRIGGER jobs_notify_insert
AFTER INSERT ON jobs.jobs
REFERENCING NEW TABLE AS new_jobs
FOR EACH STATEMENT
EXECUTE FUNCTION jobs.notify_new_jobs();
