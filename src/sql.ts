export const ENQUEUE = `
  INSERT INTO jobs.jobs (function_id, function_params, pickup_at, idempotency_key, max_attempts)
  VALUES ($1, $2::jsonb, COALESCE($3::timestamptz, now()), $4, COALESCE($5::int, 3))
  ON CONFLICT (function_id, idempotency_key) DO NOTHING
  RETURNING *;
  `

export const ENQUEUE_FALLBACK = `
  SELECT * FROM  jobs.jobs where function_id = $1 AND idempotency_key = $2;
`

export const CLAIM = `
  UPDATE jobs.jobs
  SET status          = 'IN_PROGRESS',
      locked_by       = $1,
      locked_until    = now() + INTERVAL '30 seconds',
      last_attempt_at = now(),
      attempts        = attempts + 1,
      updated_at      = now()
  WHERE id IN (
      SELECT id
      FROM jobs.jobs
      WHERE status = 'PENDING'
        AND pickup_at <= now()
      ORDER BY pickup_at ASC
      LIMIT $2
      FOR UPDATE SKIP LOCKED
  )
  RETURNING id, function_id, function_params, attempts, locked_by;
`

export const ACK = 
  `
  UPDATE jobs.jobs
  SET status       = 'COMPLETED',
      locked_by    = NULL,
      locked_until = NULL,
      updated_at   = now()
  WHERE id = $1
    AND locked_by = $2
    AND status = 'IN_PROGRESS'
  RETURNING id, status;
  `

export const FAIL = `
  UPDATE jobs.jobs
  SET status       = CASE WHEN attempts < max_attempts
                          THEN 'PENDING'::jobs.job_status
                          ELSE 'DEAD'::jobs.job_status END,
      pickup_at    = CASE WHEN attempts < max_attempts
                          THEN now() + make_interval(secs => 5 * power(2, attempts))
                          ELSE pickup_at END,
      locked_by    = NULL,
      locked_until = NULL,
      last_error   = $3,
      updated_at   = now()
  WHERE id = $1
    AND locked_by = $2
    AND status = 'IN_PROGRESS'
  RETURNING id, status;
  `

export const TICK = `
  UPDATE jobs.jobs
  SET locked_until = now() + INTERVAL '30 seconds',
      updated_at   = now()
  WHERE id = $1
    AND locked_by = $2
    AND status = 'IN_PROGRESS'
  RETURNING id;
  `

export const REAP = `
  UPDATE jobs.jobs
  SET status       = CASE WHEN attempts < max_attempts
                          THEN 'PENDING'::jobs.job_status
                          ELSE 'DEAD'::jobs.job_status END,
      pickup_at    = CASE WHEN attempts < max_attempts
                          THEN now() + make_interval(secs => 5 * power(2, attempts))
                          ELSE pickup_at END,
      locked_by    = NULL,
      locked_until = NULL,
      last_error   = 'lease expired: worker presumed dead',
      updated_at   = now()
  WHERE id IN (
      SELECT id
      FROM jobs.jobs
      WHERE status = 'IN_PROGRESS'
        AND locked_until < now()
      ORDER BY locked_until
      LIMIT 100
      FOR UPDATE SKIP LOCKED
  )
  RETURNING id, status, attempts;
  `