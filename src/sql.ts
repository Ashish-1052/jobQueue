export const ENQUEUE = `
  INSERT INTO jobs.jobs (function_id, function_params, pickup_at, idempotency_key, max_attempts)
  VALUES ($1, $2, $3, $4, $5)
  ON CONFLICT (function_id, idempotency_key) DO NOTHING
  RETURNING *;
  `