/*
    seed with generate_series
*/

INSERT INTO jobs.jobs (
    status,
    pickup_at,
    function_id,
    function_params,
    attempts,
    locked_by,
    locked_until,
    created_at
)
SELECT
    CASE
        WHEN i <= 950000 THEN 'COMPLETED'::jobs.job_status
        WHEN i <= 960000 THEN 'DEAD'::jobs.job_status
        WHEN i <= 999000 THEN 'PENDING'::jobs.job_status
        ELSE 'IN_PROGRESS'::jobs.job_status
    END,
    CASE
        WHEN i <= 950000 THEN NOW() - random() * INTERVAL '1 hour'
        WHEN i <= 960000 THEN NOW() - INTERVAL '2 days'
        WHEN i <= 990000 THEN NOW() - INTERVAL '1 minute'           -- due now
        WHEN i <= 999000 THEN NOW() + random() * INTERVAL '1 hour'  -- future
        ELSE NOW() - INTERVAL '2 days'
    END,
    'fn_' || (i % 5),
    jsonb_build_object('job_number', i, 'retry', false),
    CASE
        WHEN i <= 950000 THEN 1
        WHEN i <= 960000 THEN 3                      -- DEAD: attempts exhausted
        WHEN i <= 999000 THEN floor(random() * 3)::int
        ELSE 1                                       -- IN_PROGRESS: claim already counted
    END,
    CASE WHEN i > 999000 THEN gen_random_uuid() END,
    CASE
        WHEN i > 999000 AND i % 2 = 0 THEN NOW() - INTERVAL '5 minutes'   -- expired
        WHEN i > 999000 THEN NOW() + INTERVAL '30 seconds'                -- still leased
    END,
    NOW() - INTERVAL '30 days' + i * INTERVAL '2 seconds'
FROM generate_series(1, 1000000) AS s(i);