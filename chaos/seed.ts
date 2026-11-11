/**
 * Prepares a clean slate for the chaos test:
 *  1. creates the `chaos` schema with two evidence tables
 *  2. truncates the jobs table and the evidence tables
 *  3. bulk-inserts N jobs with generate_series (one SQL statement, fast)
 *
 * Why bulk SQL and not queue.enqueue() in a loop? 100k enqueue calls = 100k round
 * trips. We are testing crash recovery here, not enqueue speed.
 */
import { Pool } from "pg";

const connectionString =
  process.env.DATABASE_URL ?? "postgres://postgres:pw@localhost:5432/postgres";
const N = Number(process.env.JOBS ?? 5000);

async function main() {
  const pool = new Pool({ connectionString, max: 1 });

  await pool.query(`
    CREATE SCHEMA IF NOT EXISTS chaos;

    -- Proof of work. UNIQUE(job_n) means a job can only ever leave one row here,
    -- no matter how many times its handler runs. This is the "idempotent handler".
    CREATE TABLE IF NOT EXISTS chaos.results (
      job_n int PRIMARY KEY
    );

    -- One row per completed handler run. No uniqueness on purpose: if a job's
    -- handler finished twice (worker died before ack), we see two rows here.
    CREATE TABLE IF NOT EXISTS chaos.executions (
      id     bigserial PRIMARY KEY,
      job_n  int NOT NULL,
      run_at timestamptz NOT NULL DEFAULT now()
    );

    TRUNCATE chaos.results, chaos.executions;
    TRUNCATE jobs.jobs;
  `);

  // max_attempts = 10: every kill burns one attempt for each job the dead worker
  // held. With the default of 3, unlucky jobs could go DEAD purely from chaos,
  // which would be a test artifact, not a bug in the queue.
  await pool.query(
    `INSERT INTO jobs.jobs (function_id, function_params, max_attempts)
     SELECT 'record_result', jsonb_build_object('n', i), 10
     FROM generate_series(1, $1::int) AS g(i)`,
    [N],
  );

  console.log(`seeded ${N} jobs`);
  await pool.end();
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});