/**
 * One worker process. The runner launches several of these and kills them at random.
 * Run alone (to check it works) with:  node --import tsx chaos/worker.ts
 */
import { Pool } from "pg";
import { createQueue } from "../src/queue.js";

const connectionString =
  process.env.DATABASE_URL ?? "postgres://postgres:pw@localhost:5432/postgres";

// The handler's own pool, separate from the queue's pool. Handler writes are
// "business logic"; the queue's pool is for claim/ack/fail/heartbeat.
const resultsPool = new Pool({ connectionString, max: 5 });
resultsPool.on("error", (e) => console.error("results pool error", e));

const queue = createQueue({
  config: { connectionString },
  concurrency: 10,
});

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

queue.register("record_result", async (params: { n: number }) => {
  // Simulated work. Without a delay, jobs finish in ~1ms and a kill would rarely
  // land mid-job, which defeats the point of the test.
  await sleep(50 + Math.random() * 150);

  // ONE statement, so "record the result" and "log the execution" are atomic.
  //  - results: ON CONFLICT DO NOTHING  -> running twice leaves one row
  //  - executions: always inserts       -> running twice leaves two rows
  await resultsPool.query(
    `WITH r AS (
       INSERT INTO chaos.results (job_n) VALUES ($1) ON CONFLICT DO NOTHING
     )
     INSERT INTO chaos.executions (job_n) VALUES ($1)`,
    [params.n],
  );
  await sleep(1500);
});

// SIGTERM = polite shutdown (used by the runner at the very end).
// SIGKILL (the chaos kill) cannot be caught, so nothing here runs for it. That's the point.
process.on("SIGTERM", async () => {
  await queue.stop();
  await resultsPool.end();
  process.exit(0);
});

queue.start();