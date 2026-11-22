/**
 * Benchmark runner. One command, two experiments, no kills:
 *
 *   1. THROUGHPUT: seed JOBS jobs, run W worker processes until the queue drains,
 *      compute jobs/sec from the database timestamps. Repeated for W = 1, 2, 4, 8.
 *   2. LATENCY: a paced producer inserts jobs at a fixed rate (a fraction of the max
 *      throughput found in step 1) while workers run. We then compute p50/p95/p99 of
 *      "enqueue -> first claim" and "enqueue -> done" from the timestamps on each row.
 *
 * Run:   pnpm tsx bench/run.ts
 * Quick smoke test first:   JOBS=20000 REPEATS=1 LAT_SECONDS=10 pnpm tsx bench/run.ts
 *
 * Knobs (env): JOBS=100000  WORKER_COUNTS=1,2,4,8  REPEATS=3  CONCURRENCY=10
 *              LAT_WORKERS=4  LAT_FRACTIONS=0.25,0.5,0.8  LAT_SECONDS=30  WARMUP_SECONDS=5
 *
 * WARNING: this TRUNCATEs jobs.jobs. Don't point it at a database you care about.
 */
import { spawn, type ChildProcess } from "node:child_process";
import { writeFileSync } from "node:fs";
import { Pool } from "pg";

const connectionString =
  process.env.DATABASE_URL ?? "postgres://postgres:pw@localhost:5432/postgres";

const num = (k: string, d: number) => Number(process.env[k] ?? d);
const list = (k: string, d: string) => (process.env[k] ?? d).split(",").map(Number);

const JOBS = num("JOBS", 100_000);
const WORKER_COUNTS = list("WORKER_COUNTS", "1,2,4,8");
const REPEATS = num("REPEATS", 3);
const CONCURRENCY = num("CONCURRENCY", 10);
const LAT_WORKERS = num("LAT_WORKERS", 4);
const LAT_FRACTIONS = list("LAT_FRACTIONS", "0.25,0.5,0.8");
const LAT_SECONDS = num("LAT_SECONDS", 30);
const WARMUP_SECONDS = num("WARMUP_SECONDS", 5);
const DRAIN_TIMEOUT_MS = num("DRAIN_TIMEOUT_MS", 10 * 60 * 1000);

const pool = new Pool({ connectionString, max: 10 });
const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

const median = (xs: number[]) => {
  const s = [...xs].sort((a, b) => a - b);
  const m = Math.floor(s.length / 2);
  return s.length % 2 ? s[m] : (s[m - 1] + s[m]) / 2;
};

// ---------- helpers ----------

// Each worker is its own OS process (same reason as the chaos test: real replicas).
const startWorkers = (n: number): ChildProcess[] =>
  Array.from({ length: n }, () =>
    spawn(process.execPath, ["--import", "tsx", "bench/worker.ts"], {
      stdio: ["ignore", "ignore", "inherit"],
      env: { ...process.env, CONCURRENCY: String(CONCURRENCY) },
    }),
  );

// Safe to SIGKILL here: we only stop workers after every job is COMPLETED.
const stopWorkers = (ws: ChildProcess[]) => ws.forEach((w) => w.kill("SIGKILL"));

// Clean slate so one run's leftovers (dead rows, stats) don't distort the next.
async function reset() {
  await pool.query("TRUNCATE jobs.jobs");
  await pool.query("VACUUM ANALYZE jobs.jobs"); // can't run inside a transaction; single statement is fine
}

const remaining = async (): Promise<number> =>
  (
    await pool.query(
      `SELECT count(*)::int AS n FROM jobs.jobs WHERE status IN ('PENDING','IN_PROGRESS')`,
    )
  ).rows[0].n;

async function waitDrain(): Promise<void> {
  const t0 = Date.now();
  while ((await remaining()) > 0) {
    if (Date.now() - t0 > DRAIN_TIMEOUT_MS) throw new Error("drain timed out");
    await sleep(500);
  }
}

// ---------- experiment 1: throughput ----------

async function throughputRun(workers: number): Promise<number> {
  await reset();
  await pool.query(
    `INSERT INTO jobs.jobs (function_id, function_params)
     SELECT 'noop', '{}'::jsonb FROM generate_series(1, $1::int)`,
    [JOBS],
  );

  const ws = startWorkers(workers);
  try {
    await waitDrain();
  } finally {
    stopWorkers(ws);
  }

  // First claim -> last ack. Measuring from the table (not a stopwatch around the
  // script) excludes process startup time, which would otherwise pollute the number.
  const { rows } = await pool.query(
    `SELECT count(*)::float / NULLIF(extract(epoch FROM max(updated_at) - min(last_attempt_at)), 0) AS jps
     FROM jobs.jobs WHERE status = 'COMPLETED'`,
  );
  return Number(rows[0].jps);
}

// ---------- experiment 2: latency under a paced load ----------

interface LatencyResult {
  ratePerSec: number;
  sent: number;
  measured: number;
  wait: number[]; // [p50, p95, p99] ms, enqueue -> first claim
  e2e: number[]; // [p50, p95, p99] ms, enqueue -> done
}

async function latencyRun(workers: number, ratePerSec: number): Promise<LatencyResult> {
  await reset();
  const ws = startWorkers(workers);
  await sleep(3000); // let the worker processes boot and start polling

  // Use the DATABASE clock as the reference, so we never compare app time with DB time.
  const startAt: Date = (await pool.query("SELECT now() AS t")).rows[0].t;
  const start = Date.now();
  let sent = 0;

  // Coordinated omission: the schedule comes from the CLOCK, not from replies.
  // Every 10 ms we work out how many jobs SHOULD have been sent by now and insert the
  // difference, without awaiting. If the DB is slow, the load does not back off, so the
  // slowness shows up in the latency numbers instead of being hidden.
  // (All jobs in one tick share a created_at, so timing is accurate to about 10 ms.)
  const timer = setInterval(() => {
    const elapsed = (Date.now() - start) / 1000;
    if (elapsed > LAT_SECONDS) return;
    const due = Math.floor(elapsed * ratePerSec) - sent;
    if (due <= 0) return;
    sent += due;
    pool
      .query(
        `INSERT INTO jobs.jobs (function_id, function_params)
         SELECT 'noop', '{}'::jsonb FROM generate_series(1, $1::int)`,
        [due],
      )
      .catch((e) => console.error("producer error", e));
  }, 10);

  await sleep(LAT_SECONDS * 1000 + 200);
  clearInterval(timer);

  try {
    await waitDrain();
  } finally {
    stopWorkers(ws);
  }

  // Skip the warm-up window (cold pools and caches), then take percentiles in SQL.
  const { rows } = await pool.query(
    `SELECT
       percentile_cont(ARRAY[0.5, 0.95, 0.99])
         WITHIN GROUP (ORDER BY extract(epoch FROM last_attempt_at - created_at) * 1000) AS wait,
       percentile_cont(ARRAY[0.5, 0.95, 0.99])
         WITHIN GROUP (ORDER BY extract(epoch FROM updated_at - created_at) * 1000) AS e2e,
       count(*)::int AS n
     FROM jobs.jobs
     WHERE status = 'COMPLETED'
       AND created_at > $1::timestamptz + make_interval(secs => $2::float)`,
    [startAt, WARMUP_SECONDS],
  );

  return {
    ratePerSec,
    sent,
    measured: rows[0].n,
    wait: (rows[0].wait as unknown[]).map(Number),
    e2e: (rows[0].e2e as unknown[]).map(Number),
  };
}

// ---------- output ----------

const f = (x: number, d = 1) => (Number.isFinite(x) ? x.toFixed(d) : "n/a");

async function main() {
  const out: string[] = [];
  const log = (s = "") => {
    console.log(s);
    out.push(s);
  };

  // ----- throughput -----
  log(`## Throughput (${JOBS.toLocaleString()} no-op jobs, concurrency ${CONCURRENCY} per worker)\n`);
  log("| Workers | Jobs/sec (median) | Runs |");
  log("|---|---|---|");

  const medianByWorkers = new Map<number, number>();
  for (const w of WORKER_COUNTS) {
    const runs: number[] = [];
    for (let i = 0; i < REPEATS; i++) {
      console.error(`throughput: ${w} worker(s), run ${i + 1}/${REPEATS}`);
      runs.push(await throughputRun(w));
    }
    medianByWorkers.set(w, median(runs));
    log(`| ${w} | ${f(median(runs), 0)} | ${runs.map((r) => f(r, 0)).join(", ")} |`);
  }

  // ----- latency -----
  const maxTp = medianByWorkers.get(LAT_WORKERS) ?? Math.max(...medianByWorkers.values());
  log(`\n## Latency (${LAT_WORKERS} workers, ${LAT_SECONDS}s paced load, first ${WARMUP_SECONDS}s excluded)\n`);
  log(`Reference max throughput: ${f(maxTp, 0)} jobs/sec\n`);
  log("| Load | Rate (jobs/s) | Jobs measured | Wait p50 | Wait p95 | Wait p99 | End-to-end p99 |");
  log("|---|---|---|---|---|---|---|");

  for (const frac of LAT_FRACTIONS) {
    const rate = Math.max(1, Math.round(maxTp * frac));
    console.error(`latency: ${Math.round(frac * 100)}% load (${rate}/s)`);
    const r = await latencyRun(LAT_WORKERS, rate);
    log(
      `| ${Math.round(frac * 100)}% | ${r.ratePerSec} | ${r.measured} | ` +
        `${f(r.wait[0])} ms | ${f(r.wait[1])} ms | ${f(r.wait[2])} ms | ${f(r.e2e[2])} ms |`,
    );
  }

  log("\nWait = enqueue to first claim. End-to-end = enqueue to ack.");
  writeFileSync("bench/results.md", out.join("\n") + "\n");
  console.error("\nsaved to bench/results.md");
  await pool.end();
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});