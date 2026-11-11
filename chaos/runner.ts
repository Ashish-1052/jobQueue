/**
 * The chaos runner. One command does everything:
 *   1. seed N jobs
 *   2. start WORKERS worker processes
 *   3. every KILL_EVERY_MS, SIGKILL a random worker and start a replacement
 *   4. when no PENDING / IN_PROGRESS jobs remain, stop the chaos and the workers
 *   5. run verify.ts and exit with its exit code
 *
 * Run:  pnpm tsx chaos/runner.ts
 * Knobs (env): JOBS=5000 WORKERS=5 KILL_EVERY_MS=3000 MAX_RUNTIME_MS=900000
 */
import { spawn, spawnSync, type ChildProcess } from "node:child_process";
import { Pool } from "pg";

const connectionString =
  process.env.DATABASE_URL ?? "postgres://postgres:pw@localhost:5432/postgres";
const WORKERS = Number(process.env.WORKERS ?? 5);
const KILL_EVERY_MS = Number(process.env.KILL_EVERY_MS ?? 3000);
const MAX_RUNTIME_MS = Number(process.env.MAX_RUNTIME_MS ?? 15 * 60 * 1000);

// Why process.execPath + "--import tsx" instead of spawn("npx", ["tsx", ...])?
// npx would be the child, and tsx/node a grandchild. kill() would hit npx and
// leave the real worker alive. We need the handle to BE the worker process.
const startWorker = (): ChildProcess =>
  spawn(process.execPath, ["--import", "tsx", "chaos/worker.ts"], {
    stdio: ["ignore", "ignore", "inherit"], // hide the library's per-job logs, keep errors
    env: process.env,
  });

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

async function main() {
  // 1. seed
  const seed = spawnSync(process.execPath, ["--import", "tsx", "chaos/seed.ts"], {
    stdio: "inherit",
    env: process.env,
  });
  if (seed.status !== 0) process.exit(1);

  const pool = new Pool({ connectionString, max: 1 });
  const remaining = async (): Promise<number> =>
    (
      await pool.query(
        `SELECT count(*)::int AS n FROM jobs.jobs WHERE status IN ('PENDING','IN_PROGRESS')`,
      )
    ).rows[0].n;

  // 2. start workers
  const workers: ChildProcess[] = Array.from({ length: WORKERS }, startWorker);
  console.log(`started ${WORKERS} workers`);

  // 3. chaos loop
  let kills = 0;
  const chaos = setInterval(() => {
    const i = Math.floor(Math.random() * workers.length);
    workers[i]?.kill("SIGKILL"); // instant death, no cleanup code runs
    workers[i] = startWorker(); // replacement is a NEW process with a NEW workerId
    kills++;
  }, KILL_EVERY_MS);

  // 4. wait for the queue to drain
  const startedAt = Date.now();
  let left = await remaining();
  while (left > 0 && Date.now() - startedAt < MAX_RUNTIME_MS) {
    console.log(`remaining: ${left}   kills so far: ${kills}`);
    await sleep(2000);
    left = await remaining();
  }

  // stop chaos, stop workers
  clearInterval(chaos);
  workers.forEach((w) => w.kill("SIGKILL")); // all jobs are done, nothing left to protect
  await pool.end();

  const secs = ((Date.now() - startedAt) / 1000).toFixed(1);
  console.log(`\nqueue drained in ${secs}s with ${kills} worker kills`);

  // 5. verify
  const verify = spawnSync(process.execPath, ["--import", "tsx", "chaos/verify.ts"], {
    stdio: "inherit",
    env: process.env,
  });
  process.exit(verify.status ?? 1);
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});