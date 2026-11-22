/**
 * Benchmark worker: a real queue worker with a handler that does nothing.
 * A no-op handler means the numbers measure the QUEUE (claim, ack, polling),
 * not the work. Launched by bench/run.ts, one process per worker.
 */
import { createQueue } from "../src/queue.js";

const connectionString =
  process.env.DATABASE_URL ?? "postgres://postgres:pw@localhost:5432/postgres";

const queue = createQueue({
  config: { connectionString },
  concurrency: Number(process.env.CONCURRENCY ?? 10),
});

queue.register("noop", async () => {});

process.on("SIGTERM", async () => {
  await queue.stop();
  process.exit(0);
});

queue.start();