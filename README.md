# Postgres Job Queue

A durable background job queue for Node.js, written in TypeScript, that uses **PostgreSQL as the only infrastructure**. Workers claim jobs with `FOR UPDATE SKIP LOCKED`, hold them under a renewable lease, and are woken instantly by `LISTEN/NOTIFY` when new work arrives. A reaper recovers jobs from workers that crash.

Delivery is **at-least-once**: a job is never lost, but a worker that dies after running a handler and before acknowledging it means the job runs again. Handlers should be idempotent.

## Features

- **Concurrent, non-blocking claims.** `FOR UPDATE SKIP LOCKED` lets many workers claim from the same table without waiting on each other's row locks.
- **Leases and heartbeats.** A claimed job is locked for 30 s. The worker renews the lease every 15 s while the handler runs, so long jobs keep their lock and dead workers lose theirs.
- **Crash recovery.** A reaper returns jobs with expired leases to `PENDING`, or marks them `DEAD` when their attempts run out.
- **Retries with exponential backoff.** A failed job is rescheduled `5 × 2^attempts` seconds later, up to `max_attempts` (default 3), then moved to `DEAD` with its last error recorded.
- **Idempotent enqueue.** An optional idempotency key, unique per function, makes enqueueing the same job twice return the existing one instead of creating a duplicate.
- **Delayed jobs.** `pickupAt` schedules a job for later.
- **Instant wake-up with LISTEN/NOTIFY.** An insert trigger notifies idle workers so they claim immediately instead of waiting for their next poll. Polling stays on as a fallback.
- **Ownership checks on every write.** `ACK`, `FAIL` and `HEARTBEAT` only succeed if the job is still `IN_PROGRESS` and locked by the same worker, so a worker whose lease was reaped can't overwrite the job's new state.

## Tech Stack

TypeScript · Node.js · PostgreSQL · node-postgres (`pg`) · tsx

## How it works

```
enqueue() ──INSERT──► jobs.jobs (PENDING) ──trigger──► pg_notify('jobs_new')
                                                              │
                     ┌────────────────────────────────────────┘
                     ▼
   worker: LISTEN connection ──wake()──► claim loop ◄── 1 s poll (fallback)
                                             │
                    CLAIM: UPDATE … WHERE id IN (SELECT … FOR UPDATE SKIP LOCKED)
                                             │
                             run handler (heartbeat every 15 s)
                                  │                      │
                              success                 throws
                                  ▼                      ▼
                          ACK → COMPLETED     FAIL → PENDING (backoff) or DEAD

   reaper (every 20 s): IN_PROGRESS with expired lease → PENDING (backoff) or DEAD
```

### Job lifecycle

| Status | Meaning |
| --- | --- |
| `PENDING` | Waiting to run, once `pickup_at` has passed |
| `IN_PROGRESS` | Claimed by a worker, with a lease in `locked_until` |
| `COMPLETED` | Handler succeeded and the worker acknowledged it |
| `DEAD` | Attempts exhausted. `last_error` says why |

### Why LISTEN/NOTIFY *and* polling

The notification carries no data. It only means "go and look", and the table stays the single source of truth. So a missed notification, for example while the listener connection reconnects, costs latency and never loses a job. Polling also covers what an insert can't signal: delayed jobs becoming due, and retries whose backoff has expired.

The trigger runs once per statement, so a bulk insert of 10,000 rows sends one notification. It uses a transition table to skip the notification when every inserted job is scheduled for later, or when `ON CONFLICT DO NOTHING` inserted nothing. `NOTIFY` is delivered on commit, so a worker never wakes before the new rows are visible to it.

Each worker keeps a dedicated `pg.Client` for `LISTEN`, separate from the pool, because a subscription belongs to one session and a pooled connection can be handed out to other queries.

| File | Purpose |
| --- | --- |
| `src/queue.ts` | The `Queue` class: enqueue, register handlers, claim loop, heartbeats, reaper, listener |
| `src/sql.ts` | Every SQL statement the queue runs |
| `src/db.ts` | Thin wrapper around a `pg` pool |
| `migrations/init.sql` | Schema, `job_status` enum and `jobs.jobs` table |
| `migrations/indices.sql` | Partial indexes for claiming and reaping |
| `migrations/notify.sql` | The insert trigger that sends `NOTIFY` |
| `migrations/benchmark.sql` | Seeds 1M rows for checking index plans |
| `bench/` | Throughput and latency benchmark |
| `chaos/` | Crash-recovery test that kills workers at random |

## Usage

```ts
import { createQueue } from "./src/queue.js";

const queue = createQueue({
  config: { connectionString: "postgres://postgres:pw@localhost:5432/postgres" },
  concurrency: 10, // jobs this worker runs at once
});

queue.register("send_email", async (params: { to: string }) => {
  // do the work; throw to fail and retry
});

await queue.start();

// enqueue(functionId, params, pickupAt?, idempotencyKey?, maxAttempts?)
await queue.enqueue("send_email", { to: "a@example.com" });
await queue.enqueue("send_email", { to: "b@example.com" }, new Date(Date.now() + 60_000)); // in 1 minute
await queue.enqueue("send_email", { to: "c@example.com" }, undefined, "welcome-c", 5);     // deduplicated

// on shutdown
await queue.stop();
```

## Getting Started

```bash
pnpm install

# Postgres on :5432 (the scripts default to postgres://postgres:pw@localhost:5432/postgres)
docker run -d --name jobqueue-pg -e POSTGRES_PASSWORD=pw -p 5432:5432 postgres:16

psql postgres://postgres:pw@localhost:5432/postgres -f migrations/init.sql
psql postgres://postgres:pw@localhost:5432/postgres -f migrations/indices.sql
psql postgres://postgres:pw@localhost:5432/postgres -f migrations/notify.sql
```

Set `DATABASE_URL` to point the benchmark and chaos scripts at a different database.

## Benchmarks

```bash
pnpm tsx bench/runner.ts                                   # full run, writes bench/results.md
JOBS=20000 REPEATS=1 LAT_SECONDS=10 pnpm tsx bench/runner.ts   # quick smoke run
```

> **Warning:** the benchmark truncates `jobs.jobs`. Don't point it at a database you care about.

Each worker is a separate OS process running a no-op handler, so the numbers measure the queue itself (claim, ack, polling), not the work. Throughput is computed from row timestamps in the database, which excludes process startup. The latency producer schedules inserts by the clock rather than by replies, so a slow database shows up as latency instead of quietly slowing the load down.

**Setup:** MacBook Air (Apple M4, 16 GB). Workers and Postgres (in Docker) on the same machine. 100,000 jobs, concurrency 10 per worker, median of 3 runs.

| Workers | Jobs/sec (median) |
| --- | --- |
| 1 | 4,810 |
| 2 | 6,295 |
| 4 | 7,871 |
| 8 | 9,004 |

**Pickup latency** with 4 workers under a paced load (wait = enqueue to first claim):

| Load | Rate (jobs/s) | Wait p50 | Wait p95 | Wait p99 |
| --- | --- | --- | --- | --- |
| 25% | 1,968 | 2.3 ms | 7.0 ms | 148.7 ms |
| 50% | 3,936 | 2.0 ms | 7.7 ms | 13.4 ms |
| 80% | 6,297 | 6.5 ms | 20.3 ms | 28.3 ms |

Before `LISTEN/NOTIFY`, an idle worker slept for the full 1 s poll interval, and median pickup latency was about 500 ms.

> These numbers are for comparison only. They come from a single laptop where the workers and Postgres share the same cores, so they are not a production benchmark.

## Chaos test

```bash
pnpm tsx chaos/runner.ts
# knobs: JOBS=5000 WORKERS=5 KILL_EVERY_MS=3000 MAX_RUNTIME_MS=900000
```

The runner seeds jobs, starts worker processes, and **SIGKILLs a random worker every 3 s**, replacing it with a fresh one, until the queue drains. Then it checks the evidence:

- each handler writes to `chaos.results` (unique per job) and `chaos.executions` (one row per run)
- **lost jobs** = jobs with no row in `chaos.results`
- **duplicate executions** = `executions − results`

Latest run: 5,000 jobs, 5 workers, **94 kills**, drained in 283 s.

| Check | Result |
| --- | --- |
| Lost jobs | **0** |
| Stuck in `PENDING` / `IN_PROGRESS` | 0 |
| `DEAD` | 0 |
| Duplicate executions | 536 |

The duplicates are expected. They are jobs whose worker was killed after the handler wrote its result but before the `ACK`. Their lease expired, the reaper put them back, and another worker ran them again. Because the handler's write is idempotent (`ON CONFLICT DO NOTHING`), every job still has exactly one result. At-least-once delivery plus idempotent handlers gives exactly-once *effects*.

## Limitations

- **At-least-once, not exactly-once.** Handlers must tolerate running more than once.
- **`stop()` doesn't drain in-flight jobs.** It stops claiming and closes connections. Jobs still running lose their ACK and are retried after their lease expires.
- **`NOTIFY` serialises on commit.** Every transaction that sends a notification takes a brief global lock when it commits, which becomes a bottleneck at very high insert rates. One notification per statement keeps that cost low.
- **The listener needs a direct connection.** `LISTEN` doesn't work through PgBouncer in transaction-pooling mode.
- **No automated test suite yet.** Correctness is checked by the chaos test and the benchmark.
