import { Client, type PoolConfig } from "pg";
import { ConnectionPool, createPool } from "./db.ts";
import { ACK, CLAIM, ENQUEUE, ENQUEUE_FALLBACK, FAIL, HEARTBEAT, JOBS_CHANNEL, REAP } from "./sql.ts";

type Handler = (params: any) => Promise<void> | any;

interface QueueParams {
  config: PoolConfig;
  concurrency?: number;
  registry?: Map<string, Handler>;
  logger?: Logger;
}

interface Logger {
  info(msg: string, meta?: object): void;
  warn(msg: string, meta?: object): void;
  error(msg: string, meta?: object): void;
}

interface Poller {
  cancel: () => void;
  wake: () => void;
}

export class Queue {
  private db: ConnectionPool;
  private config: PoolConfig;
  private listener: Client | null = null;
  private listenRetryTimer: ReturnType<typeof setTimeout> | null = null;
  private claimLoop: Poller | null = null;
  private workerId: string;
  private registry: Map<string, Handler>;
  private _concurrency: number;
  private isRunning: boolean = false;
  private pollIntervalMs: number = 1000;
  private heartbeatIntervalMs: number = 15000;
  private reapIntervalMs: number = 20000;
  private runningCount: number = 0;
  private cancelers: Array<() => void> = []
  private logger: Logger;

  constructor(params: QueueParams) {
    this.config = params.config;
    this.db = createPool(params.config);
    this.workerId = crypto.randomUUID();
    this.registry = new Map(params?.registry);
    this._concurrency = params?.concurrency ?? 10;
    this.logger = params.logger ?? { info() {}, warn() {}, error: console.error };
  }

  enqueue = async (
    functionId: string, 
    functionParams: unknown = {}, 
    pickupAt?: Date, 
    idempotencyKey?: string, 
    maxAttempts?: number
  ) => {
    try {
      const query = ENQUEUE;
      const values = [
        functionId, 
        JSON.stringify(functionParams), 
        pickupAt ? pickupAt.toISOString() : null, 
        idempotencyKey ?? null, 
        maxAttempts ?? 3
      ];
      const res = await this.db.query(query, values);
      
      if (res && res.rows.length > 0) {
        return { job: res.rows[0], created: true };
      }
      const existing = await this.db.query(ENQUEUE_FALLBACK, [functionId, idempotencyKey]);
      return { job: existing.rows[0], created: false };
    } catch (e: any) {
      this.logger.error(e?.message);
      throw e;
    }
  }

  register = (functionId: string, handler: Handler) => {
    if (this.registry.has(functionId)) {
      throw new Error(`A handler already exists for ${functionId}`);
    }
    this.registry.set(functionId, handler);
    return { registered: functionId };
  }
  
  deregister = (functionId: string) => {
    if (!this.registry.has(functionId)) {
      throw new Error(`A handler does not exist for ${functionId}`);
    }
    this.registry.delete(functionId);
    return { deregistered: functionId };
  }

  start = async () => {
    if (this.isRunning) return;
    this.isRunning = true;
    this.logger.info(`Queue worker ${this.workerId} started with concurrency ${this._concurrency}.`);

    // TODO: Implement job polling from database
    // 1. start claiming by checking the free slots
    // 2. call the handlers on each returned jobs
    // 3. attach the ack and failure parts to the handlers after resolving
    // 4. heartbeat on the jobs that are currently being worked upon
    // 5. reaper that cleans (a separate timer I suppose)
    
    this.claimLoop = this.pollWithTimeOut(this.claim, this.pollIntervalMs, 'CLAIM');
    this.cancelers.push(this.claimLoop.cancel);
    this.cancelers.push(this.pollWithTimeOut(this.reap, this.reapIntervalMs, 'REAP').cancel);
    void this.listen();
  }

  // Push-based wake-up. A dedicated connection (not from the pool: LISTEN is
  // per-session, and a pooled connection could be handed to someone else) waits
  // for NOTIFYs sent by the insert trigger and wakes the claim loop immediately.
  // Polling stays as the safety net: if this connection dies, or a notification
  // is missed, the worker still finds jobs within pollIntervalMs.
  private listen = async () => {
    if (!this.isRunning) return;
    const client = new Client(this.config);
    let retired = false;
    const retire = (reason: string) => {
      if (retired) return;
      retired = true;
      if (!this.isRunning) return; // normal shutdown, not a failure
      this.logger.warn(`LISTEN connection lost (${reason}), falling back to polling and retrying`);
      if (this.listener === client) this.listener = null;
      client.end().catch(() => {});
      if (this.isRunning) this.listenRetryTimer = setTimeout(this.listen, 1000);
    };

    client.on('notification', () => this.claimLoop?.wake());
    client.on('error', (e) => retire(e.message));
    client.on('end', () => retire('connection ended'));

    try {
      await client.connect();
      await client.query(`LISTEN ${JOBS_CHANNEL}`);
      if (!this.isRunning) {
        retired = true;
        await client.end();
        return;
      }
      this.listener = client;
    } catch (e: any) {
      retire(e?.message ?? String(e));
    }
  }

  claim = async () => {
    try {    
      const freeSlots = this._concurrency - this.runningCount;
      if (!freeSlots) return 10; // slots full but will free up soon; don't sleep the full poll interval
      const res = await this.db.query(CLAIM, [this.workerId, freeSlots]);
      const jobPromises: Promise<any>[] = [];
      let processed = 0;
      let failed = 0;
      
      for (let job of res.rows) {
        const handler = this.registry.get(job.function_id);
        if (!handler) {
          this.fail(job.id, new Error('no handler for ' + job.function_id))
          continue;
        }
        this.runningCount += 1;
        const stopHeartbeat = this.startHeartbeat(job.id);
        const jobPromise = Promise.resolve()
          .then(() => handler(job.function_params))
          .then(async () => {
            await this.ack(job.id);
            processed++;
          })
          .catch(async (e) => {
            await this.fail(job.id, e);
            failed++
          })
          .finally(() => {
            this.runningCount -= 1;
            this.logger.info(`Finished processing job ${job.id}`);
            stopHeartbeat();
          });
        jobPromises.push(jobPromise);
      }
      void Promise.all(jobPromises); // dont await here, pool cant claim until every job finishes
      return res.rows.length === freeSlots ? 0 : this.pollIntervalMs;
    } catch (e: any) {
      this.logger.error(e?.message)
      return 0;
    }
  }

  ack = async (jobId: string) => {
    try {
      await this.db.query(ACK, [jobId, this.workerId]);
      this.logger.info(`Job acknowledged successfully for id: ${jobId}`);
    } catch (e) {
      this.logger.error(`Unable to acknowledge job for id: ${jobId}`);
    }
  }

  fail = async (jobId: string, e: any) => {
    try {
      await this.db.query(FAIL, [jobId, this.workerId, e instanceof Error ? e.message : String(e)]);
      this.logger.info(`Job failed successfully for id: ${jobId}`);
    } catch (e) {
      this.logger.error(`Unable to fail job for id: ${jobId}`);
    }
  }

  startHeartbeat = (jobId: string) => {
    let inFlight = false;
    const timer = setInterval(async () => {
      if (inFlight) return;
      inFlight = true;
      try {
        const res = await this.db.query(HEARTBEAT, [jobId, this.workerId]);
        if (res.rows.length === 0) {
          console.warn(`lease lost for job ${jobId}`);
          clearInterval(timer);
        }
      } catch (e: any) {
        this.logger.error(e?.message);
      }
    }, this.heartbeatIntervalMs); // about a third of the lease: 10s for a 30s lease
    return () => clearInterval(timer);
  }

  // Returns nothing on purpose: pollWithTimeOut treats a returned number as the
  // next delay, so returning the reaped count (often 0) made the reaper spin.
  reap = async (): Promise<void> => {
    try {
      const res = await this.db.query(REAP);
      if (res.rows.length > 0) this.logger.info(`Reaped ${res.rows.length} expired jobs`);
    } catch (e: any) {
      this.logger.error(e?.message);
    }
  }

  stop = async () => {
    this.isRunning = false;
    // Stop timers and the listener first, then close the pool, so nothing
    // tries to query a pool that has already been closed.
    this.cancelers.forEach(c => c());
    this.cancelers = [];
    this.claimLoop = null;
    if (this.listenRetryTimer) clearTimeout(this.listenRetryTimer);
    const listener = this.listener;
    this.listener = null;
    await listener?.end().catch(() => {});
    await this.db.close();
    this.logger.info(`Queue worker ${this.workerId} stopped.`);
  }

  pollWithTimeOut = (task: (...args: any[]) => Promise<any>, interval: number, taskType: string, ...args: any[]): Poller => {
      let timerId: ReturnType<typeof setTimeout> | undefined;
      let isCancelled = false;
      let isRunning = false;
      let wakeRequested = false;

      const schedule = (delay: number) => {
        if (!isCancelled) timerId = setTimeout(execute, delay);
      };

      const execute = async () => {
        isRunning = true;
        wakeRequested = false;
        let delay = interval;
        try {
          const next = await task(...args);
          if (typeof next === 'number') delay = next;
        } catch (e) {
          this.logger.error(`Error polling task type: ${taskType}`);
        } finally {
          isRunning = false;
          // A wake that arrived while the task was running may refer to jobs
          // inserted after our query looked, so run again right away.
          schedule(wakeRequested ? 0 : delay);
        }
      };

      execute();

      return {
        // Call this when you want to stop polling
        cancel: () => {
          isCancelled = true;
          clearTimeout(timerId);
        },
        // Run the task now instead of waiting out the current delay
        wake: () => {
          if (isCancelled) return;
          if (isRunning) {
            wakeRequested = true;
            return;
          }
          clearTimeout(timerId);
          schedule(0);
        },
      };
  }
}

export const createQueue = (params: QueueParams) => {
  return new Queue(params);
}