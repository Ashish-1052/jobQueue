import type { PoolConfig } from "pg";
import { ConnectionPool, createPool } from "./db.ts";
import { ACK, CLAIM, ENQUEUE, ENQUEUE_FALLBACK, FAIL, HEARTBEAT, REAP } from "./sql.ts";

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

export class Queue {
  private db: ConnectionPool;
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
    
    
    const jobsClaims = this.claim; // steps 1, 2, 3
    this.pollWithTimeOut(jobsClaims, this.pollIntervalMs, 'JOBS');
    const reap = this.reap; // 5
    this.pollWithTimeOut(reap, this.reapIntervalMs, 'REAP');
    this.cancelers.push(this.pollWithTimeOut(this.claim, this.pollIntervalMs, 'CLAIM'));
    this.cancelers.push(this.pollWithTimeOut(this.reap, this.reapIntervalMs, 'REAP'));
  }

  claim = async () => {
    try {    
      const freeSlots = this._concurrency - this.runningCount;
      if (!freeSlots) return { claimed: 0 };
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
      return { claimed: res.rows.length, processed, failed };
    } catch (e: any) {
      this.logger.error(e?.message)
      return { claimed: 0, processed: 0, failed: 0 };
    }
  }

  ack = async (jobId: string) => {
    try {
      await this.db.query(ACK, [jobId, this.workerId]);
      this.logger.info(`Job acknowledged successfully for id: ${jobId}`);
    } catch (e) {
      this.logger.info(`Unable to acknowledge job for id: ${jobId}`);
    }
  }

  fail = async (jobId: string, e: any) => {
    try {
      await this.db.query(FAIL, [jobId, this.workerId, e instanceof Error ? e.message : String(e)]);
      this.logger.info(`Job failed successfully for id: ${jobId}`);
    } catch (e) {
      this.logger.info(`Unable to fail job for id: ${jobId}`);
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

  reap = async () => {
    try {
      const res = await this.db.query(REAP);
      return res.rows.length;
    } catch (e: any) {
      this.logger.error(e?.message);
      return 0;
    }
  }
  
  stop = async () => {
    this.isRunning = false;
    await this.db.close();
    this.cancelers.forEach(c => c());
    this.cancelers = [];
    this.logger.info(`Queue worker ${this.workerId} stopped.`);
  }

  pollWithTimeOut = (task: (...args: any[]) => Promise<any>, interval: number, taskType: string) => {
    let timerId: ReturnType<typeof setTimeout>;
      let isCancelled = false;
    
      const execute = async () => {
        let delay = interval;
        try {
          const next = await task();
          if (typeof next === 'number') delay = next;
        } catch (e) {
          this.logger.error(`Error polling task type: ${taskType}`);
        } finally {
          if (!isCancelled) {
            timerId = setTimeout(execute, delay);
          }
        }
      };
    
      execute();
    
      // Call this function when you want to stop polling
      return () => {
        isCancelled = true;
        clearTimeout(timerId);
      };
  }
}

export const createQueue = (params: QueueParams) => {
  return new Queue(params);
}