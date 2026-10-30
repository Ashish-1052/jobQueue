import { ConnectionPool, createPool } from "./db.ts";
import { ACK, CLAIM, ENQUEUE, ENQUEUE_FALLBACK, FAIL, REAP, TICK } from "./sql.js";

type Handler = (...args: any[]) => Promise<any> | any;

interface QueueParams {
  config: any;
  concurrency?: number;
  registry?: Map<string, Handler>;
}

export class Queue {
  private db: ConnectionPool;
  private workerId: string;
  private registry: Map<string, Handler>;
  private _concurrency: number;
  private isRunning: boolean = false;
  private pollIntervalMs: number = 1000;
  private tickIntervalMs: number = 15000;
  private reapIntervalMs: number = 5 * 60 * 1000;
  private runningCount: number = 0;

  constructor(params: QueueParams) {
    this.db = createPool(params.config);
    this.workerId = crypto.randomUUID();
    this.registry = new Map(params?.registry);
    this._concurrency = params?.concurrency ?? 10;
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
        pickupAt ? pickupAt.toISOString() : new Date().toISOString(), 
        idempotencyKey ?? null, 
        maxAttempts ?? 3
      ];
      const res = await this.db.query(query, values);
      
      if (res && res.rows.length > 0) {
        return { job: res.rows[0], created: true };
      }
      const existing = await this.db.query(ENQUEUE_FALLBACK, [functionId, idempotencyKey]);
      return { job: existing, created: false };
    } catch (e) {
      console.error("Error to enqueue", e);
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
    console.log(`Queue worker ${this.workerId} started with concurrency ${this._concurrency}.`);

    // TODO: Implement job polling from database
    // 1. start claiming by checking the free slots
    // 2. call the handlers on each returned jobs
    // 3. attach the ack and failure parts to the handlers after resolving
    // 4. heartbeat on the jobs that are currently being worked upon
    // 5. reaper that cleans (a separate timer I suppose)
    
    
    const jobsClaims = this.claim; // steps 1, 2, 3
    this.pollWithTimeOut(jobsClaims, this.pollIntervalMs, 'JOBS');
    const tick = this.tick; // 4
    this.pollWithTimeOut(tick, this.tickIntervalMs, 'TICK');
    const reap = this.reap; // 5
    this.pollWithTimeOut(reap, this.reapIntervalMs, 'REAP');
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
        const handler = this.registry.get(job.functionId);
        if (!handler) continue;
        this.runningCount += 1;
        const jobPromise = Promise.resolve()
          .then(() => handler(JSON.parse(job.function_params)))
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
            console.log(`Finished processing job ${job.id}`);
          });
        jobPromises.push(jobPromise);
      }
      await Promise.all(jobPromises);
      return { claimed: res.rows.length, processed, failed };
    } catch (e) {
      console.error("Error claiming jobs", e)
      return { claimed: 0, processed: 0, failed: 0 };
    }
  }

  ack = async (jobId: string) => {
    try {
      this.db.query(ACK, [jobId, this.workerId]);
      console.log(`Job acknowledged successfully for id: ${jobId}`);
    } catch (e) {
      console.log(`Unable to acknowledge job for id: ${jobId}`);
    }
  }

  fail = async (jobId: string, e: Error) => {
    try {
      this.db.query(FAIL, [jobId, this.workerId, e.message]);
      console.log(`Job failed successfully for id: ${jobId}`);
    } catch (e) {
      console.log(`Unable to fail job for id: ${jobId}`);
    }
  }

  tick = async () => {
    try {
      await this.db.query(TICK);
    } catch (e) {
      console.log("unable to tick", e);
    }
  }

  reap = async () => {
    try {
      const res = await this.db.query(REAP);
      return res.rows.length;
    } catch (e) {
      return 0;
      console.log("Unable to reap", e);
    }
  }
  
  stop = async () => {
    this.isRunning = false;
    await this.db.close();
    console.log(`Queue worker ${this.workerId} stopped.`);
  }

  pollWithTimeOut = (task: (...args: any[]) => Promise<any>, interval: number, taskType: string) => {
    let timerId: ReturnType<typeof setTimeout>;
      let isCancelled = false;
    
      const execute = async () => {
        try {
          await task();
        } catch (e) {
          console.error(`Error polling task type: ${taskType}`, e);
        } finally {
          if (!isCancelled) {
            timerId = setTimeout(execute, interval);
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