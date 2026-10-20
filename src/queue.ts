import type { Pool } from "pg";
import { createPool } from "./db.js";
import { ENQUEUE } from "./sql.js";

type Handler = (...args: any[]) => Promise<any> | any;

interface QueueParams {
  config: any;
  concurrency?: number;
  registry?: Map<string, Handler>;
}

export class Queue {
  private db: Pool;
  private workerId: string;
  private registry: Map<string, Handler>;
  private _concurrency: number;
  private isRunning: boolean = false;
  private pollIntervalMs: number = 1000;

  constructor(params: QueueParams) {
    this.db = createPool(params.config);
    this.workerId = crypto.randomUUID();
    this.registry = params?.registry ?? new Map();
    this._concurrency = params?.concurrency ?? 10;
  }

  enqueue = async (
    functionId: string, 
    functionParams: Record<string, any>, 
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
      return { job: null, created: false };
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

    while (this.isRunning) {
      try {
        // TODO: Implement job polling from database
        await new Promise((resolve) => setTimeout(resolve, this.pollIntervalMs));
      } catch (err) {
        console.error("Error in worker poll loop:", err);
      }
    }
  }
  
  stop = async () => {
    this.isRunning = false;
    await this.db.end();
    console.log(`Queue worker ${this.workerId} stopped.`);
  }
}
