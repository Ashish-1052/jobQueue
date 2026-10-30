import{ Pool, type PoolConfig } from "pg";

export interface DBConfig {
  host: string;
  user: string;
  port: number;
  ssl?: boolean;
  database: string;
  password?: string;
}

export class ConnectionPool {
  private pool: Pool;

  constructor(config: PoolConfig) {
    this.pool = new Pool(config);

    this.pool.on("connect", () => {
      console.log("Connection Established with DB successfully");
    });
  
    this.pool.on("error", (err) => {
      console.error("Failed to Connect with DB", err);
    });
  }

  async close() {
    await this.pool.end();
  }

  async query(query: string, values?: unknown[]) {
    return this.pool.query(query, values);
  }
}

export const createPool = (config: PoolConfig) => {
  return new ConnectionPool(config);
}
