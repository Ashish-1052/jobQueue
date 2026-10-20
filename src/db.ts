import pg from "pg";

export interface DBConfig {
  host: string;
  user: string;
  port: number;
  ssl?: boolean;
  database: string;
  password?: string;
}

export const createPool = (config: DBConfig) => {
  const pool = new pg.Pool(config);

  pool.on("connect", () => {
    console.log("Connection Established with DB successfully");
  });

  pool.on("error", (err) => {
    console.error("Failed to Connect with DB", err);
  });

  return pool;
};
