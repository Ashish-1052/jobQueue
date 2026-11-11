/**
 * Checks the evidence after the chaos run. Exits 0 on pass, 1 on fail.
 * Run alone with:  JOBS=5000 pnpm tsx chaos/verify.ts
 */
import { Pool } from "pg";

const connectionString =
  process.env.DATABASE_URL ?? "postgres://postgres:pw@localhost:5432/postgres";
const N = Number(process.env.JOBS ?? 5000);

async function main() {
  const pool = new Pool({ connectionString, max: 1 });

  const byStatus = await pool.query(
    `SELECT status::text AS status, count(*)::int AS n FROM jobs.jobs GROUP BY status ORDER BY status`,
  );
  const results = (await pool.query(`SELECT count(*)::int AS n FROM chaos.results`)).rows[0].n;
  const executions = (await pool.query(`SELECT count(*)::int AS n FROM chaos.executions`)).rows[0].n;

  // Which job numbers 1..N have NO row in results? Those are lost jobs.
  const missing = (
    await pool.query(
      `SELECT count(*)::int AS n
       FROM generate_series(1, $1::int) AS g(i)
       LEFT JOIN chaos.results r ON r.job_n = g.i
       WHERE r.job_n IS NULL`,
      [N],
    )
  ).rows[0].n;

  const count = (s: string) => byStatus.rows.find((r) => r.status === s)?.n ?? 0;
  const stuck = count("PENDING") + count("IN_PROGRESS");
  const dead = count("DEAD");
  const duplicates = executions - results;

  console.log("\n=== chaos verification ===");
  console.log("jobs by status:      ", Object.fromEntries(byStatus.rows.map((r) => [r.status, r.n])));
  console.log("expected jobs:       ", N);
  console.log("rows in results:     ", results);
  console.log("lost (no result):    ", missing);
  console.log("stuck in queue:      ", stuck);
  console.log("dead:                ", dead);
  console.log("handler executions:  ", executions);
  console.log("duplicate executions:", duplicates);

  const pass = missing === 0 && results === N && stuck === 0 && dead === 0;
  console.log(pass ? "\nPASS: zero lost jobs" : "\nFAIL: see numbers above");

  await pool.end();
  process.exit(pass ? 0 : 1);
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});