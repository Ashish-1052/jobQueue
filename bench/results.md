## Throughput (100,000 no-op jobs, concurrency 10 per worker)

| Workers | Jobs/sec (median) | Runs |
|---|---|---|
| 1 | 4810 | 4853, 4810, 4539 |
| 2 | 6295 | 6347, 6295, 6190 |
| 4 | 7871 | 7675, 7871, 8075 |
| 8 | 9004 | 9004, 8900, 9035 |

## Latency (4 workers, 30s paced load, first 5s excluded)

Reference max throughput: 7871 jobs/sec

| Load | Rate (jobs/s) | Jobs measured | Wait p50 | Wait p95 | Wait p99 | End-to-end p99 |
|---|---|---|---|---|---|---|
| 25% | 1968 | 49210 | 2.3 ms | 7.0 ms | 148.7 ms | 152.6 ms |
| 50% | 3936 | 98432 | 2.0 ms | 7.7 ms | 13.4 ms | 15.0 ms |
| 80% | 6297 | 157412 | 6.5 ms | 20.3 ms | 28.3 ms | 29.2 ms |

Wait = enqueue to first claim. End-to-end = enqueue to ack.
