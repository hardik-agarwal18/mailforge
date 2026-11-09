# MailForge benchmarking

This guide captures reproducible end-to-end benchmark evidence for the final-year
project. Run benchmarks against a local/test MailForge deployment and the
benchmark's built-in mock SMTP receiver. **Do not point benchmark traffic at real
recipient domains or public MX servers.** The tool enforces a reserved `.test`
recipient domain and records whether traffic reached the mock receiver.

## What is measured

`cmd/delivery-benchmark` submits messages through authenticated STARTTLS SMTP,
then observes recipient delivery outcomes in PostgreSQL. Its JSON report records:

- workload size, submission concurrency, AUTH parallelism, message size, mock
  delay, and the reported worker-concurrency label;
- Go version, operating system, architecture, and logical CPU count;
- submission duration and messages/second;
- per-message submission, connection, AUTH, and SMTP transaction latency
  (average, P50, P95, P99);
- delivered, bounced, unknown, pending, deferred, and in-flight recipient counts;
- whole-run end-to-end delivered messages/second and message-created-to-delivered
  latency (average, P50, P95, P99);
- mock SMTP connections, accepted/rejected recipients, dropped DATA responses,
  and bytes received.

The end-to-end throughput is `delivered recipients / total benchmark wall time`.
It is not a raw SMTP worker-only throughput measurement. The tool does not
currently sample CPU or RAM usage; record those separately from Windows Task
Manager/Performance Monitor or your OS monitoring tools.

## 1. Prepare a safe local environment

1. Use a dedicated local/test database and local Redis instance.
2. Create a dedicated SMTP credential for benchmarking. Confirm its account and
   recipient quotas are high enough for the intended run volume; do not disable
   quotas globally just to make the benchmark pass.
3. Load the local environment in Git Bash:

   ```bash
   set -a
   source .env
   set +a
   ```

4. Start PostgreSQL, Redis, and Mailpit using the Compose instructions in the
   root README. Apply migrations and make sure the SMTP credential can submit
   using `SMTP_FROM`.

The benchmark itself starts a mock SMTP listener. The delivery worker must be
configured to relay to the same listener address; the defaults below use
`127.0.0.1:2526`.

## 2. Start MailForge processes

Start each process in its own Git Bash terminal. Load `.env` in each terminal.

Terminal A — delivery worker. Pick a concurrency level and use it consistently
for the whole run:

```bash
DELIVERY_WORKER_CONCURRENCY=3 \
SMTP_RELAY_HOST=127.0.0.1 \
SMTP_RELAY_PORT=2526 \
SMTP_RELAY_TLS_MODE=none \
go run ./cmd/delivery-worker
```

Terminal B — outbox publisher:

```bash
go run ./cmd/outbox-publisher
```

Terminal C — authenticated SMTP submission server:

```bash
go run ./cmd/smtp-submission
```

The benchmark's `--mock-addr` must match the host/port configured as the
worker's relay. Do not run another service on that port while benchmarking.

## 3. Run the submission-concurrency matrix

In a fourth terminal, load `.env` and run:

```bash
set -a
source .env
set +a

DELIVERY_WORKER_CONCURRENCY=3 \
BENCHMARK_WORKERS=3 \
BENCHMARK_TOTAL=100 \
BENCHMARK_CONCURRENCIES="1 5 10 25" \
./scripts/run-benchmark-matrix.bash
```

The script creates a timestamped directory under `artifacts/benchmarks/` and
writes one `.json` report and one `.log` file per submission-concurrency level.
The JSON reports should be retained as the raw evidence for your final report.

The value passed as `--workers` is only a label in the report. It does not start,
stop, or reconfigure a worker. The worker process must already be running with
that `DELIVERY_WORKER_CONCURRENCY` value. The script prints this warning before
running the matrix.

You can override settings with environment variables:

| Variable | Default | Purpose |
|---|---:|---|
| `BENCHMARK_TOTAL` | `100` | Messages per matrix profile |
| `BENCHMARK_CONCURRENCIES` | `1 5 10 25` | Submission sessions per profile |
| `BENCHMARK_WORKERS` | `DELIVERY_WORKER_CONCURRENCY`, then `3` | Worker count label; must match the running worker |
| `BENCHMARK_MESSAGE_BYTES` | `1024` | Synthetic message body size |
| `BENCHMARK_MOCK_DELAY` | `0s` | Artificial delay before mock SMTP's final DATA response |
| `BENCHMARK_DELIVERY_TIMEOUT` | `3m` | Maximum time to await terminal delivery outcomes |
| `BENCHMARK_POLL_INTERVAL` | `250ms` | PostgreSQL status polling interval |
| `BENCHMARK_OUTPUT_DIR` | timestamped artifacts directory | Report output directory |
| `BENCHMARK_HOST` / `BENCHMARK_PORT` | `localhost` / `1587` | Submission endpoint |
| `BENCHMARK_MOCK_ADDR` | `127.0.0.1:2526` | Mock SMTP listener address |
| `BENCHMARK_RECIPIENT_DOMAIN` | `bench.mailforge.test` | Must end in `.test` |

## 4. Compare worker concurrency

To measure worker scaling, repeat the same workload while changing the actual
worker process concurrency. For each run:

1. Stop the delivery-worker process cleanly.
2. Restart it with `DELIVERY_WORKER_CONCURRENCY=1`, then run the same benchmark
   profile and label it `BENCHMARK_WORKERS=1`.
3. Repeat with `3`, `6`, and `10` (or sensible values for your machine).
4. Keep submission concurrency, message size, mock delay, total messages,
   database, Redis, and machine conditions constant across comparisons.

Example worker start for a six-worker profile:

```bash
DELIVERY_WORKER_CONCURRENCY=6 \
SMTP_RELAY_HOST=127.0.0.1 \
SMTP_RELAY_PORT=2526 \
SMTP_RELAY_TLS_MODE=none \
go run ./cmd/delivery-worker
```

Then set `BENCHMARK_WORKERS=6` when running the benchmark. **Do not change only
the CLI label and claim the worker pool was scaled.** The `--workers` flag is
informational by design.

Check SMTP account quotas before running a large matrix. The default local
example has finite per-hour account/IP submission and recipient quotas. Use a
dedicated test credential with appropriately sized limits, or lower
`BENCHMARK_TOTAL`/the number of profiles. Do not delete production data or reset
Redis to prepare a benchmark.

## 5. Repeat and interpret results

For a defensible comparison:

- Run one small warm-up profile first and do not include it in the final table.
- Run each profile at least three times when quota and test time allow.
- Keep the same machine, Go version, Docker resource limits, database, Redis,
  message size, and mock delay.
- Report the median of the three runs for throughput and P50/P95/P99 latency;
  retain all raw JSON files.
- Treat a profile as invalid if any submission fails, delivery is incomplete,
  mock traffic is not observed, or recipients remain pending/deferred/in-flight.
- Do not compare runs taken while unrelated CPU-heavy workloads are running.

A higher worker count is not automatically better. Watch for throughput plateaus,
P95/P99 latency regressions, database pool contention, Redis pending growth, and
CPU saturation. Select the smallest worker count that achieves the throughput
and latency target without creating excessive resource use.

## 6. Capture environment and resource evidence

Fill out `docs/benchmark-results-template.md` with:

- CPU model, logical core count, installed RAM, OS version, Go version;
- Docker version and PostgreSQL/Redis image tags;
- worker concurrency, submission concurrency, message count and body size;
- median throughput and P50/P95/P99 latency;
- peak CPU and memory observed during the run;
- relevant Prometheus/Grafana screenshots for worker, outbox, Redis, and database
  behaviour.

A `delivered` result means the next-hop mock SMTP server accepted the message;
it does not establish inbox placement. The benchmark is a controlled pipeline
measurement, not a claim about public-provider deliverability.
