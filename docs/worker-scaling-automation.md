# Automated worker-scaling benchmark

Run the worker-concurrency experiment from **Git Bash on Windows** with one
command. The orchestrator builds the worker and benchmark executables once,
then runs worker concurrency `1`, `3`, and `5` while holding submission
concurrency constant at `3`.

## Preconditions

1. Start the local PostgreSQL and Redis containers from `deployments/docker-compose.yml`.
   The script verifies that `DATABASE_URL` and `REDIS_ADDR` use loopback hosts; it
   refuses remote database/Redis configurations for safety.
2. Start the outbox publisher in a separate terminal:

   ```bash
   set -a; source .env; set +a
   go run ./cmd/outbox-publisher
   ```

3. Start SMTP submission in another terminal, using the local TLS certificate:

   ```bash
   set -a; source .env; set +a
   go run ./cmd/smtp-submission
   ```

4. Stop any manually launched delivery worker. The script owns the worker lifecycle
   and requires the worker metrics port (default `127.0.0.1:9091`) to be free.
5. Ensure the benchmark queue is drained. The script checks for recipient rows in
   `pending`, `delivering`, or `deferred` state and outbox rows in `pending` or
   `publishing` state. It refuses to continue if old work could contaminate a run.
6. Ensure the mock SMTP port (default `127.0.0.1:2526`) is free. Do not run the
   bounce receiver on that port during the benchmark; the benchmark executable
   starts its own mock SMTP server on that address.

## Run the entire worker-scaling experiment

From the repository root:

```bash
./scripts/run-worker-scaling.bash
```

The script loads `.env` automatically. Defaults are:

- worker concurrency: `1 3 5`;
- submission concurrency: `3`;
- messages per profile: `100`;
- repeats per worker count: `1`;
- mock relay: `127.0.0.1:2526`, plaintext only on loopback;
- worker metrics: `127.0.0.1:9091`;
- outbox publisher metrics: `http://127.0.0.1:9096/metrics`.

The delivery worker is compiled to `bin/mailforge-delivery-worker.exe` and
started separately for each concurrency level with the matching actual
`DELIVERY_WORKER_CONCURRENCY`. The benchmark uses the same mock address as the
worker's SMTP relay. The script waits for each profile to finish, stops the
worker, confirms PostgreSQL/outbox are idle, and then moves to the next profile.

## Optional overrides

Set these before running the script:

```bash
BENCHMARK_WORKER_CONCURRENCIES="1 3 5" \
BENCHMARK_SUBMISSION_CONCURRENCY=3 \
BENCHMARK_TOTAL=100 \
BENCHMARK_REPEATS=1 \
BENCHMARK_DELIVERY_TIMEOUT=3m \
BENCHMARK_RESOURCE_INTERVAL_SECONDS=2 \
./scripts/run-worker-scaling.bash
```

Other overrides:

| Variable | Default | Purpose |
|---|---|---|
| `BENCHMARK_WORKER_CONCURRENCIES` | `1 3 5` | Actual worker process concurrency levels to start |
| `BENCHMARK_SUBMISSION_CONCURRENCY` | `3` | Fixed SMTP submission concurrency for all profiles |
| `BENCHMARK_TOTAL` | `100` | Messages per profile |
| `BENCHMARK_REPEATS` | `1` | Repeats per worker count; increase only after checking account/IP quotas |
| `BENCHMARK_MOCK_ADDR` | `127.0.0.1:2526` | Loopback mock SMTP address; the worker is configured to relay here |
| `BENCHMARK_WORKER_METRICS_ADDR` | `127.0.0.1:9091` | Worker metrics listener used for scrape snapshots |
| `BENCHMARK_OUTBOX_METRICS_URL` | `http://127.0.0.1:9096/metrics` | Outbox publisher health check |
| `BENCHMARK_POSTGRES_CONTAINER` | `mailforge-postgres` | Docker container used for queue-drain checks |
| `BENCHMARK_REDIS_CONTAINER` | `mailforge-redis` | Docker container checked with `PING` |
| `BENCHMARK_QUEUE_IDLE_TIMEOUT_SECONDS` | `90` | Maximum wait for prior work to drain |
| `BENCHMARK_WORKER_START_TIMEOUT_SECONDS` | `45` | Maximum worker startup wait |
| `BENCHMARK_RESOURCE_INTERVAL_SECONDS` | `2` | Windows/Docker/Prometheus resource sampling interval |
| `BENCHMARK_OUTPUT_DIR` | timestamped path | Explicit output directory |

The SMTP submission server should already be running with the benchmark-specific
`SMTP_AUTH_RATE_LIMIT_PREFIX` in its environment if you configured a dedicated
namespace. Restart that server after changing `.env`; changing the file does not
change an already-running process. The script does not change or clear Redis
rate-limit keys.

## Output

The script creates a timestamped directory under `artifacts/benchmarks/`:

```text
worker-scaling-YYYYMMDD-HHMMSS/
  run-info.txt
  summary.csv
  worker-1/repeat-1/
    benchmark.json
    benchmark.log
    worker.log
    worker-metrics-snapshots.txt
    outbox-metrics-snapshots.txt
    submission-metrics-snapshots.txt
    windows-resources.csv
    docker-stats.csv
  worker-3/repeat-1/...
  worker-5/repeat-1/...
```

A profile is accepted only if the benchmark exits successfully, all requested
recipients are delivered, the mock receiver reports traffic, and the PostgreSQL
queue/outbox are idle afterward. Failed profiles stop the script; fix the cause
before rerunning.

## Safety and interpretation

- Recipient domains must end in `.test`; the script rejects other domains.
- The mock SMTP address must be loopback. SMTP relay TLS is disabled only for
  that local mock endpoint; it is not a production relay configuration.
- The script does not flush Redis, truncate PostgreSQL, or delete old benchmark
  messages. It refuses to run if active recipients or pending/publishing outbox
  rows remain.
- Keep the submission server, outbox publisher, database, Redis, machine load,
  and workload constant while comparing worker counts.
- Use the median of repeated runs for final claims. One run per profile is an
  initial comparison, not a statistically strong capacity claim.
- `delivered` means the local next-hop mock SMTP server accepted the message;
  it does not measure inbox placement with public mail providers.
