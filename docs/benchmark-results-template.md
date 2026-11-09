# MailForge benchmark results

> Replace every placeholder with measurements from your own run. Do not use
> estimates or example values as measured results. Attach or link the raw JSON
> reports produced under `artifacts/benchmarks/`.

## Environment

| Field | Measured value |
|---|---|
| Date/time and timezone | TODO |
| OS and version | TODO |
| CPU model | TODO |
| Logical CPU count | TODO |
| Installed RAM | TODO |
| Go version | TODO |
| Docker Engine version | TODO |
| PostgreSQL image | `postgres:17.11-alpine` (confirm actual run) |
| Redis image | `redis:7.4.12-alpine` (confirm actual run) |
| Database and Redis placement | TODO (local/container/remote) |

## Fixed workload settings

| Parameter | Value |
|---|---|
| Messages per run | TODO |
| Message body size | TODO |
| Recipient domain | Reserved `.test` domain |
| Mock SMTP delay | TODO |
| Submission TLS/authentication | STARTTLS + AUTH |
| Worker relay | Local mock SMTP receiver |
| Warm-up excluded? | TODO |
| Repetitions per profile | TODO |

## Submission concurrency comparison

Use the median of at least three valid runs per profile when possible.

| Submission sessions | Actual worker concurrency | Median submission msg/s | Median end-to-end delivered msg/s | Submission P95 ms | End-to-end P95 ms | Delivery complete? |
|---:|---:|---:|---:|---:|---:|---|
| 1 | TODO | TODO | TODO | TODO | TODO | TODO |
| 5 | TODO | TODO | TODO | TODO | TODO | TODO |
| 10 | TODO | TODO | TODO | TODO | TODO | TODO |
| 25 | TODO | TODO | TODO | TODO | TODO | TODO |

## Worker concurrency comparison

Hold submission concurrency and all other workload settings constant.

| Actual worker concurrency | Submission sessions | Median end-to-end delivered msg/s | End-to-end P95 ms | Peak CPU % | Peak memory MB | Delivery complete? |
|---:|---:|---:|---:|---:|---:|---|
| 1 | TODO | TODO | TODO | TODO | TODO | TODO |
| 3 | TODO | TODO | TODO | TODO | TODO | TODO |
| 6 | TODO | TODO | TODO | TODO | TODO | TODO |
| 10 | TODO | TODO | TODO | TODO | TODO | TODO |

## Reliability and saturation observations

- Submission failures: TODO
- Delivered / bounced / unknown recipients: TODO
- Remaining pending / deferred / delivering recipients: TODO
- Mock SMTP accepted/rejected recipients: TODO
- Redis pending-entry or queue-backlog observations: TODO
- PostgreSQL pool saturation or slow queries: TODO
- CPU or memory saturation: TODO
- Other anomalies: TODO

## Interpretation

TODO: Explain where throughput plateaus, whether P95/P99 latency worsens as
concurrency increases, and which worker count gives the best balance of
throughput, latency, and resource consumption.

## Evidence

- Raw JSON report paths: TODO
- Prometheus/Grafana screenshots: TODO
- Git commit SHA: TODO
