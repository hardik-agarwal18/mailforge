# Mailforge Grafana dashboard

This directory adds a provisioned Prometheus datasource and an automatically provisioned Mailforge operations dashboard. It does not modify the existing base Compose file.

## Install

From the repository root, extract/copy this `deployments/grafana/` directory into the existing `deployments/` directory, merging the new files. Then run:

```bash
docker compose --env-file .env \
  -f deployments/docker-compose.yml \
  -f deployments/grafana/docker-compose.override.yml \
  up -d grafana
```

The override paths are relative to the first Compose file (`deployments/docker-compose.yml`), so the mounts resolve to `deployments/grafana/...`.

Open http://localhost:3000. The base Compose file currently configures the local Grafana login as `admin` / `mailforge_admin`. Change it before exposing Grafana beyond your machine.

## Dashboard contents

- SMTP listener and authenticated submission message rates and active connections
- SMTP AUTH successes/failures
- SMTP submission latency percentiles
- Outbound SMTP attempt outcomes and latency
- Delivery worker job outcomes and processing latency
- Redis stream entries, consumer-group lag, pending messages, consumers and oldest-pending idle time
- Prometheus target health

The dashboard expects the scrape jobs already configured in `deployments/prometheus/prometheus.yml`: `mailforge-smtp`, `mailforge-smtp-submission`, and `mailforge-delivery-worker-*`.

## Notes

- Queue gauges are exported by worker instances but describe the same Redis consumer group; panels use `max()` to avoid double-counting the same queue.
- Prometheus target health is shown for jobs matching `mailforge-.*`.
- PostgreSQL internal metrics are not included because the current Compose stack does not run a PostgreSQL exporter. Add an exporter before building database-specific panels.
- Panels may show `No data` until the relevant component has emitted metrics and Prometheus has scraped it. Generate test traffic to populate the graphs.
