# MailForge Grafana dashboards

This directory adds a provisioned Prometheus datasource and automatically provisioned MailForge dashboards. It is an optional Compose override; the base Compose file can run Grafana without the dashboards provisioned.

## Start Grafana with the provisioned dashboards

From the repository root, ensure `.env` has a unique `GRAFANA_ADMIN_PASSWORD`, then run:

```bash
docker compose --env-file .env \
  -f deployments/docker-compose.yml \
  -f deployments/grafana/docker-compose.override.yml \
  up -d grafana
```

Open <http://127.0.0.1:13001>. The login defaults to the `GRAFANA_ADMIN_USER` value from `.env` (default `admin`) and the required `GRAFANA_ADMIN_PASSWORD` secret. Keep Grafana on a private network and change the password before using the stack on a shared machine.

## Dashboard contents

- SMTP listener and authenticated submission message rates and active connections
- SMTP AUTH successes/failures
- SMTP submission latency percentiles
- Outbound SMTP attempt outcomes and latency
- Delivery worker job outcomes and processing latency
- Redis stream entries, consumer-group lag, pending messages, consumers, and oldest-pending idle time
- Prometheus target health

The dashboard expects the scrape jobs configured in `deployments/prometheus/prometheus.yml`: `mailforge-smtp`, `mailforge-smtp-submission`, and `mailforge-delivery-worker-*`.

## Notes

- Queue gauges are exported by worker instances but describe the same Redis consumer group; panels use `max()` to avoid double-counting the same queue.
- Prometheus target health is shown for jobs matching `mailforge-.*`.
- PostgreSQL-specific panels require the optional PostgreSQL exporter Compose override in `deployments/postgres-exporter/`.
- Redis-specific panels require the optional Redis exporter Compose override in `deployments/redis-exporter/`.
- Panels may show `No data` until the relevant component has emitted metrics and Prometheus has scraped it. Generate test traffic to populate the graphs.
