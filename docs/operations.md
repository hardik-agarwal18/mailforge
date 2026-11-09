# Operations guide

This guide describes local operation and the minimum operator checks for MailForge. The Compose file is a development stack, not a hardened production deployment.

## Processes

| Process | Command | Responsibility |
|---|---|---|
| SMTP submission | `go run ./cmd/smtp-submission` | STARTTLS + AUTH intake |
| Local SMTP injection | `go run ./cmd/smtp` | Loopback-only trusted test injection |
| Outbox publisher | `go run ./cmd/outbox-publisher` | Publish due PostgreSQL outbox events to Redis |
| Delivery worker | `go run ./cmd/delivery-worker` | Consume delivery jobs and perform outbound SMTP |
| Bounce receiver | `go run ./cmd/smtp-bounce` | Receive and process DSNs using VERP/HMAC |
| Delivery report | `go run ./cmd/delivery-status <id>` | Read-only recipient and event report |
| Unknown-delivery reconciliation | `go run ./cmd/delivery-reconcile --help` | Audited accepted/retry decision for `unknown` recipients |
| Outbox administration | `go run ./cmd/outbox-admin` | Inspect/replay dead outbox events (see command usage) |

Run long-lived application commands in separate processes/terminals. Each process needs the appropriate environment variables and database/Redis access.

## Local dependency stack

Start and inspect dependencies:

```bash
docker compose --env-file .env -f deployments/docker-compose.yml \
  up -d postgres redis mailpit prometheus grafana alertmanager

docker compose --env-file .env -f deployments/docker-compose.yml ps
```

Stop the containers but retain named volumes:

```bash
docker compose --env-file .env -f deployments/docker-compose.yml down
```

To remove local data volumes as well (destructive; removes local PostgreSQL, Redis, Grafana, and Prometheus state):

```bash
docker compose --env-file .env -f deployments/docker-compose.yml down -v
```

## Database migrations

MailForge uses Goose-formatted SQL migrations. Back up the database before applying migrations to any environment containing important data.

```bash
goose -dir migrations postgres "$DATABASE_URL" status
goose -dir migrations postgres "$DATABASE_URL" up
goose -dir migrations postgres "$DATABASE_URL" version
```

Migrations are forward schema changes. Review the `Down` section and recovery implications before attempting a rollback.

## Service health and diagnostics

Check local dependencies:

```bash
docker compose --env-file .env -f deployments/docker-compose.yml exec postgres \
  pg_isready -U "$POSTGRES_USER" -d "$POSTGRES_DB"

docker compose --env-file .env -f deployments/docker-compose.yml exec redis \
  redis-cli -a "$REDIS_PASSWORD" ping
```

Useful application diagnostics:

```bash
go run ./cmd/dbcheck
go run ./cmd/delivery-status -h
DATABASE_URL="$DATABASE_URL" go run ./cmd/outbox-admin list-dead
# MX diagnostic example (performs a real DNS lookup):
go run ./cmd/mxcheck recipient@example.com
```

Use the delivery-status report to inspect a message by its internal UUID or RFC Message-ID. The report is read-only. Do not update recipient status rows manually to force delivery; doing so can break the attempt history, retry schedule, or lease invariants.

## Monitoring

The local Compose stack exposes:

| Service | Local URL |
|---|---|
| Mailpit | `http://127.0.0.1:8025` |
| Grafana | `http://127.0.0.1:13001` |
| Prometheus | `http://127.0.0.1:9093` |
| Alertmanager | `http://127.0.0.1:9094` |

Application metrics are process-local HTTP endpoints. The defaults and overrides are documented in `.env.example` and the relevant command packages. Keep them on loopback or a private monitoring network and restrict access with host/network firewalls. If Prometheus runs in Docker while application processes run on the host, ensure the host metrics listeners are reachable from the Prometheus container; do not expose them to an untrusted network simply to make scraping work.

Alertmanager routes local alert email to Mailpit in the sample configuration. Inspect the captured alert at the Mailpit UI. The `mailforge-alerts@localhost` recipient is a local-development sink, not a real notification destination.

## Recovery playbook

### Redis unavailable

1. Check the Redis container and its health.
2. Check `REDIS_ADDR` and `REDIS_PASSWORD` for the publisher, submission server, and workers.
3. Restore Redis connectivity before restarting dependent processes.
4. Confirm the PostgreSQL outbox still contains pending/publishing work; PostgreSQL remains the durable record of intent.
5. Restart the publisher/worker processes and inspect queue/worker metrics.

### Worker stops while processing

1. Check worker logs and process health.
2. Inspect Redis pending entries and consumer state using the existing Redis operational tools.
3. Allow the configured pending-entry idle threshold and lease expiry/recovery mechanism to reclaim eligible work.
4. Query the recipient using `cmd/delivery-status`.
5. If the recipient is `unknown`, investigate the remote SMTP transaction before any manual replay. It may have been accepted remotely.
6. Only after evidence is available, use `cmd/delivery-reconcile` with an operator identity, reason, and evidence reference. See [Delivery reconciliation](delivery-reconciliation.md). Do not update recipient rows manually.

### Outbox events are dead-lettered

1. Inspect the event and its last error using `cmd/outbox-admin`.
2. Fix the underlying database, Redis, or configuration problem first.
3. Replay only after understanding whether the event can cause a duplicate external side effect.
4. Record the operator and reason for replay according to your operational process.

### A recipient is deferred

Check SMTP response code, enhanced status code, failure stage, last error, and `next_attempt_at` in the delivery-status report. A 4xx response is generally temporary; a 5xx response is generally permanent, but consult the remote server's response and SMTP semantics rather than relying only on the numeric prefix.

## Operational guardrails

- Do not point integration tests at production databases or Redis instances.
- Keep the Redis test DB (15 by default) separate from application DB 0.
- Preserve PostgreSQL and Redis persistent volumes during ordinary restarts.
- Do not run `docker compose down -v` unless local data loss is intended.
- Back up PostgreSQL before schema changes and test restore procedures.
- Alerting and metrics are not substitutes for structured logs, backups, and an incident process.
- Metrics listeners default to loopback. Override their addresses only on a protected monitoring network.
- The DSN receiver defaults to loopback. A non-loopback bind requires trusted peer CIDRs plus mutual TLS; see [Security](security.md#bounce-security).
