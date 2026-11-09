# MailForge

MailForge is a Go-based SMTP submission and asynchronous outbound email-delivery engine. It accepts authenticated SMTP submissions, stores messages and recipients durably in PostgreSQL, publishes delivery work through a transactional outbox and Redis Streams, and delivers messages to recipient mail servers or a configured relay.

It is a **mail-delivery backend**, not a Gmail replacement or hosted mailbox service. It does not provide a web inbox, IMAP, POP3, contacts, or a consumer-facing mail client.

## Current capabilities

- SMTP submission with STARTTLS and SMTP AUTH (PLAIN/LOGIN).
- Credential-backed sender authorization, verified sending domains, rate limits, and account/IP submission quotas.
- RFC 5322/MIME parsing and persistence, including text/HTML content, attachment metadata, and recipient handling.
- PostgreSQL message, recipient, delivery-attempt, delivery-event, and outbox persistence.
- Transactional outbox publication to Redis Streams consumer groups.
- Concurrent delivery workers with PostgreSQL leases, heartbeats, retries, dead-letter handling, and pending-entry recovery.
- Outbound SMTP delivery with MX lookup or an explicitly configured relay, TLS policy, and SMTP response classification.
- DKIM signing with configurable key material and keyset rotation support.
- VERP/HMAC bounce correlation, trusted-MTA DSN ingestion controls, and hard-bounce suppression.
- Read-only delivery status CLI plus audited operator reconciliation for ambiguous outcomes.
- Prometheus metrics defaulting to loopback, Grafana dashboards, and alert rules.
- Integration coverage for successful delivery, temporary/permanent SMTP failures, retries, duplicate events, ambiguous outcomes, Redis consumer recovery, and DSN suppression.

See [Architecture](docs/architecture.md) for the component flow, [Benchmarking](docs/benchmarking.md) for repeatable performance measurements, and [Project scope](docs/project-scope.md) for explicit limitations.

## Repository layout

| Path | Responsibility |
|---|---|
| `cmd/` | Executables for SMTP services, submission, workers, outbox publishing/admin, status, diagnostics, and benchmarks |
| `internal/message/` | Message validation, submission service, and persistence |
| `internal/mimeparser/` | MIME and message-content parsing |
| `internal/smtp/` | SMTP server and authenticated submission protocol |
| `internal/smtpauth/` | Credential and sending-domain authorization |
| `internal/outbound/` | Outbound SMTP transport and MX/relay handling |
| `internal/outbox/` | Durable outbox publishing and replay operations |
| `internal/delivery/` | Redis consumption, recipient delivery state, retries, status history, DSN handling |
| `internal/dkimsign/` | DKIM message signing and key management |
| `internal/bounce/`, `internal/bouncereceiver/`, `internal/dsn/` | Bounce addressing, inbound DSN reception, and report parsing |
| `internal/observability/` | Prometheus metrics and PostgreSQL pool instrumentation |
| `migrations/` | Ordered PostgreSQL schema migrations (15 migrations currently) |
| `deployments/` | Local Docker Compose dependencies, Prometheus, Grafana, Alertmanager, and exporters |
| `docs/` | Architecture, operations, security, testing, and CLI documentation |

## Requirements

- Go **1.26.8**, as declared in `go.mod`.
- Docker Engine and Docker Compose plugin for local PostgreSQL, Redis, and Mailpit.
- Goose v3.29.0 (or a compatible newer version) to apply the SQL migrations locally.
- OpenSSL to generate a local SMTP TLS certificate.

## Local development quick start

The following commands use Git Bash (the shell shown in the project's development workflow). Keep this setup local; the sample secrets and Mailpit plaintext relay are not production configuration.

### 1. Configure local secrets

```bash
cp .env.example .env
mkdir -p secrets
```

Edit `.env` and replace every `REPLACE_WITH_...` value with a freshly generated secret. For example:

```bash
openssl rand -hex 24   # PostgreSQL password
openssl rand -hex 24   # Redis password
openssl rand -hex 32   # VERP bounce HMAC secret
openssl rand -hex 24   # Grafana admin password
```

Create a local certificate for the submission server:

```bash
openssl req -x509 -newkey rsa:3072 -sha256 -nodes \
  -keyout secrets/smtp-key.pem \
  -out secrets/smtp-cert.pem \
  -days 365 \
  -subj "/CN=localhost" \
  -addext "subjectAltName=DNS:localhost,IP:127.0.0.1"
```

The certificate is for local development only. Use a certificate issued by your intended trust chain in any deployed environment.

### 2. Start local dependencies

```bash
docker compose --env-file .env -f deployments/docker-compose.yml \
  up -d postgres redis mailpit prometheus grafana alertmanager
```

Check service health:

```bash
docker compose --env-file .env -f deployments/docker-compose.yml ps
```

### 3. Load environment and apply migrations

```bash
set -a
source .env
set +a

goose -dir migrations postgres "$DATABASE_URL" up
```

### 4. Create an SMTP AUTH credential

The command reads the password from standard input so it does not appear in shell history or process arguments:

```bash
printf '%s\n' "$SMTP_PASSWORD" | \
  go run ./cmd/smtp-user \
    --username "$SMTP_USERNAME" \
    --email "$SMTP_FROM"
```

Use a sender address and domain that are authorized for this credential. For additional sending domains, use `cmd/smtp-domain` and complete its DNS TXT verification before sending from that domain.

### 5. Start the application processes

Run each long-lived process in a separate terminal. In each terminal, load the same `.env` first:

```bash
set -a
source .env
set +a
```

Terminal 1 — outbound delivery workers:

```bash
go run ./cmd/delivery-worker
```

Terminal 2 — transactional outbox publisher:

```bash
go run ./cmd/outbox-publisher
```

Terminal 3 — authenticated SMTP submission:

```bash
go run ./cmd/smtp-submission
```

Optional local-only SMTP injection listener:

```bash
go run ./cmd/smtp
```

Optional inbound DSN/bounce receiver (requires the bounce domain and HMAC secret configured consistently with the delivery worker):

```bash
go run ./cmd/smtp-bounce
```

### 6. Inspect local services

- Mailpit inbox: <http://127.0.0.1:8025>
- Grafana: <http://127.0.0.1:13001>
- Prometheus: <http://127.0.0.1:9093>
- Alertmanager: <http://127.0.0.1:9094>

SMTP submission listens on `127.0.0.1:1587` with the sample `.env`. The local injection listener uses `127.0.0.1:2525`; the bounce receiver defaults to `127.0.0.1:2526`. Metrics listeners are configured separately per process; do not expose metrics endpoints publicly.

For operations and recovery procedures, see [Operations](docs/operations.md). For ambiguous recipient outcomes, see [Delivery reconciliation](docs/delivery-reconciliation.md). For performance measurements, see [Benchmarking](docs/benchmarking.md).

## Automated worker-scaling experiment

On Windows Git Bash, use `./scripts/run-worker-scaling.bash` to run the actual
delivery-worker concurrency profiles `1`, `3`, and `5` with a fixed submission
concurrency of `3`. It manages the worker lifecycle and captures benchmark JSON,
Prometheus metric snapshots, Windows CPU/RAM samples, and Docker stats. See
[`docs/worker-scaling-automation.md`](docs/worker-scaling-automation.md) for
preconditions and configuration.

## Development commands

```bash
gofmt -w $(find cmd internal -name '*.go' -type f)
go vet ./...
go test ./... -count=1
go test -race -count=1 ./internal/delivery ./internal/deliverybench ./internal/smtp
go build ./cmd/...
```

The integration tests are opt-in locally. Set the PostgreSQL and Redis test variables described in [Testing](docs/testing.md) to execute them. GitHub Actions provisions disposable PostgreSQL and Redis services and supplies those variables so CI exercises the integration paths rather than silently skipping them.

A convenience `Makefile` is also provided; run `make help` to list its targets.

## Security and operational notes

- Never commit `.env`, private keys, SMTP passwords, DKIM keys, bounce HMAC secrets, or production credentials.
- The authenticated submission server requires TLS certificate and key files; it does not accept plaintext credentials.
- The sample relay is plaintext Mailpit on loopback and is only for local development.
- The unauthenticated SMTP listener is loopback-only and intended for trusted local injection/testing.
- Configure firewalling, DNS, TLS, secret storage, retention, monitoring, and outbound abuse controls before any public deployment.
- SMTP acceptance by a recipient server is not proof of inbox placement. SMTP cannot guarantee exactly-once delivery after an ambiguous connection failure; MailForge records an `unknown` outcome rather than retrying blindly in that case.

See [Security](docs/security.md), [Operations](docs/operations.md), and [Project scope](docs/project-scope.md).

## License

No license file is currently included. Add a license before distributing or accepting external contributions.

