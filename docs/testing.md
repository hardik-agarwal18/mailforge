# Testing guide

MailForge has unit tests plus opt-in integration tests. Use Go 1.26.8 or a compatible newer release, matching the `go` directive in `go.mod`.

## Test layers

| Layer | Scope | Dependencies |
|---|---|---|
| Unit tests | Parsers, state transitions, SMTP response classification, retry policy, report formatting, and utilities | No external services |
| PostgreSQL integration | Delivery leases, recipient transitions, outbox retry rows, delivery history, and DSN persistence | Disposable PostgreSQL database |
| Redis integration | Streams, consumer-group recovery, pending reclamation, and Redis AUTH Lua limiter | Disposable Redis instance |
| SMTP integration | Outbound SMTP protocol against an in-process local test server | No public MX; the SMTP receiver is started by the test |
| Race tests | Concurrent delivery/queue/limiter paths | Go race detector; PostgreSQL/Redis variables enable integration tests |
| End-to-end smoke test | Authenticated submission through PostgreSQL/outbox/Redis/worker to Mailpit-style mock SMTP | Local services and SMTP test credentials |

## Unit/static/build checks

```bash
gofmt -w $(find cmd internal -name '*.go' -type f)
go vet ./...
go test ./... -count=1
go test -race -count=1 ./internal/delivery ./internal/deliverybench ./internal/smtp
go build ./cmd/...
```

The format check in CI is non-mutating and fails if `gofmt` would change tracked Go files.

## Enable PostgreSQL and Redis integration tests locally

Use a disposable PostgreSQL database and Redis DB 15. Do not use production systems. Start the local dependencies using `.env`, then create a separate test database:

```bash
docker compose --env-file .env -f deployments/docker-compose.yml up -d postgres redis

set -a
source .env
set +a

docker compose --env-file .env -f deployments/docker-compose.yml exec postgres \
  createdb -U "$POSTGRES_USER" mailforge_test
```

Set the integration variables:

```bash
export MAILFORGE_TEST_DATABASE_URL="postgres://${POSTGRES_USER}:${POSTGRES_PASSWORD}@127.0.0.1:5434/mailforge_test?sslmode=disable"
export MAILFORGE_TEST_REDIS_ADDR="127.0.0.1:6379"
export MAILFORGE_TEST_REDIS_PASSWORD="$REDIS_PASSWORD"
export MAILFORGE_TEST_REDIS_DB="15"
export REDIS_TEST_ADDR="127.0.0.1:6379"
export REDIS_TEST_PASSWORD="$REDIS_PASSWORD"
```

The PostgreSQL test role must be able to create and drop schemas in the dedicated test database. The tests create a unique schema per PostgreSQL integration test, apply the repository migrations to that schema, and drop it afterward. Redis tests use unique test key/stream names and clean up only their own keys; they do not issue `FLUSHDB`.

Run the integration suites:

```bash
go test ./internal/delivery ./internal/deliverybench \
  -run '^TestIntegration|^TestMockSMTP' -count=1 -v

go test ./internal/smtp \
  -run '^TestRedisAuthAttemptLimiterIntegration$' -count=1 -v
```

Run the complete suite after setting the variables:

```bash
go test ./... -count=1
go test -race -count=1 ./internal/delivery ./internal/deliverybench ./internal/smtp
```

**Important:** integration tests skip when the corresponding environment variable is absent. A green test command with skipped integration tests does not prove PostgreSQL or Redis integration coverage. Check the verbose output for `SKIP` lines when running locally.

## CI behavior

`.github/workflows/ci.yml` starts PostgreSQL 17 and Redis 7.4 service containers, sets all four integration-test environment variables, then runs:

1. `gofmt` verification.
2. `go mod download`.
3. `go vet ./...`.
4. `go test ./... -count=1` (with PostgreSQL and Redis integration tests enabled).
5. Race tests for delivery, delivery benchmark, and SMTP packages.
6. `go build ./cmd/...`.

The CI PostgreSQL/Redis credentials are disposable test-only values. Do not copy them into a real environment.

## SMTP reliability scenarios

The delivery integration suite covers:

- Successful acceptance by a local SMTP receiver.
- Temporary 451 response followed by a scheduled retry and a successful second attempt.
- Permanent 550 response and a bounced terminal state.
- Duplicate delivery events without duplicate SMTP sends.
- An ambiguous SMTP outcome after an outcome-write failure.
- Exactly one retry event for a temporary failure.
- Redis pending-entry reclamation after a consumer crash, followed by processing and acknowledgement.
- Idempotent DSN handling and hard-bounce suppression.
- Delivery-status repository queries.

The temporary-retry integration test uses a five-minute test retry delay and advances both the recipient deadline and matching outbox availability inside the isolated test schema. It does not sleep for five minutes and does not alter production retry defaults.

## Interpreting an accepted message

A `delivered` status means the next-hop SMTP server accepted the message. It does not prove inbox placement. Where the remote server may have accepted DATA but the SMTP response is lost, the correct state can be `unknown`; this is intentionally not retried automatically.
