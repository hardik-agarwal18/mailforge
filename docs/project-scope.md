# Project scope and non-goals

## What MailForge is

MailForge is a self-built Go SMTP submission and asynchronous outbound email-delivery engine. The project demonstrates protocol handling, cryptographic signing, durable event-driven processing, PostgreSQL state management, Redis Streams, retry/lease design, failure recovery, and metrics/monitoring.

## Included in the current project

- Authenticated SMTP submission with STARTTLS.
- SMTP credential and sending-domain management.
- Email/MIME parsing and durable persistence.
- Transactional outbox and Redis Streams work distribution.
- Outbound SMTP delivery, MX lookup, relay support, and response classification.
- Per-recipient retry scheduling, leases, delivery history, and ambiguous-outcome handling.
- DKIM signing and key rotation support.
- VERP/HMAC bounce correlation, DSN processing, and hard-bounce suppression.
- Delivery-status CLI, operator tooling, Prometheus metrics, Grafana dashboards, alerts, CI, and documentation.

## Explicit non-goals for this release

The following are separate products/subsystems and are not required to complete the current final-year project:

- A Gmail-like web frontend or mailbox/inbox UI.
- IMAP or POP3 mailbox access.
- A complete inbound MX/mailbox hosting platform.
- Full inbound SPF/DKIM/DMARC enforcement and aggregate DMARC report ingestion.
- Spam classification, antivirus scanning, or attachment sandboxing.
- A multi-region active-active service or Kubernetes operator.
- A general-purpose multi-tenant SaaS control plane and billing system.

## Definition of done

The current project should be considered complete when all of the following are true:

1. `go test ./... -count=1` passes with PostgreSQL and Redis integration variables configured.
2. `go vet ./...` and the critical-package race tests pass.
3. CI runs on pushes and pull requests with database/Redis integration tests enabled.
4. A fresh developer can follow the README to start dependencies, migrate the schema, create an SMTP credential, and run the processes.
5. The successful delivery, temporary retry, permanent bounce, duplicate event, consumer recovery, and DSN suppression scenarios can be demonstrated.
6. Delivery-status CLI and Prometheus/Grafana evidence make outcomes observable.
7. A final benchmark report records workload parameters and latency/throughput results.

After these criteria are met, freeze feature development. Fix correctness/security defects and documentation issues, but do not expand into the non-goals above for this project submission.
