# Security notes and deployment checklist

MailForge includes several security controls, but the repository is a student/portfolio engineering project and has not been independently penetration-tested. Treat the Compose configuration as a local development environment, not a production-ready public mail service.

## Implemented controls

- SMTP submission requires TLS before SMTP AUTH; the server does not accept plaintext credentials.
- SMTP credentials are stored as password hashes (PBKDF2-based), rather than as plaintext passwords.
- Authenticated sender addresses are checked against the credential's primary address and verified sending domains.
- SMTP AUTH attempts are rate-limited using Redis-backed atomic operations; the limiter fails closed when Redis is unavailable.
- Submission quotas are tracked by authenticated account identity and source IP; account usage is keyed by the credential identity, not the envelope sender.
- SMTP listener limits restrict concurrent connections, authentication, DATA handling, and recipient count.
- DKIM signing supports managed key material and active/retiring selectors for rotation.
- Bounce tokens use HMAC validation; DSN processing is idempotent and hard-bounce suppression is persisted.
- Metrics and local infrastructure ports are intended to remain on loopback/private networks.

## Secret handling

Never commit any of the following:

- `.env` files or real environment values.
- SMTP passwords or database credentials.
- SMTP TLS private keys.
- DKIM private keys or keyset manifests containing private-key paths/material.
- VERP bounce HMAC secrets.
- Grafana admin credentials or production notification credentials.

The repository `.gitignore` excludes `.env` and `secrets/`. Keep the placeholders in `.env.example`; generate unique secrets for each environment. Use a secret manager or the deployment platform's secret facility in a real deployment.

If a secret is accidentally committed, rotate it. Removing it from the latest commit does not invalidate the exposed value or remove it from repository history.

## Network exposure

- `cmd/smtp` is a loopback-only unauthenticated listener intended for local injection. Do not bind it publicly.
- Expose `cmd/smtp-submission` only behind intended firewall/network controls and a valid trusted TLS certificate.
- The sample worker relay is plaintext Mailpit on loopback. Do not use plaintext SMTP for an untrusted network hop.
- Keep PostgreSQL, Redis, metrics endpoints, Prometheus, Grafana, and Alertmanager private. The sample Compose file binds infrastructure ports to host loopback.
- The sample Prometheus scrape targets use `host.docker.internal`. Adapt host networking carefully on Linux; do not open application metrics endpoints to the public internet just to satisfy scraping.
- Before accepting public inbound mail, implement and validate a proper public MX deployment, abuse controls, TLS policy, DNS records, monitoring, and provider/network requirements.

## Email authenticity and DNS

- DKIM signing is implemented for outbound messages when configured.
- `cmd/domain-authcheck` provides SPF/DKIM/DMARC DNS diagnostics; it is not a complete inbound SPF evaluator, DKIM verifier, or DMARC policy enforcement pipeline.
- Publishing SPF, DKIM, and DMARC records is a responsibility of the domain operator. Use a restrictive DMARC policy only after validating legitimate senders and reporting.
- DNS verification of a sending domain demonstrates control of the domain's verification record; it does not guarantee reputation or inbox placement.

## Bounce security

- Use a strong random VERP HMAC secret and keep it stable while outstanding VERP addresses can receive DSNs.
- Configure the same bounce domain and HMAC secret across all workers and the bounce receiver.
- Do not expose the sample bounce receiver to the public internet without a production TLS/network design and rate/connection controls.
- Treat parsed DSN content as untrusted input. Do not render DSN fields as trusted HTML or use them as shell input.

## Before any public deployment

- [ ] Threat-model SMTP submission, authentication, bounce ingestion, outbox administration, and status lookup.
- [ ] Use managed secrets and rotate all credentials/keys under a documented process.
- [ ] Configure valid certificates and enforce the intended TLS policy for all external SMTP hops.
- [ ] Bind database, Redis, metrics, and admin interfaces to private networks; enforce firewall rules.
- [ ] Configure SPF, DKIM, and DMARC DNS records and monitor alignment and delivery reputation.
- [ ] Add abuse detection, recipient/domain allowlists or onboarding controls, bounce/complaint handling, and sending-rate policies appropriate to the service.
- [ ] Define retention and deletion policy for message bodies, addresses, logs, and delivery events.
- [ ] Back up PostgreSQL and test restore; decide Redis persistence and recovery objectives.
- [ ] Run dependency/image vulnerability scans and an independent security review.
- [ ] Validate alert routing, log redaction, metrics access, and incident response procedures.
