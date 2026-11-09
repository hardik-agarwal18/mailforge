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
- The DSN receiver accepts loopback peers by default. A non-loopback listener cannot start without an explicit trusted MTA source-IP/CIDR allowlist; DSN processing is rate-limited per source IP.
- Submission and outbox-publisher metrics listeners default to loopback (`127.0.0.1`), not all interfaces.
- Ambiguous delivery outcomes can only be reconciled through an explicit operator command that requires an actor and reason and writes a durable audit record.

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
- Keep `SMTP_BOUNCE_LISTEN_ADDR` on loopback behind a trusted MTA wherever possible. If the receiver must bind to a non-loopback interface, set `SMTP_BOUNCE_TRUSTED_PEERS` to exact MTA IPs/CIDRs and configure `SMTP_BOUNCE_TLS_CLIENT_CA_FILE` with the CA that issued the MTA client certificate. The process refuses non-loopback binding without both a non-wildcard peer allowlist and mutual TLS (`RequireAndVerifyClientCert`).
- Restrict the front MTA so only its DSN/bounce transport can reach the receiver. Apply firewall rules in addition to the application allowlist. Never put `0.0.0.0/0` or `::/0` in `SMTP_BOUNCE_TRUSTED_PEERS`.
- Configure `SMTP_BOUNCE_MAX_DSNS_PER_PEER_PER_MINUTE` for expected MTA volume; the default is 300 per source IP per receiver process. For multiple receiver replicas, also enforce a shared limit at the front MTA or network edge. Monitor rate-limit responses and adjust deliberately.
- The VERP HMAC authenticates the return-path token and recipient correlation; it does **not** cryptographically authenticate the DSN body or prove who authored it. The trusted MTA/IP boundary is an ingress control, not end-to-end DSN authenticity. Treat all DSN fields as untrusted claims, and review suppression decisions/abuse signals before broad public use.
- Treat parsed DSN content as untrusted input. Do not render DSN fields as trusted HTML or use them as shell input.

## Before any public deployment

- [ ] Threat-model SMTP submission, authentication, bounce ingestion, outbox administration, and status lookup.
- [ ] Use managed secrets and rotate all credentials/keys under a documented process.
- [ ] Configure valid certificates and enforce the intended TLS policy for all external SMTP hops.
- [ ] Bind database, Redis, metrics, and admin interfaces to private networks; enforce firewall rules.
- [ ] Put inbound DSN reception behind a trusted MTA, configure `SMTP_BOUNCE_TRUSTED_PEERS`, verify the network boundary and test rate limits before accepting public bounce traffic.
- [ ] Configure SPF, DKIM, and DMARC DNS records and monitor alignment and delivery reputation.
- [ ] Add abuse detection, recipient/domain allowlists or onboarding controls, bounce/complaint handling, and sending-rate policies appropriate to the service.
- [ ] Define retention and deletion policy for message bodies, addresses, logs, and delivery events.
- [ ] Back up PostgreSQL and test restore; decide Redis persistence and recovery objectives.
- [ ] Run dependency/image vulnerability scans and an independent security review.
- [ ] Validate alert routing, log redaction, metrics access, and incident response procedures.
