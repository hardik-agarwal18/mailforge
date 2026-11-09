# Ambiguous delivery reconciliation

MailForge moves an SMTP attempt to `unknown` when the worker can no longer prove
whether the remote server accepted the message. This is intentional: SMTP does
not provide exactly-once delivery, and automatically retrying an ambiguous
final-DATA outcome can send duplicates.

`cmd/delivery-status` remains read-only. An operator must inspect the recipient,
remote MTA logs, connection timeline, and any provider-side evidence before
making a decision.

## Decisions

### Confirm next-hop acceptance

Use `accepted` only when remote MTA evidence confirms that the server accepted
the message. This marks the recipient `delivered` in MailForge's next-hop sense;
it does not prove inbox placement or end-user reading.

```bash
DATABASE_URL="$DATABASE_URL" go run ./cmd/delivery-reconcile \
  --recipient 8a4fd9a1-9d66-4b9a-9e46-58c4b06e7350 \
  --decision accepted \
  --actor oncall@example.com \
  --reason "Remote MX logs confirm 250 acceptance after DATA" \
  --evidence "INC-1234 / mx-02 log reference"
```

### Authorize a retry

Use `retry` only when evidence establishes that the remote server did **not**
accept the message. This schedules a fresh delivery attempt through the
transactional outbox. It can create a duplicate if the evidence is wrong.

```bash
DATABASE_URL="$DATABASE_URL" go run ./cmd/delivery-reconcile \
  --recipient 8a4fd9a1-9d66-4b9a-9e46-58c4b06e7350 \
  --decision retry \
  --actor oncall@example.com \
  --reason "Remote MX logs prove connection closed before message acceptance" \
  --evidence "INC-1234 / mx-02 log reference"
```

Both decisions require an operator identity and reason. Every successful
reconciliation writes a structured row to `delivery_reconciliations`, including
the authenticated PostgreSQL `session_user`, and a recipient status event in
the same PostgreSQL transaction. Restrict database credentials for this command
to authorized operators; the `--actor` value is an attribution label, while
`database_role` records the authenticated database principal. Retry authorization
also inserts its outbox event in that transaction. Reconciliation is allowed
only while the recipient is still `unknown`; repeated or stale decisions are
rejected.

Never use the retry decision merely because the recipient remains unknown or
because a customer requests another send. When uncertainty cannot be resolved,
leave the recipient in `unknown` and escalate rather than risking duplicate mail.
