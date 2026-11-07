-- +goose Up

-- Preserve a durable timeline for each recipient's delivery lifecycle.
ALTER TABLE recipients
    DROP CONSTRAINT IF EXISTS valid_recipient_status;

ALTER TABLE recipients
    ADD CONSTRAINT valid_recipient_status
    CHECK (status IN ('pending', 'delivering', 'deferred', 'delivered', 'bounced', 'unknown'));

ALTER TABLE recipients
    ADD COLUMN IF NOT EXISTS last_attempt_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS delivered_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS bounced_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS unknown_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS last_smtp_code INTEGER,
    ADD COLUMN IF NOT EXISTS last_enhanced_status_code TEXT,
    ADD COLUMN IF NOT EXISTS last_smtp_stage TEXT,
    ADD COLUMN IF NOT EXISTS last_error TEXT;

ALTER TABLE recipients
    ADD CONSTRAINT valid_recipient_enhanced_status_code
    CHECK (
        last_enhanced_status_code IS NULL
        OR last_enhanced_status_code ~ '^[245]\.(0|[1-9][0-9]{0,2})\.(0|[1-9][0-9]{0,2})$'
    );

ALTER TABLE delivery_attempts
    DROP CONSTRAINT IF EXISTS valid_attempt_status;

ALTER TABLE delivery_attempts
    ADD CONSTRAINT valid_attempt_status
    CHECK (status IN ('success', 'temporary_failure', 'permanent_failure', 'unknown'));

ALTER TABLE delivery_attempts
    ADD COLUMN IF NOT EXISTS remote_host TEXT,
    ADD COLUMN IF NOT EXISTS tls_used BOOLEAN NOT NULL DEFAULT FALSE,
    ADD COLUMN IF NOT EXISTS tls_mode TEXT,
    ADD COLUMN IF NOT EXISTS tls_version TEXT,
    ADD COLUMN IF NOT EXISTS tls_cipher_suite TEXT;

CREATE TABLE delivery_status_events (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    recipient_id UUID NOT NULL REFERENCES recipients(id) ON DELETE CASCADE,
    from_status TEXT,
    to_status TEXT NOT NULL,
    event_type TEXT NOT NULL,
    smtp_code INTEGER,
    enhanced_status_code TEXT,
    smtp_stage TEXT,
    diagnostic TEXT,
    remote_host TEXT,
    tls_used BOOLEAN NOT NULL DEFAULT FALSE,
    tls_mode TEXT,
    tls_version TEXT,
    tls_cipher_suite TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT valid_delivery_status_event_to
      CHECK (to_status IN ('pending', 'delivering', 'deferred', 'delivered', 'bounced', 'unknown')),
    CONSTRAINT valid_delivery_status_event_from
      CHECK (from_status IS NULL OR from_status IN ('pending', 'delivering', 'deferred', 'delivered', 'bounced', 'unknown')),
    CONSTRAINT valid_delivery_status_event_enhanced_code
      CHECK (
        enhanced_status_code IS NULL
        OR enhanced_status_code ~ '^[245]\.(0|[1-9][0-9]{0,2})\.(0|[1-9][0-9]{0,2})$'
      )
);

CREATE INDEX idx_delivery_status_events_recipient_created
    ON delivery_status_events(recipient_id, created_at, id);

CREATE INDEX idx_recipients_message_status
    ON recipients(message_id, status);

-- +goose Down

-- The previous schema has no `unknown` status. Preserve a non-retryable
-- terminal representation when rolling back the enum/check constraint.
UPDATE recipients SET status = 'bounced' WHERE status = 'unknown';
UPDATE delivery_attempts SET status = 'permanent_failure' WHERE status = 'unknown';

DROP INDEX IF EXISTS idx_recipients_message_status;
DROP INDEX IF EXISTS idx_delivery_status_events_recipient_created;
DROP TABLE IF EXISTS delivery_status_events;

ALTER TABLE delivery_attempts
    DROP COLUMN IF EXISTS tls_cipher_suite,
    DROP COLUMN IF EXISTS tls_version,
    DROP COLUMN IF EXISTS tls_mode,
    DROP COLUMN IF EXISTS tls_used,
    DROP COLUMN IF EXISTS remote_host;

ALTER TABLE delivery_attempts
    DROP CONSTRAINT IF EXISTS valid_attempt_status;
ALTER TABLE delivery_attempts
    ADD CONSTRAINT valid_attempt_status
    CHECK (status IN ('success', 'temporary_failure', 'permanent_failure'));

ALTER TABLE recipients
    DROP CONSTRAINT IF EXISTS valid_recipient_enhanced_status_code,
    DROP COLUMN IF EXISTS last_error,
    DROP COLUMN IF EXISTS last_smtp_stage,
    DROP COLUMN IF EXISTS last_enhanced_status_code,
    DROP COLUMN IF EXISTS last_smtp_code,
    DROP COLUMN IF EXISTS unknown_at,
    DROP COLUMN IF EXISTS bounced_at,
    DROP COLUMN IF EXISTS delivered_at,
    DROP COLUMN IF EXISTS last_attempt_at;

ALTER TABLE recipients
    DROP CONSTRAINT IF EXISTS valid_recipient_status;
ALTER TABLE recipients
    ADD CONSTRAINT valid_recipient_status
    CHECK (status IN ('pending', 'delivering', 'deferred', 'delivered', 'bounced'));
