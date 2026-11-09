-- +goose Up

ALTER TABLE recipients
    ADD COLUMN IF NOT EXISTS final_delivery_confirmed_at TIMESTAMPTZ;

ALTER TABLE recipients
    DROP CONSTRAINT IF EXISTS valid_recipient_status;
ALTER TABLE recipients
    ADD CONSTRAINT valid_recipient_status
    CHECK (status IN ('pending', 'delivering', 'deferred', 'delivered', 'bounced', 'unknown', 'suppressed'));

ALTER TABLE delivery_status_events
    DROP CONSTRAINT IF EXISTS valid_delivery_status_event_to;
ALTER TABLE delivery_status_events
    ADD CONSTRAINT valid_delivery_status_event_to
    CHECK (to_status IN ('pending', 'delivering', 'deferred', 'delivered', 'bounced', 'unknown', 'suppressed'));

ALTER TABLE delivery_status_events
    DROP CONSTRAINT IF EXISTS valid_delivery_status_event_from;
ALTER TABLE delivery_status_events
    ADD CONSTRAINT valid_delivery_status_event_from
    CHECK (from_status IS NULL OR from_status IN ('pending', 'delivering', 'deferred', 'delivered', 'bounced', 'unknown', 'suppressed'));

CREATE TABLE recipient_suppressions (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    email TEXT NOT NULL,
    mailbox_key TEXT NOT NULL UNIQUE,
    reason TEXT NOT NULL,
    source TEXT NOT NULL,
    source_dsn_report_id UUID,
    diagnostic TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT valid_recipient_suppression_reason
        CHECK (reason IN ('hard_bounce', 'complaint', 'manual')),
    CONSTRAINT nonempty_suppression_source
        CHECK (length(source) > 0)
);

CREATE INDEX idx_recipient_suppressions_created_at
    ON recipient_suppressions(created_at DESC);

CREATE TABLE processed_dsn_reports (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    recipient_id UUID NOT NULL REFERENCES recipients(id) ON DELETE CASCADE,
    report_sha256 CHAR(64) NOT NULL,
    message_id TEXT,
    final_recipient TEXT NOT NULL,
    action TEXT NOT NULL,
    enhanced_status_code TEXT NOT NULL,
    diagnostic TEXT,
    reporting_mta TEXT,
    remote_mta TEXT,
    original_envelope_id TEXT,
    received_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT processed_dsn_report_recipient_hash_unique
        UNIQUE (recipient_id, report_sha256),
    CONSTRAINT valid_processed_dsn_action
        CHECK (action IN ('failed', 'delayed', 'delivered', 'relayed', 'expanded')),
    CONSTRAINT valid_processed_dsn_status
        CHECK (enhanced_status_code ~ '^[245]\.(0|[1-9][0-9]{0,2})\.(0|[1-9][0-9]{0,2})$')
);

CREATE INDEX idx_processed_dsn_reports_recipient_received
    ON processed_dsn_reports(recipient_id, received_at DESC);

ALTER TABLE recipient_suppressions
    ADD CONSTRAINT recipient_suppressions_dsn_report_fk
    FOREIGN KEY (source_dsn_report_id)
    REFERENCES processed_dsn_reports(id) ON DELETE SET NULL;

-- +goose Down

DROP INDEX IF EXISTS idx_recipient_suppressions_created_at;
DROP TABLE IF EXISTS recipient_suppressions;
DROP INDEX IF EXISTS idx_processed_dsn_reports_recipient_received;
DROP TABLE IF EXISTS processed_dsn_reports;

ALTER TABLE delivery_status_events
    DROP CONSTRAINT IF EXISTS valid_delivery_status_event_to;
ALTER TABLE delivery_status_events
    ADD CONSTRAINT valid_delivery_status_event_to
    CHECK (to_status IN ('pending', 'delivering', 'deferred', 'delivered', 'bounced', 'unknown'));

ALTER TABLE delivery_status_events
    DROP CONSTRAINT IF EXISTS valid_delivery_status_event_from;
ALTER TABLE delivery_status_events
    ADD CONSTRAINT valid_delivery_status_event_from
    CHECK (from_status IS NULL OR from_status IN ('pending', 'delivering', 'deferred', 'delivered', 'bounced', 'unknown'));

ALTER TABLE recipients
    DROP CONSTRAINT IF EXISTS valid_recipient_status;
UPDATE recipients SET status = 'bounced' WHERE status = 'suppressed';
ALTER TABLE recipients
    ADD CONSTRAINT valid_recipient_status
    CHECK (status IN ('pending', 'delivering', 'deferred', 'delivered', 'bounced', 'unknown'));
ALTER TABLE recipients DROP COLUMN IF EXISTS final_delivery_confirmed_at;
