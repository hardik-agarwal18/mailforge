-- +goose Up

-- Atomic rolling one-hour counters shared by all SMTP submission replicas.
CREATE TABLE smtp_submission_usage (
    scope TEXT NOT NULL,
    identity TEXT NOT NULL,
    window_started_at TIMESTAMPTZ NOT NULL,
    message_count INTEGER NOT NULL DEFAULT 0,
    recipient_count INTEGER NOT NULL DEFAULT 0,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (scope, identity),
    CONSTRAINT valid_smtp_submission_scope CHECK (scope IN ('account', 'ip')),
    CONSTRAINT positive_smtp_submission_message_count CHECK (message_count > 0),
    CONSTRAINT positive_smtp_submission_recipient_count CHECK (recipient_count > 0),
    CONSTRAINT smtp_submission_identity_not_empty CHECK (length(trim(identity)) > 0)
);

CREATE INDEX idx_smtp_submission_usage_cleanup
ON smtp_submission_usage(updated_at);

-- +goose Down

DROP TABLE IF EXISTS smtp_submission_usage;
