
-- +goose Up

CREATE TABLE messages (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),

    message_id TEXT NOT NULL UNIQUE,

    envelope_sender TEXT NOT NULL DEFAULT '',

    raw_message BYTEA NOT NULL,

    message_size BIGINT NOT NULL,

    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT positive_message_size
        CHECK (message_size >= 0)
);

CREATE TABLE recipients (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),

    message_id UUID NOT NULL
        REFERENCES messages(id) ON DELETE CASCADE,

    email TEXT NOT NULL,

    status TEXT NOT NULL DEFAULT 'pending',

    attempts INTEGER NOT NULL DEFAULT 0,

    next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT valid_recipient_status
        CHECK (
            status IN (
                'pending',
                'delivering',
                'deferred',
                'delivered',
                'bounced'
            )
        ),

    CONSTRAINT nonnegative_attempts
        CHECK (attempts >= 0)
);

CREATE TABLE delivery_attempts (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),

    recipient_id UUID NOT NULL
        REFERENCES recipients(id) ON DELETE CASCADE,

    attempt_number INTEGER NOT NULL,

    status TEXT NOT NULL,

    smtp_code INTEGER,

    response TEXT,

    attempted_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT valid_attempt_status
        CHECK (
            status IN (
                'success',
                'temporary_failure',
                'permanent_failure'
            )
        ),

    CONSTRAINT positive_attempt_number
        CHECK (attempt_number > 0)
);

CREATE INDEX idx_recipients_pending
ON recipients(next_attempt_at)
WHERE status IN ('pending', 'deferred');

CREATE INDEX idx_recipients_message
ON recipients(message_id);

CREATE INDEX idx_delivery_attempts_recipient
ON delivery_attempts(recipient_id, attempt_number);


-- +goose Down

DROP TABLE IF EXISTS delivery_attempts;
DROP TABLE IF EXISTS recipients;
DROP TABLE IF EXISTS messages;
