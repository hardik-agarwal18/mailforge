
-- +goose Up

CREATE TABLE outbox_events (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),

    event_type TEXT NOT NULL,

    aggregate_type TEXT NOT NULL,

    aggregate_id UUID NOT NULL,

    idempotency_key TEXT NOT NULL UNIQUE,

    payload JSONB NOT NULL,

    status TEXT NOT NULL DEFAULT 'pending',

    attempts INTEGER NOT NULL DEFAULT 0,

    available_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    locked_until TIMESTAMPTZ,

    published_at TIMESTAMPTZ,

    last_error TEXT,

    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT valid_outbox_status
        CHECK (
            status IN (
                'pending',
                'publishing',
                'published',
                'dead'
            )
        ),

    CONSTRAINT nonnegative_outbox_attempts
        CHECK (attempts >= 0)
);

CREATE INDEX idx_outbox_pending
ON outbox_events(available_at, created_at)
WHERE status = 'pending';

CREATE INDEX idx_outbox_expired_leases
ON outbox_events(locked_until)
WHERE status = 'publishing';


-- +goose Down

DROP TABLE IF EXISTS outbox_events;
