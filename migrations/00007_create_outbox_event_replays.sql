-- +goose Up

-- Retain an audit trail whenever an operator manually replays a dead outbox
-- event. The original outbox row remains the source of truth for delivery.
CREATE TABLE outbox_event_replays (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    event_id UUID NOT NULL REFERENCES outbox_events(id) ON DELETE CASCADE,
    replayed_by TEXT NOT NULL,
    previous_attempts INTEGER NOT NULL,
    previous_last_error TEXT,
    replayed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT nonnegative_previous_attempts CHECK (previous_attempts >= 0),
    CONSTRAINT replayed_by_not_empty CHECK (length(trim(replayed_by)) > 0)
);

CREATE INDEX idx_outbox_event_replays_event
ON outbox_event_replays(event_id, replayed_at DESC);

-- +goose Down

DROP TABLE IF EXISTS outbox_event_replays;
