-- +goose Up

-- Keep the outbox metrics query index-backed as the event table grows.
-- Existing migrations already provide partial indexes for pending and
-- publishing rows. These indexes support pending-age ordering and dead-event
-- counts without requiring a full table scan for every metrics scrape.
CREATE INDEX idx_outbox_pending_created_at
ON outbox_events(created_at, available_at)
WHERE status = 'pending';

CREATE INDEX idx_outbox_dead_created_at
ON outbox_events(created_at)
WHERE status = 'dead';

-- +goose Down

DROP INDEX IF EXISTS idx_outbox_dead_created_at;
DROP INDEX IF EXISTS idx_outbox_pending_created_at;
