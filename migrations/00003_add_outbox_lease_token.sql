
-- +goose Up

ALTER TABLE outbox_events
ADD COLUMN lease_token UUID;

CREATE INDEX idx_outbox_publishing_lease
ON outbox_events(locked_until)
WHERE status = 'publishing';


-- +goose Down

DROP INDEX IF EXISTS idx_outbox_publishing_lease;

ALTER TABLE outbox_events
DROP COLUMN lease_token;
