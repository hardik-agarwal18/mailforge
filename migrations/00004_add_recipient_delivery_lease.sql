
-- +goose Up

ALTER TABLE recipients
    ADD COLUMN delivery_lease_until TIMESTAMPTZ,
    ADD COLUMN delivery_lease_token UUID;

CREATE INDEX idx_recipients_delivery_ready
    ON recipients(next_attempt_at)
    WHERE status IN ('pending', 'deferred');

CREATE INDEX idx_recipients_expired_delivery_lease
    ON recipients(delivery_lease_until)
    WHERE status = 'delivering';

-- +goose Down

DROP INDEX IF EXISTS idx_recipients_expired_delivery_lease;
DROP INDEX IF EXISTS idx_recipients_delivery_ready;

ALTER TABLE recipients
    DROP COLUMN IF EXISTS delivery_lease_token,
    DROP COLUMN IF EXISTS delivery_lease_until;
