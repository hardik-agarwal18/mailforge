
-- +goose Up

ALTER TABLE recipients
    ALTER COLUMN next_attempt_at DROP NOT NULL;

-- +goose Down

-- Restore NOT NULL safely by assigning a timestamp
-- to any existing NULL values first.
UPDATE recipients
SET next_attempt_at = NOW()
WHERE next_attempt_at IS NULL;

ALTER TABLE recipients
    ALTER COLUMN next_attempt_at SET NOT NULL;
