-- +goose Up

ALTER TABLE delivery_attempts
    ADD COLUMN enhanced_status_code TEXT,
    ADD COLUMN smtp_stage TEXT;

ALTER TABLE delivery_attempts
    ADD CONSTRAINT valid_enhanced_status_code
    CHECK (
        enhanced_status_code IS NULL
        OR enhanced_status_code ~ '^[245]\.(0|[1-9][0-9]{0,2})\.(0|[1-9][0-9]{0,2})$'
    );

-- +goose Down

ALTER TABLE delivery_attempts
    DROP CONSTRAINT IF EXISTS valid_enhanced_status_code,
    DROP COLUMN IF EXISTS smtp_stage,
    DROP COLUMN IF EXISTS enhanced_status_code;
