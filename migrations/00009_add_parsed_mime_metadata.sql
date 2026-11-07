-- +goose Up

-- Keep the original RFC 5322 bytes unchanged in messages.raw_message.
-- These columns provide commonly queried/indexable content while mime_metadata
-- stores the parsed header and MIME part structure (never the decoded binaries).
ALTER TABLE messages
    ADD COLUMN subject TEXT NOT NULL DEFAULT '',
    ADD COLUMN text_body TEXT NOT NULL DEFAULT '',
    ADD COLUMN html_body TEXT NOT NULL DEFAULT '',
    ADD COLUMN mime_metadata JSONB NOT NULL DEFAULT '{}'::jsonb;

-- +goose Down

ALTER TABLE messages
    DROP COLUMN IF EXISTS mime_metadata,
    DROP COLUMN IF EXISTS html_body,
    DROP COLUMN IF EXISTS text_body,
    DROP COLUMN IF EXISTS subject;
