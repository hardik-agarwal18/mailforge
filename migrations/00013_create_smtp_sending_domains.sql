-- +goose Up

-- A sending domain is owned by exactly one SMTP credential in the current
-- single-account credential model. Ownership is established by publishing the
-- generated TXT value at _mailforge-verification.<domain>.
CREATE TABLE smtp_sending_domains (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    smtp_credential_id UUID NOT NULL
        REFERENCES smtp_credentials(id) ON DELETE CASCADE,
    domain TEXT NOT NULL UNIQUE,
    verification_token TEXT NOT NULL UNIQUE,
    verified_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT smtp_sending_domains_domain_normalized
        CHECK (domain = lower(domain)),
    CONSTRAINT smtp_sending_domains_domain_length
        CHECK (char_length(domain) BETWEEN 3 AND 253),
    CONSTRAINT smtp_sending_domains_token_not_empty
        CHECK (length(verification_token) > 0)
);

CREATE INDEX idx_smtp_sending_domains_credential_verified
    ON smtp_sending_domains(smtp_credential_id, domain)
    WHERE verified_at IS NOT NULL;

-- +goose Down

DROP TABLE IF EXISTS smtp_sending_domains;
