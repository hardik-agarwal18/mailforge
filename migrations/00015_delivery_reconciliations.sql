-- +goose Up

-- Immutable, attributable decisions for recipients whose SMTP outcome is
-- ambiguous. This is separate from delivery_status_events so operator
-- identity, reason and supporting evidence remain queryable as structured data.
CREATE TABLE delivery_reconciliations (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    recipient_id UUID NOT NULL REFERENCES recipients(id) ON DELETE RESTRICT,
    decision TEXT NOT NULL,
    actor TEXT NOT NULL,
    database_role TEXT NOT NULL,
    reason TEXT NOT NULL,
    evidence TEXT,
    from_status TEXT NOT NULL DEFAULT 'unknown',
    to_status TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT valid_delivery_reconciliation_decision
        CHECK (decision IN ('accepted', 'retry')),
    CONSTRAINT valid_delivery_reconciliation_status
        CHECK (
            from_status = 'unknown'
            AND ((decision = 'accepted' AND to_status = 'delivered')
              OR (decision = 'retry' AND to_status = 'deferred'))
        ),
    CONSTRAINT nonempty_delivery_reconciliation_actor CHECK (length(btrim(actor)) > 0),
    CONSTRAINT nonempty_delivery_reconciliation_reason CHECK (length(btrim(reason)) > 0)
);

CREATE INDEX idx_delivery_reconciliations_recipient_created
    ON delivery_reconciliations(recipient_id, created_at DESC);

-- +goose Down
DROP INDEX IF EXISTS idx_delivery_reconciliations_recipient_created;
DROP TABLE IF EXISTS delivery_reconciliations;
