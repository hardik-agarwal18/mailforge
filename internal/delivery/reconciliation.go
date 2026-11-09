package delivery

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

var ErrRecipientNotUnknown = errors.New("recipient is not in unknown status; reconciliation is only allowed for ambiguous deliveries")

// UnknownReconciliation records an explicit operator decision for an
// ambiguous SMTP outcome. Decisions are deliberately limited to confirming
// next-hop acceptance or authorizing a retry after the operator has verified
// that the remote server did not accept the message.
type UnknownReconciliation struct {
	RecipientID string
	Decision    string
	Actor       string
	Reason      string
	Evidence    string
}

// ReconcileUnknownRecipient applies an operator decision atomically with its
// audit record and, for retries, a durable outbox event. It never retries an
// ambiguous delivery without an explicit operator action.
func (r *Repository) ReconcileUnknownRecipient(ctx context.Context, input UnknownReconciliation) error {
	input.RecipientID = strings.TrimSpace(input.RecipientID)
	input.Decision = strings.ToLower(strings.TrimSpace(input.Decision))
	input.Actor = strings.TrimSpace(input.Actor)
	input.Reason = strings.TrimSpace(input.Reason)
	input.Evidence = strings.TrimSpace(input.Evidence)

	if _, err := uuid.Parse(input.RecipientID); err != nil {
		return fmt.Errorf("invalid recipient UUID: %w", err)
	}
	if input.Decision != "accepted" && input.Decision != "retry" {
		return errors.New("decision must be accepted or retry")
	}
	if input.Actor == "" || len(input.Actor) > 256 {
		return errors.New("operator actor is required and must be at most 256 bytes")
	}
	if input.Reason == "" || len(input.Reason) > 2000 {
		return errors.New("operator reason is required and must be at most 2000 bytes")
	}
	if len(input.Evidence) > 4000 {
		return errors.New("operator evidence must be at most 4000 bytes")
	}

	tx, err := r.db.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin recipient reconciliation: %w", err)
	}
	defer tx.Rollback(context.Background())

	var messageID, email, currentStatus string
	var attempts int
	err = tx.QueryRow(ctx, `
		SELECT message_id::text, email, status, attempts
		FROM recipients
		WHERE id = $1::uuid
		FOR UPDATE
	`, input.RecipientID).Scan(&messageID, &email, &currentStatus, &attempts)
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrRecipientNotFound
	}
	if err != nil {
		return fmt.Errorf("lock recipient for reconciliation: %w", err)
	}
	if currentStatus != "unknown" {
		return fmt.Errorf("%w: current status is %q", ErrRecipientNotUnknown, currentStatus)
	}

	nextStatus := "delivered"
	eventType := "operator_reconciliation_accepted"
	if input.Decision == "retry" {
		nextStatus = "deferred"
		eventType = "operator_reconciliation_retry_authorized"
	}

	var payload []byte
	if input.Decision == "retry" {
		payload, err = json.Marshal(map[string]string{
			"recipient_id": input.RecipientID,
			"message_id":   messageID,
			"email":        email,
		})
		if err != nil {
			return fmt.Errorf("marshal operator retry event: %w", err)
		}
	}

	tag, err := tx.Exec(ctx, `
		UPDATE recipients
		SET status = $2,
		    next_attempt_at = CASE WHEN $2 = 'deferred' THEN NOW() ELSE NULL END,
		    delivered_at = CASE WHEN $2 = 'delivered' THEN COALESCE(delivered_at, NOW()) ELSE delivered_at END,
		    delivery_lease_until = NULL,
		    delivery_lease_token = NULL,
		    last_error = CASE WHEN $2 = 'delivered' THEN NULL ELSE last_error END,
		    updated_at = NOW()
		WHERE id = $1::uuid AND status = 'unknown'
	`, input.RecipientID, nextStatus)
	if err != nil {
		return fmt.Errorf("apply operator reconciliation: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return ErrRecipientNotUnknown
	}

	_, err = tx.Exec(ctx, `
		INSERT INTO delivery_reconciliations (
			recipient_id, decision, actor, database_role, reason, evidence, from_status, to_status
		)
		VALUES ($1::uuid, $2, $3, session_user, $4, NULLIF($5, ''), 'unknown', $6)
	`, input.RecipientID, input.Decision, input.Actor, input.Reason, input.Evidence, nextStatus)
	if err != nil {
		return fmt.Errorf("persist operator reconciliation audit: %w", err)
	}

	diagnostic := fmt.Sprintf("operator=%s; reason=%s", input.Actor, input.Reason)
	if input.Evidence != "" {
		diagnostic += "; evidence=" + input.Evidence
	}
	_, err = tx.Exec(ctx, `
		INSERT INTO delivery_status_events (
			recipient_id, from_status, to_status, event_type, diagnostic
		)
		VALUES ($1::uuid, 'unknown', $2, $3, $4)
	`, input.RecipientID, nextStatus, eventType, diagnostic)
	if err != nil {
		return fmt.Errorf("record operator reconciliation status event: %w", err)
	}

	if input.Decision == "retry" {
		idempotencyKey := fmt.Sprintf("recipient.delivery_requested:%s:%d", input.RecipientID, attempts+1)
		_, err = tx.Exec(ctx, `
			INSERT INTO outbox_events (
				event_type, aggregate_type, aggregate_id, idempotency_key,
				payload, available_at
			)
			VALUES (
				'recipient.delivery_requested', 'recipient', $1::uuid,
				$2, $3::jsonb, NOW()
			)
		`, input.RecipientID, idempotencyKey, payload)
		if err != nil {
			return fmt.Errorf("schedule operator-authorized retry: %w", err)
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit recipient reconciliation: %w", err)
	}
	return nil
}
