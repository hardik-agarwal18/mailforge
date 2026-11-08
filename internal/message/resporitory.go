package message

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"mailforge/internal/emailaddr"
	"mailforge/internal/mimeparser"
)

var ErrSubmissionQuotaExceeded = errors.New("SMTP submission quota exceeded")

type Repository struct {
	db     *pgxpool.Pool
	quotas SubmissionQuotaLimits
}

func NewRepository(db *pgxpool.Pool) *Repository {
	// Legacy/inbound SMTP paths do not inherit authenticated submission quotas.
	return &Repository{db: db}
}

func NewRepositoryWithQuota(db *pgxpool.Pool, quotas SubmissionQuotaLimits) *Repository {
	return &Repository{
		db:     db,
		quotas: normalizeSubmissionQuotaLimits(quotas),
	}
}

type DeliveryEvent struct {
	RecipientID string `json:"recipient_id"`
	MessageID   string `json:"message_id"`
	Email       string `json:"email"`
}

func (r *Repository) Create(
	ctx context.Context,
	msg StoredMessage,
) (string, error) {
	// Keep the persistence boundary defensive: every unique mailbox should
	// produce exactly one recipient row, status event, and outbox event.
	msg.Recipients = emailaddr.DeduplicateMailboxes(msg.Recipients)

	tx, err := r.db.Begin(ctx)
	if err != nil {
		return "", fmt.Errorf("begin transaction: %w", err)
	}

	defer func() {
		_ = tx.Rollback(context.Background())
	}()

	// Quota reservations share this transaction with message persistence. A
	// rejected submission rolls back both counters and all message records.
	recipientCount := len(msg.Recipients)
	if account := strings.ToLower(strings.TrimSpace(msg.Sender)); account != "" &&
		(r.quotas.MaxMessagesPerAccountPerHour > 0 || r.quotas.MaxRecipientsPerAccountPerHour > 0) {
		if err := r.reserveSubmissionQuota(
			ctx, tx, "account", quotaIdentity(account),
			r.quotas.MaxMessagesPerAccountPerHour,
			r.quotas.MaxRecipientsPerAccountPerHour,
			recipientCount,
		); err != nil {
			return "", err
		}
	}
	if clientIP := strings.TrimSpace(msg.ClientIP); clientIP != "" &&
		(r.quotas.MaxMessagesPerIPPerHour > 0 || r.quotas.MaxRecipientsPerIPPerHour > 0) {
		if err := r.reserveSubmissionQuota(
			ctx, tx, "ip", quotaIdentity(clientIP),
			r.quotas.MaxMessagesPerIPPerHour,
			r.quotas.MaxRecipientsPerIPPerHour,
			recipientCount,
		); err != nil {
			return "", err
		}
	}

	// 1. Insert the original message plus its parsed, indexed MIME representation.
	var messageID string
	parsedMetadata := `{}`
	if msg.Parsed != nil {
		metadataBytes, marshalErr := json.Marshal(msg.Parsed)
		if marshalErr != nil {
			return "", fmt.Errorf("marshal parsed MIME metadata: %w", marshalErr)
		}
		parsedMetadata = string(metadataBytes)
	}

	err = tx.QueryRow(
		ctx,
		`
		INSERT INTO messages (
			message_id,
			envelope_sender,
			raw_message,
			message_size,
			subject,
			text_body,
			html_body,
			mime_metadata
		)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
		RETURNING id::text
		`,
		msg.MessageID,
		msg.Sender,
		msg.RawMessage,
		msg.MessageSize,
		parsedSubject(msg.Parsed),
		parsedTextBody(msg.Parsed),
		parsedHTMLBody(msg.Parsed),
		parsedMetadata,
	).Scan(&messageID)

	if err != nil {
		return "", fmt.Errorf("insert message: %w", err)
	}

	// 2. Insert all recipients, initial status events, and outbox events in a
	// single set-based statement. The CTEs keep the three writes in this
	// transaction and avoid three SQL round trips per recipient.
	var recipientRows int64
	var statusRows int64
	var outboxRows int64
	err = tx.QueryRow(ctx, `
		WITH recipient_input AS (
			SELECT email
			FROM unnest($2::text[]) AS input(email)
		),
		inserted_recipients AS (
			INSERT INTO recipients (message_id, email)
			SELECT $1::uuid, email
			FROM recipient_input
			RETURNING id, email
		),
		inserted_status_events AS (
			INSERT INTO delivery_status_events (
				recipient_id, from_status, to_status, event_type, diagnostic
			)
			SELECT
				id,
				NULL,
				'pending',
				'submitted',
				'Recipient accepted for delivery'
			FROM inserted_recipients
			RETURNING recipient_id
		),
		inserted_outbox_events AS (
			INSERT INTO outbox_events (
				event_type,
				aggregate_type,
				aggregate_id,
				idempotency_key,
				payload
			)
			SELECT
				'recipient.delivery_requested',
				'recipient',
				id,
				'recipient.delivery_requested:' || id::text,
				jsonb_build_object(
					'recipient_id', id::text,
					'message_id', $1::text,
					'email', email
				)
			FROM inserted_recipients
			RETURNING id
		)
		SELECT
			(SELECT COUNT(*) FROM inserted_recipients),
			(SELECT COUNT(*) FROM inserted_status_events),
			(SELECT COUNT(*) FROM inserted_outbox_events)
	`, messageID, msg.Recipients).Scan(
		&recipientRows,
		&statusRows,
		&outboxRows,
	)
	if err != nil {
		return "", fmt.Errorf("bulk insert recipients and delivery events: %w", err)
	}
	if recipientRows != int64(len(msg.Recipients)) ||
		statusRows != int64(len(msg.Recipients)) ||
		outboxRows != int64(len(msg.Recipients)) {
		return "", fmt.Errorf(
			"bulk insert count mismatch: recipients=%d statuses=%d outbox=%d expected=%d",
			recipientRows,
			statusRows,
			outboxRows,
			len(msg.Recipients),
		)
	}

	// 5. Commit all changes atomically.
	if err := tx.Commit(ctx); err != nil {
		return "", fmt.Errorf(
			"commit transaction: %w",
			err,
		)
	}

	return messageID, nil
}

func parsedSubject(parsed *mimeparser.ParsedMessage) string {
	if parsed == nil {
		return ""
	}
	return parsed.Subject
}

func parsedTextBody(parsed *mimeparser.ParsedMessage) string {
	if parsed == nil {
		return ""
	}
	return parsed.TextBody
}

func parsedHTMLBody(parsed *mimeparser.ParsedMessage) string {
	if parsed == nil {
		return ""
	}
	return parsed.HTMLBody
}

func (r *Repository) reserveSubmissionQuota(
	ctx context.Context,
	tx pgx.Tx,
	scope string,
	identity string,
	maxMessages int,
	maxRecipients int,
	recipientCount int,
) error {
	var messageCount int
	var totalRecipients int
	err := tx.QueryRow(ctx, `
		INSERT INTO smtp_submission_usage (
			scope, identity, window_started_at, message_count, recipient_count, updated_at
		)
		VALUES ($1, $2, NOW(), 1, $3, NOW())
		ON CONFLICT (scope, identity) DO UPDATE
		SET
			window_started_at = CASE
				WHEN smtp_submission_usage.window_started_at <= NOW() - INTERVAL '1 hour'
				THEN NOW()
				ELSE smtp_submission_usage.window_started_at
			END,
			message_count = CASE
				WHEN smtp_submission_usage.window_started_at <= NOW() - INTERVAL '1 hour'
				THEN 1
				ELSE smtp_submission_usage.message_count + 1
			END,
			recipient_count = CASE
				WHEN smtp_submission_usage.window_started_at <= NOW() - INTERVAL '1 hour'
				THEN EXCLUDED.recipient_count
				ELSE smtp_submission_usage.recipient_count + EXCLUDED.recipient_count
			END,
			updated_at = NOW()
		RETURNING message_count, recipient_count
	`, scope, identity, recipientCount).Scan(&messageCount, &totalRecipients)
	if err != nil {
		return fmt.Errorf("reserve %s submission quota: %w", scope, err)
	}
	if messageCount > maxMessages {
		return fmt.Errorf("%w: %s limit is %d messages per hour", ErrSubmissionQuotaExceeded, scope, maxMessages)
	}
	if totalRecipients > maxRecipients {
		return fmt.Errorf("%w: %s limit is %d recipients per hour", ErrSubmissionQuotaExceeded, scope, maxRecipients)
	}
	return nil
}

func quotaIdentity(identity string) string {
	sum := sha256.Sum256([]byte(identity))
	return hex.EncodeToString(sum[:])
}
