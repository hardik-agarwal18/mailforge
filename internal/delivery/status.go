package delivery

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
)

// GetRecipientDeliveryStatus returns the latest durable status and useful
// operational timestamps for one envelope recipient.
func (r *Repository) GetRecipientDeliveryStatus(
	ctx context.Context,
	recipientID string,
) (*RecipientDeliveryStatus, error) {
	var status RecipientDeliveryStatus
	var nextAttemptAt, lastAttemptAt, deliveredAt, bouncedAt, unknownAt, finalDeliveryConfirmedAt pgtype.Timestamptz
	var lastSMTPCode pgtype.Int4
	var enhancedStatus, smtpStage, lastError pgtype.Text

	err := r.db.QueryRow(ctx, `
		SELECT
			r.id::text,
			r.message_id::text,
			r.email,
			r.status,
			r.attempts,
			r.created_at,
			r.updated_at,
			r.next_attempt_at,
			r.last_attempt_at,
			r.delivered_at,
			r.bounced_at,
			r.unknown_at,
			r.final_delivery_confirmed_at,
			r.last_smtp_code,
			r.last_enhanced_status_code,
			r.last_smtp_stage,
			r.last_error
		FROM recipients r
		WHERE r.id = $1::uuid
	`, recipientID).Scan(
		&status.RecipientID,
		&status.MessageID,
		&status.Email,
		&status.Status,
		&status.Attempts,
		&status.CreatedAt,
		&status.UpdatedAt,
		&nextAttemptAt,
		&lastAttemptAt,
		&deliveredAt,
		&bouncedAt,
		&unknownAt,
		&finalDeliveryConfirmedAt,
		&lastSMTPCode,
		&enhancedStatus,
		&smtpStage,
		&lastError,
	)
	if err != nil {
		if err == pgx.ErrNoRows {
			return nil, ErrRecipientNotFound
		}
		return nil, fmt.Errorf("get recipient delivery status: %w", err)
	}

	status.NextAttemptAt = nullableTimestamp(nextAttemptAt)
	status.LastAttemptAt = nullableTimestamp(lastAttemptAt)
	status.DeliveredAt = nullableTimestamp(deliveredAt)
	status.BouncedAt = nullableTimestamp(bouncedAt)
	status.UnknownAt = nullableTimestamp(unknownAt)
	status.FinalDeliveryConfirmedAt = nullableTimestamp(finalDeliveryConfirmedAt)
	if lastSMTPCode.Valid {
		code := int(lastSMTPCode.Int32)
		status.LastSMTPCode = &code
	}
	if enhancedStatus.Valid {
		status.LastEnhancedStatus = enhancedStatus.String
	}
	if smtpStage.Valid {
		status.LastSMTPStage = smtpStage.String
	}
	if lastError.Valid {
		status.LastError = lastError.String
	}
	return &status, nil
}

// ListRecipientDeliveryEvents returns the recipient's immutable status
// history in chronological order. A zero limit defaults to 250; the maximum
// is capped to keep accidental unbounded history reads from exhausting memory.
func (r *Repository) ListRecipientDeliveryEvents(
	ctx context.Context,
	recipientID string,
	limit int,
) ([]DeliveryStatusEvent, error) {
	if limit <= 0 {
		limit = 250
	}
	if limit > 1000 {
		limit = 1000
	}

	rows, err := r.db.Query(ctx, `
		SELECT
			id::text,
			recipient_id::text,
			from_status,
			to_status,
			event_type,
			smtp_code,
			enhanced_status_code,
			smtp_stage,
			diagnostic,
			remote_host,
			tls_used,
			tls_mode,
			tls_version,
			tls_cipher_suite,
			created_at
		FROM delivery_status_events
		WHERE recipient_id = $1::uuid
		ORDER BY created_at DESC, id DESC
		LIMIT $2
	`, recipientID, limit)
	if err != nil {
		return nil, fmt.Errorf("list recipient delivery events: %w", err)
	}
	defer rows.Close()

	events := make([]DeliveryStatusEvent, 0)
	for rows.Next() {
		var event DeliveryStatusEvent
		var fromStatus, enhancedStatus, smtpStage, diagnostic, remoteHost pgtype.Text
		var smtpCode pgtype.Int4
		var tlsMode, tlsVersion, tlsCipher pgtype.Text
		if err := rows.Scan(
			&event.ID,
			&event.RecipientID,
			&fromStatus,
			&event.ToStatus,
			&event.EventType,
			&smtpCode,
			&enhancedStatus,
			&smtpStage,
			&diagnostic,
			&remoteHost,
			&event.TLSUsed,
			&tlsMode,
			&tlsVersion,
			&tlsCipher,
			&event.CreatedAt,
		); err != nil {
			return nil, fmt.Errorf("scan recipient delivery event: %w", err)
		}
		if fromStatus.Valid {
			event.FromStatus = fromStatus.String
		}
		if smtpCode.Valid {
			code := int(smtpCode.Int32)
			event.SMTPCode = &code
		}
		if enhancedStatus.Valid {
			event.EnhancedStatusCode = enhancedStatus.String
		}
		if smtpStage.Valid {
			event.SMTPStage = smtpStage.String
		}
		if diagnostic.Valid {
			event.Diagnostic = diagnostic.String
		}
		if remoteHost.Valid {
			event.RemoteHost = remoteHost.String
		}
		if tlsMode.Valid {
			event.TLSMode = tlsMode.String
		}
		if tlsVersion.Valid {
			event.TLSVersion = tlsVersion.String
		}
		if tlsCipher.Valid {
			event.TLSCipherSuite = tlsCipher.String
		}
		events = append(events, event)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate recipient delivery events: %w", err)
	}
	// The query fetches the newest bounded window; return it chronologically.
	for left, right := 0, len(events)-1; left < right; left, right = left+1, right-1 {
		events[left], events[right] = events[right], events[left]
	}
	return events, nil
}

// GetMessageDeliverySummary aggregates the latest recipient states for a
// message. A message is only fully delivered when every recipient is delivered.
func (r *Repository) GetMessageDeliverySummary(
	ctx context.Context,
	messageID string,
) (*MessageDeliverySummary, error) {
	var summary MessageDeliverySummary
	err := r.db.QueryRow(ctx, `
		SELECT
			message_id::text,
			COUNT(*)::integer,
			COUNT(*) FILTER (WHERE status = 'pending')::integer,
			COUNT(*) FILTER (WHERE status = 'delivering')::integer,
			COUNT(*) FILTER (WHERE status = 'deferred')::integer,
			COUNT(*) FILTER (WHERE status = 'delivered')::integer,
			COUNT(*) FILTER (WHERE status = 'bounced')::integer,
			COUNT(*) FILTER (WHERE status = 'unknown')::integer,
			COUNT(*) FILTER (WHERE status = 'suppressed')::integer
		FROM recipients
		WHERE message_id = $1::uuid
		GROUP BY message_id
	`, messageID).Scan(
		&summary.MessageID,
		&summary.Total,
		&summary.Pending,
		&summary.Delivering,
		&summary.Deferred,
		&summary.Delivered,
		&summary.Bounced,
		&summary.Unknown,
		&summary.Suppressed,
	)
	if err != nil {
		if err == pgx.ErrNoRows {
			return nil, ErrMessageNotFound
		}
		return nil, fmt.Errorf("get message delivery summary: %w", err)
	}
	return &summary, nil
}

func nullableTimestamp(value pgtype.Timestamptz) *time.Time {
	if !value.Valid {
		return nil
	}
	result := value.Time
	return &result
}
