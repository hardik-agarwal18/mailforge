package delivery

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Repository struct {
	db *pgxpool.Pool
}

func NewRepository(db *pgxpool.Pool) *Repository {
	return &Repository{db: db}
}

func (r *Repository) ClaimRecipient(
	ctx context.Context,
	recipientID string,
) (*ClaimedDelivery, error) {

	tx, err := r.db.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin claim transaction: %w", err)
	}
	defer tx.Rollback(context.Background())

	var job ClaimedDelivery

	// Lock only an eligible recipient.
	err = tx.QueryRow(ctx, `
        SELECT
            r.id::text,
            r.message_id::text,
            r.email,
            r.attempts,
            m.envelope_sender,
            m.raw_message
        FROM recipients r
        JOIN messages m ON m.id = r.message_id
        WHERE r.id = $1::uuid
          AND (
              (
                  r.status IN ('pending', 'deferred')
                  AND (
                      r.next_attempt_at IS NULL
                      OR r.next_attempt_at <= NOW()
                  )
              )
              OR (
                  r.status = 'delivering'
                  AND r.delivery_lease_until <= NOW()
              )
          )
        FOR UPDATE OF r
    `, recipientID).Scan(
		&job.RecipientID,
		&job.MessageID,
		&job.Email,
		&job.Attempts,
		&job.Sender,
		&job.RawMessage,
	)

	if errors.Is(err, pgx.ErrNoRows) {
		// Already claimed, not ready, or already completed.
		return nil, nil
	}

	if err != nil {
		return nil, fmt.Errorf("lock recipient: %w", err)
	}

	// Give this worker a new lease.
	err = tx.QueryRow(ctx, `
        UPDATE recipients
        SET
            status = 'delivering',
            attempts = attempts + 1,
            delivery_lease_until = NOW() + INTERVAL '2 minutes',
            delivery_lease_token = gen_random_uuid(),
            updated_at = NOW()
        WHERE id = $1::uuid
        RETURNING delivery_lease_token::text, attempts
    `, job.RecipientID).Scan(
		&job.LeaseToken,
		&job.Attempts,
	)

	if err != nil {
		return nil, fmt.Errorf("assign delivery lease: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit recipient claim: %w", err)
	}

	return &job, nil
}

// MarkDelivered only succeeds if this worker still owns the lease.
func (r *Repository) MarkDelivered(
	ctx context.Context,
	job *ClaimedDelivery,
) error {
	tag, err := r.db.Exec(ctx, `
        UPDATE recipients
        SET
            status = 'delivered',
            delivery_lease_until = NULL,
            delivery_lease_token = NULL,
            next_attempt_at = NULL,
            updated_at = NOW()
        WHERE id = $1::uuid
          AND status = 'delivering'
          AND delivery_lease_token = $2::uuid
    `, job.RecipientID, job.LeaseToken)

	if err != nil {
		return fmt.Errorf("mark recipient delivered: %w", err)
	}

	if tag.RowsAffected() != 1 {
		return fmt.Errorf("delivery lease lost for recipient %s", job.RecipientID)
	}

	return nil
}

// MarkDeferred schedules a retry after a temporary failure.
func (r *Repository) MarkDeferred(
	ctx context.Context,
	job *ClaimedDelivery,
	retryAfter time.Duration,
) error {
	tag, err := r.db.Exec(ctx, `
        UPDATE recipients
        SET
            status = 'deferred',
            next_attempt_at = NOW() + $3::interval,
            delivery_lease_until = NULL,
            delivery_lease_token = NULL,
            updated_at = NOW()
        WHERE id = $1::uuid
          AND status = 'delivering'
          AND delivery_lease_token = $2::uuid
    `, job.RecipientID, job.LeaseToken, retryAfter.String())

	if err != nil {
		return fmt.Errorf("defer recipient: %w", err)
	}

	if tag.RowsAffected() != 1 {
		return fmt.Errorf("delivery lease lost for recipient %s", job.RecipientID)
	}

	return nil
}

// MarkBounced records a permanent delivery failure.
func (r *Repository) MarkBounced(
	ctx context.Context,
	job *ClaimedDelivery,
) error {
	tag, err := r.db.Exec(ctx, `
        UPDATE recipients
        SET
            status = 'bounced',
            delivery_lease_until = NULL,
            delivery_lease_token = NULL,
            next_attempt_at = NULL,
            updated_at = NOW()
        WHERE id = $1::uuid
          AND status = 'delivering'
          AND delivery_lease_token = $2::uuid
    `, job.RecipientID, job.LeaseToken)

	if err != nil {
		return fmt.Errorf("mark recipient bounced: %w", err)
	}

	if tag.RowsAffected() != 1 {
		return fmt.Errorf("delivery lease lost for recipient %s", job.RecipientID)
	}

	return nil
}

func (r *Repository) RecordSuccess(
	ctx context.Context,
	job *ClaimedDelivery,
) error {
	return r.recordOutcome(ctx, job, "success", "delivered", 0, "")
}

func (r *Repository) RecordPermanentFailure(
	ctx context.Context,
	job *ClaimedDelivery,
	smtpCode int,
	response string,
) error {
	return r.recordOutcome(
		ctx, job, "permanent_failure", "bounced", smtpCode, response,
	)
}

func (r *Repository) RecordTemporaryFailure(
	ctx context.Context,
	job *ClaimedDelivery,
	smtpCode int,
	response string,
) error {
	if job.Attempts >= 10 {
		// Retry limit reached. Stop scheduling further attempts.
		return r.recordOutcome(
			ctx, job, "temporary_failure", "bounced", smtpCode,
			response+" (maximum delivery attempts reached)",
		)
	}

	retryAfter := retryDelay(job.Attempts)

	return r.recordOutcome(
		ctx, job, "temporary_failure", "deferred", smtpCode,
		response,
		retryAfter,
	)
}

func retryDelay(attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}

	delay := 30 * time.Second

	for i := 1; i < attempt; i++ {
		delay *= 2

		if delay >= time.Hour {
			return time.Hour
		}
	}

	return delay
}

func (r *Repository) recordOutcome(
	ctx context.Context,
	job *ClaimedDelivery,
	attemptStatus string,
	recipientStatus string,
	smtpCode int,
	response string,
	retryDelays ...time.Duration,
) error {

	tx, err := r.db.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin delivery outcome transaction: %w", err)
	}
	defer tx.Rollback(context.Background())

	tag, err := tx.Exec(ctx, `
        UPDATE recipients
        SET
            status = $3,
            next_attempt_at = CASE
                WHEN $3 = 'deferred'
                THEN NOW() + $4::interval
                ELSE NULL
            END,
            delivery_lease_until = NULL,
            delivery_lease_token = NULL,
            updated_at = NOW()
        WHERE id = $1::uuid
          AND status = 'delivering'
          AND delivery_lease_token = $2::uuid
    `,
		job.RecipientID,
		job.LeaseToken,
		recipientStatus,
		retryInterval(retryDelays),
	)

	if err != nil {
		return fmt.Errorf("update recipient status: %w", err)
	}

	if tag.RowsAffected() != 1 {
		return fmt.Errorf("delivery lease lost for recipient %s", job.RecipientID)
	}

	_, err = tx.Exec(ctx, `
        INSERT INTO delivery_attempts (
            recipient_id,
            attempt_number,
            status,
            smtp_code,
            response,
            attempted_at
        )
        VALUES ($1::uuid, $2, $3, $4, $5, NOW())
    `,
		job.RecipientID,
		job.Attempts,
		attemptStatus,
		nullableSMTPCode(smtpCode),
		response,
	)

	if err != nil {
		return fmt.Errorf("insert delivery attempt: %w", err)
	}

	// Schedule the next attempt through the transactional outbox.
	// The existing publisher will publish it when available_at is reached.
	if recipientStatus == "deferred" {
		payload, err := json.Marshal(map[string]string{
			"recipient_id": job.RecipientID,
			"message_id":   job.MessageID,
			"email":        job.Email,
		})
		if err != nil {
			return fmt.Errorf("marshal retry event: %w", err)
		}

		_, err = tx.Exec(ctx, `
            INSERT INTO outbox_events (
                event_type,
                aggregate_type,
                aggregate_id,
                idempotency_key,
                payload,
                available_at
            )
            VALUES (
                'recipient.delivery_requested',
                'recipient',
                $1::uuid,
                $2,
                $3::jsonb,
                NOW() + $4::interval
            )
            ON CONFLICT (idempotency_key) DO NOTHING
        `,
			job.RecipientID,
			fmt.Sprintf(
				"recipient.delivery_requested:%s:%d",
				job.RecipientID,
				job.Attempts+1,
			),
			payload,
			retryInterval(retryDelays),
		)

		if err != nil {
			return fmt.Errorf("schedule retry event: %w", err)
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit delivery outcome: %w", err)
	}

	return nil
}

func retryInterval(delays []time.Duration) string {
	if len(delays) == 0 {
		return "0 seconds"
	}

	return delays[0].String()
}

func nullableSMTPCode(code int) any {
	if code == 0 {
		return nil
	}

	return code
}
