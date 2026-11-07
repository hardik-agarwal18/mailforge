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

const deliveryLease = 90 * time.Second

var (
	// ErrRecipientBusy means another worker owns a non-expired PostgreSQL
	// lease. The Redis event must remain pending.
	ErrRecipientBusy = errors.New("recipient is currently owned by another worker")

	// ErrRecipientScheduled means a deferred retry already exists in the
	// transactional outbox. The current event is stale/duplicate and can ACK.
	ErrRecipientScheduled = errors.New("recipient retry is already scheduled")

	// ErrRecipientNotReady means the recipient is not claimable and there is
	// no durable retry schedule that makes it safe to acknowledge this event.
	ErrRecipientNotReady = errors.New("recipient is not ready for delivery")

	ErrRecipientNotFound    = errors.New("recipient does not exist")
	ErrRecipientDataMissing = errors.New("recipient message data is missing")
)

type Repository struct {
	db          *pgxpool.Pool
	retryPolicy RetryPolicy
}

// SMTPFailureDetails are retained for diagnostics and DSN generation.
// Fields are empty for failures that did not contain an SMTP reply.
type SMTPFailureDetails struct {
	EnhancedStatusCode string
	Stage              string
}

func NewRepository(db *pgxpool.Pool) *Repository {
	return &Repository{db: db, retryPolicy: DefaultRetryPolicy()}
}

func NewRepositoryWithRetryPolicy(db *pgxpool.Pool, policy RetryPolicy) (*Repository, error) {
	if err := policy.Validate(); err != nil {
		return nil, err
	}
	return &Repository{db: db, retryPolicy: policy}, nil
}

// ClaimRecipient atomically claims an eligible recipient. PostgreSQL is the
// source of truth for recipient ownership; Redis consumer ownership is only
// used to distribute and recover delivery events.
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
				AND (
					r.delivery_lease_until IS NULL
					OR r.delivery_lease_until <= NOW()
				)
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
		var status string
		var leaseActive bool
		var retryPending bool
		var messageExists bool
		var retryEventExists bool

		lookupErr := tx.QueryRow(ctx, `
			SELECT
				r.status,
				COALESCE(r.delivery_lease_until > NOW(), FALSE),
				COALESCE(r.next_attempt_at > NOW(), FALSE),
				EXISTS (
					SELECT 1
					FROM messages m
					WHERE m.id = r.message_id
				),
				EXISTS (
					SELECT 1
					FROM outbox_events o
					WHERE o.aggregate_id = r.id
					  AND o.event_type = 'recipient.delivery_requested'
					  AND o.idempotency_key =
					      'recipient.delivery_requested:' || r.id::text || ':' || (r.attempts + 1)::text
					  AND o.status IN ('pending', 'publishing', 'published')
				)
			FROM recipients r
			WHERE r.id = $1::uuid
		`, recipientID).Scan(
			&status,
			&leaseActive,
			&retryPending,
			&messageExists,
			&retryEventExists,
		)

		if errors.Is(lookupErr, pgx.ErrNoRows) {
			return nil, ErrRecipientNotFound
		}
		if lookupErr != nil {
			return nil, fmt.Errorf("inspect unclaimable recipient: %w", lookupErr)
		}
		if !messageExists {
			return nil, ErrRecipientDataMissing
		}

		switch status {
		case "delivered", "bounced":
			// Terminal recipient state; this duplicate event is safe to ACK.
			return nil, nil

		case "delivering":
			if leaseActive {
				return nil, ErrRecipientBusy
			}
			// A non-expired or NULL lease should have matched the claim query.
			// Leave the event pending if the row changes between statements.
			return nil, ErrRecipientNotReady

		case "deferred":
			if retryPending && retryEventExists {
				// The due time and retry event are both persisted. The outbox will
				// publish this retry, so this older/duplicate event can ACK.
				return nil, ErrRecipientScheduled
			}
			return nil, ErrRecipientNotReady

		case "pending":
			return nil, ErrRecipientNotReady

		default:
			return nil, fmt.Errorf(
				"recipient %s has unexpected status %q",
				recipientID,
				status,
			)
		}
	}

	if err != nil {
		return nil, fmt.Errorf("lock recipient: %w", err)
	}

	err = tx.QueryRow(ctx, `
		UPDATE recipients
		SET
			status = 'delivering',
			attempts = attempts + 1,
			delivery_lease_until = NOW() + make_interval(secs => $2::double precision),
			delivery_lease_token = gen_random_uuid(),
			updated_at = NOW()
		WHERE id = $1::uuid
		RETURNING delivery_lease_token::text, attempts
	`,
		job.RecipientID,
		deliveryLease.Seconds(),
	).Scan(
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

// RenewLease extends only a lease that is still active and owned by this token.
func (r *Repository) RenewLease(
	ctx context.Context,
	job *ClaimedDelivery,
	leaseDuration time.Duration,
) error {
	if leaseDuration <= 0 {
		return fmt.Errorf("lease duration must be positive")
	}

	tag, err := r.db.Exec(ctx, `
		UPDATE recipients
		SET
			delivery_lease_until = NOW() + make_interval(secs => $3::double precision),
			updated_at = NOW()
		WHERE id = $1::uuid
		  AND status = 'delivering'
		  AND delivery_lease_token = $2::uuid
		  AND delivery_lease_until > NOW()
	`, job.RecipientID, job.LeaseToken, leaseDuration.Seconds())
	if err != nil {
		return fmt.Errorf("renew delivery lease: %w", err)
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
	return r.recordOutcome(ctx, job, "success", "delivered", 0, "", SMTPFailureDetails{})
}

func (r *Repository) RecordPermanentFailure(
	ctx context.Context,
	job *ClaimedDelivery,
	smtpCode int,
	response string,
) error {
	return r.RecordPermanentFailureWithDetails(ctx, job, smtpCode, response, SMTPFailureDetails{})
}

func (r *Repository) RecordPermanentFailureWithDetails(
	ctx context.Context,
	job *ClaimedDelivery,
	smtpCode int,
	response string,
	details SMTPFailureDetails,
) error {
	return r.recordOutcome(
		ctx,
		job,
		"permanent_failure",
		"bounced",
		smtpCode,
		response,
		details,
	)
}

func (r *Repository) RecordTemporaryFailure(
	ctx context.Context,
	job *ClaimedDelivery,
	smtpCode int,
	response string,
) error {
	return r.RecordTemporaryFailureWithDetails(ctx, job, smtpCode, response, SMTPFailureDetails{})
}

func (r *Repository) RecordTemporaryFailureWithDetails(
	ctx context.Context,
	job *ClaimedDelivery,
	smtpCode int,
	response string,
	details SMTPFailureDetails,
) error {
	if job == nil {
		return fmt.Errorf("claimed delivery is required")
	}
	retryAfter := r.retryPolicy.DelayForAttempt(job.Attempts)
	return r.recordOutcome(
		ctx,
		job,
		"temporary_failure",
		"deferred",
		smtpCode,
		response,
		details,
		retryAfter,
	)
}

// recordOutcome atomically changes recipient state, records the attempt,
// and inserts the next retry into the outbox when the result is deferred.
func (r *Repository) recordOutcome(
	ctx context.Context,
	job *ClaimedDelivery,
	attemptStatus string,
	recipientStatus string,
	smtpCode int,
	response string,
	details SMTPFailureDetails,
	retryDelays ...time.Duration,
) error {
	if job == nil {
		return fmt.Errorf("claimed delivery is required")
	}

	var retryAfter time.Duration
	if recipientStatus == "deferred" {
		if len(retryDelays) == 0 || retryDelays[0] <= 0 {
			return fmt.Errorf("deferred delivery requires a positive retry delay")
		}
		retryAfter = retryDelays[0]
	}

	tx, err := r.db.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin delivery outcome transaction: %w", err)
	}
	defer tx.Rollback(context.Background())

	// A temporary SMTP response remains deferred until the configured queue
	// lifetime or safety attempt limit is reached. This is local expiry policy,
	// not a reinterpretation of the remote SMTP response as a permanent error.
	if recipientStatus == "deferred" {
		var queuedAgeSeconds float64
		if err := tx.QueryRow(ctx, `
			SELECT GREATEST(EXTRACT(EPOCH FROM (NOW() - created_at)), 0)::double precision
			FROM recipients
			WHERE id = $1::uuid
		`, job.RecipientID).Scan(&queuedAgeSeconds); err != nil {
			return fmt.Errorf("check delivery queue age: %w", err)
		}

		switch {
		case job.Attempts >= r.retryPolicy.MaxAttempts:
			recipientStatus = "bounced"
			response += " (local maximum retry attempts reached)"
		case time.Duration(queuedAgeSeconds*float64(time.Second)) >= r.retryPolicy.MaxQueueAge:
			recipientStatus = "bounced"
			response += " (local maximum queue lifetime reached)"
		}
		if recipientStatus != "deferred" {
			retryAfter = 0
		}
	}

	// The expiry predicate prevents an old worker from committing after its
	// lease has expired, even if another worker has not claimed it yet.
	tag, err := tx.Exec(ctx, `
		UPDATE recipients
		SET
			status = $3,
			next_attempt_at = CASE
				WHEN $3 = 'deferred'
				THEN NOW() + make_interval(secs => $4::double precision)
				ELSE NULL
			END,
			delivery_lease_until = NULL,
			delivery_lease_token = NULL,
			updated_at = NOW()
		WHERE id = $1::uuid
		  AND status = 'delivering'
		  AND delivery_lease_token = $2::uuid
		  AND delivery_lease_until > NOW()
	`,
		job.RecipientID,
		job.LeaseToken,
		recipientStatus,
		retryAfter.Seconds(),
	)
	if err != nil {
		return fmt.Errorf("update recipient status: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("delivery lease lost or expired for recipient %s", job.RecipientID)
	}

	_, err = tx.Exec(ctx, `
		INSERT INTO delivery_attempts (
			recipient_id,
			attempt_number,
			status,
			smtp_code,
			enhanced_status_code,
			smtp_stage,
			response,
			attempted_at
		)
		VALUES ($1::uuid, $2, $3, $4, $5, $6, $7, NOW())
	`,
		job.RecipientID,
		job.Attempts,
		attemptStatus,
		nullableSMTPCode(smtpCode),
		nullableString(details.EnhancedStatusCode),
		nullableString(details.Stage),
		response,
	)
	if err != nil {
		return fmt.Errorf("insert delivery attempt: %w", err)
	}

	if recipientStatus == "deferred" {
		payload, err := json.Marshal(map[string]string{
			"recipient_id": job.RecipientID,
			"message_id":   job.MessageID,
			"email":        job.Email,
		})
		if err != nil {
			return fmt.Errorf("marshal retry event: %w", err)
		}

		idempotencyKey := fmt.Sprintf(
			"recipient.delivery_requested:%s:%d",
			job.RecipientID,
			job.Attempts+1,
		)

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
				NOW() + make_interval(secs => $4::double precision)
			)
			ON CONFLICT (idempotency_key) DO NOTHING
		`,
			job.RecipientID,
			idempotencyKey,
			payload,
			retryAfter.Seconds(),
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

func nullableSMTPCode(code int) any {
	if code == 0 {
		return nil
	}
	return code
}

func nullableString(value string) any {
	if value == "" {
		return nil
	}
	return value
}

// Compatibility methods retained for callers outside Processor.
func (r *Repository) MarkDelivered(
	ctx context.Context,
	job *ClaimedDelivery,
) error {
	return r.RecordSuccess(ctx, job)
}

func (r *Repository) MarkDeferred(
	ctx context.Context,
	job *ClaimedDelivery,
	retryAfter time.Duration,
) error {
	if retryAfter <= 0 {
		return fmt.Errorf("retry duration must be positive")
	}
	return r.recordOutcome(
		ctx,
		job,
		"temporary_failure",
		"deferred",
		0,
		"",
		SMTPFailureDetails{},
		retryAfter,
	)
}

func (r *Repository) MarkBounced(
	ctx context.Context,
	job *ClaimedDelivery,
) error {
	return r.RecordPermanentFailure(ctx, job, 0, "")
}
