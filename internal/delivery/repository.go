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
	ErrMessageNotFound      = errors.New("message does not exist")
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
	RemoteHost         string
	TLSUsed            bool
	TLSMode            string
	TLSVersion         string
	TLSCipherSuite     string
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
	var previousStatus string

	// Only recipients that are definitely safe to start are claimable here.
	// An expired `delivering` lease is handled below as an ambiguous outcome:
	// the previous process may have crashed after the remote server accepted
	// the DATA transaction, so automatically sending again could duplicate mail.
	err = tx.QueryRow(ctx, `
		SELECT
			r.id::text,
			r.message_id::text,
			r.email,
			r.attempts,
			m.envelope_sender,
			m.raw_message,
			r.status
		FROM recipients r
		JOIN messages m ON m.id = r.message_id
		WHERE r.id = $1::uuid
		  AND r.status IN ('pending', 'deferred')
		  AND (
			r.next_attempt_at IS NULL
			OR r.next_attempt_at <= NOW()
		  )
		FOR UPDATE OF r
	`, recipientID).Scan(
		&job.RecipientID,
		&job.MessageID,
		&job.Email,
		&job.Attempts,
		&job.Sender,
		&job.RawMessage,
		&previousStatus,
	)

	if errors.Is(err, pgx.ErrNoRows) {
		var status string
		var leaseActive bool
		var retryPending bool
		var messageExists bool
		var retryEventExists bool
		var leaseExpired bool
		var attempts int

		// Lock the recipient while deciding whether this event is stale,
		// scheduled, currently owned, or an abandoned in-flight delivery.
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
				),
				(r.delivery_lease_until IS NULL OR r.delivery_lease_until <= NOW()),
				r.attempts
			FROM recipients r
			WHERE r.id = $1::uuid
			FOR UPDATE OF r
		`, recipientID).Scan(
			&status,
			&leaseActive,
			&retryPending,
			&messageExists,
			&retryEventExists,
			&leaseExpired,
			&attempts,
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
		case "delivered", "bounced", "unknown":
			// Terminal automatic-delivery states. In particular, unknown requires
			// reconciliation before an operator may choose to retry it.
			return nil, nil

		case "delivering":
			if leaseActive {
				return nil, ErrRecipientBusy
			}
			if leaseExpired {
				if err := r.markExpiredDeliveryUnknown(ctx, tx, recipientID, attempts); err != nil {
					return nil, err
				}
				if err := tx.Commit(ctx); err != nil {
					return nil, fmt.Errorf("commit abandoned delivery reconciliation: %w", err)
				}
				return nil, nil
			}
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
			next_attempt_at = NULL,
			attempts = attempts + 1,
			last_attempt_at = NOW(),
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

	_, err = tx.Exec(ctx, `
		INSERT INTO delivery_status_events (
			recipient_id, from_status, to_status, event_type, diagnostic
		)
		VALUES ($1::uuid, $2, 'delivering', 'attempt_started', 'Worker claimed recipient for SMTP delivery')
	`, job.RecipientID, previousStatus)
	if err != nil {
		return nil, fmt.Errorf("record delivery claim status event: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit recipient claim: %w", err)
	}

	return &job, nil
}

// markExpiredDeliveryUnknown durably reconciles an abandoned SMTP attempt.
// SMTP cannot provide exactly-once delivery: a worker may disappear after the
// remote server accepted DATA but before PostgreSQL committed the result. The
// safe choice is to stop automatic retries and require manual reconciliation.
func (r *Repository) markExpiredDeliveryUnknown(
	ctx context.Context,
	tx pgx.Tx,
	recipientID string,
	attempts int,
) error {
	const diagnostic = "delivery lease expired while SMTP outcome was in flight; automatic retry suppressed to avoid duplicate delivery"

	tag, err := tx.Exec(ctx, `
		UPDATE recipients
		SET
			status = 'unknown',
			next_attempt_at = NULL,
			unknown_at = NOW(),
			last_error = $2,
			delivery_lease_until = NULL,
			delivery_lease_token = NULL,
			updated_at = NOW()
		WHERE id = $1::uuid
		  AND status = 'delivering'
		  AND (delivery_lease_until IS NULL OR delivery_lease_until <= NOW())
	`, recipientID, diagnostic)
	if err != nil {
		return fmt.Errorf("mark expired delivery outcome unknown: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("recipient %s changed while reconciling expired delivery lease", recipientID)
	}

	if attempts > 0 {
		_, err = tx.Exec(ctx, `
			INSERT INTO delivery_attempts (
				recipient_id, attempt_number, status, response, attempted_at
			)
			VALUES ($1::uuid, $2, 'unknown', $3, NOW())
		`, recipientID, attempts, diagnostic)
		if err != nil {
			return fmt.Errorf("record abandoned delivery attempt: %w", err)
		}
	}

	_, err = tx.Exec(ctx, `
		INSERT INTO delivery_status_events (
			recipient_id, from_status, to_status, event_type, diagnostic
		)
		VALUES ($1::uuid, 'delivering', 'unknown', 'lease_expired_ambiguous', $2)
	`, recipientID, diagnostic)
	if err != nil {
		return fmt.Errorf("record abandoned delivery status event: %w", err)
	}

	return nil
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
	return r.RecordSuccessWithDetails(ctx, job, SMTPFailureDetails{})
}

func (r *Repository) RecordSuccessWithDetails(
	ctx context.Context,
	job *ClaimedDelivery,
	details SMTPFailureDetails,
) error {
	return r.recordOutcome(ctx, job, "success", "delivered", 250, "remote SMTP server accepted message", details)
}

// RecordUnknownOutcome records a final DATA ambiguity without automatically
// retrying. The remote server may already have accepted the message.
func (r *Repository) RecordUnknownOutcome(
	ctx context.Context,
	job *ClaimedDelivery,
	response string,
	details SMTPFailureDetails,
) error {
	return r.recordOutcome(ctx, job, "unknown", "unknown", 0, response, details)
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
			delivered_at = CASE WHEN $3 = 'delivered' THEN NOW() ELSE delivered_at END,
			bounced_at = CASE WHEN $3 = 'bounced' THEN NOW() ELSE bounced_at END,
			unknown_at = CASE WHEN $3 = 'unknown' THEN NOW() ELSE unknown_at END,
			last_smtp_code = NULLIF($5, 0),
			last_enhanced_status_code = NULLIF($6, ''),
			last_smtp_stage = NULLIF($7, ''),
			last_error = CASE WHEN $3 IN ('deferred', 'bounced', 'unknown') THEN NULLIF($8, '') ELSE NULL END,
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
		smtpCode,
		details.EnhancedStatusCode,
		details.Stage,
		response,
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
			remote_host,
			tls_used,
			tls_mode,
			tls_version,
			tls_cipher_suite,
			attempted_at
		)
		VALUES ($1::uuid, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, NOW())
	`,
		job.RecipientID,
		job.Attempts,
		attemptStatus,
		nullableSMTPCode(smtpCode),
		nullableString(details.EnhancedStatusCode),
		nullableString(details.Stage),
		response,
		nullableString(details.RemoteHost),
		details.TLSUsed,
		nullableString(details.TLSMode),
		nullableString(details.TLSVersion),
		nullableString(details.TLSCipherSuite),
	)
	if err != nil {
		return fmt.Errorf("insert delivery attempt: %w", err)
	}

	eventType := attemptStatus
	if recipientStatus == "bounced" && attemptStatus == "temporary_failure" {
		eventType = "retry_exhausted"
	}
	_, err = tx.Exec(ctx, `
		INSERT INTO delivery_status_events (
			recipient_id, from_status, to_status, event_type,
			smtp_code, enhanced_status_code, smtp_stage, diagnostic,
			remote_host, tls_used, tls_mode, tls_version, tls_cipher_suite
		)
		VALUES ($1::uuid, 'delivering', $2, $3, $4, $5, $6, NULLIF($7, ''), $8, $9, $10, $11, $12)
	`,
		job.RecipientID,
		recipientStatus,
		eventType,
		nullableSMTPCode(smtpCode),
		nullableString(details.EnhancedStatusCode),
		nullableString(details.Stage),
		response,
		nullableString(details.RemoteHost),
		details.TLSUsed,
		nullableString(details.TLSMode),
		nullableString(details.TLSVersion),
		nullableString(details.TLSCipherSuite),
	)
	if err != nil {
		return fmt.Errorf("insert delivery status event: %w", err)
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
