package outbox

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

type Repository struct {
	db *pgxpool.Pool
}

func NewRepository(db *pgxpool.Pool) *Repository {
	return &Repository{db: db}
}

// ClaimBatch atomically claims available events.
// Multiple publisher instances can safely call this method.
func (r *Repository) ClaimBatch(
	ctx context.Context,
	limit int,
	leaseDuration time.Duration,
) ([]Event, error) {

	tx, err := r.db.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin claim transaction: %w", err)
	}
	defer func() {
		_ = tx.Rollback(context.Background())
	}()

	rows, err := tx.Query(
		ctx,
		`
		WITH candidates AS (
			SELECT id
			FROM outbox_events
			WHERE
				(
					status = 'pending'
					AND available_at <= NOW()
				)
				OR
				(
					status = 'publishing'
					AND locked_until <= NOW()
				)
			ORDER BY created_at
			LIMIT $1
			FOR UPDATE SKIP LOCKED
		)
		UPDATE outbox_events AS o
		SET
			status = 'publishing',
			locked_until = NOW() + $2::interval,
			lease_token = gen_random_uuid(),
			attempts = attempts + 1
		FROM candidates AS c
		WHERE o.id = c.id
		RETURNING
			o.id::text,
			o.lease_token::text,
			o.event_type,
			o.aggregate_id::text,
			o.payload,
			o.attempts
		`,
		limit,
		leaseDuration.String(),
	)
	if err != nil {
		return nil, fmt.Errorf("claim outbox events: %w", err)
	}

	var events []Event

	for rows.Next() {
		var event Event

		err := rows.Scan(
			&event.ID,
			&event.LeaseToken,
			&event.EventType,
			&event.AggregateID,
			&event.Payload,
			&event.Attempts,
		)
		if err != nil {
			rows.Close()
			return nil, fmt.Errorf("scan event: %w", err)
		}

		events = append(events, event)
	}

	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, fmt.Errorf("iterate events: %w", err)
	}

	rows.Close()

	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit claim: %w", err)
	}

	return events, nil
}

// MarkPublished marks an event as successfully published.
func (r *Repository) MarkPublished(
	ctx context.Context,
	event Event,
) error {

	result, err := r.db.Exec(
		ctx,
		`
		UPDATE outbox_events
		SET
			status = 'published',
			published_at = NOW(),
			locked_until = NULL,
			lease_token = NULL,
			last_error = NULL
		WHERE
			id = $1
			AND status = 'publishing'
			AND lease_token = $2
		`,
		event.ID,
		event.LeaseToken,
	)
	if err != nil {
		return fmt.Errorf("mark published: %w", err)
	}

	if result.RowsAffected() != 1 {
		return fmt.Errorf("event lease lost: %s", event.ID)
	}

	return nil
}

// MarkFailed schedules a retry or moves the event to dead status.
func (r *Repository) MarkFailed(
	ctx context.Context,
	event Event,
	publishErr error,
) error {

	nextStatus := "pending"

	if event.Attempts >= 10 {
		nextStatus = "dead"
	}

	delaySeconds := 1 << min(event.Attempts, 8)
	if delaySeconds > 300 {
		delaySeconds = 300
	}

	result, err := r.db.Exec(
		ctx,
		`
		UPDATE outbox_events
		SET
			status = $3,
			available_at = NOW() + make_interval(secs => $4),
			locked_until = NULL,
			lease_token = NULL,
			last_error = $5
		WHERE
			id = $1
			AND status = 'publishing'
			AND lease_token = $2
		`,
		event.ID,
		event.LeaseToken,
		nextStatus,
		delaySeconds,
		publishErr.Error(),
	)
	if err != nil {
		return fmt.Errorf("mark failed: %w", err)
	}

	if result.RowsAffected() != 1 {
		return fmt.Errorf("event lease lost while marking failure: %s", event.ID)
	}

	return nil
}
