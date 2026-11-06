package outbox

import (
	"context"
	"database/sql"
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

// ListDead returns terminally failed events for operational inspection.
func (r *Repository) ListDead(ctx context.Context, limit int) ([]DeadEvent, error) {
	if limit < 1 || limit > 1000 {
		limit = 100
	}

	rows, err := r.db.Query(ctx, `
		SELECT id::text, event_type, aggregate_id::text, attempts,
		       COALESCE(last_error, ''), created_at
		FROM outbox_events
		WHERE status = 'dead'
		ORDER BY created_at ASC
		LIMIT $1
	`, limit)
	if err != nil {
		return nil, fmt.Errorf("list dead outbox events: %w", err)
	}
	defer rows.Close()

	dead := make([]DeadEvent, 0)
	for rows.Next() {
		var event DeadEvent
		if err := rows.Scan(
			&event.ID,
			&event.EventType,
			&event.AggregateID,
			&event.Attempts,
			&event.LastError,
			&event.CreatedAt,
		); err != nil {
			return nil, fmt.Errorf("scan dead outbox event: %w", err)
		}
		dead = append(dead, event)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate dead outbox events: %w", err)
	}
	return dead, nil
}

// RequeueDead atomically records an audit row and makes one dead event eligible
// for publication again. It deliberately requires an event ID so operators do
// not accidentally replay the entire dead-letter population.
func (r *Repository) RequeueDead(ctx context.Context, eventID, replayedBy string) error {
	if eventID == "" {
		return fmt.Errorf("event ID is required")
	}
	if replayedBy == "" {
		return fmt.Errorf("replayed-by identity is required")
	}

	tx, err := r.db.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin outbox replay transaction: %w", err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()

	var attempts int
	var lastError sql.NullString
	if err := tx.QueryRow(ctx, `
		SELECT attempts, last_error
		FROM outbox_events
		WHERE id = $1 AND status = 'dead'
		FOR UPDATE
	`, eventID).Scan(&attempts, &lastError); err != nil {
		return fmt.Errorf("load dead outbox event %s: %w", eventID, err)
	}

	if _, err := tx.Exec(ctx, `
		INSERT INTO outbox_event_replays
			(event_id, replayed_by, previous_attempts, previous_last_error)
		VALUES ($1, $2, $3, $4)
	`, eventID, replayedBy, attempts, nullableString(lastError)); err != nil {
		return fmt.Errorf("audit outbox event replay: %w", err)
	}

	result, err := tx.Exec(ctx, `
		UPDATE outbox_events
		SET status = 'pending',
		    attempts = 0,
		    available_at = NOW(),
		    locked_until = NULL,
		    lease_token = NULL,
		    published_at = NULL
		WHERE id = $1 AND status = 'dead'
	`, eventID)
	if err != nil {
		return fmt.Errorf("requeue dead outbox event: %w", err)
	}
	if result.RowsAffected() != 1 {
		return fmt.Errorf("outbox event %s is no longer dead", eventID)
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit outbox event replay: %w", err)
	}
	return nil
}

func nullableString(value sql.NullString) any {
	if !value.Valid {
		return nil
	}
	return value.String
}
