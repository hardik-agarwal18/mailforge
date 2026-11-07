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

// MarkPublished marks one event as successfully published.
func (r *Repository) MarkPublished(
	ctx context.Context,
	event Event,
) error {
	return r.MarkPublishedBatch(ctx, []Event{event})
}

// MarkPublishedBatch marks a set of successfully XADDed events in one
// PostgreSQL round trip. Lease-token matching prevents a stale publisher from
// changing an event that another publisher has already reclaimed.
func (r *Repository) MarkPublishedBatch(
	ctx context.Context,
	events []Event,
) error {
	if len(events) == 0 {
		return nil
	}

	ids := make([]string, len(events))
	leaseTokens := make([]string, len(events))
	for i, event := range events {
		ids[i] = event.ID
		leaseTokens[i] = event.LeaseToken
	}

	rows, err := r.db.Query(ctx, `
		WITH claimed AS (
			SELECT id, lease_token
			FROM unnest($1::text[], $2::text[]) AS input(id, lease_token)
		), updated AS (
			UPDATE outbox_events AS event
			SET
				status = 'published',
				published_at = NOW(),
				locked_until = NULL,
				lease_token = NULL,
				last_error = NULL
			FROM claimed
			WHERE event.id = claimed.id::uuid
			  AND event.lease_token = claimed.lease_token::uuid
			  AND event.status = 'publishing'
			RETURNING event.id
		)
		SELECT id::text FROM updated
	`, ids, leaseTokens)
	if err != nil {
		return fmt.Errorf("mark published batch: %w", err)
	}
	defer rows.Close()

	updatedCount := 0
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return fmt.Errorf("scan published outbox event: %w", err)
		}
		updatedCount++
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate published outbox events: %w", err)
	}
	if updatedCount != len(events) {
		return fmt.Errorf(
			"published %d of %d outbox events; one or more leases were lost",
			updatedCount,
			len(events),
		)
	}
	return nil
}

// MarkFailed schedules a retry or moves an event to dead status.
func (r *Repository) MarkFailed(
	ctx context.Context,
	event Event,
	publishErr error,
) error {
	return r.MarkFailedBatch(ctx, []Event{event}, []error{publishErr})
}

// MarkFailedBatch schedules failed pipeline commands in one PostgreSQL
// statement while retaining per-event retry delays and diagnostics.
func (r *Repository) MarkFailedBatch(
	ctx context.Context,
	events []Event,
	publishErrors []error,
) error {
	if len(events) == 0 {
		return nil
	}
	if len(events) != len(publishErrors) {
		return fmt.Errorf("failed outbox event/error count mismatch: %d != %d", len(events), len(publishErrors))
	}

	ids := make([]string, len(events))
	leaseTokens := make([]string, len(events))
	delays := make([]int32, len(events))
	errorsText := make([]string, len(events))
	statuses := make([]string, len(events))
	for i, event := range events {
		ids[i] = event.ID
		leaseTokens[i] = event.LeaseToken
		delay := 1 << min(event.Attempts, 8)
		if delay > 300 {
			delay = 300
		}
		delays[i] = int32(delay)
		statuses[i] = "pending"
		if event.Attempts >= 10 {
			statuses[i] = "dead"
		}
		if publishErrors[i] != nil {
			errorsText[i] = publishErrors[i].Error()
		} else {
			errorsText[i] = "unknown Redis publishing error"
		}
	}

	rows, err := r.db.Query(ctx, `
		WITH failures AS (
			SELECT id, lease_token, next_status, delay_seconds, last_error
			FROM unnest(
				$1::text[],
				$2::text[],
				$3::text[],
				$4::integer[],
				$5::text[]
			) AS input(id, lease_token, next_status, delay_seconds, last_error)
		), updated AS (
			UPDATE outbox_events AS event
			SET
				status = failures.next_status,
				available_at = NOW() + make_interval(secs => failures.delay_seconds),
				locked_until = NULL,
				lease_token = NULL,
				last_error = failures.last_error
			FROM failures
			WHERE event.id = failures.id::uuid
			  AND event.lease_token = failures.lease_token::uuid
			  AND event.status = 'publishing'
			RETURNING event.id
		)
		SELECT id::text FROM updated
	`, ids, leaseTokens, statuses, delays, errorsText)
	if err != nil {
		return fmt.Errorf("mark failed outbox batch: %w", err)
	}
	defer rows.Close()

	updatedCount := 0
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return fmt.Errorf("scan failed outbox event: %w", err)
		}
		updatedCount++
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate failed outbox events: %w", err)
	}
	if updatedCount != len(events) {
		return fmt.Errorf(
			"updated %d of %d failed outbox events; one or more leases were lost",
			updatedCount,
			len(events),
		)
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
