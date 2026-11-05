package message

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

type Repository struct {
	db *pgxpool.Pool
}

func NewRepository(db *pgxpool.Pool) *Repository {
	return &Repository{
		db: db,
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

	tx, err := r.db.Begin(ctx)
	if err != nil {
		return "", fmt.Errorf("begin transaction: %w", err)
	}

	defer func() {
		_ = tx.Rollback(context.Background())
	}()

	// 1. Insert message.
	var messageID string

	err = tx.QueryRow(
		ctx,
		`
		INSERT INTO messages (
			message_id,
			envelope_sender,
			raw_message,
			message_size
		)
		VALUES ($1, $2, $3, $4)
		RETURNING id::text
		`,
		msg.MessageID,
		msg.Sender,
		msg.RawMessage,
		msg.MessageSize,
	).Scan(&messageID)

	if err != nil {
		return "", fmt.Errorf("insert message: %w", err)
	}

	// 2. Insert recipients and corresponding outbox events.
	for _, recipient := range msg.Recipients {

		var recipientID string

		err = tx.QueryRow(
			ctx,
			`
			INSERT INTO recipients (
				message_id,
				email
			)
			VALUES ($1, $2)
			RETURNING id::text
			`,
			messageID,
			recipient,
		).Scan(&recipientID)

		if err != nil {
			return "", fmt.Errorf(
				"insert recipient: %w",
				err,
			)
		}

		// 3. Construct delivery event.
		event := DeliveryEvent{
			RecipientID: recipientID,
			MessageID:   messageID,
			Email:       recipient,
		}

		payload, err := json.Marshal(event)
		if err != nil {
			return "", fmt.Errorf(
				"marshal delivery event: %w",
				err,
			)
		}

		idempotencyKey := "recipient.delivery_requested:" +
			recipientID

		// 4. Insert event into transactional outbox.
		_, err = tx.Exec(
			ctx,
			`
			INSERT INTO outbox_events (
				event_type,
				aggregate_type,
				aggregate_id,
				idempotency_key,
				payload
			)
			VALUES ($1, $2, $3, $4, $5)
			`,
			"recipient.delivery_requested",
			"recipient",
			recipientID,
			idempotencyKey,
			payload,
		)

		if err != nil {
			return "", fmt.Errorf(
				"insert outbox event: %w",
				err,
			)
		}
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
