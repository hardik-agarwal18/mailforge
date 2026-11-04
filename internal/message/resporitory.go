package message

import (
	"context"
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

func (r *Repository) Create(
	ctx context.Context,
	msg StoredMessage,
) (string, error) {

	tx, err := r.db.Begin(ctx)
	if err != nil {
		return "", fmt.Errorf("begin transaction: %w", err)
	}

	defer func() {
		_ = tx.Rollback(ctx)
	}()

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

	for _, recipient := range msg.Recipients {
		_, err = tx.Exec(
			ctx,
			`
			INSERT INTO recipients (
				message_id,
				email
			)
			VALUES ($1, $2)
			`,
			messageID,
			recipient,
		)

		if err != nil {
			return "", fmt.Errorf(
				"insert recipient %s: %w",
				recipient,
				err,
			)
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return "", fmt.Errorf("commit transaction: %w", err)
	}

	return messageID, nil
}
