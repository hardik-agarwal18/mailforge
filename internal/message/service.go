package message

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
)

const (
	defaultMaxMessageSize = 10 * 1024 * 1024
	messageDomain         = "mailforge.local"
)

type MessageCreator interface {
	Create(ctx context.Context, msg StoredMessage) (string, error)
}

type Service struct {
	repository     MessageCreator
	maxMessageSize int
}

func NewService(repository MessageCreator) *Service {
	return &Service{
		repository:     repository,
		maxMessageSize: defaultMaxMessageSize,
	}
}

func (s *Service) Accept(
	ctx context.Context,
	input NewMessage,
) (StoredMessage, error) {

	if len(input.Recipients) == 0 {
		return StoredMessage{}, fmt.Errorf(
			"at least one recipient is required",
		)
	}

	if len(input.RawMessage) > s.maxMessageSize {
		return StoredMessage{}, fmt.Errorf(
			"message exceeds maximum size of %d bytes",
			s.maxMessageSize,
		)
	}

	for i, recipient := range input.Recipients {
		input.Recipients[i] = strings.TrimSpace(recipient)

		if input.Recipients[i] == "" {
			return StoredMessage{}, fmt.Errorf(
				"recipient cannot be empty",
			)
		}
	}

	stored := StoredMessage{
		MessageID:   fmt.Sprintf("<%s@%s>", uuid.NewString(), messageDomain),
		Sender:      input.Sender,
		ClientIP:    input.ClientIP,
		Recipients:  input.Recipients,
		RawMessage:  input.RawMessage,
		MessageSize: int64(len(input.RawMessage)),
		CreatedAt:   time.Now().UTC(),
	}

	id, err := s.repository.Create(ctx, stored)
	if err != nil {
		return StoredMessage{}, fmt.Errorf(
			"persist message: %w",
			err,
		)
	}

	stored.ID = id

	return stored, nil
}
