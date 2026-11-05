package smtp

import (
	"context"

	"mailforge/internal/message"
)

type MessageAcceptor interface {
	Accept(
		ctx context.Context,
		input message.NewMessage,
	) (message.StoredMessage, error)
}
