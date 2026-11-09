package delivery

import (
	"context"

	"mailforge/internal/outbound"
)

type BounceAddressGenerator interface {
	AddressForRecipient(recipientID string) (string, error)
}

type Sender interface {
	Send(
		ctx context.Context,
		sender string,
		recipient string,
		rawMessage []byte,
	) error
}

// DetailedSender is optionally implemented by transports that can report the
// actual remote host and TLS properties used for a delivery attempt.
type DetailedSender interface {
	SendWithResult(
		ctx context.Context,
		sender string,
		recipient string,
		rawMessage []byte,
	) (outbound.DeliveryInfo, error)
}
