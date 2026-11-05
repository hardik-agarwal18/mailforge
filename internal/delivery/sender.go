package delivery

import "context"

type Sender interface {
	Send(
		ctx context.Context,
		sender string,
		recipient string,
		rawMessage []byte,
	) error
}
