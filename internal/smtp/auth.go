package smtp

import (
	"context"
	"errors"
)

var ErrInvalidCredentials = errors.New("invalid SMTP credentials")

// Identity is the authenticated submission identity and its permitted envelope sender.
type Identity struct {
	Username string
	Email    string
}

// Authenticator verifies SMTP AUTH credentials and returns the sender identity.
type Authenticator interface {
	Authenticate(ctx context.Context, username, password string) (Identity, error)
}
