package smtp

import (
	"context"
	"errors"
)

var ErrInvalidCredentials = errors.New("invalid SMTP credentials")

// Identity is an authenticated submission identity and its authorized sender domains.
type Identity struct {
	Username string
	Email    string
	// AuthorizedDomains contains only DNS-verified sending domains owned by
	// this credential. The primary Email remains authorized independently.
	AuthorizedDomains []string
}

// Authenticator verifies SMTP AUTH credentials and returns the sender identity.
type Authenticator interface {
	Authenticate(ctx context.Context, username, password string) (Identity, error)
}
