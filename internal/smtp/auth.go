package smtp

import (
	"context"
	"errors"
	"time"
)

var ErrInvalidCredentials = errors.New("invalid SMTP credentials")

// Identity is an authenticated submission identity and its authorized sender domains.
type Identity struct {
	CredentialID string
	Username     string
	Email        string
	// AuthorizedDomains contains only DNS-verified sending domains owned by
	// this credential. The primary Email remains authorized independently.
	AuthorizedDomains []string
}

// Authenticator verifies SMTP AUTH credentials and returns the sender identity.
type Authenticator interface {
	Authenticate(ctx context.Context, username, password string) (Identity, error)
}

// AuthAttemptLimiter atomically applies both the client-IP and username
// authentication-attempt limits. Implementations must fail closed: an
// unavailable limiter must return an error rather than permit authentication.
type AuthAttemptLimiter interface {
	Allow(ctx context.Context, clientIP, username string, ipLimit, usernameLimit int, window time.Duration) (bool, error)
}
