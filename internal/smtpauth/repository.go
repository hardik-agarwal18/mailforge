package smtpauth

import (
	"context"
	"errors"
	"fmt"
	"net/mail"
	"regexp"
	"strings"

	"mailforge/internal/smtp"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

var ErrInvalidCredentials = smtp.ErrInvalidCredentials

const dummyPasswordHash = "$pbkdf2-sha256$600000$bWFpbGZvcmdlLWR1bW15LXNhbHQ$wp5dnBhw+t4c2o9bsIAZhvtm2XtzZki0KdTiq4QYQiU"

var usernamePattern = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9._-]{2,63}$`)

type Repository struct {
	db *pgxpool.Pool
}

func NewRepository(db *pgxpool.Pool) *Repository {
	return &Repository{db: db}
}

// Create creates a credential. It intentionally does not upsert: changing an
// existing account password should be an explicit administrative action.
func (r *Repository) Create(
	ctx context.Context,
	username string,
	email string,
	password string,
) error {
	username = strings.ToLower(strings.TrimSpace(username))
	email = strings.ToLower(strings.TrimSpace(email))

	if !usernamePattern.MatchString(username) {
		return fmt.Errorf("username must be 3-64 characters and contain only letters, numbers, '.', '_' or '-'")
	}
	parsed, err := mail.ParseAddress(email)
	if err != nil || parsed.Address != email || !strings.Contains(email, "@") {
		return fmt.Errorf("a valid email address is required")
	}

	passwordHash, err := hashPassword(password)
	if err != nil {
		return err
	}

	_, err = r.db.Exec(ctx, `
		INSERT INTO smtp_credentials (username, email_address, password_hash)
		VALUES ($1, $2, $3)
	`, username, email, passwordHash)
	if err != nil {
		return fmt.Errorf("create SMTP credential: %w", err)
	}

	return nil
}

// Authenticate implements smtp.Authenticator. Invalid usernames and passwords
// deliberately share the same public error to avoid account enumeration.
func (r *Repository) Authenticate(
	ctx context.Context,
	username string,
	password string,
) (smtp.Identity, error) {
	username = strings.ToLower(strings.TrimSpace(username))
	if !usernamePattern.MatchString(username) || password == "" || len(password) > maxPasswordBytes {
		return smtp.Identity{}, ErrInvalidCredentials
	}

	var (
		credentialID string
		email        string
		passwordHash string
		enabled      bool
	)

	err := r.db.QueryRow(ctx, `
		SELECT id::text, email_address, password_hash, enabled
		FROM smtp_credentials
		WHERE username = $1
	`, username).Scan(&credentialID, &email, &passwordHash, &enabled)
	if errors.Is(err, pgx.ErrNoRows) {
		// Spend a password-hash verification on unknown users as well, reducing
		// the username-enumeration timing difference.
		_ = verifyPassword(password, dummyPasswordHash)
		return smtp.Identity{}, ErrInvalidCredentials
	}
	if err != nil {
		return smtp.Identity{}, fmt.Errorf("load SMTP credential: %w", err)
	}

	if !verifyPassword(password, passwordHash) || !enabled {
		return smtp.Identity{}, ErrInvalidCredentials
	}

	if _, err := r.db.Exec(ctx, `
		UPDATE smtp_credentials
		SET last_authenticated_at = NOW(), updated_at = NOW()
		WHERE username = $1
	`, username); err != nil {
		return smtp.Identity{}, fmt.Errorf("update SMTP credential activity: %w", err)
	}

	rows, err := r.db.Query(ctx, `
		SELECT domain
		FROM smtp_sending_domains
		WHERE smtp_credential_id = $1::text::uuid AND verified_at IS NOT NULL
		ORDER BY domain
	`, credentialID)
	if err != nil {
		return smtp.Identity{}, fmt.Errorf("load verified sending domains: %w", err)
	}
	defer rows.Close()

	var authorizedDomains []string
	for rows.Next() {
		var domain string
		if err := rows.Scan(&domain); err != nil {
			return smtp.Identity{}, fmt.Errorf("scan verified sending domain: %w", err)
		}
		authorizedDomains = append(authorizedDomains, domain)
	}
	if err := rows.Err(); err != nil {
		return smtp.Identity{}, fmt.Errorf("iterate verified sending domains: %w", err)
	}

	return smtp.Identity{
		CredentialID:      credentialID,
		Username:          username,
		Email:             email,
		AuthorizedDomains: authorizedDomains,
	}, nil
}
