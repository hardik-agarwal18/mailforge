package smtpauth

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"
	"net"
	"strings"
	"time"

	"mailforge/internal/emailaddr"

	"github.com/jackc/pgx/v5"
)

var (
	ErrCredentialNotFound          = errors.New("SMTP credential not found")
	ErrSendingDomainNotFound       = errors.New("sending domain not found")
	ErrSendingDomainAlreadyClaimed = errors.New("sending domain is already assigned to another SMTP credential")
	ErrDomainVerificationFailed    = errors.New("sending domain DNS verification failed")
)

const domainVerificationPrefix = "mailforge-domain-verification="

// TXTResolver is implemented by net.Resolver and can be replaced by a fake in tests.
type TXTResolver interface {
	LookupTXT(ctx context.Context, name string) ([]string, error)
}

// SendingDomain describes the DNS proof required to authorize a sending domain.
type SendingDomain struct {
	Domain            string
	VerificationName  string
	VerificationToken string
	VerifiedAt        *time.Time
}

// AddSendingDomain creates a pending domain claim, or returns the existing claim
// for the same credential. A domain can belong to only one credential in this MVP.
func (r *Repository) AddSendingDomain(
	ctx context.Context,
	username string,
	domain string,
) (SendingDomain, error) {
	username = strings.ToLower(strings.TrimSpace(username))
	domain, err := normalizeSendingDomain(domain)
	if err != nil {
		return SendingDomain{}, err
	}

	var credentialID string
	if err := r.db.QueryRow(ctx, `
		SELECT id::text
		FROM smtp_credentials
		WHERE username = $1
	`, username).Scan(&credentialID); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return SendingDomain{}, ErrCredentialNotFound
		}
		return SendingDomain{}, fmt.Errorf("load SMTP credential: %w", err)
	}

	verificationToken, err := newDomainVerificationToken()
	if err != nil {
		return SendingDomain{}, fmt.Errorf("generate domain verification token: %w", err)
	}

	var result SendingDomain
	err = r.db.QueryRow(ctx, `
		INSERT INTO smtp_sending_domains (
			smtp_credential_id, domain, verification_token
		)
		VALUES ($1::text::uuid, $2, $3)
		ON CONFLICT (domain) DO UPDATE
		SET updated_at = NOW()
		WHERE smtp_sending_domains.smtp_credential_id = EXCLUDED.smtp_credential_id
		RETURNING domain, verification_token, verified_at
	`, credentialID, domain, verificationToken).Scan(
		&result.Domain,
		&result.VerificationToken,
		&result.VerifiedAt,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return SendingDomain{}, ErrSendingDomainAlreadyClaimed
	}
	if err != nil {
		return SendingDomain{}, fmt.Errorf("add SMTP sending domain: %w", err)
	}

	result.VerificationName = verificationRecordName(result.Domain)
	return result, nil
}

// ListSendingDomains returns the domain claims belonging to one credential.
func (r *Repository) ListSendingDomains(
	ctx context.Context,
	username string,
) ([]SendingDomain, error) {
	username = strings.ToLower(strings.TrimSpace(username))
	rows, err := r.db.Query(ctx, `
		SELECT d.domain, d.verification_token, d.verified_at
		FROM smtp_sending_domains d
		JOIN smtp_credentials c ON c.id = d.smtp_credential_id
		WHERE c.username = $1
		ORDER BY d.domain
	`, username)
	if err != nil {
		return nil, fmt.Errorf("list SMTP sending domains: %w", err)
	}
	defer rows.Close()

	var domains []SendingDomain
	for rows.Next() {
		var domain SendingDomain
		if err := rows.Scan(&domain.Domain, &domain.VerificationToken, &domain.VerifiedAt); err != nil {
			return nil, fmt.Errorf("scan SMTP sending domain: %w", err)
		}
		domain.VerificationName = verificationRecordName(domain.Domain)
		domains = append(domains, domain)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate SMTP sending domains: %w", err)
	}
	return domains, nil
}

// VerifySendingDomain checks for the exact token at the required DNS TXT name.
// A failed lookup never grants authorization and never changes verified_at.
func (r *Repository) VerifySendingDomain(
	ctx context.Context,
	username string,
	domain string,
	resolver TXTResolver,
) (SendingDomain, error) {
	if resolver == nil {
		return SendingDomain{}, fmt.Errorf("DNS TXT resolver is required")
	}
	username = strings.ToLower(strings.TrimSpace(username))
	domain, err := normalizeSendingDomain(domain)
	if err != nil {
		return SendingDomain{}, err
	}

	var result SendingDomain
	err = r.db.QueryRow(ctx, `
		SELECT d.domain, d.verification_token, d.verified_at
		FROM smtp_sending_domains d
		JOIN smtp_credentials c ON c.id = d.smtp_credential_id
		WHERE c.username = $1 AND d.domain = $2
	`, username, domain).Scan(
		&result.Domain,
		&result.VerificationToken,
		&result.VerifiedAt,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return SendingDomain{}, ErrSendingDomainNotFound
	}
	if err != nil {
		return SendingDomain{}, fmt.Errorf("load SMTP sending domain: %w", err)
	}

	result.VerificationName = verificationRecordName(result.Domain)
	records, err := resolver.LookupTXT(ctx, result.VerificationName)
	if err != nil {
		return SendingDomain{}, fmt.Errorf("%w: lookup TXT %s: %v", ErrDomainVerificationFailed, result.VerificationName, err)
	}
	if !containsExactTXT(records, result.VerificationToken) {
		return SendingDomain{}, fmt.Errorf("%w: expected TXT value was not found at %s", ErrDomainVerificationFailed, result.VerificationName)
	}

	err = r.db.QueryRow(ctx, `
		UPDATE smtp_sending_domains
		SET verified_at = COALESCE(verified_at, NOW()), updated_at = NOW()
		WHERE domain = $1 AND verification_token = $2
		RETURNING verified_at
	`, result.Domain, result.VerificationToken).Scan(&result.VerifiedAt)
	if err != nil {
		return SendingDomain{}, fmt.Errorf("mark SMTP sending domain verified: %w", err)
	}
	return result, nil
}

// RemoveSendingDomain revokes authorization for a domain owned by the credential.
func (r *Repository) RemoveSendingDomain(ctx context.Context, username, domain string) error {
	username = strings.ToLower(strings.TrimSpace(username))
	domain, err := normalizeSendingDomain(domain)
	if err != nil {
		return err
	}

	result, err := r.db.Exec(ctx, `
		DELETE FROM smtp_sending_domains d
		USING smtp_credentials c
		WHERE d.smtp_credential_id = c.id
		  AND c.username = $1
		  AND d.domain = $2
	`, username, domain)
	if err != nil {
		return fmt.Errorf("remove SMTP sending domain: %w", err)
	}
	if result.RowsAffected() == 0 {
		return ErrSendingDomainNotFound
	}
	return nil
}

func normalizeSendingDomain(domain string) (string, error) {
	domain = strings.ToLower(strings.TrimSuffix(strings.TrimSpace(domain), "."))
	if net.ParseIP(domain) != nil {
		return "", fmt.Errorf("IP addresses are not valid sending domains")
	}
	if strings.Contains(domain, "@") {
		return "", fmt.Errorf("invalid sending domain %q", domain)
	}
	mailbox, err := emailaddr.ParseMailbox("mailforge-domain-check@" + domain)
	if err != nil {
		return "", fmt.Errorf("invalid sending domain %q: %w", domain, err)
	}
	return mailbox.Domain, nil
}

func newDomainVerificationToken() (string, error) {
	var entropy [32]byte
	if _, err := rand.Read(entropy[:]); err != nil {
		return "", err
	}
	return domainVerificationPrefix + base64.RawURLEncoding.EncodeToString(entropy[:]), nil
}

func verificationRecordName(domain string) string {
	return "_mailforge-verification." + domain
}

func containsExactTXT(records []string, expected string) bool {
	for _, record := range records {
		if record == expected {
			return true
		}
	}
	return false
}
