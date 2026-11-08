package message

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"

	"mailforge/internal/emailaddr"
	"mailforge/internal/mimeparser"
)

const (
	defaultMaxMessageSize = 10 * 1024 * 1024
	messageDomain         = "mailforge.local"
)

var (
	ErrInvalidSender                  = errors.New("invalid envelope sender")
	ErrInvalidRecipient               = errors.New("invalid envelope recipient")
	ErrInvalidMessageFormat           = errors.New("invalid RFC 5322 or MIME message")
	ErrUnauthorizedSubmissionIdentity = errors.New("submission sender identity is not authorized for this account")
	ErrMIMEMessageTooLarge            = errors.New("MIME message exceeds parser limits")
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

	// An empty envelope sender represents SMTP's null reverse-path (MAIL FROM:<>)
	// and is valid for delivery status notifications. Every non-empty sender and
	// every recipient must be a supported mailbox before anything is persisted.
	if err := emailaddr.ValidateReversePath(input.Sender); err != nil {
		return StoredMessage{}, fmt.Errorf("%w: %v", ErrInvalidSender, err)
	}

	if len(input.RawMessage) > s.maxMessageSize {
		return StoredMessage{}, fmt.Errorf(
			"message exceeds maximum size of %d bytes",
			s.maxMessageSize,
		)
	}

	for _, recipient := range input.Recipients {
		if err := emailaddr.ValidateMailbox(recipient); err != nil {
			return StoredMessage{}, fmt.Errorf("%w: %v", ErrInvalidRecipient, err)
		}
	}
	// Do not create multiple delivery records for the same SMTP mailbox.
	// Preserve first-seen order and spelling; local-parts are case-sensitive,
	// while domain names are case-insensitive.
	input.Recipients = emailaddr.DeduplicateMailboxes(input.Recipients)

	parsed, err := mimeparser.Parse(input.RawMessage)
	if err != nil {
		if errors.Is(err, mimeparser.ErrMIMETooLarge) {
			return StoredMessage{}, fmt.Errorf("%w: %v", ErrMIMEMessageTooLarge, err)
		}
		return StoredMessage{}, fmt.Errorf("%w: %v", ErrInvalidMessageFormat, err)
	}

	// Authenticated submission must use the credential's primary mailbox or a
	// DNS-verified sending domain for the envelope and visible author fields.
	// Ordinary inbound SMTP leaves AuthenticatedEmail empty and is intentionally
	// exempt because remote sender authenticity is a separate mail-auth concern.
	if input.AuthenticatedEmail != "" {
		if err := validateAuthenticatedSubmissionIdentity(input.AuthenticatedEmail, input.AuthorizedDomains, input.Sender, parsed); err != nil {
			return StoredMessage{}, err
		}
	}

	stored := StoredMessage{
		MessageID:   fmt.Sprintf("<%s@%s>", uuid.NewString(), messageDomain),
		Sender:      input.Sender,
		ClientIP:    input.ClientIP,
		Recipients:  input.Recipients,
		RawMessage:  input.RawMessage,
		Parsed:      parsed,
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

// validateAuthenticatedSubmissionIdentity enforces the single-mailbox identity model
// currently returned by the SMTP authenticator. Supporting aliases should extend
// the authenticated identity with an explicit allow-list, not weaken this check.
func validateAuthenticatedSubmissionIdentity(authenticatedEmail string, authorizedDomains []string, envelopeSender string, parsed *mimeparser.ParsedMessage) error {
	if err := emailaddr.ValidateMailbox(authenticatedEmail); err != nil {
		return fmt.Errorf("%w: invalid authenticated identity", ErrUnauthorizedSubmissionIdentity)
	}
	if !emailaddr.IsAuthorizedMailbox(envelopeSender, authenticatedEmail, authorizedDomains) {
		return fmt.Errorf("%w: envelope sender is not an authorized mailbox", ErrUnauthorizedSubmissionIdentity)
	}
	if len(parsed.From) != 1 || !emailaddr.IsAuthorizedMailbox(parsed.From[0].Address, authenticatedEmail, authorizedDomains) {
		return fmt.Errorf("%w: From must contain exactly one authorized mailbox", ErrUnauthorizedSubmissionIdentity)
	}
	if len(parsed.Sender) > 1 {
		return fmt.Errorf("%w: Sender must contain at most one mailbox", ErrUnauthorizedSubmissionIdentity)
	}
	if len(parsed.Sender) == 1 && !emailaddr.IsAuthorizedMailbox(parsed.Sender[0].Address, authenticatedEmail, authorizedDomains) {
		return fmt.Errorf("%w: Sender must be an authorized mailbox", ErrUnauthorizedSubmissionIdentity)
	}
	return nil
}
