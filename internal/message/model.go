package message

import (
	"time"

	"mailforge/internal/mimeparser"
)

type NewMessage struct {
	Sender string
	// AuthenticatedCredentialID is the SMTP credential's immutable database ID
	// and is set only for authenticated SMTP submission.
	AuthenticatedCredentialID string
	// AuthenticatedEmail is the credential's primary mailbox, used for sender authorization.
	AuthenticatedEmail string
	AuthorizedDomains  []string
	ClientIP           string
	Recipients         []string
	RawMessage         []byte
}

type StoredMessage struct {
	ID string
	// SubmissionAccountID is the authenticated SMTP credential UUID that submitted
	// this message. It is quota metadata only; envelope senders, usernames and
	// email addresses must not determine account-level ownership.
	// It is intentionally not persisted as part of the message record.
	SubmissionAccountID string
	MessageID           string
	Sender              string
	ClientIP            string
	Recipients          []string
	RawMessage          []byte
	Parsed              *mimeparser.ParsedMessage
	MessageSize         int64
	CreatedAt           time.Time
}
