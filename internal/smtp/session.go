package smtp

import "mailforge/internal/emailaddr"

type State int

const (
	StateConnected State = iota
	StateReady
	StateMail
	StateRecipient
	StateData
	StateClosed
)

type Session struct {
	State     State
	Hostname  string
	ESMTP     bool
	TLSActive bool

	Authenticated             bool
	AuthenticatedCredentialID string
	AuthenticatedEmail        string
	AuthorizedDomains         []string
	AuthFailures              int

	Sender      string
	Recipients  []string
	MessageData []byte
}

func NewSession() *Session {
	return &Session{
		State:      StateConnected,
		Recipients: make([]string, 0),
	}
}

func (s *Session) ResetTransaction() {
	s.Sender = ""
	s.Recipients = nil
	s.MessageData = nil
	s.State = StateReady
}

func (s *Session) CanReceiveRecipient() bool {
	return s.State == StateMail || s.State == StateRecipient
}

// HasRecipient reports whether the transaction already contains the mailbox.
// Mailbox comparison preserves local-part case and ignores domain case.
func (s *Session) HasRecipient(address string) bool {
	for _, recipient := range s.Recipients {
		if emailaddr.Equal(recipient, address) {
			return true
		}
	}
	return false
}
