package smtp

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

	Authenticated         bool
	AuthenticatedUsername string
	AuthenticatedEmail    string
	AuthFailures          int

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
