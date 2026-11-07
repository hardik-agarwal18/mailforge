package message

import (
	"time"

	"mailforge/internal/mimeparser"
)

type NewMessage struct {
	Sender     string
	ClientIP   string
	Recipients []string
	RawMessage []byte
}

type StoredMessage struct {
	ID          string
	MessageID   string
	Sender      string
	ClientIP    string
	Recipients  []string
	RawMessage  []byte
	Parsed      *mimeparser.ParsedMessage
	MessageSize int64
	CreatedAt   time.Time
}
