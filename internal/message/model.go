package message

import "time"

type NewMessage struct {
	Sender     string
	Recipients []string
	RawMessage []byte
}

type StoredMessage struct {
	ID          string
	MessageID   string
	Sender      string
	Recipients  []string
	RawMessage  []byte
	MessageSize int64
	CreatedAt   time.Time
}
