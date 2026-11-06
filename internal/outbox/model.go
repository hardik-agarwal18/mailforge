package outbox

import "time"

type Event struct {
	ID          string
	LeaseToken  string
	EventType   string
	AggregateID string
	Payload     []byte
	Attempts    int
}

type DeadEvent struct {
	ID          string
	EventType   string
	AggregateID string
	Attempts    int
	LastError   string
	CreatedAt   time.Time
}
