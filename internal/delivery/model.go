package delivery

import "time"

type DeliveryJob struct {
	EventID     string `json:"event_id,omitempty"`
	EventType   string `json:"event_type,omitempty"`
	AggregateID string `json:"aggregate_id,omitempty"`
	RecipientID string `json:"recipient_id"`
	MessageID   string `json:"message_id"`
	Email       string `json:"email"`
}

type ClaimedDelivery struct {
	RecipientID string
	MessageID   string
	Email       string
	Sender      string
	RawMessage  []byte
	Attempts    int
	LeaseToken  string
}

// RecipientDeliveryStatus is the current durable state for one envelope recipient.
type RecipientDeliveryStatus struct {
	RecipientID        string
	MessageID          string
	Email              string
	Status             string
	Attempts           int
	CreatedAt          time.Time
	UpdatedAt          time.Time
	NextAttemptAt      *time.Time
	LastAttemptAt      *time.Time
	DeliveredAt        *time.Time
	BouncedAt          *time.Time
	UnknownAt          *time.Time
	LastSMTPCode       *int
	LastEnhancedStatus string
	LastSMTPStage      string
	LastError          string
}

// DeliveryStatusEvent is one immutable transition in a recipient's lifecycle.
type DeliveryStatusEvent struct {
	ID                 string
	RecipientID        string
	FromStatus         string
	ToStatus           string
	EventType          string
	SMTPCode           *int
	EnhancedStatusCode string
	SMTPStage          string
	Diagnostic         string
	RemoteHost         string
	TLSUsed            bool
	TLSMode            string
	TLSVersion         string
	TLSCipherSuite     string
	CreatedAt          time.Time
}

// MessageDeliverySummary aggregates per-recipient outcomes for one message.
type MessageDeliverySummary struct {
	MessageID  string
	Total      int
	Pending    int
	Delivering int
	Deferred   int
	Delivered  int
	Bounced    int
	Unknown    int
}
