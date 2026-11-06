package delivery

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
