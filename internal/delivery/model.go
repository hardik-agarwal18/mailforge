package delivery

type DeliveryJob struct {
	EventID     string
	EventType   string
	AggregateID string
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
