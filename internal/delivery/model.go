package delivery

type DeliveryJob struct {
	EventID     string
	EventType   string
	AggregateID string
	RecipientID string `json:"recipient_id"`
	MessageID   string `json:"message_id"`
	Email       string `json:"email"`
}
