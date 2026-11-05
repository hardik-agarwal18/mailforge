package outbox

type Event struct {
	ID          string
	LeaseToken  string
	EventType   string
	AggregateID string
	Payload     []byte
	Attempts    int
}
