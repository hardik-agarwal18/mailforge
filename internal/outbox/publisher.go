package outbox

import (
	"context"
	"log"
	"time"

	"github.com/redis/go-redis/v9"
)

const deliveryStream = "mailforge:delivery"

type Publisher struct {
	repository *Repository
	redis      *redis.Client
	batchSize  int
	lease      time.Duration
}

func NewPublisher(
	repository *Repository,
	redisClient *redis.Client,
) *Publisher {
	return &Publisher{
		repository: repository,
		redis:      redisClient,
		batchSize:  50,
		lease:      30 * time.Second,
	}
}

// Run continuously polls PostgreSQL for pending outbox events.
func (p *Publisher) Run(ctx context.Context) {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	log.Println("Outbox publisher started")

	// Process existing events immediately.
	p.publishBatch(ctx)

	for {
		select {
		case <-ctx.Done():
			log.Println("Outbox publisher stopping")
			return

		case <-ticker.C:
			p.publishBatch(ctx)
		}
	}
}

func (p *Publisher) publishBatch(ctx context.Context) {
	events, err := p.repository.ClaimBatch(
		ctx,
		p.batchSize,
		p.lease,
	)
	if err != nil {
		log.Printf("Failed to claim events: %v", err)
		return
	}

	for _, event := range events {
		if ctx.Err() != nil {
			return
		}

		if err := p.publish(ctx, event); err != nil {
			log.Printf(
				"Failed to publish event %s: %v",
				event.ID,
				err,
			)

			if markErr := p.repository.MarkFailed(
				ctx,
				event,
				err,
			); markErr != nil {
				log.Printf(
					"Failed to update event %s: %v",
					event.ID,
					markErr,
				)
			} else if event.Attempts >= 10 {
				log.Printf("Outbox event %s moved to dead status; inspect with cmd/outbox-admin list-dead", event.ID)
			}

			continue
		}

		if err := p.repository.MarkPublished(
			ctx,
			event,
		); err != nil {
			// Redis may already contain the event.
			// Leave the lease to expire so it can be
			// retried. Consumers must be idempotent.
			log.Printf(
				"Published to Redis but failed to update PostgreSQL for %s: %v",
				event.ID,
				err,
			)
			continue
		}

		log.Printf(
			"Published event %s to Redis",
			event.ID,
		)
	}
}

func (p *Publisher) publish(
	ctx context.Context,
	event Event,
) error {
	_, err := p.redis.XAdd(
		ctx,
		&redis.XAddArgs{
			Stream: deliveryStream,
			Values: map[string]interface{}{
				"event_id":     event.ID,
				"event_type":   event.EventType,
				"aggregate_id": event.AggregateID,
				"payload":      string(event.Payload),
			},
		},
	).Result()

	return err
}
