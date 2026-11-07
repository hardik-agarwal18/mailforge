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

// SetBatchSize controls how many outbox events are claimed and pipelined per
// publishing cycle. Invalid values are ignored to retain the safe default.
func (p *Publisher) SetBatchSize(batchSize int) {
	if batchSize > 0 && batchSize <= 1000 {
		p.batchSize = batchSize
	}
}

// Run continuously polls PostgreSQL for pending outbox events.
func (p *Publisher) Run(ctx context.Context) {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	log.Printf("Outbox publisher started batch_size=%d", p.batchSize)

	// Drain existing events immediately, then repeat after each poll interval.
	p.publishAvailable(ctx)

	for {
		select {
		case <-ctx.Done():
			log.Println("Outbox publisher stopping")
			return

		case <-ticker.C:
			p.publishAvailable(ctx)
		}
	}
}

// publishAvailable drains full batches without waiting for another ticker
// interval. This prevents the polling interval from becoming a throughput cap.
func (p *Publisher) publishAvailable(ctx context.Context) {
	for ctx.Err() == nil {
		publishedCount := p.publishBatch(ctx)
		if publishedCount < p.batchSize {
			return
		}
	}
}

func (p *Publisher) publishBatch(ctx context.Context) int {
	events, err := p.repository.ClaimBatch(
		ctx,
		p.batchSize,
		p.lease,
	)
	if err != nil {
		log.Printf("Failed to claim outbox events: %v", err)
		return 0
	}
	if len(events) == 0 {
		return 0
	}

	// Pipeline XADD commands to reduce Redis network round trips. Commands
	// are inspected individually because a pipeline can partially succeed.
	pipeline := p.redis.Pipeline()
	commands := make([]*redis.StringCmd, len(events))
	for i, event := range events {
		commands[i] = pipeline.XAdd(ctx, &redis.XAddArgs{
			Stream: deliveryStream,
			Values: map[string]interface{}{
				"event_id":     event.ID,
				"event_type":   event.EventType,
				"aggregate_id": event.AggregateID,
				"payload":      string(event.Payload),
			},
		})
	}

	_, pipelineErr := pipeline.Exec(ctx)

	published := make([]Event, 0, len(events))
	failed := make([]Event, 0)
	failureErrors := make([]error, 0)

	for i, command := range commands {
		if command.Err() != nil {
			failed = append(failed, events[i])
			failureErrors = append(failureErrors, command.Err())
			log.Printf(
				"Failed to publish outbox event %s: %v",
				events[i].ID,
				command.Err(),
			)
			continue
		}
		published = append(published, events[i])
	}

	if len(failed) > 0 {
		if markErr := p.repository.MarkFailedBatch(ctx, failed, failureErrors); markErr != nil {
			// Leave these leases to expire; the publisher can safely retry them.
			log.Printf("Failed to update %d failed outbox events: %v", len(failed), markErr)
		} else {
			for _, event := range failed {
				if event.Attempts >= 10 {
					log.Printf("Outbox event %s moved to dead status; inspect with cmd/outbox-admin list-dead", event.ID)
				}
			}
		}
	}

	if len(published) > 0 {
		if markErr := p.repository.MarkPublishedBatch(ctx, published); markErr != nil {
			// Redis may already contain these events. Leave their leases to expire
			// so they can be retried; consumers must remain idempotent.
			log.Printf(
				"Published %d events to Redis but failed to update PostgreSQL: %v",
				len(published),
				markErr,
			)
		} else {
			log.Printf("Published %d/%d outbox events to Redis", len(published), len(events))
		}
	}

	// A non-nil Exec error can coexist with individually successful commands.
	// The per-command results above determine which leases are updated.
	if pipelineErr != nil && len(failed) == 0 {
		log.Printf("Redis pipeline returned an error after successful command results: %v", pipelineErr)
	}
	return len(events)
}
