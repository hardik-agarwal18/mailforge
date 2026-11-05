package delivery

import (
	"context"
	"log"
	"time"
)

func (w *Worker) collectRedisMetrics(
	ctx context.Context,
	interval time.Duration,
) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	// Collect immediately instead of waiting five seconds.
	w.updateRedisMetrics(ctx)

	for {
		select {
		case <-ctx.Done():
			return

		case <-ticker.C:
			w.updateRedisMetrics(ctx)
		}
	}
}

func (w *Worker) updateRedisMetrics(ctx context.Context) {
	if w.metrics == nil {
		return
	}

	client := w.consumer.client

	// Total retained stream entries.
	streamEntries, err := client.XLen(ctx, DeliveryStream).Result()
	if err != nil {
		log.Printf("Redis metrics: XLEN failed: %v", err)
		return
	}

	w.metrics.RedisStreamEntries.Set(float64(streamEntries))

	// Consumer group information.
	groups, err := client.XInfoGroups(ctx, DeliveryStream).Result()
	if err != nil {
		log.Printf("Redis metrics: XINFO GROUPS failed: %v", err)
		return
	}

	for _, group := range groups {
		if group.Name != ConsumerGroup {
			continue
		}

		w.metrics.RedisPendingMessages.Set(float64(group.Pending))
		w.metrics.RedisConsumers.Set(float64(group.Consumers))

		// Redis can report -1 when lag is unavailable.
		w.metrics.RedisConsumerGroupLag.Set(float64(group.Lag))

		return
	}

	log.Printf("Redis metrics: consumer group %q not found", ConsumerGroup)
}
