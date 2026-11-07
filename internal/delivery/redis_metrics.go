package delivery

import (
	"context"
	"log"
	"time"

	"github.com/redis/go-redis/v9"
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
		w.metrics.RedisOperationFailuresTotal.WithLabelValues("metrics_xlen").Inc()
		log.Printf("Redis metrics: XLEN failed: %v", err)
		return
	}

	w.metrics.RedisStreamEntries.Set(float64(streamEntries))

	// Consumer group information.
	groups, err := client.XInfoGroups(ctx, DeliveryStream).Result()
	if err != nil {
		w.metrics.RedisOperationFailuresTotal.WithLabelValues("metrics_xinfo_groups").Inc()
		log.Printf("Redis metrics: XINFO GROUPS failed: %v", err)
		return
	}

	groupFound := false

	for _, group := range groups {
		if group.Name != ConsumerGroup {
			continue
		}

		groupFound = true

		w.metrics.RedisPendingMessages.Set(float64(group.Pending))
		w.metrics.RedisConsumers.Set(float64(group.Consumers))

		// Redis can report -1 when lag is unavailable.
		if group.Lag >= 0 {
			w.metrics.RedisConsumerGroupLag.Set(float64(group.Lag))
		} else {
			// Redis reports -1 when lag cannot be determined (for example, when
			// the stream has been trimmed in a way that hides the last-delivered ID).
			w.metrics.RedisConsumerGroupLag.Set(-1)
		}

		// No pending messages means no pending idle time.
		if group.Pending == 0 {
			w.metrics.RedisOldestPendingIdle.Set(0)
			return
		}

		// Retrieve the oldest pending entry by stream ID.
		pending, err := client.XPendingExt(
			ctx,
			&redis.XPendingExtArgs{
				Stream: DeliveryStream,
				Group:  ConsumerGroup,
				Start:  "-",
				End:    "+",
				Count:  1,
			},
		).Result()

		if err != nil {
			w.metrics.RedisOperationFailuresTotal.WithLabelValues("metrics_xpending").Inc()
			log.Printf("Redis metrics: XPENDING failed: %v", err)
			return
		}

		if len(pending) == 0 {
			w.metrics.RedisOldestPendingIdle.Set(0)
			return
		}

		w.metrics.RedisOldestPendingIdle.Set(
			pending[0].Idle.Seconds(),
		)

		return
	}

	if !groupFound {
		w.metrics.RedisPendingMessages.Set(0)
		w.metrics.RedisConsumers.Set(0)
		w.metrics.RedisConsumerGroupLag.Set(0)
		w.metrics.RedisOldestPendingIdle.Set(0)
		log.Printf(
			"Redis metrics: consumer group %q not found",
			ConsumerGroup,
		)
	}
}
