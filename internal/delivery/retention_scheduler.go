package delivery

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	streamRetentionLockKey = "mailforge:locks:delivery-stream-retention"

	streamRetentionInterval = 15 * time.Minute
	streamRetentionLockTTL  = 2 * time.Minute
	streamRetentionTimeout  = 30 * time.Second
)

var releaseStreamRetentionLock = redis.NewScript(`
    if redis.call("GET", KEYS[1]) == ARGV[1] then
        return redis.call("DEL", KEYS[1])
    end
    return 0
`)

func (w *Worker) runStreamRetention(ctx context.Context) {
	ticker := time.NewTicker(streamRetentionInterval)
	defer ticker.Stop()

	// Perform an initial retention pass.
	w.runStreamRetentionOnce(ctx)

	for {
		select {
		case <-ctx.Done():
			return

		case <-ticker.C:
			w.runStreamRetentionOnce(ctx)
		}
	}
}

func (w *Worker) runStreamRetentionOnce(ctx context.Context) {
	client := w.consumer.client

	token := fmt.Sprintf(
		"%s:%d",
		w.consumer.consumerName,
		time.Now().UnixNano(),
	)

	lockCtx, cancelLock := context.WithTimeout(ctx, 5*time.Second)
	acquired, err := client.SetNX(
		lockCtx,
		streamRetentionLockKey,
		token,
		streamRetentionLockTTL,
	).Result()
	cancelLock()

	if err != nil {
		log.Printf("Redis retention: acquire lock failed: %v", err)

		if w.metrics != nil {
			w.metrics.RedisStreamRetentionErrors.Inc()
		}
		return
	}

	// Another worker is performing retention.
	if !acquired {
		return
	}

	defer func() {
		releaseCtx, cancel := context.WithTimeout(
			context.Background(),
			3*time.Second,
		)
		defer cancel()

		if err := releaseStreamRetentionLock.Run(
			releaseCtx,
			client,
			[]string{streamRetentionLockKey},
			token,
		).Err(); err != nil {
			log.Printf("Redis retention: release lock failed: %v", err)
		}
	}()

	if w.metrics != nil {
		w.metrics.RedisStreamRetentionRuns.Inc()
	}

	trimCtx, cancelTrim := context.WithTimeout(
		ctx,
		streamRetentionTimeout,
	)
	defer cancelTrim()

	trimmed, err := w.consumer.TrimAcknowledged(
		trimCtx,
		DefaultDeliveryStreamMaxLen,
	)
	if err != nil {
		log.Printf("Redis retention failed: %v", err)

		if w.metrics != nil {
			w.metrics.RedisStreamRetentionErrors.Inc()
		}
		return
	}

	if w.metrics != nil {
		w.metrics.RedisStreamTrimmedEntries.Add(float64(trimmed))
		w.metrics.RedisStreamRetentionLastSuccess.Set(
			float64(time.Now().Unix()),
		)
	}

	log.Printf(
		"Redis retention completed: trimmed=%d target_max_len=%d",
		trimmed,
		DefaultDeliveryStreamMaxLen,
	)
}
