package delivery

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

const (
	DeliveryStream           = "mailforge:delivery"
	DeliveryDeadLetterStream = "mailforge:delivery:dead"
	ConsumerGroup            = "mailforge-workers"
	pendingIdleTimeout       = 2 * time.Minute
)

var quarantineDeliveryEvent = redis.NewScript(`
		local dead_id = redis.call(
			'XADD', KEYS[1], '*',
			'source_stream', ARGV[1],
			'source_group', ARGV[2],
			'source_event_id', ARGV[3],
			'reason', ARGV[4],
			'payload', ARGV[5]
		)
		redis.call('XACK', KEYS[2], ARGV[2], ARGV[3])
		return dead_id
	`)

type Consumer struct {
	client       *redis.Client
	consumerName string
}

func NewConsumer(client *redis.Client, consumerName string) *Consumer {
	return &Consumer{
		client:       client,
		consumerName: consumerName,
	}
}

func (c *Consumer) CreateGroup(ctx context.Context) error {
	err := c.client.XGroupCreateMkStream(
		ctx,
		DeliveryStream,
		ConsumerGroup,
		"0",
	).Err()

	if err != nil && !strings.Contains(err.Error(), "BUSYGROUP") {
		return fmt.Errorf("create consumer group: %w", err)
	}

	return nil
}

// Read returns all valid jobs in the batch. Malformed entries are moved to
// the dead-letter stream and acknowledged atomically. If quarantine fails,
// valid jobs are still returned alongside the error so they are not delayed.
func (c *Consumer) Read(
	ctx context.Context,
	count int64,
) ([]DeliveryJob, error) {
	if count < 1 {
		count = 10
	}

	streams, err := c.client.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group:    ConsumerGroup,
		Consumer: c.consumerName,
		Streams:  []string{DeliveryStream, ">"},
		Count:    count,
		Block:    5 * time.Second,
	}).Result()

	if err != nil {
		if errors.Is(err, redis.Nil) {
			return nil, nil
		}
		return nil, fmt.Errorf("read delivery stream: %w", err)
	}

	var jobs []DeliveryJob
	var firstErr error

	for _, stream := range streams {
		for _, message := range stream.Messages {
			job, decodeErr := c.decodeMessage(ctx, message)
			if decodeErr != nil {
				if firstErr == nil {
					firstErr = decodeErr
				}
				continue
			}
			if job != nil {
				jobs = append(jobs, *job)
			}
		}
	}

	return jobs, firstErr
}

// ClaimStale reclaims abandoned pending events. Active jobs refresh their
// pending idle time, so normal deliveries are not stolen while SMTP is running.
func (c *Consumer) ClaimStale(
	ctx context.Context,
	start string,
	count int64,
) ([]DeliveryJob, string, error) {
	if start == "" {
		start = "0-0"
	}
	if count < 1 {
		count = 10
	}

	messages, next, err := c.client.XAutoClaim(
		ctx,
		&redis.XAutoClaimArgs{
			Stream:   DeliveryStream,
			Group:    ConsumerGroup,
			Consumer: c.consumerName,
			MinIdle:  pendingIdleTimeout,
			Start:    start,
			Count:    count,
		},
	).Result()
	if err != nil {
		return nil, start, fmt.Errorf("claim stale messages: %w", err)
	}

	var jobs []DeliveryJob
	var firstErr error

	for _, message := range messages {
		job, decodeErr := c.decodeMessage(ctx, message)
		if decodeErr != nil {
			if firstErr == nil {
				firstErr = decodeErr
			}
			continue
		}
		if job != nil {
			jobs = append(jobs, *job)
		}
	}

	return jobs, next, firstErr
}

func (c *Consumer) decodeMessage(
	ctx context.Context,
	message redis.XMessage,
) (*DeliveryJob, error) {
	rawValues, marshalErr := json.Marshal(message.Values)
	if marshalErr != nil {
		rawValues = []byte(fmt.Sprintf("%v", message.Values))
	}

	payloadValue, ok := message.Values["payload"]
	if !ok {
		return nil, c.quarantineMessage(
			ctx,
			message.ID,
			"missing payload field",
			string(rawValues),
		)
	}

	var payload []byte
	switch value := payloadValue.(type) {
	case string:
		payload = []byte(value)
	case []byte:
		payload = value
	default:
		return nil, c.quarantineMessage(
			ctx,
			message.ID,
			fmt.Sprintf("payload has unsupported type %T", payloadValue),
			string(rawValues),
		)
	}

	var job DeliveryJob
	if err := json.Unmarshal(payload, &job); err != nil {
		return nil, c.quarantineMessage(
			ctx,
			message.ID,
			fmt.Sprintf("invalid delivery JSON: %v", err),
			string(payload),
		)
	}

	job.EventID = message.ID
	job.EventType = redisString(message.Values["event_type"])
	job.AggregateID = redisString(message.Values["aggregate_id"])

	if job.RecipientID == "" || job.MessageID == "" || job.Email == "" {
		return nil, c.quarantineMessage(
			ctx,
			message.ID,
			"payload is missing recipient_id, message_id, or email",
			string(payload),
		)
	}

	if _, err := uuid.Parse(job.RecipientID); err != nil {
		return nil, c.quarantineMessage(
			ctx,
			message.ID,
			fmt.Sprintf("invalid recipient_id: %v", err),
			string(payload),
		)
	}
	if _, err := uuid.Parse(job.MessageID); err != nil {
		return nil, c.quarantineMessage(
			ctx,
			message.ID,
			fmt.Sprintf("invalid message_id: %v", err),
			string(payload),
		)
	}

	return &job, nil
}

func redisString(value interface{}) string {
	if value == nil {
		return ""
	}
	return fmt.Sprint(value)
}

// QuarantineEvent atomically copies an event to the dead-letter stream and
// acknowledges the original pending entry. It is for structurally valid Redis
// events that reference a recipient that no longer exists.
func (c *Consumer) QuarantineEvent(
	ctx context.Context,
	eventID string,
	reason string,
	payload []byte,
) error {
	if eventID == "" {
		return fmt.Errorf("event ID is required for quarantine")
	}
	return c.quarantineMessage(ctx, eventID, reason, string(payload))
}

func (c *Consumer) quarantineMessage(
	ctx context.Context,
	eventID string,
	reason string,
	payload string,
) error {
	if eventID == "" {
		return fmt.Errorf("event ID is required for quarantine")
	}

	_, err := quarantineDeliveryEvent.Run(
		ctx,
		c.client,
		[]string{DeliveryDeadLetterStream, DeliveryStream},
		DeliveryStream,
		ConsumerGroup,
		eventID,
		reason,
		payload,
	).Result()
	if err != nil {
		return fmt.Errorf("quarantine event %s: %w", eventID, err)
	}

	return nil
}

// RefreshPending resets the Redis pending-entry idle timer while the same
// delivery is actively being processed. PostgreSQL lease ownership remains
// authoritative; this only prevents XAUTOCLAIM from stealing a live job.
func (c *Consumer) RefreshPending(
	ctx context.Context,
	eventID string,
) error {
	if eventID == "" {
		return fmt.Errorf("event ID is required")
	}

	// XCLAIM with MIN-IDLE 0 reassigns the pending entry to this consumer and
	// resets its idle time. IDLE 0 makes the reset explicit on Redis 6.2+.
	return c.client.Do(
		ctx,
		"XCLAIM",
		DeliveryStream,
		ConsumerGroup,
		c.consumerName,
		0,
		eventID,
		"IDLE",
		0,
		"JUSTID",
	).Err()
}

func (c *Consumer) Acknowledge(ctx context.Context, eventID string) error {
	if eventID == "" {
		return fmt.Errorf("event ID is required for acknowledgement")
	}

	// XACK returning zero is idempotent: it means the entry was already
	// acknowledged or is no longer pending.
	return c.client.XAck(
		ctx,
		DeliveryStream,
		ConsumerGroup,
		eventID,
	).Err()
}
