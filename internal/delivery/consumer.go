package delivery

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	DeliveryStream = "mailforge:delivery"
	ConsumerGroup  = "mailforge-workers"
)

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

	if err != nil && err.Error() != "BUSYGROUP Consumer Group name already exists" {
		return fmt.Errorf("create consumer group: %w", err)
	}

	return nil
}

func (c *Consumer) Read(
	ctx context.Context,
	count int64,
) ([]DeliveryJob, error) {

	streams, err := c.client.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group:    ConsumerGroup,
		Consumer: c.consumerName,
		Streams:  []string{DeliveryStream, ">"},
		Count:    count,
		Block:    5 * time.Second,
	}).Result()

	if err != nil {
		if err == redis.Nil {
			return nil, nil
		}

		return nil, fmt.Errorf("read delivery stream: %w", err)
	}

	var jobs []DeliveryJob

	for _, stream := range streams {
		for _, message := range stream.Messages {
			payload, ok := message.Values["payload"].(string)
			if !ok {
				return nil, fmt.Errorf(
					"event %s has invalid payload",
					message.ID,
				)
			}

			var job DeliveryJob

			if err := json.Unmarshal([]byte(payload), &job); err != nil {
				return nil, fmt.Errorf(
					"decode event %s: %w",
					message.ID,
					err,
				)
			}

			job.EventID = message.ID

			if job.RecipientID == "" || job.MessageID == "" {
				return nil, fmt.Errorf(
					"event %s is missing required fields",
					message.ID,
				)
			}

			jobs = append(jobs, job)
		}
	}

	return jobs, nil
}

func (c *Consumer) ClaimStale(
	ctx context.Context,
	start string,
	count int64,
) ([]DeliveryJob, string, error) {
	if start == "" {
		start = "0-0"
	}

	messages, next, err := c.client.XAutoClaim(
		ctx,
		&redis.XAutoClaimArgs{
			Stream:   DeliveryStream,
			Group:    ConsumerGroup,
			Consumer: c.consumerName,
			MinIdle:  3 * time.Minute,
			Start:    start,
			Count:    count,
		},
	).Result()

	if err != nil {
		return nil, start, fmt.Errorf("claim stale messages: %w", err)
	}

	var jobs []DeliveryJob

	for _, message := range messages {
		payload, ok := message.Values["payload"].(string)
		if !ok {
			return nil, next, fmt.Errorf(
				"event %s has invalid payload",
				message.ID,
			)
		}

		var job DeliveryJob

		if err := json.Unmarshal([]byte(payload), &job); err != nil {
			return nil, next, fmt.Errorf(
				"decode event %s: %w",
				message.ID,
				err,
			)
		}

		job.EventID = message.ID

		if job.RecipientID == "" || job.MessageID == "" {
			return nil, next, fmt.Errorf(
				"event %s is missing required fields",
				message.ID,
			)
		}

		jobs = append(jobs, job)
	}

	return jobs, next, nil
}

func (c *Consumer) Acknowledge(ctx context.Context, eventID string) error {
	return c.client.XAck(
		ctx,
		DeliveryStream,
		ConsumerGroup,
		eventID,
	).Err()
}
