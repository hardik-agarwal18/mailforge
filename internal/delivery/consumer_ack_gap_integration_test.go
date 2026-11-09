package delivery

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"

	"mailforge/internal/deliverybench"
)

// TestIntegrationCommittedDeliveryWithLostRedisAckDoesNotResend models a
// worker crash after the SMTP success and PostgreSQL delivery transaction have
// committed, but before the Redis stream entry is acknowledged. A recovery
// consumer must reclaim the pending entry, observe the terminal PostgreSQL
// state, and ACK it without sending the email again.
func TestIntegrationCommittedDeliveryWithLostRedisAckDoesNotResend(t *testing.T) {
	db := integrationPostgres(t)
	redisClient, firstConsumer := integrationRedis(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	recipientID, messageID := integrationCreateRecipient(t, ctx, db)
	payload, err := json.Marshal(DeliveryJob{
		RecipientID: recipientID,
		MessageID:   messageID,
		Email:       "recipient@example.test",
	})
	if err != nil {
		t.Fatalf("marshal delivery payload: %v", err)
	}

	entryID, err := redisClient.XAdd(ctx, &redis.XAddArgs{
		Stream: firstConsumer.stream,
		Values: map[string]interface{}{
			"event_type":   "recipient.delivery_requested",
			"aggregate_id": recipientID,
			"payload":      string(payload),
		},
	}).Result()
	if err != nil {
		t.Fatalf("append delivery event: %v", err)
	}

	jobs, err := firstConsumer.Read(ctx, 1)
	if err != nil {
		t.Fatalf("first consumer read delivery event: %v", err)
	}
	if len(jobs) != 1 || jobs[0].EventID != entryID {
		t.Fatalf("first consumer jobs = %#v, want one event %s", jobs, entryID)
	}

	mock, relay := startIntegrationMockSMTP(t, deliverybench.MockSMTPConfig{})
	firstProcessor := NewProcessor(NewRepository(db), relay)
	if err := firstProcessor.Process(ctx, jobs[0]); err != nil {
		t.Fatalf("process delivery before simulated ACK gap: %v", err)
	}

	// The SMTP receiver accepted the message and the PostgreSQL transaction
	// committed. Deliberately do not XACK: this is the crash window between
	// Processor.Process returning and Worker.acknowledge running.
	if stats := mock.Stats(); stats.AcceptedMessages != 1 || stats.AcceptedRecipients != 1 {
		t.Fatalf("mock SMTP stats after first delivery = %+v, want one accepted message and recipient", stats)
	}

	var status string
	var attempts int
	var attemptCount int
	if err := db.QueryRow(ctx, `
		SELECT r.status, r.attempts, COUNT(a.id)::integer
		FROM recipients r
		LEFT JOIN delivery_attempts a ON a.recipient_id = r.id
		WHERE r.id = $1::uuid
		GROUP BY r.id
	`, recipientID).Scan(&status, &attempts, &attemptCount); err != nil {
		t.Fatalf("read committed delivery state before recovery: %v", err)
	}
	if status != "delivered" || attempts != 1 || attemptCount != 1 {
		t.Fatalf("committed delivery state = %s/%d attempts/%d rows, want delivered/1/1", status, attempts, attemptCount)
	}

	pendingBefore, err := redisClient.XPending(ctx, firstConsumer.stream, firstConsumer.group).Result()
	if err != nil {
		t.Fatalf("inspect pending entry before simulated recovery: %v", err)
	}
	if pendingBefore.Count != 1 {
		t.Fatalf("pending entries before recovery = %d, want 1", pendingBefore.Count)
	}

	// Wait until XAUTOCLAIM is allowed to reclaim the event from the dead
	// consumer, then recover it under a new consumer identity.
	time.Sleep(firstConsumer.pendingIdleTimeout + 25*time.Millisecond)
	recoveryConsumer := *firstConsumer
	recoveryConsumer.consumerName = "ack-gap-recovery-" + uuid.NewString()
	reclaimed, _, err := recoveryConsumer.ClaimStale(ctx, "0-0", 1)
	if err != nil {
		t.Fatalf("reclaim delivery event after simulated worker crash: %v", err)
	}
	if len(reclaimed) != 1 || reclaimed[0].EventID != entryID {
		t.Fatalf("reclaimed jobs = %#v, want original event %s", reclaimed, entryID)
	}

	// PostgreSQL is authoritative. Since the first worker durably committed
	// delivered, processing the reclaimed Redis event must be a no-op.
	recoveryProcessor := NewProcessor(NewRepository(db), relay)
	if err := recoveryProcessor.Process(ctx, reclaimed[0]); err != nil {
		t.Fatalf("process reclaimed event for already-delivered recipient: %v", err)
	}
	if err := recoveryConsumer.Acknowledge(ctx, entryID); err != nil {
		t.Fatalf("acknowledge recovered duplicate event: %v", err)
	}

	if stats := mock.Stats(); stats.AcceptedMessages != 1 || stats.AcceptedRecipients != 1 {
		t.Fatalf("mock SMTP stats after Redis ACK recovery = %+v, want exactly one accepted message and recipient", stats)
	}

	if err := db.QueryRow(ctx, `
		SELECT r.status, r.attempts, COUNT(a.id)::integer
		FROM recipients r
		LEFT JOIN delivery_attempts a ON a.recipient_id = r.id
		WHERE r.id = $1::uuid
		GROUP BY r.id
	`, recipientID).Scan(&status, &attempts, &attemptCount); err != nil {
		t.Fatalf("read delivery state after ACK recovery: %v", err)
	}
	if status != "delivered" || attempts != 1 || attemptCount != 1 {
		t.Fatalf("final delivery state = %s/%d attempts/%d rows, want delivered/1/1", status, attempts, attemptCount)
	}

	pendingAfter, err := redisClient.XPending(ctx, recoveryConsumer.stream, recoveryConsumer.group).Result()
	if err != nil {
		t.Fatalf("inspect pending entries after ACK recovery: %v", err)
	}
	if pendingAfter.Count != 0 {
		t.Fatalf("pending entries after ACK recovery = %d, want 0", pendingAfter.Count)
	}
}
