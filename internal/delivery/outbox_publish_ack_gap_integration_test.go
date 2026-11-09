package delivery

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"

	"mailforge/internal/deliverybench"
	"mailforge/internal/outbox"
)

// TestIntegrationOutboxPublishAckGapIsAtLeastOnceAndConsumerIsIdempotent
// reproduces the failure window where Redis accepts XADD but PostgreSQL cannot
// record the event as published. After the outbox lease expires, a restarted
// publisher must publish the same logical event again. The delivery consumer
// must tolerate both stream entries without sending the email twice.
func TestIntegrationOutboxPublishAckGapIsAtLeastOnceAndConsumerIsIdempotent(t *testing.T) {
	db := integrationPostgres(t)
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()

	redisAddr := strings.TrimSpace(os.Getenv("MAILFORGE_TEST_REDIS_ADDR"))
	if redisAddr == "" {
		t.Skip("set MAILFORGE_TEST_REDIS_ADDR to run outbox publish/ack gap integration test")
	}

	redisDB := 0
	if raw := strings.TrimSpace(os.Getenv("MAILFORGE_TEST_REDIS_DB")); raw != "" {
		if _, err := fmt.Sscanf(raw, "%d", &redisDB); err != nil || redisDB < 0 {
			t.Fatalf("invalid MAILFORGE_TEST_REDIS_DB %q", raw)
		}
	}
	redisClient := redis.NewClient(&redis.Options{
		Addr:         redisAddr,
		Password:     os.Getenv("MAILFORGE_TEST_REDIS_PASSWORD"),
		DB:           redisDB,
		MaxRetries:   -1,
		DialTimeout:  time.Second,
		ReadTimeout:  time.Second,
		WriteTimeout: time.Second,
	})
	if err := redisClient.Ping(ctx).Err(); err != nil {
		redisClient.Close()
		t.Fatalf("connect integration Redis: %v", err)
	}

	stream := DeliveryStream
	group := "mailforge-outbox-gap-" + strings.ReplaceAll(uuid.NewString(), "-", "")
	consumer := NewConsumer(redisClient, "outbox-gap-consumer")
	consumer.stream = stream
	consumer.group = group
	if err := redisClient.XGroupCreateMkStream(ctx, stream, group, "$").Err(); err != nil {
		redisClient.Close()
		t.Fatalf("create isolated delivery consumer group: %v", err)
	}
	var streamEntryIDs []string
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		if len(streamEntryIDs) > 0 {
			if err := redisClient.XDel(cleanupCtx, stream, streamEntryIDs...).Err(); err != nil {
				t.Errorf("delete publish/ack gap stream entries: %v", err)
			}
		}
		if err := redisClient.XGroupDestroy(cleanupCtx, stream, group).Err(); err != nil && !strings.Contains(err.Error(), "NOGROUP") {
			t.Errorf("destroy publish/ack gap consumer group: %v", err)
		}
		if err := redisClient.Close(); err != nil {
			t.Errorf("close publish/ack gap Redis client: %v", err)
		}
	})

	recipientID, messageID := integrationCreateRecipient(t, ctx, db)
	job := DeliveryJob{
		RecipientID: recipientID,
		MessageID:   messageID,
		Email:       "recipient@example.test",
	}
	payload, err := json.Marshal(job)
	if err != nil {
		t.Fatalf("marshal delivery job: %v", err)
	}

	var outboxID string
	if err := db.QueryRow(ctx, `
		INSERT INTO outbox_events (
			event_type, aggregate_type, aggregate_id, idempotency_key, payload
		)
		VALUES (
			'recipient.delivery_requested', 'recipient', $1::uuid, $2, $3::jsonb
		)
		RETURNING id::text
	`, recipientID, "publish-ack-gap:"+uuid.NewString(), payload).Scan(&outboxID); err != nil {
		t.Fatalf("insert delivery outbox event: %v", err)
	}

	// Fail only the PostgreSQL status transition to 'published'. The trigger
	// leaves the event in 'publishing' after Redis has already accepted XADD,
	// which models a crash or database failure in the publish/acknowledgement
	// gap without stopping or corrupting the shared database service.
	if _, err := db.Exec(ctx, `
		CREATE FUNCTION test_fail_outbox_mark_published() RETURNS trigger
		LANGUAGE plpgsql AS $$
		BEGIN
			IF NEW.id::text = TG_ARGV[0] AND NEW.status = 'published' THEN
				RAISE EXCEPTION 'simulated PostgreSQL failure after Redis XADD';
			END IF;
			RETURN NEW;
		END;
		$$
	`); err != nil {
		t.Fatalf("create outbox failure trigger function: %v", err)
	}
	if _, err := db.Exec(ctx, `
		CREATE TRIGGER test_fail_outbox_mark_published
		BEFORE UPDATE OF status ON outbox_events
		FOR EACH ROW
		EXECUTE FUNCTION test_fail_outbox_mark_published('`+outboxID+`')
	`); err != nil {
		t.Fatalf("create outbox failure trigger: %v", err)
	}
	triggerInstalled := true
	t.Cleanup(func() {
		if triggerInstalled {
			cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cleanupCancel()
			_, _ = db.Exec(cleanupCtx, `DROP TRIGGER IF EXISTS test_fail_outbox_mark_published ON outbox_events`)
			_, _ = db.Exec(cleanupCtx, `DROP FUNCTION IF EXISTS test_fail_outbox_mark_published()`)
		}
	})

	firstPublisherCtx, stopFirstPublisher := context.WithCancel(ctx)
	firstPublisherDone := make(chan struct{})
	go func() {
		defer close(firstPublisherDone)
		outbox.NewPublisher(outbox.NewRepository(db), redisClient).Run(firstPublisherCtx)
	}()

	firstEntries, err := waitForOutboxStreamEntries(ctx, redisClient, stream, outboxID, 1)
	if err != nil {
		stopFirstPublisher()
		<-firstPublisherDone
		t.Fatalf("first publisher did not XADD event before acknowledgement failure: %v", err)
	}
	streamEntryIDs = append(streamEntryIDs, firstEntries[0].ID)
	stopFirstPublisher()
	select {
	case <-firstPublisherDone:
	case <-ctx.Done():
		t.Fatal("first outbox publisher did not stop")
	}

	var firstStatus string
	var firstAttempts int
	if err := db.QueryRow(ctx, `
		SELECT status, attempts FROM outbox_events WHERE id = $1::uuid
	`, outboxID).Scan(&firstStatus, &firstAttempts); err != nil {
		t.Fatalf("read outbox state after simulated acknowledgement failure: %v", err)
	}
	if firstStatus != "publishing" || firstAttempts != 1 {
		t.Fatalf("outbox after first XADD = %s/%d attempts, want publishing/1", firstStatus, firstAttempts)
	}

	firstEntries, err = waitForOutboxStreamEntries(ctx, redisClient, stream, outboxID, 1)
	if err != nil || len(firstEntries) != 1 {
		t.Fatalf("Redis entries after first publish = %d, err=%v; want exactly 1", len(firstEntries), err)
	}

	if _, err := db.Exec(ctx, `DROP TRIGGER test_fail_outbox_mark_published ON outbox_events`); err != nil {
		t.Fatalf("remove simulated outbox acknowledgement failure: %v", err)
	}
	if _, err := db.Exec(ctx, `DROP FUNCTION test_fail_outbox_mark_published()`); err != nil {
		t.Fatalf("remove simulated trigger function: %v", err)
	}
	triggerInstalled = false

	// Expire the lease directly instead of waiting for the production
	// 30-second lease. This is equivalent to the original publisher process
	// disappearing and the recovery publisher taking over after lease expiry.
	if _, err := db.Exec(ctx, `
		UPDATE outbox_events
		SET locked_until = NOW() - INTERVAL '1 second'
		WHERE id = $1::uuid AND status = 'publishing'
	`, outboxID); err != nil {
		t.Fatalf("expire outbox publisher lease: %v", err)
	}

	recoveryCtx, stopRecoveryPublisher := context.WithCancel(ctx)
	recoveryDone := make(chan struct{})
	go func() {
		defer close(recoveryDone)
		outbox.NewPublisher(outbox.NewRepository(db), redisClient).Run(recoveryCtx)
	}()
	if err := waitForOutboxStatus(ctx, db, outboxID, "published"); err != nil {
		stopRecoveryPublisher()
		<-recoveryDone
		t.Fatalf("recovery publisher did not mark outbox event published: %v", err)
	}
	stopRecoveryPublisher()
	select {
	case <-recoveryDone:
	case <-ctx.Done():
		t.Fatal("recovery outbox publisher did not stop")
	}

	secondEntries, err := waitForOutboxStreamEntries(ctx, redisClient, stream, outboxID, 2)
	if err != nil {
		t.Fatalf("recovery publisher did not republish the event: %v", err)
	}
	if len(secondEntries) != 2 {
		t.Fatalf("Redis contains %d entries for outbox event %s, want exactly 2 at-least-once publications", len(secondEntries), outboxID)
	}
	streamEntryIDs = append(streamEntryIDs, secondEntries[1].ID)

	var finalOutboxStatus string
	var finalOutboxAttempts int
	if err := db.QueryRow(ctx, `
		SELECT status, attempts FROM outbox_events WHERE id = $1::uuid
	`, outboxID).Scan(&finalOutboxStatus, &finalOutboxAttempts); err != nil {
		t.Fatalf("read recovered outbox state: %v", err)
	}
	if finalOutboxStatus != "published" || finalOutboxAttempts != 2 {
		t.Fatalf("recovered outbox state = %s/%d attempts, want published/2", finalOutboxStatus, finalOutboxAttempts)
	}

	mock, relay := startIntegrationMockSMTP(t, deliverybench.MockSMTPConfig{})
	worker := NewWorker(consumer, NewProcessor(NewRepository(db), relay), 1)
	workerCtx, stopWorker := context.WithCancel(ctx)
	workerDone := make(chan error, 1)
	go func() {
		workerDone <- worker.Run(workerCtx)
	}()

	if err := waitForRecipientStatus(ctx, db, recipientID, "delivered"); err != nil {
		stopWorker()
		<-workerDone
		t.Fatalf("worker did not deliver event duplicated by outbox recovery: %v", err)
	}
	if err := waitForRedisGroupDrainedThrough(ctx, redisClient, stream, group, secondEntries[1].ID); err != nil {
		stopWorker()
		<-workerDone
		t.Fatalf("worker did not consume and acknowledge both duplicate stream entries: %v", err)
	}
	stopWorker()
	select {
	case err := <-workerDone:
		if err != nil {
			t.Fatalf("delivery worker returned error: %v", err)
		}
	case <-ctx.Done():
		t.Fatal("delivery worker did not stop")
	}

	var recipientStatus string
	var deliveryAttempts int
	if err := db.QueryRow(ctx, `
		SELECT r.status, COUNT(a.id)::integer
		FROM recipients r
		LEFT JOIN delivery_attempts a ON a.recipient_id = r.id
		WHERE r.id = $1::uuid
		GROUP BY r.status
	`, recipientID).Scan(&recipientStatus, &deliveryAttempts); err != nil {
		t.Fatalf("read recipient state after duplicate stream events: %v", err)
	}
	if recipientStatus != "delivered" || deliveryAttempts != 1 {
		t.Fatalf("recipient after duplicate publications = %s/%d delivery attempts, want delivered/1", recipientStatus, deliveryAttempts)
	}
	stats := mock.Stats()
	if stats.AcceptedMessages != 1 || stats.AcceptedRecipients != 1 {
		t.Fatalf("mock SMTP stats after duplicate publications = %+v, want exactly one accepted message and recipient", stats)
	}
}

func waitForOutboxStreamEntries(
	ctx context.Context,
	client *redis.Client,
	stream string,
	outboxID string,
	want int,
) ([]redis.XMessage, error) {
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		entries, err := client.XRange(ctx, stream, "-", "+").Result()
		if err != nil && err != redis.Nil {
			return nil, err
		}
		matching := make([]redis.XMessage, 0, want)
		for _, entry := range entries {
			if fmt.Sprint(entry.Values["event_id"]) == outboxID {
				matching = append(matching, entry)
			}
		}
		if len(matching) >= want {
			return matching, nil
		}
		select {
		case <-ctx.Done():
			return matching, fmt.Errorf("timed out waiting for %d Redis entries for outbox event %s (found %d): %w", want, outboxID, len(matching), ctx.Err())
		case <-ticker.C:
		}
	}
}

func waitForRedisGroupDrainedThrough(
	ctx context.Context,
	client *redis.Client,
	stream string,
	group string,
	lastEntryID string,
) error {
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		groups, err := client.XInfoGroups(ctx, stream).Result()
		if err != nil {
			return err
		}
		for _, info := range groups {
			if info.Name == group && info.LastDeliveredID == lastEntryID && info.Pending == 0 {
				return nil
			}
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("timed out waiting for consumer group %s to drain through %s: %w", group, lastEntryID, ctx.Err())
		case <-ticker.C:
		}
	}
}
