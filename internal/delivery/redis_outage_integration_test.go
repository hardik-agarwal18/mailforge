package delivery

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"

	"mailforge/internal/deliverybench"
	"mailforge/internal/outbox"
)

// TestIntegrationOutboxSurvivesRedisOutageAndResumesDelivery verifies the
// transactional outbox boundary end to end. It deliberately points the first
// publisher at an unavailable loopback port instead of stopping a shared Redis
// service, then reconnects to the configured disposable integration Redis DB.
func TestIntegrationOutboxSurvivesRedisOutageAndResumesDelivery(t *testing.T) {
	db := integrationPostgres(t)
	ctx, cancel := context.WithTimeout(context.Background(), 35*time.Second)
	defer cancel()

	redisAddr := strings.TrimSpace(os.Getenv("MAILFORGE_TEST_REDIS_ADDR"))
	if redisAddr == "" {
		t.Skip("set MAILFORGE_TEST_REDIS_ADDR to run Redis outage integration test")
	}

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
	`, recipientID, "redis-outage-test:"+uuid.NewString(), payload).Scan(&outboxID); err != nil {
		t.Fatalf("insert transactional delivery outbox event: %v", err)
	}

	// Model an outage as seen by the publisher without stopping Redis for
	// other local development processes or tests.
	unavailableAddr := reserveThenCloseTCPAddress(t)
	unavailableRedis := redis.NewClient(&redis.Options{
		Addr:         unavailableAddr,
		MaxRetries:   -1,
		DialTimeout:  150 * time.Millisecond,
		ReadTimeout:  150 * time.Millisecond,
		WriteTimeout: 150 * time.Millisecond,
		PoolSize:     1,
	})
	outagePublisher := outbox.NewPublisher(outbox.NewRepository(db), unavailableRedis)
	outageCtx, stopOutagePublisher := context.WithTimeout(ctx, 900*time.Millisecond)
	outagePublisher.Run(outageCtx)
	stopOutagePublisher()
	if err := unavailableRedis.Close(); err != nil {
		t.Errorf("close unavailable Redis client: %v", err)
	}

	var status string
	var attempts int
	var availableAt time.Time
	var lastError string
	if err := db.QueryRow(ctx, `
		SELECT status, attempts, available_at, COALESCE(last_error, '')
		FROM outbox_events WHERE id = $1::uuid
	`, outboxID).Scan(&status, &attempts, &availableAt, &lastError); err != nil {
		t.Fatalf("read outbox event after Redis outage: %v", err)
	}
	if status != "pending" || attempts != 1 || lastError == "" {
		t.Fatalf(
			"outbox after Redis outage = status %q, attempts %d, last_error %q; want pending/1/non-empty error",
			status, attempts, lastError,
		)
	}

	// The event must remain durable in PostgreSQL and be scheduled for a
	// publisher retry rather than being lost or marked as published.
	if !availableAt.After(time.Now()) {
		t.Fatalf("outbox retry availability %s is not in the future after Redis outage", availableAt)
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
		t.Fatalf("connect recovered integration Redis: %v", err)
	}

	stream := DeliveryStream
	group := "mailforge-outage-test-" + strings.ReplaceAll(uuid.NewString(), "-", "")
	consumer := NewConsumer(redisClient, "outage-recovery-consumer")
	consumer.stream = stream
	consumer.group = group
	if err := redisClient.XGroupCreateMkStream(ctx, stream, group, "$").Err(); err != nil {
		redisClient.Close()
		t.Fatalf("create isolated consumer group on delivery stream: %v", err)
	}
	var publishedEntryID string
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		if publishedEntryID != "" {
			if err := redisClient.XDel(cleanupCtx, stream, publishedEntryID).Err(); err != nil {
				t.Errorf("delete test delivery stream entry: %v", err)
			}
		}
		if err := redisClient.XGroupDestroy(cleanupCtx, stream, group).Err(); err != nil && !strings.Contains(err.Error(), "NOGROUP") {
			t.Errorf("destroy test delivery consumer group: %v", err)
		}
		if err := redisClient.Close(); err != nil {
			t.Errorf("close recovered integration Redis client: %v", err)
		}
	})

	if wait := time.Until(availableAt); wait > 0 {
		timer := time.NewTimer(wait + 25*time.Millisecond)
		defer timer.Stop()
		select {
		case <-timer.C:
		case <-ctx.Done():
			t.Fatal("outbox retry did not become due before integration test deadline")
		}
	}

	// Restart the publisher against Redis. Its startup drain should publish
	// the durable PostgreSQL event and mark it published only after XADD works.
	recoveredPublisher := outbox.NewPublisher(outbox.NewRepository(db), redisClient)
	publisherCtx, stopPublisher := context.WithCancel(ctx)
	publisherDone := make(chan struct{})
	go func() {
		defer close(publisherDone)
		recoveredPublisher.Run(publisherCtx)
	}()
	if err := waitForOutboxStatus(ctx, db, outboxID, "published"); err != nil {
		stopPublisher()
		<-publisherDone
		t.Fatalf("outbox event was not published after Redis recovery: %v", err)
	}
	stopPublisher()
	select {
	case <-publisherDone:
	case <-ctx.Done():
		t.Fatal("recovered outbox publisher did not stop")
	}

	entries, err := redisClient.XRange(ctx, stream, "-", "+").Result()
	if err != nil {
		t.Fatalf("inspect delivery stream after publishing: %v", err)
	}
	matchingEntries := 0
	for _, entry := range entries {
		if fmt.Sprint(entry.Values["event_id"]) == outboxID {
			matchingEntries++
			publishedEntryID = entry.ID
		}
	}
	if matchingEntries != 1 {
		t.Fatalf("Redis entries for outbox event %s = %d, want exactly 1", outboxID, matchingEntries)
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
		t.Fatalf("delivery worker did not deliver the recovered event: %v", err)
	}
	if err := waitForNoPendingRedisEntries(ctx, redisClient, stream, group); err != nil {
		stopWorker()
		<-workerDone
		t.Fatalf("worker did not acknowledge recovered delivery event: %v", err)
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

	var finalStatus string
	var deliveryAttempts int
	if err := db.QueryRow(ctx, `
		SELECT r.status, COUNT(a.id)::integer
		FROM recipients r
		LEFT JOIN delivery_attempts a ON a.recipient_id = r.id
		WHERE r.id = $1::uuid
		GROUP BY r.status
	`, recipientID).Scan(&finalStatus, &deliveryAttempts); err != nil {
		t.Fatalf("read recovered recipient delivery state: %v", err)
	}
	if finalStatus != "delivered" || deliveryAttempts != 1 {
		t.Fatalf("recovered delivery = %s with %d attempts, want delivered with exactly 1 attempt", finalStatus, deliveryAttempts)
	}
	stats := mock.Stats()
	if stats.AcceptedMessages != 1 || stats.AcceptedRecipients != 1 {
		t.Fatalf("mock SMTP stats after Redis recovery = %+v, want exactly one accepted message and recipient", stats)
	}

	// A second publisher pass must not duplicate an event already marked
	// published in PostgreSQL.
	secondPublisherCtx, stopSecondPublisher := context.WithTimeout(ctx, 250*time.Millisecond)
	outbox.NewPublisher(outbox.NewRepository(db), redisClient).Run(secondPublisherCtx)
	stopSecondPublisher()
	entries, err = redisClient.XRange(ctx, stream, "-", "+").Result()
	if err != nil {
		t.Fatalf("inspect delivery stream after duplicate publisher pass: %v", err)
	}
	matchingEntries = 0
	for _, entry := range entries {
		if fmt.Sprint(entry.Values["event_id"]) == outboxID {
			matchingEntries++
		}
	}
	if matchingEntries != 1 {
		t.Fatalf("Redis entries after second publisher pass = %d, want exactly 1", matchingEntries)
	}
}

func reserveThenCloseTCPAddress(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve unavailable Redis test port: %v", err)
	}
	addr := listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatalf("close reserved Redis test port: %v", err)
	}
	return addr
}

func waitForOutboxStatus(ctx context.Context, db *pgxpool.Pool, outboxID, expected string) error {
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		var status string
		err := db.QueryRow(ctx, `SELECT status FROM outbox_events WHERE id = $1::uuid`, outboxID).Scan(&status)
		if err != nil {
			return err
		}
		if status == expected {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("timed out waiting for outbox status %q (last status %q): %w", expected, status, ctx.Err())
		case <-ticker.C:
		}
	}
}

func waitForRecipientStatus(ctx context.Context, db *pgxpool.Pool, recipientID, expected string) error {
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		var status string
		err := db.QueryRow(ctx, `SELECT status FROM recipients WHERE id = $1::uuid`, recipientID).Scan(&status)
		if err != nil {
			return err
		}
		if status == expected {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("timed out waiting for recipient status %q (last status %q): %w", expected, status, ctx.Err())
		case <-ticker.C:
		}
	}
}

func waitForNoPendingRedisEntries(ctx context.Context, client *redis.Client, stream, group string) error {
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		pending, err := client.XPending(ctx, stream, group).Result()
		if err != nil {
			return err
		}
		if pending.Count == 0 {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("timed out waiting for consumer group pending count to reach zero (currently %d): %w", pending.Count, ctx.Err())
		case <-ticker.C:
		}
	}
}
