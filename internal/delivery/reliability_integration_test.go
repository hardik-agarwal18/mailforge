package delivery

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"

	"mailforge/internal/bounce"
	"mailforge/internal/deliverybench"
	"mailforge/internal/outbound"
)

// These integration tests use isolated PostgreSQL schemas and Redis streams.
// Set MAILFORGE_TEST_DATABASE_URL and/or MAILFORGE_TEST_REDIS_ADDR to enable
// them. The Redis database must be a disposable test database; the tests only
// create and delete uniquely named streams.

func TestIntegrationSuccessfulDeliveryThroughSMTPRelay(t *testing.T) {
	db := integrationPostgres(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	recipientID, messageID := integrationCreateRecipient(t, ctx, db)
	mock, relay := startIntegrationMockSMTP(t, deliverybench.MockSMTPConfig{})
	processor := NewProcessor(NewRepository(db), relay)
	job := DeliveryJob{
		RecipientID: recipientID,
		MessageID:   messageID,
		Email:       "recipient@example.test",
	}

	if err := processor.Process(ctx, job); err != nil {
		t.Fatalf("process successful SMTP relay delivery: %v", err)
	}

	stats := mock.Stats()
	if stats.AcceptedMessages != 1 || stats.AcceptedRecipients != 1 {
		t.Fatalf("mock SMTP stats = %+v, want one accepted message and recipient", stats)
	}

	var status string
	var attempts int
	var smtpCode int
	var remoteHost string
	var tlsMode string
	if err := db.QueryRow(ctx, `
		SELECT r.status, r.attempts, a.smtp_code, a.remote_host, a.tls_mode
		FROM recipients r
		JOIN delivery_attempts a ON a.recipient_id = r.id
		WHERE r.id = $1::uuid
	`, recipientID).Scan(&status, &attempts, &smtpCode, &remoteHost, &tlsMode); err != nil {
		t.Fatalf("read successful SMTP delivery result: %v", err)
	}
	if status != "delivered" || attempts != 1 || smtpCode != 250 {
		t.Fatalf("delivery result = %s/%d attempts/SMTP %d, want delivered/1/250", status, attempts, smtpCode)
	}
	if remoteHost != "127.0.0.1" || tlsMode != string(outbound.TLSModeNone) {
		t.Fatalf("transport details = host %q / TLS mode %q, want loopback / none", remoteHost, tlsMode)
	}
}

func TestIntegrationTemporarySMTPFailureRetriesAndEventuallyDelivers(t *testing.T) {
	db := integrationPostgres(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	recipientID, messageID := integrationCreateRecipient(t, ctx, db)
	mock, relay := startIntegrationMockSMTP(t, deliverybench.MockSMTPConfig{
		RejectFirst: 1,
		RejectCode:  451,
	})
	policy := RetryPolicy{
		InitialDelay: 40 * time.Millisecond,
		MaxDelay:     100 * time.Millisecond,
		MaxQueueAge:  time.Hour,
		MaxAttempts:  3,
	}
	repository, err := NewRepositoryWithRetryPolicy(db, policy)
	if err != nil {
		t.Fatalf("create repository with fast integration retry policy: %v", err)
	}
	processor := NewProcessor(repository, relay)
	job := DeliveryJob{
		RecipientID: recipientID,
		MessageID:   messageID,
		Email:       "recipient@example.test",
	}

	if err := processor.Process(ctx, job); err != nil {
		t.Fatalf("process initial temporary SMTP failure: %v", err)
	}

	var status string
	var attempts int
	var smtpCode int
	var enhancedStatus string
	var smtpStage string
	var nextAttemptAt time.Time
	if err := db.QueryRow(ctx, `
		SELECT status, attempts, last_smtp_code,
		       last_enhanced_status_code, last_smtp_stage, next_attempt_at
		FROM recipients WHERE id = $1::uuid
	`, recipientID).Scan(&status, &attempts, &smtpCode, &enhancedStatus, &smtpStage, &nextAttemptAt); err != nil {
		t.Fatalf("read deferred recipient state: %v", err)
	}
	if status != "deferred" || attempts != 1 || smtpCode != 451 {
		t.Fatalf("first attempt state = %s/%d/SMTP %d, want deferred/1/451", status, attempts, smtpCode)
	}
	if enhancedStatus != "4.3.0" || smtpStage != "rcpt_to" {
		t.Fatalf("temporary SMTP diagnostics = %q at %q, want 4.3.0 at rcpt_to", enhancedStatus, smtpStage)
	}

	var retryEvents int
	var outboxAvailableAt time.Time
	if err := db.QueryRow(ctx, `
		SELECT COUNT(*)::integer, MIN(available_at)
		FROM outbox_events
		WHERE aggregate_id = $1::uuid
		  AND event_type = 'recipient.delivery_requested'
	`, recipientID).Scan(&retryEvents, &outboxAvailableAt); err != nil {
		t.Fatalf("count transactional retry events: %v", err)
	}
	if retryEvents != 1 {
		t.Fatalf("retry outbox events = %d, want exactly 1", retryEvents)
	}
	if !outboxAvailableAt.Equal(nextAttemptAt) {
		t.Fatalf("recipient retry time %s differs from outbox availability %s", nextAttemptAt, outboxAvailableAt)
	}
	if err := processor.Process(ctx, job); !errors.Is(err, ErrRecipientScheduled) {
		t.Fatalf("duplicate pre-deadline delivery event error = %v, want ErrRecipientScheduled", err)
	}

	if wait := time.Until(nextAttemptAt); wait > 0 {
		timer := time.NewTimer(wait + 20*time.Millisecond)
		defer timer.Stop()
		select {
		case <-timer.C:
		case <-ctx.Done():
			t.Fatal("retry became due after integration test deadline")
		}
	}

	if err := processor.Process(ctx, job); err != nil {
		t.Fatalf("process scheduled SMTP retry: %v", err)
	}
	var finalStatus string
	var finalAttempts int
	if err := db.QueryRow(ctx, `
		SELECT status, attempts FROM recipients WHERE id = $1::uuid
	`, recipientID).Scan(&finalStatus, &finalAttempts); err != nil {
		t.Fatalf("read final retry state: %v", err)
	}
	if finalStatus != "delivered" || finalAttempts != 2 {
		t.Fatalf("final retry state = %s/%d attempts, want delivered/2", finalStatus, finalAttempts)
	}

	stats := mock.Stats()
	if stats.RejectedRecipients != 1 || stats.AcceptedRecipients != 1 || stats.AcceptedMessages != 1 {
		t.Fatalf("mock SMTP stats = %+v, want one 451 rejection followed by one accepted message", stats)
	}

	var temporaryAttempts int
	var successfulAttempts int
	if err := db.QueryRow(ctx, `
		SELECT
			COUNT(*) FILTER (WHERE status = 'temporary_failure')::integer,
			COUNT(*) FILTER (WHERE status = 'success')::integer
		FROM delivery_attempts
		WHERE recipient_id = $1::uuid
	`, recipientID).Scan(&temporaryAttempts, &successfulAttempts); err != nil {
		t.Fatalf("read persisted retry attempts: %v", err)
	}
	if temporaryAttempts != 1 || successfulAttempts != 1 {
		t.Fatalf("persisted attempts = temporary %d / success %d, want 1 / 1", temporaryAttempts, successfulAttempts)
	}
}

func TestIntegrationPermanentSMTPFailureMarksRecipientBounced(t *testing.T) {
	db := integrationPostgres(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	recipientID, messageID := integrationCreateRecipient(t, ctx, db)
	mock, relay := startIntegrationMockSMTP(t, deliverybench.MockSMTPConfig{
		RejectFirst: 1,
		RejectCode:  550,
	})
	processor := NewProcessor(NewRepository(db), relay)
	job := DeliveryJob{
		RecipientID: recipientID,
		MessageID:   messageID,
		Email:       "recipient@example.test",
	}

	if err := processor.Process(ctx, job); err != nil {
		t.Fatalf("process permanent SMTP rejection: %v", err)
	}

	var status string
	var attempts int
	var smtpCode int
	var enhancedStatus string
	if err := db.QueryRow(ctx, `
		SELECT status, attempts, last_smtp_code, last_enhanced_status_code
		FROM recipients WHERE id = $1::uuid
	`, recipientID).Scan(&status, &attempts, &smtpCode, &enhancedStatus); err != nil {
		t.Fatalf("read permanent SMTP failure result: %v", err)
	}
	if status != "bounced" || attempts != 1 || smtpCode != 550 {
		t.Fatalf("permanent failure state = %s/%d/SMTP %d, want bounced/1/550", status, attempts, smtpCode)
	}
	if enhancedStatus != "5.1.1" {
		t.Fatalf("permanent enhanced status = %q, want 5.1.1", enhancedStatus)
	}
	stats := mock.Stats()
	if stats.RejectedRecipients != 1 || stats.AcceptedMessages != 0 {
		t.Fatalf("mock SMTP stats = %+v, want one rejected recipient and no accepted message", stats)
	}
}

func TestIntegrationDuplicateEventsDoNotSendTwice(t *testing.T) {
	db := integrationPostgres(t)
	redisClient, consumer := integrationRedis(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	recipientID, messageID := integrationCreateRecipient(t, ctx, db)
	sender := &integrationSender{started: make(chan struct{}), release: make(chan struct{})}
	processor := NewProcessor(NewRepository(db), sender)

	payload, err := json.Marshal(DeliveryJob{
		RecipientID: recipientID,
		MessageID:   messageID,
		Email:       "recipient@example.test",
	})
	if err != nil {
		t.Fatalf("marshal delivery payload: %v", err)
	}

	for i := 0; i < 2; i++ {
		if err := redisClient.XAdd(ctx, &redis.XAddArgs{
			Stream: consumer.stream,
			Values: map[string]interface{}{
				"event_type":   "recipient.delivery_requested",
				"aggregate_id": recipientID,
				"payload":      string(payload),
			},
		}).Err(); err != nil {
			t.Fatalf("append duplicate delivery event %d: %v", i, err)
		}
	}

	jobs, err := consumer.Read(ctx, 2)
	if err != nil {
		t.Fatalf("read duplicate delivery events: %v", err)
	}
	if len(jobs) != 2 {
		t.Fatalf("read %d delivery jobs, want 2", len(jobs))
	}

	firstDone := make(chan error, 1)
	go func() {
		firstDone <- processor.Process(ctx, jobs[0])
	}()

	select {
	case <-sender.started:
	case <-ctx.Done():
		t.Fatal("first delivery did not reach the SMTP sender")
	}

	// While the first SMTP operation owns the PostgreSQL lease, the duplicate
	// event must not start another SMTP send.
	secondErr := processor.Process(ctx, jobs[1])
	if !errors.Is(secondErr, ErrRecipientBusy) {
		t.Fatalf("second duplicate error = %v, want ErrRecipientBusy", secondErr)
	}
	if got := sender.calls.Load(); got != 1 {
		t.Fatalf("SMTP sends while first attempt is active = %d, want 1", got)
	}

	close(sender.release)
	if err := <-firstDone; err != nil {
		t.Fatalf("first delivery failed: %v", err)
	}

	// Once the recipient is durably delivered, replaying the second stream
	// entry must be a no-op and therefore safe to acknowledge.
	if err := processor.Process(ctx, jobs[1]); err != nil {
		t.Fatalf("replay duplicate event after delivery: %v", err)
	}
	if got := sender.calls.Load(); got != 1 {
		t.Fatalf("total SMTP sends = %d, want 1", got)
	}

	var status string
	var attempts int
	if err := db.QueryRow(ctx, `
		SELECT status, attempts FROM recipients WHERE id = $1::uuid
	`, recipientID).Scan(&status, &attempts); err != nil {
		t.Fatalf("read recipient state: %v", err)
	}
	if status != "delivered" || attempts != 1 {
		t.Fatalf("recipient state = %s/%d attempts, want delivered/1", status, attempts)
	}
}

func TestIntegrationExpiredInFlightLeaseBecomesUnknownAfterOutcomeWriteFailure(t *testing.T) {
	db := integrationPostgres(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	recipientID, messageID := integrationCreateRecipient(t, ctx, db)
	sender := &integrationSender{}
	repository := NewRepository(db)
	processor := NewProcessor(repository, sender)

	// Simulate a PostgreSQL failure after the remote SMTP server accepted DATA
	// but before the successful result can be committed.
	_, err := db.Exec(ctx, `
		CREATE FUNCTION test_reject_delivered_state() RETURNS trigger AS $$
		BEGIN
			IF OLD.status = 'delivering' AND NEW.status = 'delivered' THEN
				RAISE EXCEPTION 'injected outcome persistence failure';
			END IF;
			RETURN NEW;
		END;
		$$ LANGUAGE plpgsql
	`)
	if err != nil {
		t.Fatalf("create outcome failure function: %v", err)
	}
	_, err = db.Exec(ctx, `
		CREATE TRIGGER test_reject_delivered_state
		BEFORE UPDATE ON recipients
		FOR EACH ROW EXECUTE FUNCTION test_reject_delivered_state()
	`)
	if err != nil {
		t.Fatalf("install outcome failure trigger: %v", err)
	}

	job := DeliveryJob{
		RecipientID: recipientID,
		MessageID:   messageID,
		Email:       "recipient@example.test",
	}
	if err := processor.Process(ctx, job); err == nil {
		t.Fatal("Process returned nil; expected injected outcome persistence failure")
	}
	if got := sender.calls.Load(); got != 1 {
		t.Fatalf("SMTP sends after injected failure = %d, want 1", got)
	}

	var staleLeaseToken string
	var staleAttempts int
	if err := db.QueryRow(ctx, `
		SELECT delivery_lease_token::text, attempts
		FROM recipients WHERE id = $1::uuid
	`, recipientID).Scan(&staleLeaseToken, &staleAttempts); err != nil {
		t.Fatalf("read lease token after injected failure: %v", err)
	}
	staleClaim := &ClaimedDelivery{
		RecipientID: recipientID,
		MessageID:   messageID,
		Email:       "recipient@example.test",
		Attempts:    staleAttempts,
		LeaseToken:  staleLeaseToken,
	}

	if _, err := db.Exec(ctx, `DROP TRIGGER test_reject_delivered_state ON recipients`); err != nil {
		t.Fatalf("drop outcome failure trigger: %v", err)
	}
	if _, err := db.Exec(ctx, `DROP FUNCTION test_reject_delivered_state()`); err != nil {
		t.Fatalf("drop outcome failure function: %v", err)
	}
	if _, err := db.Exec(ctx, `
		UPDATE recipients
		SET delivery_lease_until = NOW() - INTERVAL '1 second'
		WHERE id = $1::uuid
	`, recipientID); err != nil {
		t.Fatalf("expire delivery lease: %v", err)
	}

	// The next event replay reconciles an abandoned SMTP attempt to unknown
	// instead of sending again, because the remote acceptance is ambiguous.
	if err := processor.Process(ctx, job); err != nil {
		t.Fatalf("reconcile expired delivery lease: %v", err)
	}
	if got := sender.calls.Load(); got != 1 {
		t.Fatalf("SMTP sends after expired-lease recovery = %d, want 1", got)
	}
	if err := repository.RecordSuccess(ctx, staleClaim); err == nil {
		t.Fatal("stale lease owner unexpectedly committed a delivery success")
	}

	var status string
	var attemptStatus string
	var eventType string
	if err := db.QueryRow(ctx, `
		SELECT r.status, a.status, e.event_type
		FROM recipients r
		JOIN delivery_attempts a ON a.recipient_id = r.id
		JOIN delivery_status_events e ON e.recipient_id = r.id
		WHERE r.id = $1::uuid
		  AND e.event_type = 'lease_expired_ambiguous'
		LIMIT 1
	`, recipientID).Scan(&status, &attemptStatus, &eventType); err != nil {
		t.Fatalf("read reconciled delivery state: %v", err)
	}
	if status != "unknown" || attemptStatus != "unknown" || eventType != "lease_expired_ambiguous" {
		t.Fatalf("reconciled state = %s/%s/%s, want unknown/unknown/lease_expired_ambiguous", status, attemptStatus, eventType)
	}
}

func TestIntegrationTemporaryFailureSchedulesExactlyOneRetry(t *testing.T) {
	db := integrationPostgres(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	recipientID, messageID := integrationCreateRecipient(t, ctx, db)
	sender := &integrationSender{sendErr: &outbound.DeliveryError{
		Message:   "451 4.3.0 temporary local error",
		SMTPCode:  451,
		Stage:     "mail_from",
		Permanent: false,
	}}
	processor := NewProcessor(NewRepository(db), sender)
	job := DeliveryJob{
		RecipientID: recipientID,
		MessageID:   messageID,
		Email:       "recipient@example.test",
	}

	if err := processor.Process(ctx, job); err != nil {
		t.Fatalf("record temporary SMTP failure: %v", err)
	}
	if got := sender.calls.Load(); got != 1 {
		t.Fatalf("SMTP sends after temporary failure = %d, want 1", got)
	}

	// Replaying the old event must observe the durable deferred state and its
	// already-created retry outbox event rather than schedule another retry.
	err := processor.Process(ctx, job)
	if !errors.Is(err, ErrRecipientScheduled) {
		t.Fatalf("replayed event error = %v, want ErrRecipientScheduled", err)
	}

	var status string
	var retryCount int
	if err := db.QueryRow(ctx, `
		SELECT status FROM recipients WHERE id = $1::uuid
	`, recipientID).Scan(&status); err != nil {
		t.Fatalf("read deferred recipient status: %v", err)
	}
	if err := db.QueryRow(ctx, `
		SELECT COUNT(*)::integer
		FROM outbox_events
		WHERE aggregate_id = $1::uuid
		  AND event_type = 'recipient.delivery_requested'
		  AND idempotency_key = 'recipient.delivery_requested:' || $1::text || ':2'
	`, recipientID).Scan(&retryCount); err != nil {
		t.Fatalf("count scheduled retry events: %v", err)
	}
	if status != "deferred" || retryCount != 1 {
		t.Fatalf("recipient state/retry count = %s/%d, want deferred/1", status, retryCount)
	}
	if got := sender.calls.Load(); got != 1 {
		t.Fatalf("total SMTP sends after duplicate retry event = %d, want 1", got)
	}
}

func TestIntegrationRedisPendingEntryCanBeReclaimedAfterConsumerCrash(t *testing.T) {
	redisClient, firstConsumer := integrationRedis(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	payload, err := json.Marshal(DeliveryJob{
		RecipientID: uuid.NewString(),
		MessageID:   uuid.NewString(),
		Email:       "recipient@example.test",
	})
	if err != nil {
		t.Fatalf("marshal pending event: %v", err)
	}

	entryID, err := redisClient.XAdd(ctx, &redis.XAddArgs{
		Stream: firstConsumer.stream,
		Values: map[string]interface{}{"payload": string(payload)},
	}).Result()
	if err != nil {
		t.Fatalf("append pending event: %v", err)
	}

	jobs, err := firstConsumer.Read(ctx, 1)
	if err != nil {
		t.Fatalf("read pending event: %v", err)
	}
	if len(jobs) != 1 || jobs[0].EventID != entryID {
		t.Fatalf("read result = %#v, want one event %s", jobs, entryID)
	}
	// Deliberately do not ACK: this is the simulated consumer crash.

	time.Sleep(firstConsumer.pendingIdleTimeout + 20*time.Millisecond)
	secondConsumer := *firstConsumer
	secondConsumer.consumerName = "recovery-" + uuid.NewString()

	reclaimed, _, err := secondConsumer.ClaimStale(ctx, "0-0", 1)
	if err != nil {
		t.Fatalf("claim abandoned pending event: %v", err)
	}
	if len(reclaimed) != 1 || reclaimed[0].EventID != entryID {
		t.Fatalf("reclaimed jobs = %#v, want event %s", reclaimed, entryID)
	}

	if err := secondConsumer.Acknowledge(ctx, entryID); err != nil {
		t.Fatalf("acknowledge reclaimed event: %v", err)
	}
	pending, err := redisClient.XPending(ctx, secondConsumer.stream, secondConsumer.group).Result()
	if err != nil {
		t.Fatalf("inspect pending entries: %v", err)
	}
	if pending.Count != 0 {
		t.Fatalf("pending count after recovery ACK = %d, want 0", pending.Count)
	}
}

func TestIntegrationReclaimedPendingDeliveryIsProcessedAndAcknowledged(t *testing.T) {
	db := integrationPostgres(t)
	redisClient, firstConsumer := integrationRedis(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
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
		t.Fatalf("first consumer jobs = %#v, want event %s", jobs, entryID)
	}
	// Simulate the process dying after XREADGROUP persisted the Redis pending
	// entry, but before the application starts SMTP delivery.

	time.Sleep(firstConsumer.pendingIdleTimeout + 20*time.Millisecond)
	secondConsumer := *firstConsumer
	secondConsumer.consumerName = "recovery-" + uuid.NewString()
	reclaimed, _, err := secondConsumer.ClaimStale(ctx, "0-0", 1)
	if err != nil {
		t.Fatalf("recovery consumer claim stale delivery: %v", err)
	}
	if len(reclaimed) != 1 || reclaimed[0].EventID != entryID {
		t.Fatalf("reclaimed jobs = %#v, want event %s", reclaimed, entryID)
	}

	sender := &integrationSender{}
	processor := NewProcessor(NewRepository(db), sender)
	if err := processor.Process(ctx, reclaimed[0]); err != nil {
		t.Fatalf("process reclaimed delivery: %v", err)
	}
	if got := sender.calls.Load(); got != 1 {
		t.Fatalf("SMTP sends after recovery = %d, want exactly 1", got)
	}
	if err := secondConsumer.Acknowledge(ctx, entryID); err != nil {
		t.Fatalf("acknowledge processed recovery event: %v", err)
	}

	var status string
	var attempts int
	if err := db.QueryRow(ctx, `
		SELECT status, attempts FROM recipients WHERE id = $1::uuid
	`, recipientID).Scan(&status, &attempts); err != nil {
		t.Fatalf("read recovered recipient state: %v", err)
	}
	if status != "delivered" || attempts != 1 {
		t.Fatalf("recovered recipient state = %s/%d, want delivered/1", status, attempts)
	}

	pending, err := redisClient.XPending(ctx, secondConsumer.stream, secondConsumer.group).Result()
	if err != nil {
		t.Fatalf("inspect recovered pending entries: %v", err)
	}
	if pending.Count != 0 {
		t.Fatalf("pending entries after recovered delivery = %d, want 0", pending.Count)
	}
}

type integrationSender struct {
	calls   atomic.Int64
	started chan struct{}
	release chan struct{}
	sendErr error
	once    sync.Once
}

func startIntegrationMockSMTP(
	t *testing.T,
	config deliverybench.MockSMTPConfig,
) (*deliverybench.MockSMTPServer, *outbound.Client) {
	t.Helper()

	server, err := deliverybench.StartMockSMTPServer("127.0.0.1:0", config)
	if err != nil {
		t.Fatalf("start integration SMTP receiver: %v", err)
	}
	t.Cleanup(func() {
		if err := server.Close(); err != nil {
			t.Errorf("close integration SMTP receiver: %v", err)
		}
	})

	host, rawPort, err := net.SplitHostPort(server.Addr().String())
	if err != nil {
		t.Fatalf("split integration SMTP receiver address: %v", err)
	}
	port, err := strconv.Atoi(rawPort)
	if err != nil {
		t.Fatalf("parse integration SMTP receiver port: %v", err)
	}

	client, err := outbound.NewRelayClient(outbound.Config{
		RelayHost:        host,
		RelayPort:        port,
		RelayTLSMode:     outbound.TLSModeNone,
		EHLOName:         "mailforge-integration.test",
		ConnectTimeout:   time.Second,
		OperationTimeout: time.Second,
		DeliveryTimeout:  5 * time.Second,
	})
	if err != nil {
		t.Fatalf("create integration SMTP relay client: %v", err)
	}
	return server, client
}

func (s *integrationSender) Send(ctx context.Context, _, _ string, _ []byte) error {
	s.calls.Add(1)
	if s.started != nil {
		s.once.Do(func() { close(s.started) })
	}
	if s.release != nil {
		select {
		case <-s.release:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return s.sendErr
}

func TestIntegrationProcessDSNIsIdempotentAndSuppressesHardBounce(t *testing.T) {
	db := integrationPostgres(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	recipientID, _ := integrationCreateRecipient(t, ctx, db)
	generator, err := bounce.NewGenerator("bounces.example.test", []byte(strings.Repeat("k", 32)))
	if err != nil {
		t.Fatal(err)
	}
	verp, err := generator.AddressForRecipient(recipientID)
	if err != nil {
		t.Fatal(err)
	}
	report := []byte("From: Mail Delivery Subsystem <mailer-daemon@example.net>\r\n" +
		"Message-ID: <dsn-test@example.net>\r\n" +
		"MIME-Version: 1.0\r\n" +
		"Content-Type: multipart/report; report-type=delivery-status; boundary=dsn-test\r\n\r\n" +
		"--dsn-test\r\nContent-Type: text/plain; charset=utf-8\r\n\r\nDelivery failed.\r\n" +
		"--dsn-test\r\nContent-Type: message/delivery-status\r\n\r\n" +
		"Reporting-MTA: dns; mx.example.net\r\n\r\n" +
		"Final-Recipient: rfc822; recipient@example.test\r\n" +
		"Action: failed\r\nStatus: 5.1.1\r\n" +
		"Diagnostic-Code: smtp; 550 5.1.1 user unknown\r\n\r\n" +
		"--dsn-test--\r\n")

	repository := NewRepository(db)
	first, err := repository.ProcessDSN(ctx, generator, verp, report)
	if err != nil {
		t.Fatalf("ProcessDSN: %v", err)
	}
	if first.Duplicate || !first.Suppressed || first.Action != "failed" {
		t.Fatalf("first DSN result = %#v", first)
	}

	duplicate, err := repository.ProcessDSN(ctx, generator, verp, report)
	if err != nil {
		t.Fatalf("ProcessDSN duplicate: %v", err)
	}
	if !duplicate.Duplicate {
		t.Fatalf("duplicate result = %#v", duplicate)
	}

	// The same raw DSN body can legitimately be received for a second
	// send to the same mailbox. Idempotency must be scoped to recipient ID,
	// not globally to the raw report hash.
	secondRecipientID, _ := integrationCreateRecipient(t, ctx, db)
	secondVERP, err := generator.AddressForRecipient(secondRecipientID)
	if err != nil {
		t.Fatal(err)
	}
	secondDelivery, err := repository.ProcessDSN(ctx, generator, secondVERP, report)
	if err != nil {
		t.Fatalf("ProcessDSN for second delivery with identical report bytes: %v", err)
	}
	if secondDelivery.Duplicate || !secondDelivery.Suppressed {
		t.Fatalf("second delivery result = %#v, want distinct processed report", secondDelivery)
	}
	var processedReports int
	if err := db.QueryRow(ctx, `SELECT COUNT(*) FROM processed_dsn_reports WHERE final_recipient = 'recipient@example.test'`).Scan(&processedReports); err != nil {
		t.Fatal(err)
	}
	if processedReports != 2 {
		t.Fatalf("processed DSN report rows = %d, want 2 per-recipient rows", processedReports)
	}

	var status string
	if err := db.QueryRow(ctx, `SELECT status FROM recipients WHERE id = $1::uuid`, recipientID).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != "bounced" {
		t.Fatalf("recipient status = %s, want bounced", status)
	}
	var suppressions int
	if err := db.QueryRow(ctx, `SELECT COUNT(*) FROM recipient_suppressions WHERE mailbox_key = 'recipient@example.test'`).Scan(&suppressions); err != nil {
		t.Fatal(err)
	}
	if suppressions != 1 {
		t.Fatalf("suppression rows = %d, want 1", suppressions)
	}

	// Any message submitted after the hard bounce is blocked at claim time,
	// even if it already has an outbox event.
	var messageUUID string
	if err := db.QueryRow(ctx, `
		INSERT INTO messages (message_id, envelope_sender, raw_message, message_size)
		VALUES ($1, 'sender@example.test', $2, 40) RETURNING id::text
	`, fmt.Sprintf("<%s@example.test>", uuid.NewString()), []byte("From: sender@example.test\r\n\r\nHi")).Scan(&messageUUID); err != nil {
		t.Fatal(err)
	}
	var blockedID string
	if err := db.QueryRow(ctx, `
		INSERT INTO recipients (message_id, email, status, next_attempt_at)
		VALUES ($1::uuid, 'recipient@example.test', 'pending', NOW()) RETURNING id::text
	`, messageUUID).Scan(&blockedID); err != nil {
		t.Fatal(err)
	}
	job, err := repository.ClaimRecipient(ctx, blockedID)
	if err != nil {
		t.Fatalf("ClaimRecipient for suppressed address: %v", err)
	}
	if job != nil {
		t.Fatal("suppressed recipient was claimed for SMTP delivery")
	}
	if err := db.QueryRow(ctx, `SELECT status FROM recipients WHERE id = $1::uuid`, blockedID).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != "suppressed" {
		t.Fatalf("new recipient status = %s, want suppressed", status)
	}
}

func integrationCreateRecipient(
	t *testing.T,
	ctx context.Context,
	db *pgxpool.Pool,
) (recipientID string, messageID string) {
	t.Helper()
	var messageUUID string
	if err := db.QueryRow(ctx, `
		INSERT INTO messages (message_id, envelope_sender, raw_message, message_size)
		VALUES ($1, 'sender@example.test', $2, $3)
		RETURNING id::text
	`, fmt.Sprintf("<%s@example.test>", uuid.NewString()), []byte("From: sender@example.test\r\n\r\nHello"), 45).Scan(&messageUUID); err != nil {
		t.Fatalf("insert test message: %v", err)
	}
	if err := db.QueryRow(ctx, `
		INSERT INTO recipients (message_id, email, status, next_attempt_at)
		VALUES ($1::uuid, 'recipient@example.test', 'pending', NOW())
		RETURNING id::text
	`, messageUUID).Scan(&recipientID); err != nil {
		t.Fatalf("insert test recipient: %v", err)
	}
	return recipientID, messageUUID
}

func integrationPostgres(t *testing.T) *pgxpool.Pool {
	t.Helper()
	dsn := strings.TrimSpace(os.Getenv("MAILFORGE_TEST_DATABASE_URL"))
	if dsn == "" {
		t.Skip("set MAILFORGE_TEST_DATABASE_URL to run PostgreSQL integration tests")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	admin, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("connect integration PostgreSQL admin pool: %v", err)
	}
	schema := "mailforge_test_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	if _, err := admin.Exec(ctx, "CREATE SCHEMA "+schema); err != nil {
		admin.Close()
		t.Fatalf("create isolated PostgreSQL schema: %v", err)
	}

	config, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		_, _ = admin.Exec(ctx, "DROP SCHEMA "+schema+" CASCADE")
		admin.Close()
		t.Fatalf("parse integration PostgreSQL config: %v", err)
	}
	if config.ConnConfig.RuntimeParams == nil {
		config.ConnConfig.RuntimeParams = make(map[string]string)
	}
	config.ConnConfig.RuntimeParams["search_path"] = schema
	config.MaxConns = 4

	db, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		_, _ = admin.Exec(ctx, "DROP SCHEMA "+schema+" CASCADE")
		admin.Close()
		t.Fatalf("connect isolated PostgreSQL pool: %v", err)
	}
	if err := db.Ping(ctx); err != nil {
		db.Close()
		_, _ = admin.Exec(ctx, "DROP SCHEMA "+schema+" CASCADE")
		admin.Close()
		t.Fatalf("ping isolated PostgreSQL pool: %v", err)
	}

	t.Cleanup(func() {
		db.Close()
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanupCancel()
		if _, err := admin.Exec(cleanupCtx, "DROP SCHEMA "+schema+" CASCADE"); err != nil {
			t.Errorf("drop integration schema %s: %v", schema, err)
		}
		admin.Close()
	})

	migrationFiles, err := filepath.Glob(filepath.Join("..", "..", "migrations", "*.sql"))
	if err != nil {
		t.Fatalf("list migrations: %v", err)
	}
	sort.Strings(migrationFiles)
	for _, filename := range migrationFiles {
		contents, err := os.ReadFile(filename)
		if err != nil {
			t.Fatalf("read migration %s: %v", filename, err)
		}
		parts := strings.SplitN(string(contents), "-- +goose Up", 2)
		if len(parts) != 2 {
			t.Fatalf("migration %s has no Goose Up section", filename)
		}
		upSQL := strings.SplitN(parts[1], "-- +goose Down", 2)[0]
		if _, err := db.Exec(ctx, upSQL); err != nil {
			t.Fatalf("apply migration %s: %v", filepath.Base(filename), err)
		}
	}

	return db
}

func integrationRedis(t *testing.T) (*redis.Client, *Consumer) {
	t.Helper()
	addr := strings.TrimSpace(os.Getenv("MAILFORGE_TEST_REDIS_ADDR"))
	if addr == "" {
		t.Skip("set MAILFORGE_TEST_REDIS_ADDR to run Redis integration tests")
	}

	dbNumber := 0
	if raw := strings.TrimSpace(os.Getenv("MAILFORGE_TEST_REDIS_DB")); raw != "" {
		if _, err := fmt.Sscanf(raw, "%d", &dbNumber); err != nil || dbNumber < 0 {
			t.Fatalf("invalid MAILFORGE_TEST_REDIS_DB %q", raw)
		}
	}

	client := redis.NewClient(&redis.Options{
		Addr:     addr,
		Password: os.Getenv("MAILFORGE_TEST_REDIS_PASSWORD"),
		DB:       dbNumber,
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := client.Ping(ctx).Err(); err != nil {
		client.Close()
		t.Fatalf("connect integration Redis: %v", err)
	}

	suffix := strings.ReplaceAll(uuid.NewString(), "-", "")
	consumer := NewConsumer(client, "test-"+suffix)
	consumer.stream = "mailforge:test:delivery:" + suffix
	consumer.deadLetterStream = "mailforge:test:delivery:dead:" + suffix
	consumer.group = "mailforge-test-workers-" + suffix
	consumer.pendingIdleTimeout = 50 * time.Millisecond
	if err := consumer.CreateGroup(ctx); err != nil {
		client.Close()
		t.Fatalf("create integration consumer group: %v", err)
	}

	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		if err := client.Del(cleanupCtx, consumer.stream, consumer.deadLetterStream).Err(); err != nil {
			t.Errorf("delete integration Redis streams: %v", err)
		}
		if err := client.Close(); err != nil {
			t.Errorf("close integration Redis client: %v", err)
		}
	})
	return client, consumer
}

func TestIntegrationReconcileUnknownRecipientCreatesAuditAndOutbox(t *testing.T) {
	db := integrationPostgres(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	recipientID, _ := integrationCreateRecipient(t, ctx, db)
	if _, err := db.Exec(ctx, `UPDATE recipients SET status = 'unknown', attempts = 1, unknown_at = NOW() WHERE id = $1::uuid`, recipientID); err != nil {
		t.Fatalf("prepare unknown recipient: %v", err)
	}

	err := NewRepository(db).ReconcileUnknownRecipient(ctx, UnknownReconciliation{
		RecipientID: recipientID,
		Decision:    "retry",
		Actor:       "oncall@example.test",
		Reason:      "Remote MTA logs confirm the connection ended before DATA acceptance",
		Evidence:    "INC-1234 / mx-relay-02 log window 12:00-12:02 UTC",
	})
	if err != nil {
		t.Fatalf("reconcile unknown recipient: %v", err)
	}

	var status, decision, actor, databaseRole, reason string
	var outboxCount int
	if err := db.QueryRow(ctx, `SELECT status FROM recipients WHERE id = $1::uuid`, recipientID).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(ctx, `SELECT decision, actor, database_role, reason FROM delivery_reconciliations WHERE recipient_id = $1::uuid`, recipientID).Scan(&decision, &actor, &databaseRole, &reason); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(ctx, `SELECT COUNT(*) FROM outbox_events WHERE aggregate_id = $1::uuid AND idempotency_key = 'recipient.delivery_requested:' || $1::text || ':2'`, recipientID).Scan(&outboxCount); err != nil {
		t.Fatal(err)
	}
	if status != "deferred" || decision != "retry" || actor != "oncall@example.test" || databaseRole == "" || !strings.Contains(reason, "Remote MTA logs") || outboxCount != 1 {
		t.Fatalf("reconciliation result status=%q decision=%q actor=%q database_role=%q reason=%q outbox=%d", status, decision, actor, databaseRole, reason, outboxCount)
	}

	if err := NewRepository(db).ReconcileUnknownRecipient(ctx, UnknownReconciliation{
		RecipientID: recipientID, Decision: "retry", Actor: "oncall@example.test", Reason: "duplicate decision",
	}); !errors.Is(err, ErrRecipientNotUnknown) {
		t.Fatalf("second reconciliation error = %v, want ErrRecipientNotUnknown", err)
	}
}
