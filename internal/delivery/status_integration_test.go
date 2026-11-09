package delivery

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestIntegrationDeliveryStatusQueries(t *testing.T) {
	db := integrationPostgres(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	rfcMessageID := fmt.Sprintf("<%s@example.test>", uuid.NewString())
	var messageUUID string
	if err := db.QueryRow(ctx, `
		INSERT INTO messages (message_id, envelope_sender, raw_message, message_size)
		VALUES ($1, 'sender@example.test', $2, 40)
		RETURNING id::text
	`, rfcMessageID, []byte("From: sender@example.test\r\n\r\nHello")).Scan(&messageUUID); err != nil {
		t.Fatalf("insert message: %v", err)
	}

	var recipientID string
	if err := db.QueryRow(ctx, `
		INSERT INTO recipients (
			message_id, email, status, attempts, next_attempt_at,
			last_attempt_at, last_smtp_code, last_enhanced_status_code,
			last_smtp_stage, last_error
		)
		VALUES (
			$1::uuid, 'recipient@example.test', 'deferred', 3, NOW() + INTERVAL '5 minutes',
			NOW(), 451, '4.7.1', 'DATA', 'temporary remote failure'
		)
		RETURNING id::text
	`, messageUUID).Scan(&recipientID); err != nil {
		t.Fatalf("insert recipient: %v", err)
	}
	if _, err := db.Exec(ctx, `
		INSERT INTO delivery_status_events (
			recipient_id, from_status, to_status, event_type,
			smtp_code, enhanced_status_code, smtp_stage, diagnostic
		)
		VALUES ($1::uuid, 'delivering', 'deferred', 'smtp_temporary_failure', 451, '4.7.1', 'DATA', 'temporary remote failure')
	`, recipientID); err != nil {
		t.Fatalf("insert delivery event: %v", err)
	}

	repository := NewRepository(db)
	resolvedFromUUID, err := repository.ResolveMessageID(ctx, messageUUID)
	if err != nil {
		t.Fatalf("resolve internal UUID: %v", err)
	}
	resolvedFromRFCID, err := repository.ResolveMessageID(ctx, rfcMessageID)
	if err != nil {
		t.Fatalf("resolve RFC Message-ID: %v", err)
	}
	if resolvedFromUUID != messageUUID || resolvedFromRFCID != messageUUID {
		t.Fatalf("resolved UUIDs = %q and %q, want %q", resolvedFromUUID, resolvedFromRFCID, messageUUID)
	}

	summary, err := repository.GetMessageDeliverySummary(ctx, messageUUID)
	if err != nil {
		t.Fatalf("get message summary: %v", err)
	}
	if summary.Total != 1 || summary.Deferred != 1 {
		t.Fatalf("summary = %+v, want one deferred recipient", summary)
	}

	recipients, err := repository.ListMessageRecipientDeliveryStatuses(ctx, messageUUID)
	if err != nil {
		t.Fatalf("list message recipients: %v", err)
	}
	if len(recipients) != 1 {
		t.Fatalf("recipient count = %d, want 1", len(recipients))
	}
	if recipients[0].RecipientID != recipientID || recipients[0].Status != "deferred" || recipients[0].Attempts != 3 {
		t.Fatalf("recipient status = %+v", recipients[0])
	}
	if recipients[0].LastSMTPCode == nil || *recipients[0].LastSMTPCode != 451 {
		t.Fatalf("last SMTP code = %v, want 451", recipients[0].LastSMTPCode)
	}
	if recipients[0].LastError != "temporary remote failure" {
		t.Fatalf("last error = %q", recipients[0].LastError)
	}

	events, err := repository.ListRecipientDeliveryEvents(ctx, recipientID, 10)
	if err != nil {
		t.Fatalf("list recipient delivery events: %v", err)
	}
	if len(events) != 1 || events[0].ToStatus != "deferred" || events[0].SMTPCode == nil || *events[0].SMTPCode != 451 {
		t.Fatalf("delivery events = %+v, want one deferred event with SMTP 451", events)
	}

	if _, err := repository.ResolveMessageID(ctx, "<missing@example.test>"); !errors.Is(err, ErrMessageNotFound) {
		t.Fatalf("missing Message-ID error = %v, want ErrMessageNotFound", err)
	}
	if _, err := repository.ResolveMessageID(ctx, uuid.NewString()); !errors.Is(err, ErrMessageNotFound) {
		t.Fatalf("missing UUID error = %v, want ErrMessageNotFound", err)
	}
}
