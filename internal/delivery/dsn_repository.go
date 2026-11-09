package delivery

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"

	"mailforge/internal/bounce"
	"mailforge/internal/dsn"
	"mailforge/internal/emailaddr"
)

var (
	ErrDSNRecipientMismatch = errors.New("DSN recipient does not match the VERP recipient")
	ErrDSNRecipientMissing  = errors.New("DSN recipient does not exist")
)

type DSNProcessResult struct {
	RecipientID string
	Action      string
	Status      string
	Duplicate   bool
	Suppressed  bool
}

// ProcessDSN validates a per-recipient VERP return-path, parses its DSN, and
// applies the status update and suppression decision in one PostgreSQL
// transaction. It is safe to call repeatedly with the same report.
func (r *Repository) ProcessDSN(
	ctx context.Context,
	generator *bounce.Generator,
	envelopeRecipient string,
	rawReport []byte,
) (DSNProcessResult, error) {
	if generator == nil {
		return DSNProcessResult{}, errors.New("bounce address generator is required")
	}
	report, err := dsn.Parse(rawReport)
	if err != nil {
		return DSNProcessResult{}, err
	}
	recipientID, err := generator.RecipientID(envelopeRecipient)
	if err != nil {
		return DSNProcessResult{}, fmt.Errorf("verify DSN VERP address: %w", err)
	}
	finalMailbox, err := emailaddr.ParseMailbox(report.FinalRecipient)
	if err != nil {
		return DSNProcessResult{}, fmt.Errorf("invalid DSN Final-Recipient: %w", err)
	}
	mailboxKey := finalMailbox.Local + "@" + strings.ToLower(finalMailbox.Domain)
	reportHash := sha256.Sum256(rawReport)
	reportHashHex := hex.EncodeToString(reportHash[:])

	tx, err := r.db.Begin(ctx)
	if err != nil {
		return DSNProcessResult{}, fmt.Errorf("begin DSN transaction: %w", err)
	}
	defer tx.Rollback(context.Background())

	var email, currentStatus string
	err = tx.QueryRow(ctx, `
		SELECT email, status
		FROM recipients
		WHERE id = $1::uuid
		FOR UPDATE
	`, recipientID).Scan(&email, &currentStatus)
	if errors.Is(err, pgx.ErrNoRows) {
		return DSNProcessResult{}, ErrDSNRecipientMissing
	}
	if err != nil {
		return DSNProcessResult{}, fmt.Errorf("load DSN recipient: %w", err)
	}
	if !emailaddr.Equal(email, report.FinalRecipient) {
		return DSNProcessResult{}, fmt.Errorf("%w: expected %q, report says %q", ErrDSNRecipientMismatch, email, report.FinalRecipient)
	}

	var reportID string
	err = tx.QueryRow(ctx, `
		INSERT INTO processed_dsn_reports (
			recipient_id, report_sha256, message_id, final_recipient,
			action, enhanced_status_code, diagnostic, reporting_mta,
			remote_mta, original_envelope_id
		)
		VALUES ($1::uuid, $2, NULLIF($3, ''), $4, $5, $6, NULLIF($7, ''), NULLIF($8, ''), NULLIF($9, ''), NULLIF($10, ''))
		ON CONFLICT (recipient_id, report_sha256) DO NOTHING
		RETURNING id::text
	`, recipientID, reportHashHex, report.MessageID, report.FinalRecipient, report.Action, report.Status,
		report.DiagnosticCode, report.ReportingMTA, report.RemoteMTA, report.OriginalEnvelopeID).Scan(&reportID)
	if errors.Is(err, pgx.ErrNoRows) {
		if err := tx.Commit(ctx); err != nil {
			return DSNProcessResult{}, fmt.Errorf("commit duplicate DSN: %w", err)
		}
		return DSNProcessResult{RecipientID: recipientID, Action: report.Action, Status: report.Status, Duplicate: true}, nil
	}
	if err != nil {
		return DSNProcessResult{}, fmt.Errorf("persist processed DSN report: %w", err)
	}

	result := DSNProcessResult{RecipientID: recipientID, Action: report.Action, Status: report.Status}
	nextStatus := currentStatus
	eventType := "dsn_" + report.Action
	switch report.Action {
	case "failed":
		if currentStatus != "suppressed" {
			nextStatus = "bounced"
			_, err = tx.Exec(ctx, `
				UPDATE recipients
				SET status = 'bounced', bounced_at = COALESCE(bounced_at, NOW()),
				    next_attempt_at = NULL, delivery_lease_until = NULL,
				    delivery_lease_token = NULL, last_enhanced_status_code = $2,
				    last_error = NULLIF($3, ''), updated_at = NOW()
				WHERE id = $1::uuid
			`, recipientID, report.Status, truncateDiagnostic(report.DiagnosticCode))
			if err != nil {
				return DSNProcessResult{}, fmt.Errorf("apply DSN failure to recipient: %w", err)
			}
		}
		if isHardBounceStatus(report.Status) {
			_, err = tx.Exec(ctx, `
				INSERT INTO recipient_suppressions (
					email, mailbox_key, reason, source, source_dsn_report_id, diagnostic
				)
				VALUES ($1, $2, 'hard_bounce', 'dsn', $3::uuid, NULLIF($4, ''))
				ON CONFLICT (mailbox_key) DO UPDATE
				SET reason = EXCLUDED.reason,
				    source = EXCLUDED.source,
				    source_dsn_report_id = EXCLUDED.source_dsn_report_id,
				    diagnostic = EXCLUDED.diagnostic,
				    updated_at = NOW()
			`, email, mailboxKey, reportID, truncateDiagnostic(report.DiagnosticCode))
			if err != nil {
				return DSNProcessResult{}, fmt.Errorf("suppress hard-bounced recipient: %w", err)
			}
			result.Suppressed = true
		}
	case "delivered":
		_, err = tx.Exec(ctx, `
			UPDATE recipients
			SET final_delivery_confirmed_at = COALESCE(final_delivery_confirmed_at, NOW()), updated_at = NOW()
			WHERE id = $1::uuid
		`, recipientID)
		if err != nil {
			return DSNProcessResult{}, fmt.Errorf("record final delivery confirmation: %w", err)
		}
	case "delayed", "relayed", "expanded":
		// These DSN actions are useful audit information, but do not create a
		// second send or turn next-hop acceptance into a local retry.
	}

	_, err = tx.Exec(ctx, `
		INSERT INTO delivery_status_events (
			recipient_id, from_status, to_status, event_type,
			enhanced_status_code, diagnostic, remote_host
		)
		VALUES ($1::uuid, $2, $3, $4, $5, NULLIF($6, ''), NULLIF($7, ''))
	`, recipientID, currentStatus, nextStatus, eventType, report.Status, truncateDiagnostic(report.DiagnosticCode), report.RemoteMTA)
	if err != nil {
		return DSNProcessResult{}, fmt.Errorf("record DSN status event: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return DSNProcessResult{}, fmt.Errorf("commit DSN status update: %w", err)
	}
	return result, nil
}

func isHardBounceStatus(status string) bool {
	return status == "5.1.1" || status == "5.1.3"
}

func truncateDiagnostic(value string) string {
	if len(value) > 2000 {
		return value[:2000]
	}
	return value
}

// SuppressionKey is exported for tests and administration tooling. It keeps
// the local-part case-sensitive and normalizes only the DNS domain.
func SuppressionKey(address string) (string, error) {
	mailbox, err := emailaddr.ParseMailbox(address)
	if err != nil {
		return "", err
	}
	return mailbox.Local + "@" + strings.ToLower(mailbox.Domain), nil
}
