package main

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"time"

	"mailforge/internal/delivery"
)

func TestOverallStatus(t *testing.T) {
	tests := []struct {
		name    string
		summary delivery.MessageDeliverySummary
		want    string
	}{
		{
			name:    "all delivered",
			summary: delivery.MessageDeliverySummary{Total: 2, Delivered: 2},
			want:    "DELIVERED",
		},
		{
			name:    "partially delivered",
			summary: delivery.MessageDeliverySummary{Total: 2, Delivered: 1, Deferred: 1},
			want:    "PARTIALLY DELIVERED",
		},
		{
			name:    "actively delivering",
			summary: delivery.MessageDeliverySummary{Total: 2, Delivering: 1, Pending: 1},
			want:    "DELIVERING",
		},
		{
			name:    "pending",
			summary: delivery.MessageDeliverySummary{Total: 1, Pending: 1},
			want:    "PENDING",
		},
		{
			name:    "deferred",
			summary: delivery.MessageDeliverySummary{Total: 1, Deferred: 1},
			want:    "DEFERRED",
		},
		{
			name:    "unknown",
			summary: delivery.MessageDeliverySummary{Total: 1, Unknown: 1},
			want:    "UNKNOWN — INVESTIGATION REQUIRED",
		},
		{
			name:    "terminal failures",
			summary: delivery.MessageDeliverySummary{Total: 2, Bounced: 1, Suppressed: 1},
			want:    "FAILED",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := overallStatus(&test.summary); got != test.want {
				t.Fatalf("overallStatus() = %q, want %q", got, test.want)
			}
		})
	}
}

func TestWriteReportIncludesRecipientDiagnosticsAndHistory(t *testing.T) {
	now := time.Date(2026, time.November, 9, 12, 0, 0, 0, time.FixedZone("IST", 5*60*60+30*60))
	smtpCode := 451
	summary := &delivery.MessageDeliverySummary{
		MessageID: "internal-message-uuid",
		Total:     1,
		Deferred:  1,
	}
	recipients := []recipientReport{{
		Status: delivery.RecipientDeliveryStatus{
			RecipientID:        "recipient-uuid",
			MessageID:          "internal-message-uuid",
			Email:              "person@example.test",
			Status:             "deferred",
			Attempts:           3,
			LastSMTPCode:       &smtpCode,
			LastEnhancedStatus: "4.7.1",
			LastSMTPStage:      "DATA",
			LastError:          "temporary failure\nplease retry",
		},
		HistoryLoaded: true,
		Events: []delivery.DeliveryStatusEvent{{
			FromStatus:         "delivering",
			ToStatus:           "deferred",
			EventType:          "smtp_temporary_failure",
			SMTPCode:           &smtpCode,
			EnhancedStatusCode: "4.7.1",
			SMTPStage:          "DATA",
			Diagnostic:         "temporary remote failure",
			CreatedAt:          now,
		}},
	}}

	var output bytes.Buffer
	if err := writeReport(&output, "<message@example.test>", summary, recipients, now); err != nil {
		t.Fatalf("writeReport() error = %v", err)
	}

	for _, want := range []string{
		"MAILFORGE DELIVERY REPORT",
		"<message@example.test>",
		"DEFERRED",
		"person@example.test",
		"451 (4.7.1)",
		"3",
		"temporary failure please retry",
		"smtp_temporary_failure",
		"temporary remote failure",
	} {
		if !strings.Contains(output.String(), want) {
			t.Errorf("report does not contain %q:\n%s", want, output.String())
		}
	}
}

func TestOneLineCollapsesWhitespaceAndTruncates(t *testing.T) {
	got := oneLine("first line\nsecond line", 30)
	if got != "first line second line" {
		t.Fatalf("oneLine() = %q, want collapsed whitespace", got)
	}

	got = oneLine(strings.Repeat("x", 20), 8)
	if got != "xxxxx..." {
		t.Fatalf("oneLine() truncated = %q, want %q", got, "xxxxx...")
	}
}

func TestRunValidatesArgumentsBeforeConnectingToDatabase(t *testing.T) {
	getenv := func(string) string { return "" }
	var stdout, stderr bytes.Buffer

	err := run(context.Background(), []string{"--events", "1001", "message-id"}, &stdout, &stderr, getenv)
	if err == nil || !strings.Contains(err.Error(), "between 0 and 1000") {
		t.Fatalf("invalid event limit error = %v, want range validation error", err)
	}

	err = run(context.Background(), []string{"message-id"}, &stdout, &stderr, getenv)
	if err == nil || !strings.Contains(err.Error(), "DATABASE_URL is required") {
		t.Fatalf("missing DATABASE_URL error = %v, want required-env error", err)
	}

	err = run(context.Background(), []string{"--help"}, &stdout, &stderr, getenv)
	if err != nil {
		t.Fatalf("help command error = %v, want nil", err)
	}
}
