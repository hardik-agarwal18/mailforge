package dsn

import "testing"

const validReport = "From: Mail Delivery Subsystem <mailer-daemon@example.net>\r\n" +
	"Message-ID: <dsn-1@example.net>\r\n" +
	"MIME-Version: 1.0\r\n" +
	"Content-Type: multipart/report; report-type=delivery-status; boundary=dsn-boundary\r\n" +
	"\r\n" +
	"--dsn-boundary\r\n" +
	"Content-Type: text/plain; charset=utf-8\r\n\r\nDelivery failed.\r\n" +
	"--dsn-boundary\r\n" +
	"Content-Type: message/delivery-status\r\n\r\n" +
	"Reporting-MTA: dns; mx.example.net\r\n" +
	"\r\n" +
	"Final-Recipient: rfc822; alice@example.com\r\n" +
	"Action: failed\r\n" +
	"Status: 5.1.1\r\n" +
	"Remote-MTA: dns; mx.example.com\r\n" +
	"Diagnostic-Code: smtp; 550 5.1.1 user unknown\r\n" +
	"\r\n--dsn-boundary--\r\n"

func TestParseFailedDSN(t *testing.T) {
	report, err := Parse([]byte(validReport))
	if err != nil {
		t.Fatal(err)
	}
	if report.Action != "failed" || report.Status != "5.1.1" {
		t.Fatalf("report action/status = %s/%s", report.Action, report.Status)
	}
	if report.FinalRecipient != "alice@example.com" {
		t.Fatalf("recipient = %q", report.FinalRecipient)
	}
	if report.DiagnosticCode != "550 5.1.1 user unknown" {
		t.Fatalf("diagnostic = %q", report.DiagnosticCode)
	}
	if report.ReportingMTA != "dns; mx.example.net" {
		t.Fatalf("reporting MTA = %q", report.ReportingMTA)
	}
}

func TestParseRejectsNonDSNAndMultipleRecipientBlocks(t *testing.T) {
	badType := []byte("From: x@example.com\r\nContent-Type: text/plain\r\n\r\nno")
	if _, err := Parse(badType); err == nil {
		t.Fatal("non-DSN accepted")
	}
	multiple := []byte(validReport[:len(validReport)-len("\r\n--dsn-boundary--\r\n")] +
		"\r\nFinal-Recipient: rfc822; bob@example.com\r\nAction: failed\r\nStatus: 5.1.1\r\n\r\n--dsn-boundary--\r\n")
	if _, err := Parse(multiple); err == nil {
		t.Fatal("multi-recipient DSN accepted")
	}
}

func TestParseRejectsActionStatusMismatch(t *testing.T) {
	text := validReport
	text = replaceOnce(text, "Action: failed", "Action: delayed")
	if _, err := Parse([]byte(text)); err == nil {
		t.Fatal("mismatched action/status accepted")
	}
}

func replaceOnce(value, old, next string) string {
	for i := 0; i+len(old) <= len(value); i++ {
		if value[i:i+len(old)] == old {
			return value[:i] + next + value[i+len(old):]
		}
	}
	return value
}

func TestParseAcceptsQuotedFinalRecipient(t *testing.T) {
	quoted := replaceOnce(validReport, "alice@example.com", `"alice smith"@example.com`)
	report, err := Parse([]byte(quoted))
	if err != nil {
		t.Fatalf("Parse() rejected valid quoted mailbox: %v", err)
	}
	if report.FinalRecipient != `"alice smith"@example.com` {
		t.Fatalf("FinalRecipient = %q", report.FinalRecipient)
	}
}
