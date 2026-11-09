package message

import "testing"

func TestDefaultSubmissionQuotaLimitsArePositive(t *testing.T) {
	limits := DefaultSubmissionQuotaLimits()
	if limits.MaxMessagesPerAccountPerHour <= 0 || limits.MaxMessagesPerIPPerHour <= 0 ||
		limits.MaxRecipientsPerAccountPerHour <= 0 || limits.MaxRecipientsPerIPPerHour <= 0 {
		t.Fatalf("submission quotas must be positive: %+v", limits)
	}
}

func TestQuotaIdentityDoesNotExposeInput(t *testing.T) {
	input := "sender@example.com"
	got := quotaIdentity(input)
	if got == input || len(got) != 64 {
		t.Fatalf("quotaIdentity(%q) = %q, want a 64-character SHA-256 hex digest", input, got)
	}
	if got != quotaIdentity(input) {
		t.Fatal("quota identity must be deterministic")
	}
}

func TestSubmissionQuotaAccountUsesCredentialIDNotEnvelopeSender(t *testing.T) {
	first := StoredMessage{
		Sender:              "billing@verified.example.org",
		SubmissionAccountID: "00000000-0000-4000-8000-00000000000A",
	}
	second := StoredMessage{
		Sender:              "notices@verified.example.org",
		SubmissionAccountID: "00000000-0000-4000-8000-00000000000a",
	}

	firstAccount := submissionQuotaAccountID(first)
	secondAccount := submissionQuotaAccountID(second)
	if firstAccount != "00000000-0000-4000-8000-00000000000a" {
		t.Fatalf("submission quota account = %q, want normalized authenticated credential UUID", firstAccount)
	}
	if firstAccount != secondAccount {
		t.Fatalf("different envelope aliases produced different quota accounts: %q != %q", firstAccount, secondAccount)
	}
}

func TestUnauthenticatedMessageHasNoAccountQuotaIdentity(t *testing.T) {
	got := submissionQuotaAccountID(StoredMessage{Sender: "remote-sender@example.net"})
	if got != "" {
		t.Fatalf("unauthenticated submission quota account = %q, want empty", got)
	}
}
