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
