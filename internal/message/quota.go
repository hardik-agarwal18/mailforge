package message

import "strings"

// SubmissionQuotaLimits are enforced transactionally by the PostgreSQL
// repository, so all SMTP submission replicas share the same hourly quotas.
type SubmissionQuotaLimits struct {
	MaxMessagesPerAccountPerHour   int
	MaxMessagesPerIPPerHour        int
	MaxRecipientsPerAccountPerHour int
	MaxRecipientsPerIPPerHour      int
}

func DefaultSubmissionQuotaLimits() SubmissionQuotaLimits {
	return SubmissionQuotaLimits{
		MaxMessagesPerAccountPerHour:   1000,
		MaxMessagesPerIPPerHour:        5000,
		MaxRecipientsPerAccountPerHour: 10000,
		MaxRecipientsPerIPPerHour:      50000,
	}
}

func normalizeSubmissionQuotaLimits(limits SubmissionQuotaLimits) SubmissionQuotaLimits {
	defaults := DefaultSubmissionQuotaLimits()
	if limits.MaxMessagesPerAccountPerHour <= 0 {
		limits.MaxMessagesPerAccountPerHour = defaults.MaxMessagesPerAccountPerHour
	}
	if limits.MaxMessagesPerIPPerHour <= 0 {
		limits.MaxMessagesPerIPPerHour = defaults.MaxMessagesPerIPPerHour
	}
	if limits.MaxRecipientsPerAccountPerHour <= 0 {
		limits.MaxRecipientsPerAccountPerHour = defaults.MaxRecipientsPerAccountPerHour
	}
	if limits.MaxRecipientsPerIPPerHour <= 0 {
		limits.MaxRecipientsPerIPPerHour = defaults.MaxRecipientsPerIPPerHour
	}
	return limits
}

// submissionQuotaAccountID returns the canonical SMTP credential UUID that owns
// a submission quota. Envelope senders, usernames and email addresses are not
// stable account keys. An empty result means the message was not submitted
// through authenticated SMTP and must not consume an account quota.
func submissionQuotaAccountID(msg StoredMessage) string {
	return strings.ToLower(strings.TrimSpace(msg.SubmissionAccountID))
}
