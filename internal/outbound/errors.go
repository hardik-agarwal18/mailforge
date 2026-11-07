package outbound

import (
	"errors"
	"net"
	"net/textproto"
	"regexp"
	"strings"
)

// DeliveryError is a classified failure for one envelope recipient.
// SMTPCode is the three-digit SMTP reply. EnhancedStatusCode is populated
// only when a syntactically valid RFC 3463 status class agrees with SMTPCode.
// Stage identifies the command/phase at which the failure occurred.
type DeliveryError struct {
	Message            string
	Permanent          bool
	SMTPCode           int
	EnhancedStatusCode string
	Stage              string
}

func (e *DeliveryError) Error() string {
	return e.Message
}

var enhancedStatusPattern = regexp.MustCompile(`(?:^|[^0-9A-Za-z.])([245]\.[0-9]{1,3}\.[0-9]{1,3})(?:$|[^0-9A-Za-z.])`)

// classifyError preserves the historical helper API for callers which do not
// know the SMTP stage. SMTP reply severity is determined from the first digit
// of the three-digit reply, not from the server's free-form text.
func classifyError(err error) error {
	return classifyErrorAtStage("", err)
}

func classifyErrorAtStage(stage string, err error) error {
	if err == nil {
		return nil
	}

	var smtpErr *textproto.Error
	if errors.As(err, &smtpErr) {
		return &DeliveryError{
			Message:            err.Error(),
			SMTPCode:           smtpErr.Code,
			EnhancedStatusCode: parseEnhancedStatusCode(smtpErr.Msg, smtpErr.Code),
			Stage:              stage,
			Permanent:          smtpReplyIsPermanent(smtpErr.Code),
		}
	}

	// A definitive DNS negative response (NXDOMAIN/NODATA) is permanent;
	// resolver timeouts and temporary DNS failures remain retryable.
	var dnsErr *net.DNSError
	if errors.As(err, &dnsErr) && dnsErr.IsNotFound {
		return &DeliveryError{
			Message:   err.Error(),
			Stage:     stage,
			Permanent: true,
		}
	}

	// Network, temporary DNS, TLS, and unexpected protocol failures are
	// retryable. SMTP servers can recover without changing the message.
	return &DeliveryError{
		Message:   err.Error(),
		Stage:     stage,
		Permanent: false,
	}
}

func smtpReplyIsPermanent(code int) bool {
	return code >= 500 && code <= 599
}

func parseEnhancedStatusCode(response string, smtpCode int) string {
	match := enhancedStatusPattern.FindStringSubmatch(strings.TrimSpace(response))
	if len(match) != 2 {
		return ""
	}

	// RFC 3463 status classes are 2, 4 and 5. The class must agree with the
	// basic SMTP reply; a contradictory/malformed enhanced code is ignored.
	if match[1][0] != byte('0'+smtpCode/100) {
		return ""
	}

	// RFC 3463 forbids leading zeroes in the subject and detail sub-codes.
	parts := strings.Split(match[1], ".")
	for _, part := range parts[1:] {
		if len(part) > 1 && part[0] == '0' {
			return ""
		}
	}
	return match[1]
}
