package outbound

import (
	"errors"
	"fmt"
	"net/textproto"
)

type DeliveryError struct {
	Message   string
	Permanent bool
	SMTPCode  int
}

func (e *DeliveryError) Error() string {
	return e.Message
}

func classifyError(err error) error {
	if err == nil {
		return nil
	}

	var smtpErr *textproto.Error

	if errors.As(err, &smtpErr) {
		return &DeliveryError{
			Message:   err.Error(),
			SMTPCode:  smtpErr.Code,
			Permanent: smtpErr.Code >= 500 && smtpErr.Code < 600,
		}
	}

	// Network, DNS and TLS failures are temporary by default.
	return &DeliveryError{
		Message:   err.Error(),
		Permanent: false,
	}
}

func newDeliveryError(message string, permanent bool) error {
	return &DeliveryError{
		Message:   fmt.Sprintf("%s", message),
		Permanent: permanent,
	}
}
