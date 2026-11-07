package outbound

import (
	"errors"
	"fmt"
	"net"
	"net/textproto"
	"testing"
)

func TestClassifyError(t *testing.T) {
	tests := []struct {
		name      string
		code      int
		permanent bool
	}{
		{"temporary unavailable", 421, false},
		{"mailbox busy", 450, false},
		{"temporary processing failure", 451, false},
		{"mailbox unavailable", 550, true},
		{"mailbox disabled", 551, true},
		{"transaction failed", 554, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			smtpErr := &textproto.Error{
				Code: tt.code,
				Msg:  tt.name,
			}

			wrapped := fmt.Errorf("RCPT TO: %w", smtpErr)

			got := classifyError(wrapped)

			var deliveryErr *DeliveryError
			if !errors.As(got, &deliveryErr) {
				t.Fatal("expected DeliveryError")
			}

			if deliveryErr.Permanent != tt.permanent {
				t.Fatalf(
					"expected permanent=%t, got %t",
					tt.permanent,
					deliveryErr.Permanent,
				)
			}

			if deliveryErr.SMTPCode != tt.code {
				t.Fatalf(
					"expected SMTP code %d, got %d",
					tt.code,
					deliveryErr.SMTPCode,
				)
			}
		})
	}
}

func TestClassifyNetworkErrorAsTemporary(t *testing.T) {
	got := classifyError(errors.New("connection refused"))

	var deliveryErr *DeliveryError
	if !errors.As(got, &deliveryErr) {
		t.Fatal("expected DeliveryError")
	}

	if deliveryErr.Permanent {
		t.Fatal("network error must be temporary")
	}
}

func TestClassifyNilError(t *testing.T) {
	if got := classifyError(nil); got != nil {
		t.Fatalf("expected nil, got %v", got)
	}
}

func TestClassifyDNSNotFoundAsPermanent(t *testing.T) {
	dnsErr := &net.DNSError{Err: "no such host", Name: "does-not-exist.invalid", IsNotFound: true}
	got := classifyError(fmt.Errorf("MX lookup: %w", dnsErr))
	var deliveryErr *DeliveryError
	if !errors.As(got, &deliveryErr) {
		t.Fatalf("expected DeliveryError, got %T", got)
	}
	if !deliveryErr.Permanent {
		t.Fatalf("DNS not-found should be permanent: %#v", deliveryErr)
	}
}
