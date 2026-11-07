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

func TestClassifyErrorRetainsEnhancedStatusAndStage(t *testing.T) {
	smtpErr := &textproto.Error{Code: 451, Msg: "4.7.0 [remote] temporarily rate limited"}
	got := classifyErrorAtStage("rcpt_to", fmt.Errorf("RCPT TO: %w", smtpErr))

	var deliveryErr *DeliveryError
	if !errors.As(got, &deliveryErr) {
		t.Fatalf("expected DeliveryError, got %T", got)
	}
	if deliveryErr.Stage != "rcpt_to" {
		t.Fatalf("Stage = %q, want rcpt_to", deliveryErr.Stage)
	}
	if deliveryErr.EnhancedStatusCode != "4.7.0" {
		t.Fatalf("EnhancedStatusCode = %q, want 4.7.0", deliveryErr.EnhancedStatusCode)
	}
	if deliveryErr.Permanent {
		t.Fatal("451 must be temporary")
	}
}

func TestClassifyErrorIgnoresMalformedOrContradictoryEnhancedStatus(t *testing.T) {
	tests := []struct {
		name string
		code int
		msg  string
	}{
		{name: "class mismatch", code: 550, msg: "4.7.0 throttled"},
		{name: "leading zero", code: 550, msg: "5.01.1 invalid code"},
		{name: "malformed", code: 550, msg: "5.x.1 invalid code"},
		{name: "no enhanced code", code: 550, msg: "mailbox unavailable"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got := classifyError(&textproto.Error{Code: test.code, Msg: test.msg})
			var deliveryErr *DeliveryError
			if !errors.As(got, &deliveryErr) {
				t.Fatalf("expected DeliveryError, got %T", got)
			}
			if deliveryErr.EnhancedStatusCode != "" {
				t.Fatalf("EnhancedStatusCode = %q, want empty", deliveryErr.EnhancedStatusCode)
			}
			if !deliveryErr.Permanent {
				t.Fatal("basic 550 reply must remain permanent regardless of enhanced text")
			}
		})
	}
}

func TestSMTPReplySeverityUsesFirstDigitForUnknownCodes(t *testing.T) {
	for _, test := range []struct {
		code      int
		permanent bool
	}{
		{code: 499, permanent: false},
		{code: 500, permanent: true},
		{code: 599, permanent: true},
		{code: 399, permanent: false},
	} {
		got := classifyError(&textproto.Error{Code: test.code, Msg: "vendor-specific reply"})
		var deliveryErr *DeliveryError
		if !errors.As(got, &deliveryErr) {
			t.Fatalf("code %d: expected DeliveryError", test.code)
		}
		if deliveryErr.Permanent != test.permanent {
			t.Errorf("code %d: permanent = %t, want %t", test.code, deliveryErr.Permanent, test.permanent)
		}
	}
}
