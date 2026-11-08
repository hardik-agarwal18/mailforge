package smtpauth

import (
	"strings"
	"testing"
)

func TestNormalizeSendingDomain(t *testing.T) {
	tests := []struct {
		input string
		want  string
		valid bool
	}{
		{input: " Example.COM ", want: "example.com", valid: true},
		{input: "example.com.", want: "example.com", valid: true},
		{input: "mail.example.com", want: "mail.example.com", valid: true},
		{input: "localhost", valid: false},
		{input: "example..com", valid: false},
		{input: "user@example.com", valid: false},
		{input: "-bad.example.com", valid: false},
		{input: "127.0.0.1", valid: false},
		{input: "example.com/path", valid: false},
	}
	for _, test := range tests {
		t.Run(test.input, func(t *testing.T) {
			got, err := normalizeSendingDomain(test.input)
			if test.valid {
				if err != nil {
					t.Fatalf("normalizeSendingDomain() error = %v", err)
				}
				if got != test.want {
					t.Fatalf("normalizeSendingDomain() = %q, want %q", got, test.want)
				}
				return
			}
			if err == nil {
				t.Fatalf("normalizeSendingDomain(%q) unexpectedly succeeded as %q", test.input, got)
			}
		})
	}
}

func TestNewDomainVerificationToken(t *testing.T) {
	first, err := newDomainVerificationToken()
	if err != nil {
		t.Fatalf("newDomainVerificationToken() error = %v", err)
	}
	second, err := newDomainVerificationToken()
	if err != nil {
		t.Fatalf("newDomainVerificationToken() error = %v", err)
	}
	if !strings.HasPrefix(first, domainVerificationPrefix) {
		t.Fatalf("token %q does not have expected prefix", first)
	}
	if first == second {
		t.Fatal("two generated verification tokens unexpectedly match")
	}
	if len(strings.TrimPrefix(first, domainVerificationPrefix)) != 43 {
		t.Fatalf("encoded token length = %d, want 43 for 256 bits of entropy", len(strings.TrimPrefix(first, domainVerificationPrefix)))
	}
}

func TestVerificationTXTHelpers(t *testing.T) {
	const token = "mailforge-domain-verification=expected-token"
	if !containsExactTXT([]string{"unrelated", token}, token) {
		t.Fatal("containsExactTXT() did not find exact TXT value")
	}
	if containsExactTXT([]string{"prefix-" + token, "expected-token"}, token) {
		t.Fatal("containsExactTXT() accepted a non-exact TXT value")
	}
	if got := verificationRecordName("example.com"); got != "_mailforge-verification.example.com" {
		t.Fatalf("verificationRecordName() = %q", got)
	}
}
