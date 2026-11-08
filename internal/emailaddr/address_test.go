package emailaddr

import (
	"strings"
	"testing"
)

func TestParseMailboxAcceptsSupportedAddresses(t *testing.T) {
	tests := []string{
		"user@example.com",
		"first.last+tag@example.co.uk",
		"user_name@example-domain.com",
		"!#$%&'*+-/=?^_`{|}~@example.com",
		`"user name"@example.com`,
		`"user@name"@example.com`,
		`"escaped\\quote"@example.com`,
		`"escaped\"quote"@example.com`,
		"user@xn--bcher-kva.example",
	}

	for _, address := range tests {
		t.Run(address, func(t *testing.T) {
			if _, err := ParseMailbox(address); err != nil {
				t.Fatalf("ParseMailbox(%q) error = %v", address, err)
			}
		})
	}
}

func TestValidateMailboxRejectsMalformedAddresses(t *testing.T) {
	tests := []struct {
		name    string
		address string
	}{
		{name: "empty", address: ""},
		{name: "missing at", address: "user.example.com"},
		{name: "empty local", address: "@example.com"},
		{name: "empty domain", address: "user@"},
		{name: "multiple at signs", address: "user@@example.com"},
		{name: "extra at in domain", address: "user@example.com@other.com"},
		{name: "leading dot", address: ".user@example.com"},
		{name: "trailing dot", address: "user.@example.com"},
		{name: "consecutive dots", address: "user..name@example.com"},
		{name: "domain starts with hyphen", address: "user@-example.com"},
		{name: "domain ends with hyphen", address: "user@example-.com"},
		{name: "empty domain label", address: "user@example..com"},
		{name: "single-label domain", address: "user@localhost"},
		{name: "domain underscore", address: "user@exam_ple.com"},
		{name: "whitespace in dot atom", address: "user name@example.com"},
		{name: "display name", address: "User Name <user@example.com>"},
		{name: "domain literal not supported", address: "user@[192.0.2.1]"},
		{name: "unicode local part", address: "δοκιμή@example.com"},
		{name: "unicode domain", address: "user@bücher.example"},
		{name: "unclosed quote", address: `"user@example.com`},
		{name: "quoted local followed by data", address: `"user"extra@example.com`},
		{name: "invalid quoted escape", address: "\"user\\\x01\"@example.com"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if err := ValidateMailbox(test.address); err == nil {
				t.Fatalf("ValidateMailbox(%q) unexpectedly succeeded", test.address)
			}
		})
	}
}

func TestValidateMailboxEnforcesLengthLimits(t *testing.T) {
	localTooLong := strings.Repeat("a", maxLocalLength+1) + "@example.com"
	labelTooLong := "user@" + strings.Repeat("a", maxLabelLength+1) + ".com"
	domainTooLong := "user@" + strings.Repeat("a.", 126) + "a.com"
	mailboxTooLong := strings.Repeat("a", 64) + "@" + strings.Repeat("b", 63) + "." + strings.Repeat("c", 63) + "." + strings.Repeat("d", 63) + "." + strings.Repeat("e", 10)

	for name, address := range map[string]string{
		"local part": localTooLong,
		"label":      labelTooLong,
		"domain":     domainTooLong,
		"mailbox":    mailboxTooLong,
	} {
		t.Run(name, func(t *testing.T) {
			if err := ValidateMailbox(address); err == nil {
				t.Fatalf("expected length validation error for %q", name)
			}
		})
	}
}

func TestValidateReversePathAllowsNullSender(t *testing.T) {
	if err := ValidateReversePath(""); err != nil {
		t.Fatalf("null reverse-path must be accepted: %v", err)
	}
	if err := ValidateReversePath("sender@example.com"); err != nil {
		t.Fatalf("valid sender rejected: %v", err)
	}
	if err := ValidateReversePath("sender@"); err == nil {
		t.Fatal("malformed sender unexpectedly accepted")
	}
}

func TestEqualUsesCaseSensitiveLocalPartAndCaseInsensitiveDomain(t *testing.T) {
	if !Equal("User@Example.com", "User@example.COM") {
		t.Fatal("domain comparison should be case-insensitive")
	}
	if Equal("User@example.com", "user@example.com") {
		t.Fatal("local-part comparison should be case-sensitive")
	}
	if Equal("not-an-address", "not-an-address") {
		t.Fatal("invalid mailbox must not compare equal")
	}
}

func TestParseMailboxAcceptsMaximumAddrSpecLength(t *testing.T) {
	local := strings.Repeat("a", maxLocalLength)
	domain := strings.Repeat("b", 63) + "." + strings.Repeat("c", 63) + "." + strings.Repeat("d", 61)
	address := local + "@" + domain
	if len(address) != maxMailboxLength {
		t.Fatalf("test address length = %d, want %d", len(address), maxMailboxLength)
	}
	if err := ValidateMailbox(address); err != nil {
		t.Fatalf("maximum-length valid mailbox rejected: %v", err)
	}
}

func TestIsAuthorizedMailbox(t *testing.T) {
	tests := []struct {
		name    string
		address string
		primary string
		domains []string
		want    bool
	}{
		{name: "primary exact mailbox", address: "sender@example.com", primary: "sender@example.com", want: true},
		{name: "primary local part remains case sensitive", address: "Sender@example.com", primary: "sender@example.com", want: false},
		{name: "verified domain", address: "billing@verified.example.org", primary: "sender@example.com", domains: []string{"verified.example.org"}, want: true},
		{name: "verified domain is case insensitive", address: "billing@VERIFIED.example.org", primary: "sender@example.com", domains: []string{"verified.example.org"}, want: true},
		{name: "unverified domain", address: "billing@unverified.example.org", primary: "sender@example.com", domains: []string{"verified.example.org"}, want: false},
		{name: "invalid mailbox", address: "not-an-email", primary: "sender@example.com", domains: []string{"verified.example.org"}, want: false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := IsAuthorizedMailbox(test.address, test.primary, test.domains); got != test.want {
				t.Fatalf("IsAuthorizedMailbox(%q) = %v, want %v", test.address, got, test.want)
			}
		})
	}
}
