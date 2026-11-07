// Package emailaddr validates the mailbox syntax supported by MailForge.
//
// MailForge currently accepts ASCII SMTP mailbox syntax (RFC 5321): dot-atom
// or quoted local-parts and DNS-style domains. Internationalized local-parts
// (SMTPUTF8) and address-literal domains are deliberately rejected until the
// corresponding protocol and routing support is implemented.
package emailaddr

import (
	"fmt"
	"strings"
)

const (
	maxMailboxLength = 254
	maxLocalLength   = 64
	maxDomainLength  = 253
	maxLabelLength   = 63
)

// Mailbox is a parsed SMTP mailbox. Local is preserved exactly as supplied;
// Domain is preserved for routing, while comparisons treat it case-insensitively.
type Mailbox struct {
	Local  string
	Domain string
}

// ParseMailbox parses and validates an addr-spec accepted by MailForge.
// It does not accept display names, comments, angle brackets, SMTP parameters,
// SMTPUTF8 local-parts, or domain address literals.
func ParseMailbox(address string) (Mailbox, error) {
	if address == "" {
		return Mailbox{}, fmt.Errorf("mailbox is empty")
	}
	if len(address) > maxMailboxLength {
		return Mailbox{}, fmt.Errorf("mailbox exceeds %d octets", maxMailboxLength)
	}

	local, domain, err := splitMailbox(address)
	if err != nil {
		return Mailbox{}, err
	}
	if len(local) == 0 {
		return Mailbox{}, fmt.Errorf("local-part is empty")
	}
	if len(local) > maxLocalLength {
		return Mailbox{}, fmt.Errorf("local-part exceeds %d octets", maxLocalLength)
	}
	if err := validateLocalPart(local); err != nil {
		return Mailbox{}, err
	}
	if err := validateDomain(domain); err != nil {
		return Mailbox{}, err
	}

	return Mailbox{Local: local, Domain: domain}, nil
}

// ValidateMailbox reports whether address is a supported non-null mailbox.
func ValidateMailbox(address string) error {
	_, err := ParseMailbox(address)
	return err
}

// ValidateReversePath validates the SMTP reverse-path. The empty string is
// allowed because MAIL FROM:<> is the required null reverse-path used by DSNs
// and other automatically generated failure notifications.
func ValidateReversePath(address string) error {
	if address == "" {
		return nil
	}
	return ValidateMailbox(address)
}

// Equal reports whether two valid mailboxes identify the same SMTP mailbox
// under SMTP's comparison rules: local-parts are case-sensitive and domains
// are case-insensitive. It intentionally does not rewrite or lowercase input.
func Equal(left, right string) bool {
	leftMailbox, leftErr := ParseMailbox(left)
	rightMailbox, rightErr := ParseMailbox(right)
	if leftErr != nil || rightErr != nil {
		return false
	}
	return leftMailbox.Local == rightMailbox.Local &&
		strings.EqualFold(leftMailbox.Domain, rightMailbox.Domain)
}

func splitMailbox(address string) (local, domain string, err error) {
	if address[0] == '"' {
		closeQuote, quoteErr := quotedLocalPartEnd(address)
		if quoteErr != nil {
			return "", "", quoteErr
		}
		if closeQuote+1 >= len(address) || address[closeQuote+1] != '@' {
			return "", "", fmt.Errorf("quoted local-part must be followed by @ and a domain")
		}
		return address[:closeQuote+1], address[closeQuote+2:], nil
	}

	at := strings.IndexByte(address, '@')
	if at <= 0 {
		return "", "", fmt.Errorf("mailbox must contain a local-part and domain separated by @")
	}
	if strings.IndexByte(address[at+1:], '@') >= 0 {
		return "", "", fmt.Errorf("mailbox contains more than one unquoted @")
	}
	return address[:at], address[at+1:], nil
}

// quotedLocalPartEnd returns the index of the closing quote. It only accepts
// the printable ASCII qtext and quoted-pair forms supported without SMTPUTF8.
func quotedLocalPartEnd(address string) (int, error) {
	for i := 1; i < len(address); i++ {
		ch := address[i]
		switch {
		case ch == '\\':
			if i+1 >= len(address) {
				return 0, fmt.Errorf("quoted local-part ends with an incomplete escape")
			}
			escaped := address[i+1]
			if escaped < 32 || escaped > 126 {
				return 0, fmt.Errorf("quoted local-part contains an invalid escape")
			}
			i++
		case ch == '"':
			return i, nil
		case isQuotedText(ch):
			// Valid quoted text.
		default:
			return 0, fmt.Errorf("quoted local-part contains an invalid character")
		}
	}
	return 0, fmt.Errorf("quoted local-part is not terminated")
}

func validateLocalPart(local string) error {
	if local[0] == '"' {
		closeQuote, err := quotedLocalPartEnd(local)
		if err != nil {
			return err
		}
		if closeQuote != len(local)-1 {
			return fmt.Errorf("unexpected data after quoted local-part")
		}
		return nil
	}

	if local[0] == '.' || local[len(local)-1] == '.' {
		return fmt.Errorf("dot-atom local-part cannot start or end with a dot")
	}
	previousDot := false
	for i := 0; i < len(local); i++ {
		ch := local[i]
		if ch == '.' {
			if previousDot {
				return fmt.Errorf("dot-atom local-part cannot contain consecutive dots")
			}
			previousDot = true
			continue
		}
		if !isAText(ch) {
			return fmt.Errorf("local-part contains a character not permitted in dot-atom syntax")
		}
		previousDot = false
	}
	return nil
}

func validateDomain(domain string) error {
	if domain == "" {
		return fmt.Errorf("domain is empty")
	}
	if len(domain) > maxDomainLength {
		return fmt.Errorf("domain exceeds %d octets", maxDomainLength)
	}
	if strings.HasPrefix(domain, "[") || strings.HasSuffix(domain, "]") {
		return fmt.Errorf("address-literal domains are not supported")
	}
	if !isASCII(domain) {
		return fmt.Errorf("non-ASCII domains must be supplied in IDNA ASCII form")
	}

	labels := strings.Split(domain, ".")
	if len(labels) < 2 {
		return fmt.Errorf("domain must be a fully-qualified DNS name")
	}
	for _, label := range labels {
		if len(label) == 0 {
			return fmt.Errorf("domain cannot contain empty labels")
		}
		if len(label) > maxLabelLength {
			return fmt.Errorf("domain label exceeds %d octets", maxLabelLength)
		}
		if !isAlphaNumeric(label[0]) || !isAlphaNumeric(label[len(label)-1]) {
			return fmt.Errorf("domain labels must begin and end with a letter or digit")
		}
		for i := 0; i < len(label); i++ {
			if !isAlphaNumeric(label[i]) && label[i] != '-' {
				return fmt.Errorf("domain label contains an invalid character")
			}
		}
	}
	return nil
}

func isAText(ch byte) bool {
	return isAlphaNumeric(ch) || strings.ContainsRune("!#$%&'*+-/=?^_`{|}~", rune(ch))
}

func isQuotedText(ch byte) bool {
	return ch == 32 || ch == 33 || (ch >= 35 && ch <= 91) || (ch >= 93 && ch <= 126)
}

func isAlphaNumeric(ch byte) bool {
	return (ch >= 'a' && ch <= 'z') ||
		(ch >= 'A' && ch <= 'Z') ||
		(ch >= '0' && ch <= '9')
}

func isASCII(value string) bool {
	for i := 0; i < len(value); i++ {
		if value[i] >= 128 {
			return false
		}
	}
	return true
}
