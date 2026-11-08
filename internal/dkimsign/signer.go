// Package dkimsign provides the outbound DKIM signing configuration used by
// MailForge. It delegates RFC 6376 canonicalization and signature generation
// to the maintained go-msgauth implementation.
package dkimsign

import (
	"bytes"
	"crypto"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"net"
	"net/mail"
	"os"
	"strings"

	"github.com/emersion/go-msgauth/dkim"
)

const minimumRSABits = 2048

// Signer signs complete RFC 5322 messages with RSA-SHA256 and relaxed/relaxed
// canonicalization. The key is loaded once at process startup and reused for
// each recipient delivery. A Signer is safe for concurrent use because the
// private key and options are immutable after construction.
type Signer struct {
	options dkim.SignOptions
}

// NewSigner validates the signing identity and private key. Domain must be
// the domain MailForge publishes the selector record under. For DMARC
// alignment, it should align with the RFC 5322 From domain.
func NewSigner(domain, selector string, key *rsa.PrivateKey) (*Signer, error) {
	domain = strings.ToLower(strings.TrimSpace(domain))
	selector = strings.ToLower(strings.TrimSpace(selector))
	if err := validateDNSName(domain); err != nil {
		return nil, fmt.Errorf("invalid DKIM domain: %w", err)
	}
	if err := validateSelector(selector); err != nil {
		return nil, fmt.Errorf("invalid DKIM selector: %w", err)
	}
	if key == nil {
		return nil, errors.New("DKIM RSA private key is required")
	}
	if key.N == nil || key.N.BitLen() < minimumRSABits {
		return nil, fmt.Errorf("DKIM RSA key must be at least %d bits", minimumRSABits)
	}
	if err := key.Validate(); err != nil {
		return nil, fmt.Errorf("invalid DKIM RSA private key: %w", err)
	}
	key.Precompute()

	return &Signer{options: dkim.SignOptions{
		Domain:                 domain,
		Selector:               selector,
		Signer:                 key,
		Hash:                   crypto.SHA256,
		HeaderCanonicalization: dkim.CanonicalizationRelaxed,
		BodyCanonicalization:   dkim.CanonicalizationRelaxed,
		// A nil HeaderKeys makes go-msgauth sign each header field that is
		// actually present. We separately require exactly one From field,
		// as RFC 6376 requires From to be signed.
	}}, nil
}

// LoadRSAPrivateKey loads an unencrypted PKCS#1 or PKCS#8 RSA private key.
// The private key must be at least 2048 bits. Encrypted PEM is intentionally
// unsupported so secrets can be injected by a deployment secret manager
// without embedding passphrases in process arguments.
func LoadRSAPrivateKey(path string) (*rsa.PrivateKey, error) {
	path = strings.TrimSpace(path)
	if path == "" {
		return nil, errors.New("DKIM private key file path is required")
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read DKIM private key: %w", err)
	}
	block, rest := pem.Decode(data)
	if block == nil {
		return nil, errors.New("DKIM private key file does not contain a PEM block")
	}
	if len(bytes.TrimSpace(rest)) != 0 {
		return nil, errors.New("DKIM private key file contains unexpected data after the PEM block")
	}
	if x509.IsEncryptedPEMBlock(block) {
		return nil, errors.New("encrypted DKIM PEM keys are not supported; use a deployment secret manager to protect an unencrypted key file")
	}

	var key *rsa.PrivateKey
	switch block.Type {
	case "RSA PRIVATE KEY":
		key, err = x509.ParsePKCS1PrivateKey(block.Bytes)
	case "PRIVATE KEY":
		var parsed any
		parsed, err = x509.ParsePKCS8PrivateKey(block.Bytes)
		if err == nil {
			var ok bool
			key, ok = parsed.(*rsa.PrivateKey)
			if !ok {
				return nil, fmt.Errorf("DKIM PKCS#8 key has unsupported type %T; RSA is required", parsed)
			}
		}
	default:
		return nil, fmt.Errorf("unsupported DKIM PEM block %q; expected RSA PRIVATE KEY or PRIVATE KEY", block.Type)
	}
	if err != nil {
		return nil, fmt.Errorf("parse DKIM RSA private key: %w", err)
	}
	if key.N == nil || key.N.BitLen() < minimumRSABits {
		return nil, fmt.Errorf("DKIM RSA key must be at least %d bits", minimumRSABits)
	}
	if err := key.Validate(); err != nil {
		return nil, fmt.Errorf("validate DKIM RSA private key: %w", err)
	}
	key.Precompute()
	return key, nil
}

// Sign returns a new message with a DKIM-Signature field prepended. It does
// not mutate raw. The caller must remove Bcc/Resent-Bcc before calling Sign so
// the signature covers exactly the message bytes sent to the recipient.
func (s *Signer) Sign(raw []byte) ([]byte, error) {
	if s == nil {
		return nil, errors.New("DKIM signer is not configured")
	}
	if err := validateMessage(raw); err != nil {
		return nil, err
	}

	parsed, err := mail.ReadMessage(bytes.NewReader(raw))
	if err != nil {
		return nil, fmt.Errorf("parse message before DKIM signing: %w", err)
	}
	from := parsed.Header["From"]
	if len(from) != 1 || strings.TrimSpace(from[0]) == "" {
		return nil, errors.New("DKIM signing requires exactly one non-empty From header")
	}
	fromAddresses, err := parsed.Header.AddressList("From")
	if err != nil {
		return nil, fmt.Errorf("DKIM signing requires a valid From mailbox: %w", err)
	}
	if len(fromAddresses) == 0 {
		return nil, errors.New("DKIM signing requires at least one mailbox in the From header")
	}

	var signed bytes.Buffer
	if err := dkim.Sign(&signed, bytes.NewReader(raw), &s.options); err != nil {
		return nil, fmt.Errorf("generate DKIM signature: %w", err)
	}
	return signed.Bytes(), nil
}

func validateMessage(raw []byte) error {
	if len(raw) == 0 {
		return errors.New("cannot DKIM-sign an empty message")
	}

	// Validate line endings before checking the header/body separator. A
	// message using LF-only separators (for example, "\n\n") must report
	// the malformed bare LF rather than the less-specific missing-separator
	// error. This also prevents malformed line endings from reaching the DKIM
	// canonicalization/signing library.
	for i, b := range raw {
		if b == '\n' && (i == 0 || raw[i-1] != '\r') {
			return fmt.Errorf("cannot DKIM-sign message containing bare LF at byte %d", i)
		}
		if b == '\r' && (i+1 >= len(raw) || raw[i+1] != '\n') {
			return fmt.Errorf("cannot DKIM-sign message containing bare CR at byte %d", i)
		}
	}
	if !bytes.Contains(raw, []byte("\r\n\r\n")) {
		return errors.New("cannot DKIM-sign message without an RFC 5322 header/body separator")
	}
	return nil
}

func validateDNSName(name string) error {
	if name == "" || len(name) > 253 || strings.HasSuffix(name, ".") || strings.Contains(name, "..") {
		return errors.New("must be a fully-qualified DNS name without a trailing dot")
	}
	if strings.ContainsAny(name, " \t\r\n/_") {
		return errors.New("contains whitespace or unsupported characters")
	}
	if net.ParseIP(name) != nil {
		return errors.New("must be a DNS name, not an IP address")
	}
	for _, label := range strings.Split(name, ".") {
		if len(label) == 0 || len(label) > 63 || !isAlphaNumeric(label[0]) || !isAlphaNumeric(label[len(label)-1]) {
			return fmt.Errorf("invalid DNS label %q", label)
		}
		for i := 0; i < len(label); i++ {
			if !isAlphaNumeric(label[i]) && label[i] != '-' {
				return fmt.Errorf("invalid character in DNS label %q", label)
			}
		}
	}
	if !strings.Contains(name, ".") {
		return errors.New("must contain at least one dot")
	}
	return nil
}

func validateSelector(selector string) error {
	if selector == "" || len(selector) > 63 || !isAlphaNumeric(selector[0]) || !isAlphaNumeric(selector[len(selector)-1]) {
		return errors.New("must be a DNS label of 1-63 letters, digits, or hyphens")
	}
	for i := 0; i < len(selector); i++ {
		if !isAlphaNumeric(selector[i]) && selector[i] != '-' {
			return errors.New("must contain only ASCII letters, digits, or hyphens")
		}
	}
	return nil
}

func isAlphaNumeric(ch byte) bool {
	return (ch >= 'a' && ch <= 'z') || (ch >= '0' && ch <= '9')
}
