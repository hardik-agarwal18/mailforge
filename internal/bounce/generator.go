// Package bounce creates and verifies per-recipient VERP return paths.
// The token contains a recipient identifier authenticated with an HMAC so a
// DSN can be correlated without storing a separate token column per recipient.
package bounce

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"

	"github.com/google/uuid"

	"mailforge/internal/emailaddr"
)

const (
	macBytes = 16 // 128-bit tag; local-part remains below SMTP's 64-octet limit.
	version  = "mailforge-verp-v1:"
)

type Generator struct {
	domain string
	key    []byte
}

func NewGenerator(domain string, key []byte) (*Generator, error) {
	domain = strings.ToLower(strings.TrimSuffix(strings.TrimSpace(domain), "."))
	if _, err := emailaddr.ParseMailbox("bounce-check@" + domain); err != nil {
		return nil, fmt.Errorf("invalid bounce domain: %w", err)
	}
	if len(key) < 32 {
		return nil, errors.New("bounce HMAC key must contain at least 32 bytes")
	}
	return &Generator{domain: domain, key: append([]byte(nil), key...)}, nil
}

// AddressForRecipient returns a unique, deterministic return-path for one
// recipient. The signature prevents a token for one recipient being modified
// to target another recipient.
func (g *Generator) AddressForRecipient(recipientID string) (string, error) {
	if g == nil {
		return "", errors.New("bounce address generator is not configured")
	}
	id, err := uuid.Parse(recipientID)
	if err != nil {
		return "", fmt.Errorf("invalid recipient ID: %w", err)
	}
	compactID := hex.EncodeToString(id[:])
	tag := g.sign(compactID)
	local := "b+" + compactID + "." + base64.RawURLEncoding.EncodeToString(tag)
	address := local + "@" + g.domain
	if err := emailaddr.ValidateMailbox(address); err != nil {
		return "", fmt.Errorf("generated invalid VERP address: %w", err)
	}
	return address, nil
}

// RecipientID verifies a VERP return-path and extracts its recipient UUID.
func (g *Generator) RecipientID(address string) (string, error) {
	if g == nil {
		return "", errors.New("bounce address generator is not configured")
	}
	mailbox, err := emailaddr.ParseMailbox(address)
	if err != nil {
		return "", fmt.Errorf("invalid bounce recipient: %w", err)
	}
	if !strings.EqualFold(mailbox.Domain, g.domain) {
		return "", errors.New("bounce recipient domain does not match configured domain")
	}
	if !strings.HasPrefix(mailbox.Local, "b+") {
		return "", errors.New("bounce recipient has an invalid VERP prefix")
	}
	parts := strings.Split(strings.TrimPrefix(mailbox.Local, "b+"), ".")
	if len(parts) != 2 || len(parts[0]) != 32 {
		return "", errors.New("bounce recipient has an invalid VERP token")
	}
	idBytes, err := hex.DecodeString(parts[0])
	if err != nil || len(idBytes) != 16 {
		return "", errors.New("bounce recipient contains an invalid recipient identifier")
	}
	providedTag, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil || len(providedTag) != macBytes {
		return "", errors.New("bounce recipient contains an invalid signature")
	}
	expectedTag := g.sign(parts[0])
	if !hmac.Equal(providedTag, expectedTag) {
		return "", errors.New("bounce recipient signature is invalid")
	}
	id, err := uuid.FromBytes(idBytes)
	if err != nil {
		return "", fmt.Errorf("decode recipient identifier: %w", err)
	}
	return id.String(), nil
}

func (g *Generator) Domain() string { return g.domain }

func (g *Generator) sign(compactID string) []byte {
	mac := hmac.New(sha256.New, g.key)
	_, _ = mac.Write([]byte(version + compactID))
	return mac.Sum(nil)[:macBytes]
}

// DecodeHexKey parses the deployment representation used by
// SMTP_BOUNCE_HMAC_SECRET. The decoded secret must be at least 32 bytes.
func DecodeHexKey(value string) ([]byte, error) {
	key, err := hex.DecodeString(strings.TrimSpace(value))
	if err != nil {
		return nil, fmt.Errorf("decode bounce HMAC secret as hex: %w", err)
	}
	if len(key) < 32 {
		return nil, errors.New("bounce HMAC secret must decode to at least 32 bytes")
	}
	return key, nil
}
