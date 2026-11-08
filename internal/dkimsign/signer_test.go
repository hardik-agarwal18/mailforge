package dkimsign

import (
	"bytes"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/base64"
	"encoding/pem"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/emersion/go-msgauth/dkim"
)

func TestSignerProducesVerifiableSignature(t *testing.T) {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("generate test key: %v", err)
	}
	signer, err := NewSigner("Example.COM", "mailforge-test", key)
	if err != nil {
		t.Fatalf("NewSigner: %v", err)
	}

	raw := []byte("From: sender@example.com\r\n" +
		"To: recipient@example.net\r\n" +
		"Subject: DKIM signing test\r\n" +
		"Date: Sun, 08 Nov 2026 10:00:00 +0530\r\n" +
		"Message-ID: <dkim-test@example.com>\r\n\r\n" +
		"A body with  whitespace.  \r\n\r\n")
	signed, err := signer.Sign(raw)
	if err != nil {
		t.Fatalf("Sign: %v", err)
	}
	if !bytes.HasPrefix(signed, []byte("DKIM-Signature:")) {
		t.Fatalf("signed message does not start with DKIM-Signature: %q", signed[:min(len(signed), 80)])
	}
	if bytes.Contains(raw, []byte("DKIM-Signature:")) {
		t.Fatal("Sign mutated the original message")
	}

	publicTXT := "v=DKIM1; k=rsa; p=" + base64.StdEncoding.EncodeToString(x509.MarshalPKCS1PublicKey(&key.PublicKey))
	verifications, err := dkim.VerifyWithOptions(bytes.NewReader(signed), &dkim.VerifyOptions{
		LookupTXT: func(name string) ([]string, error) {
			if name != "mailforge-test._domainkey.example.com" {
				t.Fatalf("unexpected DKIM DNS lookup name %q", name)
			}
			return []string{publicTXT}, nil
		},
	})
	if err != nil {
		t.Fatalf("VerifyWithOptions: %v", err)
	}
	if len(verifications) != 1 || verifications[0].Err != nil {
		t.Fatalf("DKIM verification = %#v, want one valid signature", verifications)
	}
	if verifications[0].Domain != "example.com" {
		t.Fatalf("signature domain = %q, want example.com", verifications[0].Domain)
	}

	tampered := bytes.Replace(signed, []byte("A body with"), []byte("A body Xith"), 1)
	if bytes.Equal(tampered, signed) {
		t.Fatal("test message body was not modified")
	}
	tamperedVerifications, err := dkim.VerifyWithOptions(bytes.NewReader(tampered), &dkim.VerifyOptions{
		LookupTXT: func(string) ([]string, error) { return []string{publicTXT}, nil },
	})
	if err != nil {
		t.Fatalf("VerifyWithOptions on tampered message: %v", err)
	}
	if len(tamperedVerifications) != 1 || tamperedVerifications[0].Err == nil {
		t.Fatalf("tampered message verification = %#v, want invalid signature", tamperedVerifications)
	}
}

func TestSignerRejectsMissingOrDuplicateFrom(t *testing.T) {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("generate test key: %v", err)
	}
	signer, err := NewSigner("example.com", "mailforge-test", key)
	if err != nil {
		t.Fatalf("NewSigner: %v", err)
	}

	for _, tc := range []struct {
		name string
		raw  string
	}{
		{name: "missing", raw: "To: recipient@example.net\r\n\r\nbody\r\n"},
		{name: "duplicate", raw: "From: sender@example.com\r\nFrom: attacker@example.net\r\n\r\nbody\r\n"},
		{name: "empty", raw: "From: \r\n\r\nbody\r\n"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := signer.Sign([]byte(tc.raw)); err == nil {
				t.Fatal("Sign succeeded; want invalid From header error")
			}
		})
	}
}

func TestSignerRejectsBareLF(t *testing.T) {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("generate test key: %v", err)
	}
	signer, err := NewSigner("example.com", "mailforge-test", key)
	if err != nil {
		t.Fatalf("NewSigner: %v", err)
	}
	if _, err := signer.Sign([]byte("From: sender@example.com\n\nbody\n")); err == nil || !strings.Contains(err.Error(), "bare LF") {
		t.Fatalf("Sign error = %v, want bare-LF validation error", err)
	}
}

func TestLoadRSAPrivateKeySupportsPKCS1AndPKCS8(t *testing.T) {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("generate test key: %v", err)
	}
	for _, tc := range []struct {
		name string
		pem  []byte
	}{
		{name: "PKCS1", pem: pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)})},
		{name: "PKCS8", pem: mustPKCS8PEM(t, key)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "dkim.pem")
			if err := os.WriteFile(path, tc.pem, 0o600); err != nil {
				t.Fatalf("write key: %v", err)
			}
			loaded, err := LoadRSAPrivateKey(path)
			if err != nil {
				t.Fatalf("LoadRSAPrivateKey: %v", err)
			}
			if loaded.N.Cmp(key.N) != 0 || loaded.D.Cmp(key.D) != 0 {
				t.Fatal("loaded key does not match generated key")
			}
		})
	}
}

func mustPKCS8PEM(t *testing.T, key *rsa.PrivateKey) []byte {
	t.Helper()
	der, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatalf("marshal PKCS8 key: %v", err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der})
}

func TestNewSignerRejectsInvalidIdentityAndWeakKey(t *testing.T) {
	strong, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("generate test key: %v", err)
	}
	weak, err := rsa.GenerateKey(rand.Reader, 1024)
	if err != nil {
		t.Fatalf("generate weak test key: %v", err)
	}
	cases := []struct {
		name     string
		domain   string
		selector string
		key      *rsa.PrivateKey
	}{
		{name: "empty domain", domain: "", selector: "s1", key: strong},
		{name: "domain injection", domain: "example.com\r\nTXT", selector: "s1", key: strong},
		{name: "IP domain", domain: "192.0.2.1", selector: "s1", key: strong},
		{name: "selector injection", domain: "example.com", selector: "bad.selector", key: strong},
		{name: "weak key", domain: "example.com", selector: "s1", key: weak},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := NewSigner(tc.domain, tc.selector, tc.key); err == nil {
				t.Fatal("NewSigner succeeded; want validation error")
			}
		})
	}
}
