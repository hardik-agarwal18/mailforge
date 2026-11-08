// Command dkim-keygen creates an RSA key pair for MailForge outbound DKIM
// signing. It writes the private key with owner-only permissions and prints
// the DNS TXT record containing the corresponding public key.
package main

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/base64"
	"encoding/pem"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"mailforge/internal/dkimsign"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "dkim-keygen:", err)
		os.Exit(1)
	}
}

func run() error {
	domain := flag.String("domain", "", "DKIM signing domain (for example example.com)")
	selector := flag.String("selector", "mailforge2026", "DKIM selector DNS label")
	out := flag.String("out", filepath.Join("secrets", "dkim-private.pem"), "output path for the private key")
	bits := flag.Int("bits", 2048, "RSA key size in bits (minimum 2048)")
	flag.Parse()

	if strings.TrimSpace(*domain) == "" {
		return fmt.Errorf("-domain is required")
	}
	if strings.TrimSpace(*selector) == "" {
		return fmt.Errorf("-selector is required")
	}
	if *bits < 2048 || *bits > 4096 {
		return fmt.Errorf("-bits must be between 2048 and 4096")
	}
	if strings.TrimSpace(*out) == "" {
		return fmt.Errorf("-out must not be empty")
	}

	key, err := rsa.GenerateKey(rand.Reader, *bits)
	if err != nil {
		return fmt.Errorf("generate RSA key: %w", err)
	}
	if _, err := dkimsign.NewSigner(*domain, *selector, key); err != nil {
		return err
	}

	privatePEM := pem.EncodeToMemory(&pem.Block{
		Type:  "RSA PRIVATE KEY",
		Bytes: x509.MarshalPKCS1PrivateKey(key),
	})
	if err := os.MkdirAll(filepath.Dir(*out), 0o700); err != nil {
		return fmt.Errorf("create private key directory: %w", err)
	}
	file, err := os.OpenFile(*out, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return fmt.Errorf("create private key file (refusing to overwrite): %w", err)
	}
	if _, err := file.Write(privatePEM); err != nil {
		_ = file.Close()
		_ = os.Remove(*out)
		return fmt.Errorf("write private key: %w", err)
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		_ = os.Remove(*out)
		return fmt.Errorf("sync private key: %w", err)
	}
	if err := file.Close(); err != nil {
		_ = os.Remove(*out)
		return fmt.Errorf("close private key file: %w", err)
	}

	publicDER := x509.MarshalPKCS1PublicKey(&key.PublicKey)
	publicBase64 := base64.StdEncoding.EncodeToString(publicDER)
	txtValue := "v=DKIM1; k=rsa; p=" + publicBase64

	fmt.Println("DKIM RSA key pair generated.")
	fmt.Printf("Private key: %s\n", *out)
	fmt.Printf("DNS record name: %s._domainkey.%s\n", *selector, strings.ToLower(strings.TrimSpace(*domain)))
	fmt.Println("DNS TXT value (paste as one value in your DNS provider):")
	fmt.Println(txtValue)
	fmt.Println("\nZone-file form (adjacent quoted chunks form one TXT record):")
	for start := 0; start < len(txtValue); start += 200 {
		end := start + 200
		if end > len(txtValue) {
			end = len(txtValue)
		}
		fmt.Printf("\"%s\"\n", txtValue[start:end])
	}
	fmt.Println("\nKeep the private key secret. Do not commit it or send it to anyone.")
	return nil
}
