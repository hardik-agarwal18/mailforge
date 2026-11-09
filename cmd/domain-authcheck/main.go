// Command domain-authcheck inspects the published SPF, DKIM and DMARC records
// for a domain and compares the DKIM public key with the configured private key.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"net"
	"os"
	"strings"
	"time"

	"mailforge/internal/dkimsign"
	"mailforge/internal/domainauth"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "domain-authcheck:", err)
		os.Exit(1)
	}
}

func run() error {
	domain := flag.String("domain", "", "sending domain, for example example.com")
	selector := flag.String("selector", "", "active DKIM selector")
	keyFile := flag.String("key-file", "", "active DKIM RSA private key file")
	flag.Parse()
	if strings.TrimSpace(*domain) == "" || strings.TrimSpace(*selector) == "" || strings.TrimSpace(*keyFile) == "" {
		return fmt.Errorf("-domain, -selector and -key-file are required")
	}
	key, err := dkimsign.LoadRSAPrivateKey(*keyFile)
	if err != nil {
		return err
	}
	signer, err := dkimsign.NewSigner(*domain, *selector, key)
	if err != nil {
		return err
	}
	expected, err := signer.DNSRecord()
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	result, err := domainauth.Inspect(ctx, net.DefaultResolver, *domain, *selector, expected)
	if err != nil {
		return err
	}
	encoder := json.NewEncoder(os.Stdout)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(result); err != nil {
		return err
	}
	if result.SPF.Status == "fail" || result.DKIM.Status == "fail" || result.DMARC.Status == "fail" {
		return fmt.Errorf("one or more domain authentication records failed validation")
	}
	return nil
}
