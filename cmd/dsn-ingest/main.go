// Command dsn-ingest imports one DSN message into MailForge. It is an
// operator-facing ingestion adapter until the dedicated inbound DSN receiver
// is deployed; pass the SMTP envelope RCPT TO used for this DSN.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"mailforge/internal/bounce"
	"mailforge/internal/database"
	"mailforge/internal/delivery"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "dsn-ingest:", err)
		os.Exit(1)
	}
}

func run() error {
	envelopeRecipient := flag.String("envelope-recipient", "", "VERP address from the inbound DSN SMTP RCPT TO")
	filePath := flag.String("file", "", "path to the complete RFC 5322 DSN message")
	flag.Parse()
	if strings.TrimSpace(*envelopeRecipient) == "" || strings.TrimSpace(*filePath) == "" {
		return fmt.Errorf("-envelope-recipient and -file are required")
	}
	databaseURL := strings.TrimSpace(os.Getenv("DATABASE_URL"))
	if databaseURL == "" {
		return fmt.Errorf("DATABASE_URL is required")
	}
	domain := strings.TrimSpace(os.Getenv("SMTP_BOUNCE_DOMAIN"))
	secret := strings.TrimSpace(os.Getenv("SMTP_BOUNCE_HMAC_SECRET"))
	if domain == "" || secret == "" {
		return fmt.Errorf("SMTP_BOUNCE_DOMAIN and SMTP_BOUNCE_HMAC_SECRET are required")
	}
	key, err := bounce.DecodeHexKey(secret)
	if err != nil {
		return err
	}
	generator, err := bounce.NewGenerator(domain, key)
	if err != nil {
		return err
	}
	raw, err := os.ReadFile(*filePath)
	if err != nil {
		return fmt.Errorf("read DSN file: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	db, err := database.NewPostgresPool(ctx, database.PostgresConfig{DatabaseURL: databaseURL, MaxConns: 4, MinConns: 1})
	if err != nil {
		return err
	}
	defer db.Close()

	result, err := delivery.NewRepository(db).ProcessDSN(ctx, generator, *envelopeRecipient, raw)
	if err != nil {
		return err
	}
	encoder := json.NewEncoder(os.Stdout)
	encoder.SetIndent("", "  ")
	return encoder.Encode(result)
}
