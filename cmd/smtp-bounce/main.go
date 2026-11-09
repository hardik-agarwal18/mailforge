// Command smtp-bounce runs MailForge's restricted inbound SMTP endpoint for
// per-recipient VERP delivery-status notifications.
package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"mailforge/internal/bounce"
	"mailforge/internal/bouncereceiver"
	"mailforge/internal/database"
	"mailforge/internal/delivery"
	"mailforge/internal/dsn"
)

type dsnProcessor struct {
	repository *delivery.Repository
	generator  *bounce.Generator
}

func (p *dsnProcessor) ProcessDSN(ctx context.Context, envelopeRecipient string, raw []byte) error {
	result, err := p.repository.ProcessDSN(ctx, p.generator, envelopeRecipient, raw)
	if err != nil {
		if errors.Is(err, dsn.ErrInvalidReport) ||
			errors.Is(err, delivery.ErrDSNRecipientMismatch) ||
			errors.Is(err, delivery.ErrDSNRecipientMissing) {
			return err
		}
		return bouncereceiver.TemporaryError{Err: err}
	}
	log.Printf(
		"DSN committed recipient_id=%s action=%s status=%s duplicate=%t suppressed=%t",
		result.RecipientID,
		result.Action,
		result.Status,
		result.Duplicate,
		result.Suppressed,
	)
	return nil
}

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	databaseURL := strings.TrimSpace(os.Getenv("DATABASE_URL"))
	if databaseURL == "" {
		return errors.New("DATABASE_URL is required")
	}
	domain := strings.TrimSpace(os.Getenv("SMTP_BOUNCE_DOMAIN"))
	secret := strings.TrimSpace(os.Getenv("SMTP_BOUNCE_HMAC_SECRET"))
	if domain == "" || secret == "" {
		return errors.New("SMTP_BOUNCE_DOMAIN and SMTP_BOUNCE_HMAC_SECRET are required")
	}
	key, err := bounce.DecodeHexKey(secret)
	if err != nil {
		return err
	}
	generator, err := bounce.NewGenerator(domain, key)
	if err != nil {
		return err
	}

	dbCtx, cancelDB := context.WithTimeout(ctx, 10*time.Second)
	defer cancelDB()
	pool, err := database.NewPostgresPool(dbCtx, database.PostgresConfig{
		DatabaseURL: databaseURL,
		MaxConns:    10,
		MinConns:    1,
	})
	if err != nil {
		return fmt.Errorf("PostgreSQL initialization failed: %w", err)
	}
	defer pool.Close()

	var tlsConfig *tls.Config
	certFile := strings.TrimSpace(os.Getenv("SMTP_BOUNCE_TLS_CERT_FILE"))
	keyFile := strings.TrimSpace(os.Getenv("SMTP_BOUNCE_TLS_KEY_FILE"))
	clientCAFile := strings.TrimSpace(os.Getenv("SMTP_BOUNCE_TLS_CLIENT_CA_FILE"))
	if (certFile == "") != (keyFile == "") {
		return errors.New("SMTP_BOUNCE_TLS_CERT_FILE and SMTP_BOUNCE_TLS_KEY_FILE must be set together")
	}
	if clientCAFile != "" && certFile == "" {
		return errors.New("SMTP_BOUNCE_TLS_CLIENT_CA_FILE requires the bounce server TLS certificate and key")
	}
	if certFile != "" {
		certificate, err := tls.LoadX509KeyPair(certFile, keyFile)
		if err != nil {
			return fmt.Errorf("load inbound bounce TLS certificate: %w", err)
		}
		tlsConfig = &tls.Config{Certificates: []tls.Certificate{certificate}, MinVersion: tls.VersionTLS12}
		if clientCAFile != "" {
			caPEM, err := os.ReadFile(clientCAFile)
			if err != nil {
				return fmt.Errorf("read inbound bounce client CA: %w", err)
			}
			clientCAs := x509.NewCertPool()
			if ok := clientCAs.AppendCertsFromPEM(caPEM); !ok {
				return errors.New("SMTP_BOUNCE_TLS_CLIENT_CA_FILE contains no valid CA certificates")
			}
			tlsConfig.ClientCAs = clientCAs
			tlsConfig.ClientAuth = tls.RequireAndVerifyClientCert
		}
	}

	maxConnections, err := envPositiveInt("SMTP_BOUNCE_MAX_CONNECTIONS", 100)
	if err != nil {
		return err
	}
	maxDSNsPerPeer, err := envPositiveInt("SMTP_BOUNCE_MAX_DSNS_PER_PEER_PER_MINUTE", 300)
	if err != nil {
		return err
	}
	processTimeout, err := envDuration("SMTP_BOUNCE_PROCESS_TIMEOUT", 20*time.Second)
	if err != nil {
		return err
	}
	config := bouncereceiver.Config{
		Address:                 envString("SMTP_BOUNCE_LISTEN_ADDR", "127.0.0.1:2526"),
		Hostname:                envString("SMTP_BOUNCE_HOSTNAME", "localhost"),
		MaxConnections:          maxConnections,
		MaxDSNsPerPeerPerMinute: maxDSNsPerPeer,
		TrustedPeerCIDRs:        envCSV("SMTP_BOUNCE_TRUSTED_PEERS"),
		ProcessTimeout:          processTimeout,
		TLSConfig:               tlsConfig,
	}
	processor := &dsnProcessor{repository: delivery.NewRepository(pool), generator: generator}
	server, err := bouncereceiver.New(config, generator, processor)
	if err != nil {
		return fmt.Errorf("configure inbound DSN receiver: %w", err)
	}

	log.Printf("MailForge inbound DSN receiver listening on %s for domain %s", config.Address, generator.Domain())
	if tlsConfig == nil {
		log.Printf("WARNING: inbound STARTTLS is disabled; use a trusted front MTA or configure SMTP_BOUNCE_TLS_CERT_FILE and SMTP_BOUNCE_TLS_KEY_FILE")
	}
	if err := server.ListenAndServe(ctx); err != nil {
		return fmt.Errorf("inbound DSN receiver stopped: %w", err)
	}
	log.Printf("MailForge inbound DSN receiver stopped")
	return nil
}

func envCSV(name string) []string {
	value := strings.TrimSpace(os.Getenv(name))
	if value == "" {
		return nil
	}
	parts := strings.Split(value, ",")
	result := make([]string, 0, len(parts))
	for _, part := range parts {
		if part = strings.TrimSpace(part); part != "" {
			result = append(result, part)
		}
	}
	return result
}

func envString(name, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(name)); value != "" {
		return value
	}
	return fallback
}

func envPositiveInt(name string, fallback int) (int, error) {
	value := strings.TrimSpace(os.Getenv(name))
	if value == "" {
		return fallback, nil
	}
	parsed, err := strconv.Atoi(value)
	if err != nil || parsed <= 0 {
		return 0, fmt.Errorf("%s must be a positive integer", name)
	}
	return parsed, nil
}

func envDuration(name string, fallback time.Duration) (time.Duration, error) {
	value := strings.TrimSpace(os.Getenv(name))
	if value == "" {
		return fallback, nil
	}
	parsed, err := time.ParseDuration(value)
	if err != nil || parsed <= 0 {
		return 0, fmt.Errorf("%s must be a positive Go duration", name)
	}
	return parsed, nil
}
