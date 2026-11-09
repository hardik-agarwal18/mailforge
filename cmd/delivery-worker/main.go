package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"

	"mailforge/internal/bounce"
	"mailforge/internal/database"
	"mailforge/internal/delivery"
	"mailforge/internal/dkimsign"
	"mailforge/internal/observability"
	"mailforge/internal/outbound"
)

const (
	defaultRedisAddr        = "127.0.0.1:6379"
	defaultMetricsAddr      = "127.0.0.1:9091"
	defaultWorkerCount      = 3
	maxWorkerCount          = 512
	defaultConnectTimeout   = 15 * time.Second
	defaultOperationTimeout = 30 * time.Second
	defaultDeliveryTimeout  = 2 * time.Minute
)

func main() {
	if err := run(); err != nil {
		log.Printf("Delivery worker terminated: %v", err)
		os.Exit(1)
	}
}

func run() error {
	// --------------------------------------------------
	// Application context and graceful shutdown
	// --------------------------------------------------

	ctx, stop := signal.NotifyContext(
		context.Background(),
		os.Interrupt,
		syscall.SIGTERM,
	)
	defer stop()

	// --------------------------------------------------
	// Configuration
	// --------------------------------------------------

	databaseURL := strings.TrimSpace(os.Getenv("DATABASE_URL"))
	if databaseURL == "" {
		return fmt.Errorf("DATABASE_URL is required")
	}

	redisAddr := envString("REDIS_ADDR", defaultRedisAddr)
	metricsAddr := envString("METRICS_ADDR", defaultMetricsAddr)

	workerConcurrency, err := envInt(
		"DELIVERY_WORKER_CONCURRENCY",
		defaultWorkerCount,
	)
	if err != nil {
		return err
	}

	if workerConcurrency < 1 || workerConcurrency > maxWorkerCount {
		return fmt.Errorf(
			"DELIVERY_WORKER_CONCURRENCY must be between 1 and %d",
			maxWorkerCount,
		)
	}

	connectTimeout, err := envDuration(
		"SMTP_CONNECT_TIMEOUT",
		defaultConnectTimeout,
	)
	if err != nil {
		return err
	}

	operationTimeout, err := envDuration(
		"SMTP_OPERATION_TIMEOUT",
		defaultOperationTimeout,
	)
	if err != nil {
		return err
	}

	deliveryTimeout, err := envDuration(
		"SMTP_DELIVERY_TIMEOUT",
		defaultDeliveryTimeout,
	)
	if err != nil {
		return err
	}
	// Validate and load DKIM credentials before opening database/Redis
	// connections so incomplete or invalid signing configuration fails fast.
	dkimSigner, dkimManager, err := loadDKIMSignerFromEnv()
	if err != nil {
		return err
	}
	bounceGenerator, err := loadBounceGeneratorFromEnv()
	if err != nil {
		return err
	}
	if bounceGenerator != nil {
		log.Printf("Recipient-specific DSN return-path enabled: domain=%s", bounceGenerator.Domain())
	} else {
		log.Println("WARNING: DSN VERP return-path disabled; remote bounces cannot be correlated")
	}
	if dkimManager != nil {
		go dkimManager.Watch(ctx, 5*time.Second)
		log.Printf("Outbound DKIM hot rotation enabled: domain=%s selector=%s", dkimManager.Domain(), dkimManager.Selector())
	} else if dkimSigner != nil {
		identity := dkimSigner.(interface {
			Domain() string
			Selector() string
		})
		log.Printf("Outbound DKIM signing enabled: domain=%s selector=%s", identity.Domain(), identity.Selector())
	} else {
		log.Println("Outbound DKIM signing disabled")
	}

	defaultRetryPolicy := delivery.DefaultRetryPolicy()
	retryInitialDelay, err := envDuration("DELIVERY_RETRY_INITIAL_DELAY", defaultRetryPolicy.InitialDelay)
	if err != nil {
		return err
	}
	retryMaxDelay, err := envDuration("DELIVERY_RETRY_MAX_DELAY", defaultRetryPolicy.MaxDelay)
	if err != nil {
		return err
	}
	retryMaxQueueAge, err := envDuration("DELIVERY_RETRY_MAX_QUEUE_AGE", defaultRetryPolicy.MaxQueueAge)
	if err != nil {
		return err
	}
	retryMaxAttempts, err := envInt("DELIVERY_RETRY_MAX_ATTEMPTS", defaultRetryPolicy.MaxAttempts)
	if err != nil {
		return err
	}
	retryPolicy := delivery.RetryPolicy{
		InitialDelay: retryInitialDelay,
		MaxDelay:     retryMaxDelay,
		MaxQueueAge:  retryMaxQueueAge,
		MaxAttempts:  retryMaxAttempts,
	}
	if err := retryPolicy.Validate(); err != nil {
		return fmt.Errorf("invalid delivery retry policy: %w", err)
	}

	// --------------------------------------------------
	// PostgreSQL
	// --------------------------------------------------

	db, err := database.NewPostgresPool(
		ctx,
		database.PostgresConfig{
			DatabaseURL: databaseURL,
			MaxConns:    20,
			MinConns:    2,
		},
	)
	if err != nil {
		return fmt.Errorf("PostgreSQL connection failed: %w", err)
	}
	defer db.Close()

	log.Println("PostgreSQL connected")

	// --------------------------------------------------
	// Redis
	// --------------------------------------------------

	redisClient := redis.NewClient(&redis.Options{
		Addr:         redisAddr,
		Password:     os.Getenv("REDIS_PASSWORD"),
		DB:           0,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 5 * time.Second,
	})

	defer func() {
		if err := redisClient.Close(); err != nil {
			log.Printf("Redis shutdown failed: %v", err)
		}
	}()

	if err := redisClient.Ping(ctx).Err(); err != nil {
		return fmt.Errorf("Redis connection failed: %w", err)
	}

	log.Printf("Redis connected: %s", redisAddr)

	// --------------------------------------------------
	// Prometheus metrics server
	// --------------------------------------------------

	metrics := observability.NewMetrics()
	go observability.CollectPostgresPoolMetrics(ctx, db, metrics, 5*time.Second)

	metricsServer := &http.Server{
		Addr:              metricsAddr,
		Handler:           metrics.Handler(),
		ReadHeaderTimeout: 5 * time.Second,
	}

	metricsServerErrors := make(chan error, 1)

	go func() {
		log.Printf(
			"Delivery worker metrics listening on %s",
			metricsAddr,
		)

		err := metricsServer.ListenAndServe()

		if err != nil && err != http.ErrServerClosed {
			metricsServerErrors <- err
		}
	}()

	defer func() {
		shutdownCtx, cancel := context.WithTimeout(
			context.Background(),
			5*time.Second,
		)
		defer cancel()

		if err := metricsServer.Shutdown(shutdownCtx); err != nil {
			log.Printf(
				"Metrics server shutdown failed: %v",
				err,
			)
		}
	}()

	// --------------------------------------------------
	// Delivery repository
	// --------------------------------------------------

	repository, err := delivery.NewRepositoryWithRetryPolicy(db, retryPolicy)
	if err != nil {
		return fmt.Errorf("configure delivery repository: %w", err)
	}

	// --------------------------------------------------
	// Redis consumer
	// --------------------------------------------------

	hostname, err := os.Hostname()
	if err != nil {
		hostname = "unknown"
	}

	consumerName := fmt.Sprintf(
		"%s-%d",
		hostname,
		os.Getpid(),
	)

	consumer := delivery.NewConsumer(
		redisClient,
		consumerName,
	)

	log.Printf("Redis consumer: %s", consumerName)

	// --------------------------------------------------
	// SMTP client configuration
	// --------------------------------------------------

	smtpConfig := outbound.Config{
		EHLOName:         envString("SMTP_EHLO_NAME", "localhost"),
		ConnectTimeout:   connectTimeout,
		OperationTimeout: operationTimeout,
		DeliveryTimeout:  deliveryTimeout,
		RequireSTARTTLS:  strings.EqualFold(os.Getenv("SMTP_REQUIRE_STARTTLS"), "true"),
	}
	smtpConfig.DKIMSigner = dkimSigner

	var smtpClient *outbound.Client

	relayHost := strings.TrimSpace(
		os.Getenv("SMTP_RELAY_HOST"),
	)

	if relayHost != "" {
		relayPort := 1025

		if port := os.Getenv("SMTP_RELAY_PORT"); port != "" {
			parsedPort, err := strconv.Atoi(port)
			if err != nil {
				return fmt.Errorf(
					"invalid SMTP_RELAY_PORT: %w",
					err,
				)
			}

			if parsedPort < 1 || parsedPort > 65535 {
				return fmt.Errorf(
					"SMTP_RELAY_PORT must be between 1 and 65535",
				)
			}

			relayPort = parsedPort
		}

		smtpConfig.RelayHost = relayHost
		smtpConfig.RelayPort = relayPort

		relayTLSMode := strings.ToLower(strings.TrimSpace(os.Getenv("SMTP_RELAY_TLS_MODE")))
		if relayTLSMode == "" {
			switch relayPort {
			case 465:
				relayTLSMode = string(outbound.TLSModeImplicit)
			case 587:
				relayTLSMode = string(outbound.TLSModeRequiredSTARTTLS)
			case 25:
				if smtpConfig.RequireSTARTTLS {
					relayTLSMode = string(outbound.TLSModeRequiredSTARTTLS)
				} else {
					relayTLSMode = string(outbound.TLSModeStartTLS)
				}
			default:
				if smtpConfig.RequireSTARTTLS {
					relayTLSMode = string(outbound.TLSModeRequiredSTARTTLS)
				} else {
					relayTLSMode = string(outbound.TLSModeNone)
				}
			}
		}
		smtpConfig.RelayTLSMode = outbound.TLSMode(relayTLSMode)

		smtpClient, err = outbound.NewRelayClient(smtpConfig)
		if err != nil {
			return fmt.Errorf(
				"SMTP relay configuration failed: %w",
				err,
			)
		}

		log.Printf(
			"Using SMTP relay %s:%d (EHLO=%s, tls_mode=%s)",
			relayHost,
			relayPort,
			smtpConfig.EHLOName,
			smtpConfig.RelayTLSMode,
		)
		if smtpConfig.RelayTLSMode == outbound.TLSModeNone {
			log.Printf("WARNING: SMTP relay transport TLS is disabled for %s:%d", relayHost, relayPort)
		}
	} else {
		smtpClient, err = outbound.NewClientWithConfig(
			smtpConfig,
		)
		if err != nil {
			return fmt.Errorf(
				"SMTP client configuration failed: %w",
				err,
			)
		}

		directTLSMode := outbound.TLSModeStartTLS
		if smtpConfig.RequireSTARTTLS {
			directTLSMode = outbound.TLSModeRequiredSTARTTLS
		}
		log.Printf("Using direct MX delivery (EHLO=%s, tls_mode=%s)", smtpConfig.EHLOName, directTLSMode)
	}

	// --------------------------------------------------
	// Delivery processor
	// --------------------------------------------------

	processor := delivery.NewProcessor(
		repository,
		smtpClient,
		metrics,
	)
	if bounceGenerator != nil {
		processor.SetBounceAddressGenerator(bounceGenerator)
	}

	// --------------------------------------------------
	// Delivery worker
	// --------------------------------------------------

	worker := delivery.NewWorker(
		consumer,
		processor,
		workerConcurrency,
		metrics,
	)

	log.Printf(
		"Starting MailForge delivery worker: concurrency=%d smtp_connect_timeout=%s smtp_operation_timeout=%s smtp_delivery_timeout=%s retry_initial=%s retry_max=%s retry_max_queue_age=%s retry_max_attempts=%d",
		workerConcurrency,
		connectTimeout,
		operationTimeout,
		deliveryTimeout,
		retryPolicy.InitialDelay,
		retryPolicy.MaxDelay,
		retryPolicy.MaxQueueAge,
		retryPolicy.MaxAttempts,
	)

	// --------------------------------------------------
	// Run worker
	// --------------------------------------------------

	workerErr := make(chan error, 1)
	go func() { workerErr <- worker.Run(ctx) }()

	select {
	case metricsErr := <-metricsServerErrors:
		stop()
		if workerShutdownErr := <-workerErr; workerShutdownErr != nil {
			return fmt.Errorf("metrics server failed: %v; worker shutdown failed: %w", metricsErr, workerShutdownErr)
		}
		return fmt.Errorf("delivery worker metrics server failed: %w", metricsErr)
	case err := <-workerErr:
		if err != nil {
			return fmt.Errorf("delivery worker failed: %w", err)
		}
	}

	log.Println("MailForge delivery worker stopped gracefully")
	return nil
}

func loadDKIMSignerFromEnv() (dkimsign.MessageSigner, *dkimsign.KeyManager, error) {
	keysetPath := strings.TrimSpace(os.Getenv("SMTP_DKIM_KEYSET_FILE"))
	domain := strings.TrimSpace(os.Getenv("SMTP_DKIM_DOMAIN"))
	selector := strings.TrimSpace(os.Getenv("SMTP_DKIM_SELECTOR"))
	keyPath := strings.TrimSpace(os.Getenv("SMTP_DKIM_PRIVATE_KEY_FILE"))

	legacyConfigured := 0
	for _, value := range []string{domain, selector, keyPath} {
		if value != "" {
			legacyConfigured++
		}
	}
	if keysetPath != "" {
		if legacyConfigured != 0 {
			return nil, nil, fmt.Errorf("use SMTP_DKIM_KEYSET_FILE or the legacy SMTP_DKIM_DOMAIN/SELECTOR/PRIVATE_KEY_FILE settings, not both")
		}
		manager, err := dkimsign.NewKeyManager(keysetPath)
		if err != nil {
			return nil, nil, fmt.Errorf("load DKIM keyset: %w", err)
		}
		return manager, manager, nil
	}
	if legacyConfigured == 0 {
		return nil, nil, nil
	}
	if legacyConfigured != 3 {
		return nil, nil, fmt.Errorf("DKIM configuration is incomplete: set SMTP_DKIM_DOMAIN, SMTP_DKIM_SELECTOR, and SMTP_DKIM_PRIVATE_KEY_FILE together")
	}
	key, err := dkimsign.LoadRSAPrivateKey(keyPath)
	if err != nil {
		return nil, nil, fmt.Errorf("load outbound DKIM key: %w", err)
	}
	signer, err := dkimsign.NewSigner(domain, selector, key)
	if err != nil {
		return nil, nil, fmt.Errorf("configure outbound DKIM signer: %w", err)
	}
	return signer, nil, nil
}

func loadBounceGeneratorFromEnv() (*bounce.Generator, error) {
	domain := strings.TrimSpace(os.Getenv("SMTP_BOUNCE_DOMAIN"))
	secret := strings.TrimSpace(os.Getenv("SMTP_BOUNCE_HMAC_SECRET"))
	if domain == "" && secret == "" {
		return nil, nil
	}
	if domain == "" || secret == "" {
		return nil, fmt.Errorf("bounce configuration is incomplete: set SMTP_BOUNCE_DOMAIN and SMTP_BOUNCE_HMAC_SECRET together")
	}
	key, err := bounce.DecodeHexKey(secret)
	if err != nil {
		return nil, err
	}
	generator, err := bounce.NewGenerator(domain, key)
	if err != nil {
		return nil, err
	}
	return generator, nil
}

// --------------------------------------------------
// Environment helpers
// --------------------------------------------------

func envString(name string, fallback string) string {
	value := strings.TrimSpace(os.Getenv(name))

	if value == "" {
		return fallback
	}

	return value
}

func envInt(name string, fallback int) (int, error) {
	value := os.Getenv(name)

	if value == "" {
		return fallback, nil
	}

	parsed, err := strconv.Atoi(value)
	if err != nil {
		return 0, fmt.Errorf(
			"%s must be an integer: %w",
			name,
			err,
		)
	}

	return parsed, nil
}

func envDuration(
	name string,
	fallback time.Duration,
) (time.Duration, error) {
	value := os.Getenv(name)

	if value == "" {
		return fallback, nil
	}

	parsed, err := time.ParseDuration(value)
	if err != nil {
		return 0, fmt.Errorf(
			"%s must be a valid duration: %w",
			name,
			err,
		)
	}

	if parsed <= 0 {
		return 0, fmt.Errorf(
			"%s must be positive",
			name,
		)
	}

	return parsed, nil
}
