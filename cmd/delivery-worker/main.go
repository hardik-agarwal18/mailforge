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

	"mailforge/internal/database"
	"mailforge/internal/delivery"
	"mailforge/internal/observability"
	"mailforge/internal/outbound"
)

const (
	defaultRedisAddr        = "127.0.0.1:6379"
	defaultMetricsAddr      = "127.0.0.1:9091"
	defaultWorkerCount      = 3
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

	if workerConcurrency < 1 {
		return fmt.Errorf(
			"DELIVERY_WORKER_CONCURRENCY must be at least 1",
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

	repository := delivery.NewRepository(db)

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

		smtpClient, err = outbound.NewRelayClient(smtpConfig)
		if err != nil {
			return fmt.Errorf(
				"SMTP relay configuration failed: %w",
				err,
			)
		}

		log.Printf(
			"Using SMTP relay %s:%d (EHLO=%s, require_starttls=%t)",
			relayHost,
			relayPort,
			smtpConfig.EHLOName,
			smtpConfig.RequireSTARTTLS,
		)
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

		log.Printf("Using direct MX delivery (EHLO=%s, require_starttls=%t)", smtpConfig.EHLOName, smtpConfig.RequireSTARTTLS)
	}

	// --------------------------------------------------
	// Delivery processor
	// --------------------------------------------------

	processor := delivery.NewProcessor(
		repository,
		smtpClient,
		metrics,
	)

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
		"Starting MailForge delivery worker: concurrency=%d smtp_connect_timeout=%s smtp_operation_timeout=%s smtp_delivery_timeout=%s",
		workerConcurrency,
		connectTimeout,
		operationTimeout,
		deliveryTimeout,
	)

	// --------------------------------------------------
	// Run worker
	// --------------------------------------------------

	if err := worker.Run(ctx); err != nil {
		return fmt.Errorf("delivery worker failed: %w", err)
	}

	log.Println("MailForge delivery worker stopped gracefully")

	return nil
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
