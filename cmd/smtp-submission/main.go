package main

import (
	"context"
	"crypto/tls"
	"errors"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"mailforge/internal/database"
	"mailforge/internal/message"
	"mailforge/internal/observability"
	"mailforge/internal/redisclient"

	"github.com/redis/go-redis/v9"
	"mailforge/internal/smtp"
	"mailforge/internal/smtpauth"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		log.Fatal("DATABASE_URL is required")
	}

	certFile := os.Getenv("SMTP_TLS_CERT_FILE")
	keyFile := os.Getenv("SMTP_TLS_KEY_FILE")
	if certFile == "" || keyFile == "" {
		log.Fatal("SMTP_TLS_CERT_FILE and SMTP_TLS_KEY_FILE are required; submission never accepts plaintext credentials")
	}

	certificate, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		log.Fatalf("load SMTP TLS certificate: %v", err)
	}

	dbCtx, cancelDB := context.WithTimeout(ctx, 10*time.Second)
	defer cancelDB()

	pool, err := database.NewPostgresPool(dbCtx, database.PostgresConfig{
		DatabaseURL: databaseURL,
		MaxConns:    20,
		MinConns:    2,
	})
	if err != nil {
		log.Fatalf("PostgreSQL initialization failed: %v", err)
	}
	defer pool.Close()

	redisClient, err := redisclient.NewFromEnv(redis.Options{
		Addr:         "127.0.0.1:6379",
		DB:           0,
		PoolSize:     20,
		DialTimeout:  3 * time.Second,
		ReadTimeout:  3 * time.Second,
		WriteTimeout: 3 * time.Second,
		MaxRetries:   1,
	})
	if err != nil {
		log.Fatalf("Redis configuration failed: %v", err)
	}
	defer func() {
		if err := redisClient.Close(); err != nil {
			log.Printf("Redis shutdown failed: %v", err)
		}
	}()

	redisCtx, cancelRedis := context.WithTimeout(ctx, 5*time.Second)
	if err := redisClient.Ping(redisCtx).Err(); err != nil {
		cancelRedis()
		log.Fatalf("Redis is required for distributed SMTP authentication rate limiting: %v", err)
	}
	cancelRedis()
	rateLimiter, err := smtp.NewRedisAuthAttemptLimiter(
		redisClient,
		envString("SMTP_AUTH_RATE_LIMIT_PREFIX", "mailforge:smtp:auth:v1"),
	)
	if err != nil {
		log.Fatalf("configure distributed SMTP authentication rate limiter: %v", err)
	}
	log.Println("Distributed SMTP authentication rate limiting enabled via Redis (fail-closed)")

	limits := smtp.DefaultServerLimits()
	limits.MaxConnections = envInt("SMTP_MAX_CONNECTIONS", limits.MaxConnections)
	limits.MaxRecipients = envInt("SMTP_MAX_RECIPIENTS", limits.MaxRecipients)
	limits.MaxConcurrentAuth = envInt("SMTP_MAX_CONCURRENT_AUTH", limits.MaxConcurrentAuth)
	limits.MaxConcurrentData = envInt("SMTP_MAX_CONCURRENT_DATA", limits.MaxConcurrentData)
	limits.AuthAttemptsPerIP = envInt("SMTP_AUTH_ATTEMPTS_PER_IP", limits.AuthAttemptsPerIP)
	limits.AuthAttemptsPerUsername = envInt("SMTP_AUTH_ATTEMPTS_PER_USERNAME", limits.AuthAttemptsPerUsername)
	limits.AuthWindow = time.Duration(envInt("SMTP_AUTH_WINDOW_SECONDS", int(limits.AuthWindow.Seconds()))) * time.Second

	quotas := message.DefaultSubmissionQuotaLimits()
	quotas.MaxMessagesPerAccountPerHour = envInt("SMTP_MAX_SUBMISSIONS_PER_ACCOUNT_PER_HOUR", quotas.MaxMessagesPerAccountPerHour)
	quotas.MaxMessagesPerIPPerHour = envInt("SMTP_MAX_SUBMISSIONS_PER_IP_PER_HOUR", quotas.MaxMessagesPerIPPerHour)
	quotas.MaxRecipientsPerAccountPerHour = envInt("SMTP_MAX_RECIPIENTS_PER_ACCOUNT_PER_HOUR", quotas.MaxRecipientsPerAccountPerHour)
	quotas.MaxRecipientsPerIPPerHour = envInt("SMTP_MAX_RECIPIENTS_PER_IP_PER_HOUR", quotas.MaxRecipientsPerIPPerHour)

	messageRepository := message.NewRepositoryWithQuota(pool, quotas)
	messageService := message.NewService(messageRepository)
	credentialRepository := smtpauth.NewRepository(pool)
	metrics := observability.NewMetrics()
	go observability.CollectPostgresPoolMetrics(ctx, pool, metrics, 5*time.Second)

	tlsConfig := &tls.Config{
		Certificates: []tls.Certificate{certificate},
		MinVersion:   tls.VersionTLS12,
	}

	submissionAddr := os.Getenv("SMTP_SUBMISSION_ADDR")
	if submissionAddr == "" {
		submissionAddr = ":587"
	}

	submissionServer, err := smtp.NewSubmissionServerWithRateLimiter(
		submissionAddr,
		messageService,
		metrics,
		tlsConfig,
		credentialRepository,
		limits,
		rateLimiter,
	)
	if err != nil {
		log.Fatalf("configure SMTP submission server: %v", err)
	}

	metricsAddr := os.Getenv("SUBMISSION_METRICS_ADDR")
	if metricsAddr == "" {
		metricsAddr = "127.0.0.1:9095"
	}
	metricsServer := &http.Server{
		Addr:              metricsAddr,
		Handler:           metrics.Handler(),
		ReadHeaderTimeout: 5 * time.Second,
	}
	go func() {
		log.Printf("SMTP submission metrics listening on %s", metricsAddr)
		if err := metricsServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Printf("SMTP submission metrics server failed: %v", err)
			stop()
		}
	}()
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := metricsServer.Shutdown(shutdownCtx); err != nil {
			log.Printf("metrics server shutdown failed: %v", err)
		}
	}()

	log.Printf("Mailforge authenticated SMTP submission listening on %s", submissionAddr)
	if err := submissionServer.StartContext(ctx); err != nil {
		log.Fatalf("SMTP submission server failed: %v", err)
	}
}

func envString(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}

func envInt(name string, fallback int) int {
	value := os.Getenv(name)
	if value == "" {
		return fallback
	}
	parsed, err := strconv.Atoi(value)
	if err != nil || parsed <= 0 {
		log.Fatalf("%s must be a positive integer", name)
	}
	return parsed
}
