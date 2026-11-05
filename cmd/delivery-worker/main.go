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

func main() {
	ctx, stop := signal.NotifyContext(
		context.Background(),
		os.Interrupt,
		syscall.SIGTERM,
	)
	defer stop()

	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		log.Fatal("DATABASE_URL is required")
	}

	db, err := database.NewPostgresPool(ctx, database.PostgresConfig{
		DatabaseURL: databaseURL,
		MaxConns:    20,
		MinConns:    2,
	})
	if err != nil {
		log.Fatalf("PostgreSQL connection failed: %v", err)
	}
	defer db.Close()

	redisAddr := os.Getenv("REDIS_ADDR")
	if redisAddr == "" {
		redisAddr = "127.0.0.1:6379"
	}

	redisClient := redis.NewClient(&redis.Options{
		Addr:         redisAddr,
		Password:     os.Getenv("REDIS_PASSWORD"),
		DB:           0,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 5 * time.Second,
	})
	defer redisClient.Close()

	if err := redisClient.Ping(ctx).Err(); err != nil {
		log.Fatalf("Redis connection failed: %v", err)
	}

	metrics := observability.NewMetrics()
	metricsAddr := os.Getenv("METRICS_ADDR")
	if metricsAddr == "" {
		metricsAddr = "127.0.0.1:9091"
	}

	metricsServer := &http.Server{
		Addr:              metricsAddr,
		Handler:           metrics.Handler(),
		ReadHeaderTimeout: 5 * time.Second,
	}

	go func() {
		log.Printf(
			"Delivery worker metrics listening on %s",
			metricsAddr,
		)

		if err := metricsServer.ListenAndServe(); err != nil &&
			err != http.ErrServerClosed {
			log.Printf("Metrics server failed: %v", err)
		}
	}()

	defer func() {
		shutdownCtx, cancel := context.WithTimeout(
			context.Background(),
			5*time.Second,
		)
		defer cancel()

		if err := metricsServer.Shutdown(shutdownCtx); err != nil {
			log.Printf("Metrics server shutdown failed: %v", err)
		}
	}()

	repository := delivery.NewRepository(db)

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

	smtpClient := outbound.NewClient()

	relayHost := os.Getenv("SMTP_RELAY_HOST")

	if relayHost != "" {
		relayPort := 1025

		if port := os.Getenv("SMTP_RELAY_PORT"); port != "" {
			parsedPort, err := strconv.Atoi(port)
			if err != nil {
				log.Fatalf("Invalid SMTP_RELAY_PORT: %v", err)
			}
			relayPort = parsedPort
		}

		smtpClient, err = outbound.NewRelayClient(outbound.Config{
			RelayHost: relayHost,
			RelayPort: relayPort,
			RequireSTARTTLS: strings.EqualFold(
				os.Getenv("SMTP_REQUIRE_STARTTLS"),
				"true",
			),
		})
		if err != nil {
			log.Fatalf("SMTP relay configuration failed: %v", err)
		}

		log.Printf("Using SMTP relay %s:%d", relayHost, relayPort)
	} else {
		log.Println("Using direct MX delivery")
	}

	processor := delivery.NewProcessor(
		repository,
		smtpClient,
		metrics,
	)

	worker := delivery.NewWorker(
		consumer,
		processor,
		3,
		metrics,
	)

	log.Println("MailForge delivery worker starting")

	if err := worker.Run(ctx); err != nil {
		log.Fatalf("Worker failed: %v", err)
	}
}
