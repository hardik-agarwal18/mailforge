package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"

	"mailforge/internal/database"
	"mailforge/internal/delivery"
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

	repository := delivery.NewRepository(db)
	consumer := delivery.NewConsumer(redisClient, "worker-1")

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

	processor := delivery.NewProcessor(repository, smtpClient)

	worker := delivery.NewWorker(
		consumer,
		processor,
		3,
	)

	log.Println("MailForge delivery worker starting")

	if err := worker.Run(ctx); err != nil {
		log.Fatalf("Worker failed: %v", err)
	}
}
