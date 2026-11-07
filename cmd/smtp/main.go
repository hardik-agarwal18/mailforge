package main

import (
	"context"
	"errors"
	"log"
	"net/http"
	"os"
	"time"

	"mailforge/internal/database"
	"mailforge/internal/message"
	"mailforge/internal/observability"
	"mailforge/internal/smtp"
)

func main() {
	databaseURL := os.Getenv("DATABASE_URL")

	if databaseURL == "" {
		log.Fatal("DATABASE_URL is required")
	}

	ctx, cancel := context.WithTimeout(
		context.Background(),
		10*time.Second,
	)

	pool, err := database.NewPostgresPool(
		ctx,
		database.PostgresConfig{
			DatabaseURL: databaseURL,
			MaxConns:    20,
			MinConns:    2,
		},
	)

	cancel()

	if err != nil {
		log.Fatal("PostgreSQL initialization failed:", err)
	}
	defer pool.Close()

	// Repository
	messageRepository := message.NewRepository(pool)

	// Service
	messageService := message.NewService(messageRepository)

	metrics := observability.NewMetrics()
	go observability.CollectPostgresPoolMetrics(context.Background(), pool, metrics, 5*time.Second)

	metricsAddr := os.Getenv("METRICS_ADDR")
	if metricsAddr == "" {
		metricsAddr = "127.0.0.1:9090"
	}

	metricsServer := &http.Server{
		Addr:              metricsAddr,
		Handler:           metrics.Handler(),
		ReadHeaderTimeout: 5 * time.Second,
	}

	go func() {
		log.Println("Prometheus metrics listening on", metricsAddr)

		if err := metricsServer.ListenAndServe(); err != nil &&
			!errors.Is(err, http.ErrServerClosed) {
			log.Println("Metrics server failed:", err)
		}
	}()

	// SMTP server
	smtpServer := smtp.NewServer(
		"127.0.0.1:2525",
		messageService,
		metrics,
	)

	log.Println("Starting MailForge SMTP server")

	if err := smtpServer.Start(); err != nil {
		log.Fatal("SMTP server failed:", err)
	}
}
