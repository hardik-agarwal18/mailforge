package main

import (
	"context"
	"log"
	"os"
	"time"

	"mailforge/internal/database"
	"mailforge/internal/message"
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

	// SMTP server
	smtpServer := smtp.NewServer(
		"127.0.0.1:2525",
		messageService,
	)

	log.Println("Starting MailForge SMTP server")

	if err := smtpServer.Start(); err != nil {
		log.Fatal("SMTP server failed:", err)
	}
}
