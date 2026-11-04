package main

import (
	"context"
	"log"
	"os"
	"time"

	"mailforge/internal/database"
	"mailforge/internal/message"
)

func main() {
	ctx, cancel := context.WithTimeout(
		context.Background(),
		10*time.Second,
	)
	defer cancel()

	pool, err := database.NewPostgresPool(
		ctx,
		database.PostgresConfig{
			DatabaseURL: os.Getenv("DATABASE_URL"),
			MaxConns:    10,
			MinConns:    2,
		},
	)
	if err != nil {
		log.Fatal(err)
	}
	defer pool.Close()

	repository := message.NewRepository(pool)
	service := message.NewService(repository)

	result, err := service.Accept(
		ctx,
		message.NewMessage{
			Sender: "hardik@example.com",
			Recipients: []string{
				"receiver1@example.com",
				"receiver2@example.com",
			},
			RawMessage: []byte(
				"Subject: MailForge test\r\n" +
					"\r\n" +
					"Hello from MailForge!\r\n",
			),
		},
	)
	if err != nil {
		log.Fatal("Message persistence failed:", err)
	}

	log.Println("Message persisted successfully!")
	log.Println("Database ID:", result.ID)
	log.Println("Message-ID:", result.MessageID)
	log.Println("Recipients:", result.Recipients)
	log.Println("Message size:", result.MessageSize)
}
