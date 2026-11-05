package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/redis/go-redis/v9"

	"mailforge/internal/database"
	"mailforge/internal/delivery"
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
		MaxConns:    10,
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
		Addr:     redisAddr,
		Password: os.Getenv("REDIS_PASSWORD"),
		DB:       0,
	})
	defer redisClient.Close()

	if err := redisClient.Ping(ctx).Err(); err != nil {
		log.Fatalf("Redis connection failed: %v", err)
	}

	repository := delivery.NewRepository(db)
	consumer := delivery.NewConsumer(redisClient, "worker-1")
	processor := delivery.NewProcessor(repository)

	worker := delivery.NewWorker(consumer, processor)

	log.Println("MailForge delivery worker starting")

	if err := worker.Run(ctx); err != nil {
		log.Fatalf("Worker failed: %v", err)
	}
}
