package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"mailforge/internal/database"
	"mailforge/internal/outbox"

	"github.com/redis/go-redis/v9"
)

func main() {
	databaseURL := os.Getenv("DATABASE_URL")
	redisAddr := os.Getenv("REDIS_ADDR")
	redisPassword := os.Getenv("REDIS_PASSWORD")

	if databaseURL == "" {
		log.Fatal("DATABASE_URL is required")
	}

	if redisAddr == "" {
		redisAddr = "127.0.0.1:6379"
	}

	ctx, stop := signal.NotifyContext(
		context.Background(),
		os.Interrupt,
		syscall.SIGTERM,
	)
	defer stop()

	initCtx, cancel := context.WithTimeout(
		ctx,
		10*time.Second,
	)
	defer cancel()

	pool, err := database.NewPostgresPool(
		initCtx,
		database.PostgresConfig{
			DatabaseURL: databaseURL,
			MaxConns:    10,
			MinConns:    2,
		},
	)
	if err != nil {
		log.Fatal("PostgreSQL connection failed:", err)
	}
	defer pool.Close()

	redisClient := redis.NewClient(&redis.Options{
		Addr:         redisAddr,
		Password:     redisPassword,
		DB:           0,
		PoolSize:     10,
		ReadTimeout:  3 * time.Second,
		WriteTimeout: 3 * time.Second,
	})

	defer redisClient.Close()

	if err := redisClient.Ping(initCtx).Err(); err != nil {
		log.Fatal("Redis connection failed:", err)
	}

	log.Println("Connected to PostgreSQL")
	log.Println("Connected to Redis")

	repository := outbox.NewRepository(pool)

	publisher := outbox.NewPublisher(
		repository,
		redisClient,
	)

	publisher.Run(ctx)
}
