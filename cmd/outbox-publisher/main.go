package main

import (
	"context"
	"errors"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"mailforge/internal/database"
	"mailforge/internal/observability"
	"mailforge/internal/outbox"
	"mailforge/internal/redisclient"

	"github.com/redis/go-redis/v9"
)

func main() {
	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		log.Fatal("DATABASE_URL is required")
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	initCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	pool, err := database.NewPostgresPool(initCtx, database.PostgresConfig{
		DatabaseURL: databaseURL,
		MaxConns:    10,
		MinConns:    2,
	})
	if err != nil {
		log.Fatal("PostgreSQL connection failed:", err)
	}
	defer pool.Close()

	redisClient, err := redisclient.NewFromEnv(redis.Options{
		Addr:         "127.0.0.1:6379",
		DB:           0,
		PoolSize:     10,
		ReadTimeout:  3 * time.Second,
		WriteTimeout: 3 * time.Second,
	})
	if err != nil {
		log.Fatal("Redis configuration failed:", err)
	}
	defer func() {
		if err := redisClient.Close(); err != nil {
			log.Printf("Redis shutdown failed: %v", err)
		}
	}()

	if err := redisClient.Ping(initCtx).Err(); err != nil {
		log.Fatal("Redis connection failed:", err)
	}
	log.Println("Connected to PostgreSQL and Redis")

	metrics := observability.NewMetrics()
	go observability.CollectPostgresPoolMetrics(ctx, pool, metrics, 5*time.Second)

	metricsAddr := os.Getenv("OUTBOX_METRICS_ADDR")
	if metricsAddr == "" {
		metricsAddr = "127.0.0.1:9096"
	}
	metricsServer := &http.Server{
		Addr:              metricsAddr,
		Handler:           metrics.Handler(),
		ReadHeaderTimeout: 5 * time.Second,
	}
	metricsServerErrors := make(chan error, 1)
	go func() {
		log.Printf("Outbox publisher metrics listening on %s", metricsAddr)
		if err := metricsServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			metricsServerErrors <- err
		}
	}()
	defer func() {
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer shutdownCancel()
		if err := metricsServer.Shutdown(shutdownCtx); err != nil {
			log.Printf("Metrics server shutdown failed: %v", err)
		}
	}()

	repository := outbox.NewRepository(pool)
	publisher := outbox.NewPublisher(repository, redisClient, metrics)

	batchSize := 50
	if rawBatchSize := os.Getenv("OUTBOX_PUBLISH_BATCH_SIZE"); rawBatchSize != "" {
		parsedBatchSize, parseErr := strconv.Atoi(rawBatchSize)
		if parseErr != nil || parsedBatchSize < 1 || parsedBatchSize > 1000 {
			log.Fatal("OUTBOX_PUBLISH_BATCH_SIZE must be an integer between 1 and 1000")
		}
		batchSize = parsedBatchSize
	}
	publisher.SetBatchSize(batchSize)

	publisherDone := make(chan struct{})
	go func() {
		defer close(publisherDone)
		publisher.Run(ctx)
	}()

	select {
	case err := <-metricsServerErrors:
		stop()
		<-publisherDone
		log.Fatalf("outbox publisher metrics server failed: %v", err)
	case <-publisherDone:
	}
}
