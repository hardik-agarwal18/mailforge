package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"mailforge/internal/database"
	"mailforge/internal/outbox"
)

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}

	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		log.Fatal("DATABASE_URL is required")
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	pool, err := database.NewPostgresPool(ctx, database.PostgresConfig{
		DatabaseURL: databaseURL,
		MaxConns:    4,
		MinConns:    1,
	})
	if err != nil {
		log.Fatalf("PostgreSQL connection failed: %v", err)
	}
	defer pool.Close()

	repository := outbox.NewRepository(pool)
	switch os.Args[1] {
	case "list-dead":
		listDead(ctx, repository)
	case "replay":
		if len(os.Args) != 3 || strings.TrimSpace(os.Args[2]) == "" {
			usage()
			os.Exit(2)
		}
		actor := os.Getenv("OUTBOX_REPLAYED_BY")
		if actor == "" {
			actor = os.Getenv("USER")
		}
		if actor == "" {
			actor = os.Getenv("USERNAME")
		}
		if actor == "" {
			log.Fatal("OUTBOX_REPLAYED_BY (or USER/USERNAME) must identify the operator")
		}

		replayCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		defer cancel()
		if err := repository.RequeueDead(replayCtx, os.Args[2], actor); err != nil {
			log.Fatalf("Replay failed: %v", err)
		}
		fmt.Printf("Requeued dead outbox event %s\n", os.Args[2])
	default:
		usage()
		os.Exit(2)
	}
}

func listDead(ctx context.Context, repository *outbox.Repository) {
	queryCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	events, err := repository.ListDead(queryCtx, 100)
	if err != nil {
		log.Fatalf("List dead events failed: %v", err)
	}
	if len(events) == 0 {
		fmt.Println("No dead outbox events.")
		return
	}

	for _, event := range events {
		fmt.Printf("ID: %s\n  type: %s\n  aggregate: %s\n  attempts: %d\n  created: %s\n  last error: %s\n",
			event.ID,
			event.EventType,
			event.AggregateID,
			event.Attempts,
			event.CreatedAt.Format(time.RFC3339),
			event.LastError,
		)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, "Usage:")
	fmt.Fprintln(os.Stderr, "  DATABASE_URL=... go run ./cmd/outbox-admin list-dead")
	fmt.Fprintln(os.Stderr, "  DATABASE_URL=... OUTBOX_REPLAYED_BY=operator go run ./cmd/outbox-admin replay <event-uuid>")
}
