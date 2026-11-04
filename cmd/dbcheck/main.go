package main

import (
	"context"
	"log"
	"os"
	"time"

	"mailforge/internal/database"
)

func main() {
	databaseURL := os.Getenv("DATABASE_URL")

	ctx, cancel := context.WithTimeout(
		context.Background(),
		10*time.Second,
	)
	defer cancel()

	pool, err := database.NewPostgresPool(
		ctx,
		database.PostgresConfig{
			DatabaseURL: databaseURL,
			MaxConns:    10,
			MinConns:    2,
		},
	)

	if err != nil {
		log.Fatal("Database connection failed:", err)
	}

	defer pool.Close()

	log.Println("Connected to PostgreSQL successfully!")

	var databaseName string

	err = pool.QueryRow(
		ctx,
		"SELECT current_database()",
	).Scan(&databaseName)

	if err != nil {
		log.Fatal("Query failed:", err)
	}

	log.Println("Database:", databaseName)
	log.Println("Total connections:", pool.Stat().TotalConns())
}
