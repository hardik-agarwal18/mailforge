// smtp-user creates an SMTP submission credential. The password is read from
// stdin so it does not appear in shell history or process arguments.
package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"strings"
	"time"

	"mailforge/internal/database"
	"mailforge/internal/smtpauth"
)

func main() {
	username := flag.String("username", "", "SMTP AUTH username")
	email := flag.String("email", "", "Allowed envelope sender email address")
	flag.Parse()

	if *username == "" || *email == "" {
		flag.Usage()
		os.Exit(2)
	}

	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		log.Fatal("DATABASE_URL is required")
	}

	passwordBytes, err := io.ReadAll(io.LimitReader(os.Stdin, 1025))
	if err != nil {
		log.Fatalf("read password from stdin: %v", err)
	}
	if len(passwordBytes) > 1024 {
		log.Fatal("password input is too large")
	}
	password := strings.TrimRight(string(passwordBytes), "\r\n")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	pool, err := database.NewPostgresPool(ctx, database.PostgresConfig{
		DatabaseURL: databaseURL,
		MaxConns:    4,
		MinConns:    1,
	})
	if err != nil {
		log.Fatalf("PostgreSQL initialization failed: %v", err)
	}
	defer pool.Close()

	repository := smtpauth.NewRepository(pool)
	if err := repository.Create(ctx, *username, *email, password); err != nil {
		log.Fatalf("create SMTP credential: %v", err)
	}

	fmt.Printf("SMTP credential created for %s (%s)\n", *username, *email)
}
