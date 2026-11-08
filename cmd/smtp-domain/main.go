// smtp-domain manages DNS-verified sending domains for SMTP AUTH credentials.
// The command is intended for an operator with direct access to DATABASE_URL.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"time"

	"mailforge/internal/database"
	"mailforge/internal/smtpauth"
)

func main() {
	action := flag.String("action", "", "Operation: add, verify, list or remove")
	username := flag.String("username", "", "SMTP AUTH username that owns the domain")
	domain := flag.String("domain", "", "Sending domain (required for add, verify and remove)")
	flag.Parse()

	if *action == "" || *username == "" {
		flag.Usage()
		os.Exit(2)
	}
	if (*action == "add" || *action == "verify" || *action == "remove") && *domain == "" {
		flag.Usage()
		os.Exit(2)
	}

	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		log.Fatal("DATABASE_URL is required")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
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
	switch *action {
	case "add":
		result, err := repository.AddSendingDomain(ctx, *username, *domain)
		if err != nil {
			fatalDomainError(err)
		}
		printDomain(result)
		if result.VerifiedAt == nil {
			fmt.Println("Publish the TXT record above, wait for DNS propagation, then run with --action verify.")
		} else {
			fmt.Printf("Domain is already verified (since %s).\n", result.VerifiedAt.UTC().Format(time.RFC3339))
		}

	case "verify":
		result, err := repository.VerifySendingDomain(ctx, *username, *domain, net.DefaultResolver)
		if err != nil {
			fatalDomainError(err)
		}
		fmt.Printf("Verified %s at %s.\n", result.Domain, result.VerifiedAt.UTC().Format(time.RFC3339))
		fmt.Println("New SMTP AUTH sessions will receive this domain as an authorized sender domain.")

	case "list":
		domains, err := repository.ListSendingDomains(ctx, *username)
		if err != nil {
			log.Fatalf("list sending domains: %v", err)
		}
		if len(domains) == 0 {
			fmt.Printf("No sending domains configured for %s.\n", *username)
			return
		}
		for i, result := range domains {
			if i > 0 {
				fmt.Println()
			}
			printDomain(result)
			if result.VerifiedAt == nil {
				fmt.Println("Status: PENDING")
			} else {
				fmt.Printf("Status: VERIFIED (%s)\n", result.VerifiedAt.UTC().Format(time.RFC3339))
			}
		}

	case "remove":
		if err := repository.RemoveSendingDomain(ctx, *username, *domain); err != nil {
			fatalDomainError(err)
		}
		fmt.Printf("Removed sending-domain authorization for %s.\n", *domain)
		fmt.Println("Existing authenticated SMTP sessions retain their authorization until they reconnect.")

	default:
		flag.Usage()
		os.Exit(2)
	}
}

func printDomain(domain smtpauth.SendingDomain) {
	fmt.Printf("Domain: %s\n", domain.Domain)
	fmt.Println("DNS record type: TXT")
	fmt.Printf("DNS record name: %s\n", domain.VerificationName)
	fmt.Printf("DNS record value: %s\n", domain.VerificationToken)
}

func fatalDomainError(err error) {
	switch {
	case errors.Is(err, smtpauth.ErrCredentialNotFound):
		log.Fatal("SMTP credential does not exist")
	case errors.Is(err, smtpauth.ErrSendingDomainNotFound):
		log.Fatal("sending domain is not configured for this SMTP credential")
	case errors.Is(err, smtpauth.ErrSendingDomainAlreadyClaimed):
		log.Fatal("this domain is already assigned to another SMTP credential")
	case errors.Is(err, smtpauth.ErrDomainVerificationFailed):
		log.Fatalf("domain verification did not pass: %v", err)
	default:
		log.Fatal(err)
	}
}
