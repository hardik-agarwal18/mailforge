package main

import (
	"context"
	"fmt"
	"log"
	"os"

	"mailforge/internal/outbound"
)

func main() {
	if len(os.Args) != 2 {
		log.Fatal("Usage: go run ./cmd/mxcheck <email-address>")
	}

	resolver := outbound.NewMXResolver()

	records, err := resolver.Lookup(context.Background(), os.Args[1])
	if err != nil {
		log.Fatal(err)
	}

	for _, record := range records {
		fmt.Printf(
			"Host: %s | Priority: %d\n",
			record.Host,
			record.Pref,
		)
	}
}
