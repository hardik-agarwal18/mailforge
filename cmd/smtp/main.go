package main

import (
	"log"

	"mailforge/internal/smtp"
)

func main() {
	server := smtp.NewServer("127.0.0.1:2525")

	if err := server.Start(); err != nil {
		log.Fatal(err)
	}
}
