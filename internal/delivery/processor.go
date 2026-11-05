package delivery

import (
	"context"
	"fmt"
	"log"
)

type Processor struct{}

func NewProcessor() *Processor {
	return &Processor{}
}

func (p *Processor) Process(ctx context.Context, job DeliveryJob) error {
	log.Printf(
		"Processing delivery: event=%s recipient=%s email=%s",
		job.EventID,
		job.RecipientID,
		job.Email,
	)

	// Actual delivery logic will be implemented later.
	// For now, this only verifies that the event was consumed.

	if job.Email == "" {
		return fmt.Errorf("recipient email is empty")
	}

	return nil
}
