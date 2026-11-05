package delivery

import (
	"context"
	"fmt"
	"log"
)

type Processor struct {
	repository *Repository
}

func NewProcessor(repository *Repository) *Processor {
	return &Processor{
		repository: repository,
	}
}

func (p *Processor) Process(
	ctx context.Context,
	event DeliveryJob,
) error {

	job, err := p.repository.ClaimRecipient(ctx, event.RecipientID)
	if err != nil {
		return fmt.Errorf("claim recipient: %w", err)
	}

	// Another worker may have already processed or claimed it.
	if job == nil {
		log.Printf(
			"Recipient already processed or unavailable: %s",
			event.RecipientID,
		)
		return nil
	}

	log.Printf(
		"Claimed recipient=%s email=%s attempts=%d message_size=%d",
		job.RecipientID,
		job.Email,
		job.Attempts,
		len(job.RawMessage),
	)

	// Actual SMTP delivery will be implemented in the next phase.
	// Do not mark this recipient as delivered here.

	return fmt.Errorf("outbound SMTP delivery not implemented yet")
}
