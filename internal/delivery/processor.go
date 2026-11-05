package delivery

import (
	"context"
	"errors"
	"fmt"
	"log"

	"mailforge/internal/outbound"
)

type Processor struct {
	repository *Repository
	sender     Sender
}

func NewProcessor(repository *Repository, sender Sender) *Processor {
	return &Processor{
		repository: repository,
		sender:     sender,
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

	if job == nil {
		log.Printf(
			"Recipient already processed or unavailable: %s",
			event.RecipientID,
		)
		return nil
	}

	err = p.sender.Send(
		ctx,
		job.Sender,
		job.Email,
		job.RawMessage,
	)

	if err == nil {
		return p.repository.RecordSuccess(ctx, job)
	}

	var deliveryErr *outbound.DeliveryError

	if !errors.As(err, &deliveryErr) {
		return fmt.Errorf("unclassified delivery failure: %w", err)
	}

	log.Printf(
		"Delivery failed: recipient=%s smtp_code=%d permanent=%t error=%v",
		job.RecipientID,
		deliveryErr.SMTPCode,
		deliveryErr.Permanent,
		deliveryErr,
	)

	if deliveryErr.Permanent {
		return p.repository.RecordPermanentFailure(
			ctx,
			job,
			deliveryErr.SMTPCode,
			deliveryErr.Message,
		)
	}

	return p.repository.RecordTemporaryFailure(
		ctx,
		job,
		deliveryErr.SMTPCode,
		deliveryErr.Message,
	)
}
