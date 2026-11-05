package delivery

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"mailforge/internal/observability"
	"mailforge/internal/outbound"
)

type Processor struct {
	repository *Repository
	sender     Sender
	metrics    *observability.Metrics
}

func NewProcessor(
	repository *Repository,
	sender Sender,
	metrics ...*observability.Metrics,
) *Processor {
	processor := &Processor{
		repository: repository,
		sender:     sender,
	}

	if len(metrics) > 0 {
		processor.metrics = metrics[0]
	}

	return processor
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

	// Measure the actual SMTP operation.
	startedAt := time.Now()
	err = p.sender.Send(
		ctx,
		job.Sender,
		job.Email,
		job.RawMessage,
	)

	duration := time.Since(startedAt)

	// SMTP succeeded.
	if err == nil {
		if p.metrics != nil {
			p.metrics.RecordDeliveryAttempt(
				duration,
				"success",
			)
		}

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

	// Permanent SMTP failure.
	if deliveryErr.Permanent {
		if p.metrics != nil {
			p.metrics.RecordDeliveryAttempt(
				duration,
				"permanent_failure",
			)
		}

		return p.repository.RecordPermanentFailure(
			ctx,
			job,
			deliveryErr.SMTPCode,
			deliveryErr.Message,
		)
	}

	// Temporary SMTP failure.
	if p.metrics != nil {
		p.metrics.RecordDeliveryAttempt(
			duration,
			"temporary_failure",
		)
	}

	return p.repository.RecordTemporaryFailure(
		ctx,
		job,
		deliveryErr.SMTPCode,
		deliveryErr.Message,
	)
}
