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

const (
	leaseHeartbeatInterval = 20 * time.Second
	leaseRenewTimeout      = 5 * time.Second
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

	// A nil job is returned only for a terminal recipient state (delivered or
	// bounced). It is safe for Worker to acknowledge this duplicate event.
	if job == nil {
		log.Printf(
			"Recipient is already in a terminal state: recipient=%s event=%s",
			event.RecipientID,
			event.EventID,
		)
		return nil
	}

	// The outbound SMTP client owns the configured SMTP_DELIVERY_TIMEOUT.
	// Do not impose a second, shorter timeout here: main.go configures the
	// sender with the deployment's delivery timeout.
	deliveryCtx, deliveryCancel := context.WithCancel(ctx)
	defer deliveryCancel()

	// Keep the database heartbeat independent of the SMTP operation context.
	heartbeatCtx, heartbeatCancel := context.WithCancel(ctx)
	defer heartbeatCancel()

	stopHeartbeat := make(chan struct{})
	heartbeatFinished := make(chan struct{})
	leaseErr := make(chan error, 1)

	go p.maintainLease(
		heartbeatCtx,
		deliveryCancel, // Losing the PostgreSQL lease must stop SMTP work.
		job,
		stopHeartbeat,
		heartbeatFinished,
		leaseErr,
	)

	startedAt := time.Now()
	sendErr := p.sender.Send(
		deliveryCtx,
		job.Sender,
		job.Email,
		job.RawMessage,
	)
	duration := time.Since(startedAt)

	// Stop the heartbeat and wait for any in-flight renewal to finish before
	// committing the outcome.
	close(stopHeartbeat)
	<-heartbeatFinished
	heartbeatCancel()
	deliveryCancel()

	select {
	case leaseFailure := <-leaseErr:
		return fmt.Errorf("delivery lease lost: %w", leaseFailure)
	default:
	}

	if ctx.Err() != nil {
		return fmt.Errorf("delivery interrupted: %w", ctx.Err())
	}

	if sendErr == nil {
		if p.metrics != nil {
			p.metrics.RecordDeliveryAttempt(duration, "success")
		}
		return p.repository.RecordSuccess(ctx, job)
	}

	var deliveryErr *outbound.DeliveryError
	if !errors.As(sendErr, &deliveryErr) {
		// Leave the Redis event pending. A generic error has no reliable SMTP
		// classification; do not fabricate a permanent/temporary result.
		return fmt.Errorf("unclassified delivery failure: %w", sendErr)
	}

	log.Printf(
		"Delivery failed: recipient=%s smtp_code=%d permanent=%t error=%v",
		job.RecipientID,
		deliveryErr.SMTPCode,
		deliveryErr.Permanent,
		deliveryErr,
	)

	if deliveryErr.Permanent {
		if p.metrics != nil {
			p.metrics.RecordDeliveryAttempt(duration, "permanent_failure")
		}
		return p.repository.RecordPermanentFailure(
			ctx,
			job,
			deliveryErr.SMTPCode,
			deliveryErr.Message,
		)
	}

	if p.metrics != nil {
		p.metrics.RecordDeliveryAttempt(duration, "temporary_failure")
	}

	return p.repository.RecordTemporaryFailure(
		ctx,
		job,
		deliveryErr.SMTPCode,
		deliveryErr.Message,
	)
}

func (p *Processor) maintainLease(
	ctx context.Context,
	cancelDelivery context.CancelFunc,
	job *ClaimedDelivery,
	stop <-chan struct{},
	finished chan<- struct{},
	leaseErr chan<- error,
) {
	defer close(finished)

	ticker := time.NewTicker(leaseHeartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-stop:
			return
		case <-ctx.Done():
			return
		case <-ticker.C:
			renewCtx, renewCancel := context.WithTimeout(
				ctx,
				leaseRenewTimeout,
			)

			err := p.repository.RenewLease(
				renewCtx,
				job,
				deliveryLease,
			)
			renewCancel()

			if err != nil {
				select {
				case leaseErr <- err:
				default:
				}
				cancelDelivery()
				return
			}
		}
	}
}
