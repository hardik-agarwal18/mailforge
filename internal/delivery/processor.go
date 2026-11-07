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
	leaseHeartbeatInterval    = 20 * time.Second
	leaseRenewTimeout         = 5 * time.Second
	outcomePersistenceTimeout = 10 * time.Second
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

	// A nil job means the recipient is terminal, including an expired in-flight
	// lease that ClaimRecipient conservatively reconciled to unknown. It is safe
	// for Worker to acknowledge this duplicate/stale event.
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
	if p.metrics != nil {
		p.metrics.DeliveryAttemptsInFlight.Inc()
	}
	var transportInfo outbound.DeliveryInfo
	var sendErr error
	if detailedSender, ok := p.sender.(DetailedSender); ok {
		transportInfo, sendErr = detailedSender.SendWithResult(
			deliveryCtx,
			job.Sender,
			job.Email,
			job.RawMessage,
		)
	} else {
		sendErr = p.sender.Send(
			deliveryCtx,
			job.Sender,
			job.Email,
			job.RawMessage,
		)
	}
	duration := time.Since(startedAt)
	if p.metrics != nil {
		p.metrics.DeliveryAttemptsInFlight.Dec()
	}

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

	// Once the SMTP call has returned, its outcome must be committed even
	// during graceful shutdown. Otherwise a successful remote acceptance can
	// be forgotten and replayed after the PostgreSQL lease expires. Preserve
	// context values but detach cancellation for this bounded state transition.
	outcomeCtx, outcomeCancel := context.WithTimeout(context.WithoutCancel(ctx), outcomePersistenceTimeout)
	defer outcomeCancel()

	if sendErr == nil {
		if p.metrics != nil {
			p.metrics.RecordDeliveryAttempt(duration, "success")
		}
		return p.repository.RecordSuccessWithDetails(outcomeCtx, job, SMTPFailureDetails{
			RemoteHost:     transportInfo.RemoteHost,
			TLSUsed:        transportInfo.TLSUsed,
			TLSMode:        transportInfo.TLSMode,
			TLSVersion:     transportInfo.TLSVersion,
			TLSCipherSuite: transportInfo.TLSCipherSuite,
		})
	}

	var deliveryErr *outbound.DeliveryError
	if !errors.As(sendErr, &deliveryErr) {
		if p.metrics != nil {
			p.metrics.RecordDeliveryFailure("unknown", "unclassified")
		}
		// Leave the Redis event pending. A generic error has no reliable SMTP
		// classification; do not fabricate a permanent/temporary result.
		return fmt.Errorf("unclassified delivery failure: %w", sendErr)
	}

	log.Printf(
		"Delivery failed: recipient=%s smtp_stage=%s smtp_code=%d enhanced_status=%s permanent=%t error=%v",
		job.RecipientID,
		deliveryErr.Stage,
		deliveryErr.SMTPCode,
		deliveryErr.EnhancedStatusCode,
		deliveryErr.Permanent,
		deliveryErr,
	)

	details := SMTPFailureDetails{
		EnhancedStatusCode: deliveryErr.EnhancedStatusCode,
		Stage:              deliveryErr.Stage,
		RemoteHost:         transportInfo.RemoteHost,
		TLSUsed:            transportInfo.TLSUsed,
		TLSMode:            transportInfo.TLSMode,
		TLSVersion:         transportInfo.TLSVersion,
		TLSCipherSuite:     transportInfo.TLSCipherSuite,
	}

	if deliveryErr.Ambiguous {
		if p.metrics != nil {
			p.metrics.RecordDeliveryAttempt(duration, "unknown")
			p.metrics.RecordDeliveryFailure(deliveryErr.Stage, "ambiguous")
		}
		return p.repository.RecordUnknownOutcome(outcomeCtx, job, deliveryErr.Message, details)
	}

	if deliveryErr.Permanent {
		if p.metrics != nil {
			p.metrics.RecordDeliveryAttempt(duration, "permanent_failure")
			p.metrics.RecordDeliveryFailure(deliveryErr.Stage, "permanent")
		}
		return p.repository.RecordPermanentFailureWithDetails(
			outcomeCtx,
			job,
			deliveryErr.SMTPCode,
			deliveryErr.Message,
			details,
		)
	}

	if p.metrics != nil {
		p.metrics.RecordDeliveryAttempt(duration, "temporary_failure")
		p.metrics.RecordDeliveryFailure(deliveryErr.Stage, "temporary")
	}

	return p.repository.RecordTemporaryFailureWithDetails(
		outcomeCtx,
		job,
		deliveryErr.SMTPCode,
		deliveryErr.Message,
		details,
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
				if p.metrics != nil {
					p.metrics.DeliveryLeaseRenewFailures.Inc()
				}
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
