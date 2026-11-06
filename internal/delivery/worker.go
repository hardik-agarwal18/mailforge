package delivery

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"sync"
	"time"

	"mailforge/internal/observability"
)

const redisPendingHeartbeatInterval = 30 * time.Second

type Worker struct {
	consumer    *Consumer
	processor   *Processor
	concurrency int
	metrics     *observability.Metrics
}

func NewWorker(
	consumer *Consumer,
	processor *Processor,
	concurrency int,
	metrics ...*observability.Metrics,
) *Worker {
	if concurrency < 1 {
		concurrency = 1
	}

	worker := &Worker{
		consumer:    consumer,
		processor:   processor,
		concurrency: concurrency,
	}

	if len(metrics) > 0 {
		worker.metrics = metrics[0]
	}

	return worker
}

func (w *Worker) Run(ctx context.Context) error {
	if err := w.consumer.CreateGroup(ctx); err != nil {
		return err
	}

	if w.metrics != nil {
		go w.collectRedisMetrics(ctx, 5*time.Second)
	}
	go w.runStreamRetention(ctx)

	log.Printf("Delivery worker started with concurrency=%d", w.concurrency)

	claimCursor := "0-0"

	for {
		if ctx.Err() != nil {
			return nil
		}

		staleJobs, nextCursor, claimErr := w.consumer.ClaimStale(
			ctx,
			claimCursor,
			10,
		)
		if nextCursor != "" {
			claimCursor = nextCursor
		}
		if claimErr != nil {
			if ctx.Err() != nil {
				return nil
			}
			log.Printf("Failed to recover pending jobs: %v", claimErr)
		}

		// Process valid events even if a different event in the same batch
		// could not be quarantined.
		w.processBatch(ctx, staleJobs)

		if ctx.Err() != nil {
			return nil
		}

		jobs, readErr := w.consumer.Read(ctx, 10)
		if readErr != nil {
			if ctx.Err() != nil {
				return nil
			}
			log.Printf("Failed to read delivery jobs: %v", readErr)
		}
		w.processBatch(ctx, jobs)

		// Avoid a tight loop during Redis outages. Read normally blocks for up
		// to five seconds, but XAUTOCLAIM failures do not.
		if claimErr != nil && readErr != nil && len(staleJobs) == 0 && len(jobs) == 0 {
			if !waitContext(ctx, time.Second) {
				return nil
			}
		}
	}
}

func waitContext(ctx context.Context, duration time.Duration) bool {
	timer := time.NewTimer(duration)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func (w *Worker) processBatch(
	ctx context.Context,
	jobs []DeliveryJob,
) {
	semaphore := make(chan struct{}, w.concurrency)
	var wg sync.WaitGroup

loop:
	for _, job := range jobs {
		select {
		case semaphore <- struct{}{}:
		case <-ctx.Done():
			break loop
		}

		wg.Add(1)
		go func(job DeliveryJob) {
			defer wg.Done()
			defer func() { <-semaphore }()
			w.processJob(ctx, job)
		}(job)
	}

	wg.Wait()
}

func (w *Worker) processJob(
	ctx context.Context,
	job DeliveryJob,
) {
	startedAt := time.Now()

	if w.metrics != nil {
		w.metrics.DeliveryJobsActive.Inc()
		defer w.metrics.DeliveryJobsActive.Dec()
		defer func() {
			w.metrics.DeliveryJobDuration.Observe(
				time.Since(startedAt).Seconds(),
			)
		}()
	}

	// Keep the Redis PEL entry alive during long SMTP operations. PostgreSQL
	// lease tokens still decide which worker may actually send/commit.
	pendingCtx, stopPendingHeartbeat := context.WithCancel(ctx)
	pendingHeartbeatDone := make(chan struct{})
	go w.keepPendingAlive(pendingCtx, job.EventID, pendingHeartbeatDone)

	processErr := w.processor.Process(ctx, job)

	stopPendingHeartbeat()
	<-pendingHeartbeatDone

	if processErr != nil {
		if errors.Is(processErr, ErrRecipientBusy) {
			log.Printf(
				"Recipient is owned by another worker; leaving event pending: event=%s",
				job.EventID,
			)
			return
		}

		if errors.Is(processErr, ErrRecipientNotReady) {
			log.Printf(
				"Recipient is not ready; leaving event pending: event=%s",
				job.EventID,
			)
			return
		}

		if errors.Is(processErr, ErrRecipientScheduled) {
			// The deferred recipient already has a retry event committed through
			// the PostgreSQL transactional outbox. This old/duplicate event can
			// safely be acknowledged.
			w.acknowledge(ctx, job.EventID)
			return
		}

		if errors.Is(processErr, ErrRecipientNotFound) || errors.Is(processErr, ErrRecipientDataMissing) {
			payload, _ := json.Marshal(job)
			if err := w.consumer.QuarantineEvent(
				ctx,
				job.EventID,
				processErr.Error(),
				payload,
			); err != nil {
				log.Printf("Failed to quarantine orphan event %s: %v", job.EventID, err)
				return
			}
			log.Printf("Quarantined orphan delivery event %s", job.EventID)
			return
		}

		if w.metrics != nil {
			w.metrics.DeliveryJobsTotal.
				WithLabelValues("processing_error").
				Inc()
		}

		log.Printf("Processing failed: event=%s error=%v", job.EventID, processErr)
		// Leave the event pending. It will be recovered after pendingIdleTimeout.
		return
	}

	w.acknowledge(ctx, job.EventID)
}

func (w *Worker) acknowledge(ctx context.Context, eventID string) {
	if err := w.consumer.Acknowledge(ctx, eventID); err != nil {
		if w.metrics != nil {
			w.metrics.DeliveryJobsTotal.
				WithLabelValues("acknowledgement_error").
				Inc()
		}
		log.Printf("Acknowledgement failed: event=%s error=%v", eventID, err)
		return
	}

	if w.metrics != nil {
		w.metrics.DeliveryJobsTotal.
			WithLabelValues("acknowledged").
			Inc()
	}
	log.Printf("Acknowledged event %s", eventID)
}

func (w *Worker) keepPendingAlive(
	ctx context.Context,
	eventID string,
	done chan<- struct{},
) {
	defer close(done)

	ticker := time.NewTicker(redisPendingHeartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			heartbeatCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
			err := w.consumer.RefreshPending(heartbeatCtx, eventID)
			cancel()
			if err != nil {
				// Do not cancel SMTP solely because Redis PEL maintenance failed.
				// PostgreSQL lease ownership remains the safety boundary.
				log.Printf("Redis pending heartbeat failed: event=%s error=%v", eventID, err)
			}
		}
	}
}
