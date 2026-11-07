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

// pendingJob carries the Redis pending-entry heartbeat from the moment an
// event is read until its processing/acknowledgement path has completed. This
// includes time spent waiting in the bounded in-memory dispatch queue.
type pendingJob struct {
	job           DeliveryJob
	enqueuedAt    time.Time
	stopHeartbeat context.CancelFunc
	heartbeatDone <-chan struct{}
}

func (j pendingJob) finish() {
	if j.stopHeartbeat != nil {
		j.stopHeartbeat()
	}
	if j.heartbeatDone != nil {
		<-j.heartbeatDone
	}
}

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

	// The queue is deliberately bounded. At most concurrency jobs can wait
	// in memory in addition to the concurrency jobs currently being processed.
	// That gives a predictable memory/back-pressure boundary while keeping
	// the workers fed between Redis polls.
	jobQueue, stopWorkers := startWorkerPool(
		w.concurrency,
		w.concurrency,
		func(task pendingJob) {
			w.processJob(ctx, task)
		},
	)
	defer stopWorkers()
	if w.metrics != nil {
		w.metrics.SetWorkerPoolStats(w.concurrency, 0, cap(jobQueue))
		go w.collectWorkerQueueMetrics(ctx, jobQueue, time.Second)
	}

	log.Printf(
		"Delivery worker started with concurrency=%d queue_capacity=%d",
		w.concurrency,
		w.concurrency,
	)

	claimCursor := "0-0"

	for ctx.Err() == nil {
		available := cap(jobQueue) - len(jobQueue)
		if available < 1 {
			// All queue slots are occupied. Workers are still consuming; avoid
			// polling Redis until back-pressure has eased.
			if !waitContext(ctx, 25*time.Millisecond) {
				break
			}
			continue
		}

		staleJobs, nextCursor, claimErr := w.consumer.ClaimStale(
			ctx,
			claimCursor,
			int64(available),
		)
		if nextCursor != "" {
			claimCursor = nextCursor
		}
		if claimErr != nil {
			if w.metrics != nil {
				w.metrics.RedisOperationFailuresTotal.WithLabelValues("claim_stale").Inc()
			}
			if ctx.Err() != nil {
				break
			}
			log.Printf("Failed to recover pending jobs: %v", claimErr)
		} else if w.metrics != nil && len(staleJobs) > 0 {
			w.metrics.RedisMessagesReclaimed.Add(float64(len(staleJobs)))
		}

		if !w.dispatch(ctx, jobQueue, staleJobs) {
			break
		}

		if ctx.Err() != nil {
			break
		}

		// ClaimStale may have consumed all currently available queue slots.
		// Read only the remaining capacity so Redis does not hand us a batch
		// larger than the bounded queue can accept.
		available = cap(jobQueue) - len(jobQueue)
		var jobs []DeliveryJob
		var readErr error
		if available > 0 {
			jobs, readErr = w.consumer.Read(ctx, int64(available))
		}
		if readErr != nil {
			if w.metrics != nil {
				w.metrics.RedisOperationFailuresTotal.WithLabelValues("read").Inc()
			}
			if ctx.Err() != nil {
				break
			}
			log.Printf("Failed to read delivery jobs: %v", readErr)
		}
		if !w.dispatch(ctx, jobQueue, jobs) {
			break
		}

		// XREADGROUP normally blocks for up to five seconds. If both Redis
		// recovery and read operations failed immediately, back off to avoid
		// a hot loop during an outage.
		if claimErr != nil && readErr != nil && len(staleJobs) == 0 && len(jobs) == 0 {
			if !waitContext(ctx, time.Second) {
				break
			}
		}
	}

	return nil
}

// startWorkerPool creates long-lived workers and a bounded dispatch channel.
// The caller owns the returned channel and must call stop exactly once.
func startWorkerPool(
	workerCount int,
	queueCapacity int,
	process func(pendingJob),
) (chan pendingJob, func()) {
	if workerCount < 1 {
		workerCount = 1
	}
	if queueCapacity < 0 {
		queueCapacity = 0
	}

	jobs := make(chan pendingJob, queueCapacity)
	var workers sync.WaitGroup
	workers.Add(workerCount)

	for i := 0; i < workerCount; i++ {
		go func() {
			defer workers.Done()
			for job := range jobs {
				process(job)
			}
		}()
	}

	var stopOnce sync.Once
	stop := func() {
		stopOnce.Do(func() {
			close(jobs)
			workers.Wait()
		})
	}

	return jobs, stop
}

func (w *Worker) dispatch(
	ctx context.Context,
	queue chan<- pendingJob,
	jobs []DeliveryJob,
) bool {
	for _, job := range jobs {
		// Start the heartbeat before enqueueing: an event is already in the
		// Redis PEL, and it must not look abandoned while waiting for a worker.
		heartbeatCtx, stopHeartbeat := context.WithCancel(ctx)
		heartbeatDone := make(chan struct{})
		go w.keepPendingAlive(heartbeatCtx, job.EventID, heartbeatDone)

		task := pendingJob{
			job:           job,
			enqueuedAt:    time.Now(),
			stopHeartbeat: stopHeartbeat,
			heartbeatDone: heartbeatDone,
		}

		select {
		case queue <- task:
			if w.metrics != nil {
				w.metrics.DeliveryJobsEnqueuedTotal.Inc()
				w.metrics.DeliveryQueueDepth.Set(float64(len(queue)))
			}
		case <-ctx.Done():
			task.finish()
			return false
		}
	}
	return true
}

func (w *Worker) collectWorkerQueueMetrics(ctx context.Context, queue <-chan pendingJob, interval time.Duration) {
	if interval <= 0 {
		interval = time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if w.metrics != nil {
				w.metrics.DeliveryQueueDepth.Set(float64(len(queue)))
				w.metrics.DeliveryQueueCapacity.Set(float64(cap(queue)))
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

func (w *Worker) processJob(
	ctx context.Context,
	task pendingJob,
) {
	defer task.finish()
	job := task.job
	startedAt := time.Now()

	if w.metrics != nil {
		if !task.enqueuedAt.IsZero() {
			w.metrics.DeliveryJobQueueWait.Observe(time.Since(task.enqueuedAt).Seconds())
		}
		w.metrics.DeliveryJobsActive.Inc()
		defer w.metrics.DeliveryJobsActive.Dec()
		defer func() {
			w.metrics.DeliveryJobDuration.Observe(
				time.Since(startedAt).Seconds(),
			)
		}()
	}

	processErr := w.processor.Process(ctx, job)

	if processErr != nil {
		if errors.Is(processErr, ErrRecipientBusy) {
			if w.metrics != nil {
				w.metrics.DeliveryJobsTotal.WithLabelValues("recipient_busy").Inc()
			}
			log.Printf(
				"Recipient is owned by another worker; leaving event pending: event=%s",
				job.EventID,
			)
			return
		}

		if errors.Is(processErr, ErrRecipientNotReady) {
			if w.metrics != nil {
				w.metrics.DeliveryJobsTotal.WithLabelValues("recipient_not_ready").Inc()
			}
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
				if w.metrics != nil {
					w.metrics.RedisOperationFailuresTotal.WithLabelValues("quarantine").Inc()
				}
				log.Printf("Failed to quarantine orphan event %s: %v", job.EventID, err)
				return
			}
			if w.metrics != nil {
				w.metrics.DeliveryJobsTotal.WithLabelValues("quarantined").Inc()
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
			w.metrics.RedisOperationFailuresTotal.WithLabelValues("acknowledge").Inc()
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
				if w.metrics != nil {
					w.metrics.RedisOperationFailuresTotal.WithLabelValues("pending_heartbeat").Inc()
				}
				// Do not cancel SMTP solely because Redis PEL maintenance failed.
				// PostgreSQL lease ownership remains the safety boundary.
				log.Printf("Redis pending heartbeat failed: event=%s error=%v", eventID, err)
			}
		}
	}
}
