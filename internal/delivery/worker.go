package delivery

import (
	"context"
	"log"
	"sync"
)

type Worker struct {
	consumer    *Consumer
	processor   *Processor
	concurrency int
}

func NewWorker(
	consumer *Consumer,
	processor *Processor,
	concurrency int,
) *Worker {
	if concurrency < 1 {
		concurrency = 1
	}

	return &Worker{
		consumer:    consumer,
		processor:   processor,
		concurrency: concurrency,
	}
}

func (w *Worker) Run(ctx context.Context) error {
	if err := w.consumer.CreateGroup(ctx); err != nil {
		return err
	}

	log.Printf(
		"Delivery worker started with concurrency=%d",
		w.concurrency,
	)

	claimCursor := "0-0"

	for {
		if ctx.Err() != nil {
			return nil
		}

		// Recover abandoned events.
		staleJobs, nextCursor, err := w.consumer.ClaimStale(
			ctx,
			claimCursor,
			10,
		)

		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			log.Printf("Failed to recover pending jobs: %v", err)
		} else {
			claimCursor = nextCursor
			w.processBatch(ctx, staleJobs)
		}

		if ctx.Err() != nil {
			return nil
		}

		// Read new events.
		jobs, err := w.consumer.Read(ctx, 10)

		if err != nil {
			if ctx.Err() != nil {
				return nil
			}

			log.Printf("Failed to read jobs: %v", err)
			continue
		}

		w.processBatch(ctx, jobs)
	}
}

func (w *Worker) processBatch(
	ctx context.Context,
	jobs []DeliveryJob,
) {
	semaphore := make(chan struct{}, w.concurrency)

	var wg sync.WaitGroup

	for _, job := range jobs {
		if ctx.Err() != nil {
			break
		}

		semaphore <- struct{}{}
		wg.Add(1)

		go func(job DeliveryJob) {
			defer wg.Done()
			defer func() {
				<-semaphore
			}()

			w.processJob(ctx, job)
		}(job)
	}

	wg.Wait()
}

func (w *Worker) processJob(
	ctx context.Context,
	job DeliveryJob,
) {
	if err := w.processor.Process(ctx, job); err != nil {
		log.Printf(
			"Processing failed: event=%s error=%v",
			job.EventID,
			err,
		)
		return
	}

	if err := w.consumer.Acknowledge(ctx, job.EventID); err != nil {
		log.Printf(
			"Acknowledgement failed: event=%s error=%v",
			job.EventID,
			err,
		)
		return
	}

	log.Printf(
		"Acknowledged event %s",
		job.EventID,
	)
}
