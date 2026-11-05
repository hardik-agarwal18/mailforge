package delivery

import (
	"context"
	"log"
)

type Worker struct {
	consumer  *Consumer
	processor *Processor
}

func NewWorker(
	consumer *Consumer,
	processor *Processor,
) *Worker {
	return &Worker{
		consumer:  consumer,
		processor: processor,
	}
}

func (w *Worker) Run(ctx context.Context) error {
	if err := w.consumer.CreateGroup(ctx); err != nil {
		return err
	}

	log.Println("Delivery worker started")

	claimCursor := "0-0"

	for {
		if ctx.Err() != nil {
			return nil
		}

		// First, recover abandoned messages.
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

			for _, job := range staleJobs {
				w.processJob(ctx, job)
			}
		}

		// Then read new messages.
		jobs, err := w.consumer.Read(ctx, 10)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}

			log.Printf("Failed to read jobs: %v", err)
			continue
		}

		for _, job := range jobs {
			w.processJob(ctx, job)
		}
	}
}

func (w *Worker) processJob(ctx context.Context, job DeliveryJob) {
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

	log.Printf("Acknowledged event %s", job.EventID)
}
