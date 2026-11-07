package delivery

import (
	"fmt"
	"sync/atomic"
	"testing"
	"time"
)

func TestStartWorkerPoolBoundsConcurrencyAndProcessesEveryJob(t *testing.T) {
	const (
		workerCount   = 4
		queueCapacity = 3
		jobCount      = 40
	)

	var active int64
	var maxActive int64
	var processed int64

	queue, stop := startWorkerPool(workerCount, queueCapacity, func(task pendingJob) {
		current := atomic.AddInt64(&active, 1)
		for {
			previous := atomic.LoadInt64(&maxActive)
			if current <= previous || atomic.CompareAndSwapInt64(&maxActive, previous, current) {
				break
			}
		}

		time.Sleep(5 * time.Millisecond)
		atomic.AddInt64(&processed, 1)
		atomic.AddInt64(&active, -1)
	})

	if got := cap(queue); got != queueCapacity {
		t.Fatalf("queue capacity = %d, want %d", got, queueCapacity)
	}

	for i := 0; i < jobCount; i++ {
		queue <- pendingJob{job: DeliveryJob{EventID: fmt.Sprintf("event-%d", i)}}
	}
	stop()

	if got := atomic.LoadInt64(&processed); got != jobCount {
		t.Fatalf("processed jobs = %d, want %d", got, jobCount)
	}
	if got := atomic.LoadInt64(&maxActive); got > workerCount {
		t.Fatalf("max active jobs = %d, exceeds worker count %d", got, workerCount)
	}
}

func TestStartWorkerPoolNormalizesInvalidSizes(t *testing.T) {
	queue, stop := startWorkerPool(0, -1, func(pendingJob) {})
	if cap(queue) != 0 {
		t.Fatalf("queue capacity = %d, want 0", cap(queue))
	}
	stop()
}
