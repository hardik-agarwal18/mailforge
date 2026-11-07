package observability

import (
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type Metrics struct {
	Registry *prometheus.Registry

	SMTPConnectionsTotal     prometheus.Counter
	SMTPConnectionsActive    prometheus.Gauge
	SMTPMessagesAccepted     prometheus.Counter
	SMTPMessagesRejected     prometheus.Counter
	SMTPSubmissionDuration   prometheus.Histogram
	SMTPAuthenticationsTotal *prometheus.CounterVec

	DeliveryJobsTotal         *prometheus.CounterVec
	DeliveryJobsEnqueuedTotal prometheus.Counter
	DeliveryJobsActive        prometheus.Gauge
	DeliveryQueueDepth        prometheus.Gauge
	DeliveryQueueCapacity     prometheus.Gauge
	DeliveryWorkerConcurrency prometheus.Gauge
	DeliveryJobDuration       prometheus.Histogram
	DeliveryJobQueueWait      prometheus.Histogram

	DeliveryAttemptsTotal      *prometheus.CounterVec
	DeliveryAttemptsInFlight   prometheus.Gauge
	DeliveryAttemptDuration    prometheus.Histogram
	DeliveryFailureStages      *prometheus.CounterVec
	DeliveryLeaseRenewFailures prometheus.Counter

	RedisStreamEntries          prometheus.Gauge
	RedisConsumerGroupLag       prometheus.Gauge
	RedisPendingMessages        prometheus.Gauge
	RedisConsumers              prometheus.Gauge
	RedisOldestPendingIdle      prometheus.Gauge
	RedisMessagesReclaimed      prometheus.Counter
	RedisOperationFailuresTotal *prometheus.CounterVec

	RedisStreamTrimmedEntries       prometheus.Counter
	RedisStreamRetentionRuns        prometheus.Counter
	RedisStreamRetentionErrors      prometheus.Counter
	RedisStreamRetentionLastSuccess prometheus.Gauge

	PostgresPoolTotalConnections        prometheus.Gauge
	PostgresPoolAcquiredConnections     prometheus.Gauge
	PostgresPoolIdleConnections         prometheus.Gauge
	PostgresPoolConstructingConnections prometheus.Gauge
	PostgresPoolMaxConnections          prometheus.Gauge
	PostgresPoolAcquireCount            prometheus.Gauge
	PostgresPoolAcquireDurationSeconds  prometheus.Gauge
	PostgresPoolEmptyAcquireWaitSeconds prometheus.Gauge
	PostgresPoolEmptyAcquireCount       prometheus.Gauge
	PostgresPoolCanceledAcquireCount    prometheus.Gauge
	PostgresPoolNewConnectionsCount     prometheus.Gauge

	OutboxPendingEvents       prometheus.Gauge
	OutboxPublishingEvents    prometheus.Gauge
	OutboxDeadEvents          prometheus.Gauge
	OutboxOldestPendingAge    prometheus.Gauge
	OutboxPublishBatchesTotal *prometheus.CounterVec
	OutboxPublishEventsTotal  *prometheus.CounterVec
	OutboxPublishErrorsTotal  *prometheus.CounterVec
	OutboxPublishBatchSize    prometheus.Histogram
	OutboxPublishDuration     prometheus.Histogram
}

func NewMetrics() *Metrics {
	registry := prometheus.NewRegistry()

	m := &Metrics{
		Registry: registry,

		SMTPConnectionsTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "mailforge_smtp_connections_total",
			Help: "Total SMTP connections accepted.",
		}),
		SMTPConnectionsActive: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "mailforge_smtp_connections_active",
			Help: "Currently active SMTP connections.",
		}),
		SMTPMessagesAccepted: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "mailforge_smtp_messages_accepted_total",
			Help: "Total SMTP messages accepted.",
		}),
		SMTPMessagesRejected: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "mailforge_smtp_messages_rejected_total",
			Help: "Total SMTP messages rejected.",
		}),
		SMTPAuthenticationsTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "mailforge_smtp_authentications_total",
			Help: "SMTP AUTH credential checks accepted or rejected, by outcome.",
		}, []string{"outcome"}),
		SMTPSubmissionDuration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "mailforge_smtp_submission_duration_seconds",
			Help:    "Time taken to accept an SMTP message.",
			Buckets: []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10},
		}),

		DeliveryJobsTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "mailforge_delivery_jobs_total",
			Help: "Delivery stream job handling outcomes, including acknowledgements, pending retries, quarantines, and processing errors.",
		}, []string{"outcome"}),
		DeliveryJobsEnqueuedTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "mailforge_delivery_jobs_enqueued_total",
			Help: "Delivery jobs placed into the worker dispatch queue.",
		}),
		DeliveryJobsActive: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "mailforge_delivery_jobs_active",
			Help: "Currently active delivery jobs being processed by workers.",
		}),
		DeliveryQueueDepth: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "mailforge_delivery_queue_depth",
			Help: "Current number of delivery jobs waiting in the in-memory worker queue.",
		}),
		DeliveryQueueCapacity: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "mailforge_delivery_queue_capacity",
			Help: "Configured capacity of the in-memory worker queue.",
		}),
		DeliveryWorkerConcurrency: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "mailforge_delivery_worker_concurrency",
			Help: "Configured number of concurrent delivery workers in this process.",
		}),
		DeliveryJobDuration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "mailforge_delivery_job_duration_seconds",
			Help:    "Time taken to process and acknowledge a delivery job.",
			Buckets: prometheus.DefBuckets,
		}),
		DeliveryJobQueueWait: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "mailforge_delivery_job_queue_wait_seconds",
			Help:    "Time a delivery job waits in the in-memory dispatch queue before a worker starts it.",
			Buckets: []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30},
		}),
		DeliveryAttemptsTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "mailforge_delivery_attempts_total",
			Help: "Total SMTP delivery attempts by outcome.",
		}, []string{"outcome"}),
		DeliveryAttemptsInFlight: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "mailforge_delivery_attempts_in_flight",
			Help: "SMTP delivery attempts currently executing.",
		}),
		DeliveryAttemptDuration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "mailforge_delivery_attempt_duration_seconds",
			Help:    "Time spent performing an SMTP delivery attempt, including MX lookup and SMTP transport.",
			Buckets: prometheus.DefBuckets,
		}),
		DeliveryFailureStages: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "mailforge_delivery_failures_total",
			Help: "Classified outbound delivery failures by bounded SMTP stage and classification.",
		}, []string{"stage", "classification"}),
		DeliveryLeaseRenewFailures: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "mailforge_delivery_lease_renewal_failures_total",
			Help: "Failed PostgreSQL delivery lease renewal operations.",
		}),

		RedisStreamEntries: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "mailforge_redis_stream_entries",
			Help: "Total number of entries currently retained in the Redis delivery stream.",
		}),
		RedisConsumerGroupLag: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "mailforge_redis_consumer_group_lag",
			Help: "Number of delivery stream entries not yet delivered to the consumer group.",
		}),
		RedisPendingMessages: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "mailforge_redis_pending_messages",
			Help: "Number of delivered but unacknowledged Redis stream messages.",
		}),
		RedisConsumers: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "mailforge_redis_consumers",
			Help: "Number of consumers registered in the delivery consumer group.",
		}),
		RedisOldestPendingIdle: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "mailforge_redis_oldest_pending_idle_seconds",
			Help: "Idle time of the oldest pending Redis delivery message in seconds.",
		}),
		RedisMessagesReclaimed: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "mailforge_redis_messages_reclaimed_total",
			Help: "Total Redis stream messages reclaimed from stale consumers.",
		}),
		RedisOperationFailuresTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "mailforge_redis_operation_failures_total",
			Help: "Redis operation failures by bounded operation name.",
		}, []string{"operation"}),
		RedisStreamTrimmedEntries: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "mailforge_redis_stream_trimmed_entries_total",
			Help: "Total Redis delivery stream entries trimmed.",
		}),
		RedisStreamRetentionRuns: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "mailforge_redis_stream_retention_runs_total",
			Help: "Number of Redis stream retention runs acquired by this worker.",
		}),
		RedisStreamRetentionErrors: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "mailforge_redis_stream_retention_errors_total",
			Help: "Number of Redis stream retention failures.",
		}),
		RedisStreamRetentionLastSuccess: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "mailforge_redis_stream_retention_last_success_timestamp_seconds",
			Help: "Unix timestamp of the last successful Redis stream retention run.",
		}),

		PostgresPoolTotalConnections: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "mailforge_postgres_pool_total_connections",
			Help: "Total PostgreSQL connections currently managed by the application pool.",
		}),
		PostgresPoolAcquiredConnections: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "mailforge_postgres_pool_acquired_connections",
			Help: "PostgreSQL connections currently acquired by application operations.",
		}),
		PostgresPoolIdleConnections: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "mailforge_postgres_pool_idle_connections",
			Help: "Idle PostgreSQL connections currently available in the application pool.",
		}),
		PostgresPoolConstructingConnections: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "mailforge_postgres_pool_constructing_connections",
			Help: "PostgreSQL connections currently being established by the application pool.",
		}),
		PostgresPoolMaxConnections: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "mailforge_postgres_pool_max_connections",
			Help: "Configured maximum number of PostgreSQL connections in the application pool.",
		}),
		PostgresPoolAcquireCount: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "mailforge_postgres_pool_acquire_count",
			Help: "Cumulative PostgreSQL connection acquisitions since this process started.",
		}),
		PostgresPoolAcquireDurationSeconds: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "mailforge_postgres_pool_acquire_duration_seconds",
			Help: "Cumulative seconds spent acquiring PostgreSQL connections since this process started.",
		}),
		PostgresPoolEmptyAcquireWaitSeconds: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "mailforge_postgres_pool_empty_acquire_wait_seconds",
			Help: "Cumulative seconds spent waiting for a PostgreSQL connection when the pool was empty.",
		}),
		PostgresPoolNewConnectionsCount: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "mailforge_postgres_pool_new_connections_count",
			Help: "Cumulative number of PostgreSQL connections opened by this pool since process start.",
		}),
		PostgresPoolCanceledAcquireCount: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "mailforge_postgres_pool_canceled_acquire_count",
			Help: "Cumulative PostgreSQL pool acquisitions canceled before completion.",
		}),
		PostgresPoolEmptyAcquireCount: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "mailforge_postgres_pool_empty_acquire_count",
			Help: "Cumulative PostgreSQL pool acquisitions that had to wait for a new connection.",
		}),

		OutboxPendingEvents: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "mailforge_outbox_pending_events",
			Help: "Number of outbox events currently pending publication, including scheduled retries.",
		}),
		OutboxPublishingEvents: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "mailforge_outbox_publishing_events",
			Help: "Number of outbox events currently leased by publishers.",
		}),
		OutboxDeadEvents: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "mailforge_outbox_dead_events",
			Help: "Number of outbox events in the terminal dead state.",
		}),
		OutboxOldestPendingAge: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "mailforge_outbox_oldest_pending_age_seconds",
			Help: "Age of the oldest currently-due pending outbox event.",
		}),
		OutboxPublishBatchesTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "mailforge_outbox_publish_batches_total",
			Help: "Outbox publish batches by overall outcome.",
		}, []string{"outcome"}),
		OutboxPublishEventsTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "mailforge_outbox_publish_events_total",
			Help: "Outbox events by Redis XADD command outcome.",
		}, []string{"outcome"}),
		OutboxPublishErrorsTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "mailforge_outbox_publish_errors_total",
			Help: "Outbox publisher errors by bounded processing stage.",
		}, []string{"stage"}),
		OutboxPublishBatchSize: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "mailforge_outbox_publish_batch_size",
			Help:    "Number of outbox events claimed per publish batch.",
			Buckets: []float64{1, 5, 10, 25, 50, 100, 250, 500, 1000},
		}),
		OutboxPublishDuration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "mailforge_outbox_publish_duration_seconds",
			Help:    "Time spent processing a non-empty outbox publish batch, including PostgreSQL state updates.",
			Buckets: prometheus.DefBuckets,
		}),
	}

	m.SMTPAuthenticationsTotal.WithLabelValues("success")
	m.SMTPAuthenticationsTotal.WithLabelValues("failure")
	for _, outcome := range []string{"acknowledged", "acknowledgement_error", "processing_error", "recipient_busy", "recipient_not_ready", "quarantined"} {
		m.DeliveryJobsTotal.WithLabelValues(outcome)
	}
	for _, outcome := range []string{"success", "temporary_failure", "permanent_failure", "unknown"} {
		m.DeliveryAttemptsTotal.WithLabelValues(outcome)
	}
	for _, outcome := range []string{"success", "partial_failure", "failure"} {
		m.OutboxPublishBatchesTotal.WithLabelValues(outcome)
	}
	for _, outcome := range []string{"redis_accepted", "redis_failed"} {
		m.OutboxPublishEventsTotal.WithLabelValues(outcome)
	}
	for _, stage := range []string{"claim", "pipeline", "mark_failed", "mark_published", "stats"} {
		m.OutboxPublishErrorsTotal.WithLabelValues(stage)
	}
	for _, operation := range []string{"claim_stale", "read", "acknowledge", "quarantine", "pending_heartbeat", "metrics_xlen", "metrics_xinfo_groups", "metrics_xpending"} {
		m.RedisOperationFailuresTotal.WithLabelValues(operation)
	}

	registry.MustRegister(
		m.SMTPConnectionsTotal, m.SMTPConnectionsActive,
		m.SMTPMessagesAccepted, m.SMTPMessagesRejected,
		m.SMTPSubmissionDuration, m.SMTPAuthenticationsTotal,
		m.DeliveryJobsTotal, m.DeliveryJobsEnqueuedTotal,
		m.DeliveryJobsActive, m.DeliveryQueueDepth, m.DeliveryQueueCapacity,
		m.DeliveryWorkerConcurrency, m.DeliveryJobDuration, m.DeliveryJobQueueWait,
		m.DeliveryAttemptsTotal, m.DeliveryAttemptsInFlight, m.DeliveryAttemptDuration,
		m.DeliveryFailureStages, m.DeliveryLeaseRenewFailures,
		m.RedisStreamEntries, m.RedisConsumerGroupLag, m.RedisPendingMessages,
		m.RedisConsumers, m.RedisOldestPendingIdle, m.RedisMessagesReclaimed,
		m.RedisOperationFailuresTotal, m.RedisStreamTrimmedEntries,
		m.RedisStreamRetentionRuns, m.RedisStreamRetentionErrors,
		m.RedisStreamRetentionLastSuccess,
		m.PostgresPoolTotalConnections, m.PostgresPoolAcquiredConnections,
		m.PostgresPoolIdleConnections, m.PostgresPoolConstructingConnections,
		m.PostgresPoolMaxConnections, m.PostgresPoolAcquireCount,
		m.PostgresPoolAcquireDurationSeconds, m.PostgresPoolEmptyAcquireWaitSeconds,
		m.PostgresPoolEmptyAcquireCount, m.PostgresPoolCanceledAcquireCount,
		m.PostgresPoolNewConnectionsCount,
		m.OutboxPendingEvents, m.OutboxPublishingEvents, m.OutboxDeadEvents,
		m.OutboxOldestPendingAge, m.OutboxPublishBatchesTotal,
		m.OutboxPublishEventsTotal, m.OutboxPublishErrorsTotal,
		m.OutboxPublishBatchSize, m.OutboxPublishDuration,
	)

	return m
}

func (m *Metrics) RecordSubmission(duration time.Duration, accepted bool) {
	m.SMTPSubmissionDuration.Observe(duration.Seconds())
	if accepted {
		m.SMTPMessagesAccepted.Inc()
	} else {
		m.SMTPMessagesRejected.Inc()
	}
}

func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.Registry, promhttp.HandlerOpts{})
}

func (m *Metrics) RecordConnectionOpened() {
	m.SMTPConnectionsTotal.Inc()
	m.SMTPConnectionsActive.Inc()
}

func (m *Metrics) RecordConnectionClosed() {
	m.SMTPConnectionsActive.Dec()
}

func (m *Metrics) RecordDeliveryAttempt(duration time.Duration, outcome string) {
	m.DeliveryAttemptsTotal.WithLabelValues(outcome).Inc()
	m.DeliveryAttemptDuration.Observe(duration.Seconds())
}

func (m *Metrics) RecordDeliveryFailure(stage, classification string) {
	if stage == "" {
		stage = "unknown"
	}
	m.DeliveryFailureStages.WithLabelValues(stage, classification).Inc()
}

func (m *Metrics) RecordAuthentication(success bool) {
	outcome := "failure"
	if success {
		outcome = "success"
	}
	m.SMTPAuthenticationsTotal.WithLabelValues(outcome).Inc()
}

func (m *Metrics) SetWorkerPoolStats(concurrency, queueDepth, queueCapacity int) {
	m.DeliveryWorkerConcurrency.Set(float64(concurrency))
	m.DeliveryQueueDepth.Set(float64(queueDepth))
	m.DeliveryQueueCapacity.Set(float64(queueCapacity))
}

func (m *Metrics) SetPostgresPoolStats(stats PostgresPoolSnapshot) {
	m.PostgresPoolTotalConnections.Set(float64(stats.TotalConnections))
	m.PostgresPoolAcquiredConnections.Set(float64(stats.AcquiredConnections))
	m.PostgresPoolIdleConnections.Set(float64(stats.IdleConnections))
	m.PostgresPoolConstructingConnections.Set(float64(stats.ConstructingConnections))
	m.PostgresPoolMaxConnections.Set(float64(stats.MaxConnections))
	m.PostgresPoolAcquireCount.Set(float64(stats.AcquireCount))
	m.PostgresPoolAcquireDurationSeconds.Set(stats.AcquireDurationSeconds)
	m.PostgresPoolEmptyAcquireWaitSeconds.Set(stats.EmptyAcquireWaitSeconds)
	m.PostgresPoolEmptyAcquireCount.Set(float64(stats.EmptyAcquireCount))
	m.PostgresPoolCanceledAcquireCount.Set(float64(stats.CanceledAcquireCount))
	m.PostgresPoolNewConnectionsCount.Set(float64(stats.NewConnectionsCount))
}

func (m *Metrics) SetOutboxStats(pending, publishing, dead int64, oldestPendingAge time.Duration) {
	m.OutboxPendingEvents.Set(float64(pending))
	m.OutboxPublishingEvents.Set(float64(publishing))
	m.OutboxDeadEvents.Set(float64(dead))
	m.OutboxOldestPendingAge.Set(oldestPendingAge.Seconds())
}

type PostgresPoolSnapshot struct {
	TotalConnections        int32
	AcquiredConnections     int32
	IdleConnections         int32
	ConstructingConnections int32
	MaxConnections          int32
	AcquireCount            int64
	AcquireDurationSeconds  float64
	EmptyAcquireWaitSeconds float64
	EmptyAcquireCount       int64
	CanceledAcquireCount    int64
	NewConnectionsCount     int64
}
