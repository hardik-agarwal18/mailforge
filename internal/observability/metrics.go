package observability

import (
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type Metrics struct {
	Registry *prometheus.Registry

	SMTPConnectionsTotal  prometheus.Counter
	SMTPConnectionsActive prometheus.Gauge

	SMTPMessagesAccepted prometheus.Counter
	SMTPMessagesRejected prometheus.Counter

	SMTPSubmissionDuration prometheus.Histogram

	DeliveryJobsTotal   *prometheus.CounterVec
	DeliveryJobsActive  prometheus.Gauge
	DeliveryJobDuration prometheus.Histogram

	DeliveryAttemptsTotal   *prometheus.CounterVec
	DeliveryAttemptDuration prometheus.Histogram

	// Redis queue metrics
	RedisStreamEntries     prometheus.Gauge
	RedisConsumerGroupLag  prometheus.Gauge
	RedisPendingMessages   prometheus.Gauge
	RedisConsumers         prometheus.Gauge
	RedisOldestPendingIdle prometheus.Gauge
}

func NewMetrics() *Metrics {
	registry := prometheus.NewRegistry()

	m := &Metrics{
		Registry: registry,

		SMTPConnectionsTotal: prometheus.NewCounter(
			prometheus.CounterOpts{
				Name: "mailforge_smtp_connections_total",
				Help: "Total SMTP connections accepted.",
			},
		),

		SMTPConnectionsActive: prometheus.NewGauge(
			prometheus.GaugeOpts{
				Name: "mailforge_smtp_connections_active",
				Help: "Currently active SMTP connections.",
			},
		),

		SMTPMessagesAccepted: prometheus.NewCounter(
			prometheus.CounterOpts{
				Name: "mailforge_smtp_messages_accepted_total",
				Help: "Total SMTP messages accepted.",
			},
		),

		SMTPMessagesRejected: prometheus.NewCounter(
			prometheus.CounterOpts{
				Name: "mailforge_smtp_messages_rejected_total",
				Help: "Total SMTP messages rejected.",
			},
		),

		SMTPSubmissionDuration: prometheus.NewHistogram(
			prometheus.HistogramOpts{
				Name: "mailforge_smtp_submission_duration_seconds",
				Help: "Time taken to accept an SMTP message.",
				Buckets: []float64{
					0.001, 0.005, 0.01, 0.025,
					0.05, 0.1, 0.25, 0.5,
					1, 2.5, 5, 10,
				},
			},
		),

		DeliveryJobsTotal: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "mailforge_delivery_jobs_total",
				Help: "Total delivery jobs handled by workers.",
			},
			[]string{"outcome"},
		),

		DeliveryJobsActive: prometheus.NewGauge(
			prometheus.GaugeOpts{
				Name: "mailforge_delivery_jobs_active",
				Help: "Currently active delivery jobs.",
			},
		),

		DeliveryJobDuration: prometheus.NewHistogram(
			prometheus.HistogramOpts{
				Name:    "mailforge_delivery_job_duration_seconds",
				Help:    "Time taken to process and acknowledge a delivery job.",
				Buckets: prometheus.DefBuckets,
			},
		),

		DeliveryAttemptsTotal: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "mailforge_delivery_attempts_total",
				Help: "Total SMTP delivery attempts by outcome.",
			},
			[]string{"outcome"},
		),

		DeliveryAttemptDuration: prometheus.NewHistogram(
			prometheus.HistogramOpts{
				Name:    "mailforge_delivery_attempt_duration_seconds",
				Help:    "Time spent performing an SMTP delivery attempt.",
				Buckets: prometheus.DefBuckets,
			},
		),

		RedisStreamEntries: prometheus.NewGauge(
			prometheus.GaugeOpts{
				Name: "mailforge_redis_stream_entries",
				Help: "Total number of entries currently retained in the Redis delivery stream.",
			},
		),

		RedisConsumerGroupLag: prometheus.NewGauge(
			prometheus.GaugeOpts{
				Name: "mailforge_redis_consumer_group_lag",
				Help: "Number of delivery stream entries not yet delivered to the consumer group.",
			},
		),

		RedisPendingMessages: prometheus.NewGauge(
			prometheus.GaugeOpts{
				Name: "mailforge_redis_pending_messages",
				Help: "Number of delivered but unacknowledged Redis stream messages.",
			},
		),

		RedisConsumers: prometheus.NewGauge(
			prometheus.GaugeOpts{
				Name: "mailforge_redis_consumers",
				Help: "Number of consumers registered in the delivery consumer group.",
			},
		),

		RedisOldestPendingIdle: prometheus.NewGauge(
			prometheus.GaugeOpts{
				Name: "mailforge_redis_oldest_pending_idle_seconds",
				Help: "Idle time of the oldest pending Redis delivery message in seconds.",
			},
		),
	}

	registry.MustRegister(
		m.SMTPConnectionsTotal,
		m.SMTPConnectionsActive,
		m.SMTPMessagesAccepted,
		m.SMTPMessagesRejected,
		m.SMTPSubmissionDuration,
		m.DeliveryJobsTotal,
		m.DeliveryJobsActive,
		m.DeliveryJobDuration,
		m.DeliveryAttemptsTotal,
		m.DeliveryAttemptDuration,
		m.RedisStreamEntries,
		m.RedisConsumerGroupLag,
		m.RedisPendingMessages,
		m.RedisConsumers,
		m.RedisOldestPendingIdle,
	)

	return m
}

func (m *Metrics) RecordSubmission(
	duration time.Duration,
	accepted bool,
) {
	m.SMTPSubmissionDuration.Observe(duration.Seconds())

	if accepted {
		m.SMTPMessagesAccepted.Inc()
	} else {
		m.SMTPMessagesRejected.Inc()
	}
}

func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(
		m.Registry,
		promhttp.HandlerOpts{},
	)
}

func (m *Metrics) RecordConnectionOpened() {
	m.SMTPConnectionsTotal.Inc()
	m.SMTPConnectionsActive.Inc()
}

func (m *Metrics) RecordConnectionClosed() {
	m.SMTPConnectionsActive.Dec()
}

func (m *Metrics) RecordDeliveryAttempt(
	duration time.Duration,
	outcome string,
) {
	m.DeliveryAttemptsTotal.
		WithLabelValues(outcome).
		Inc()

	m.DeliveryAttemptDuration.Observe(duration.Seconds())
}
