package observability

import (
	"math"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	dto "github.com/prometheus/client_model/go"
)

func findMetric(
	t *testing.T,
	m *Metrics,
	name string,
) *dto.MetricFamily {
	t.Helper()

	families, err := m.Registry.Gather()
	if err != nil {
		t.Fatalf("failed to gather metrics: %v", err)
	}

	for _, family := range families {
		if family.GetName() == name {
			return family
		}
	}

	t.Fatalf("metric %q not found", name)
	return nil
}

func TestMetricsRecording(t *testing.T) {
	m := NewMetrics()

	m.RecordConnectionOpened()
	m.RecordAuthentication(true)
	m.RecordAuthentication(false)
	m.RecordSubmission(100*time.Millisecond, true)
	m.RecordSubmission(200*time.Millisecond, false)
	m.RecordConnectionClosed()

	tests := []struct {
		name string
		want float64
	}{
		{"mailforge_smtp_connections_total", 1},
		{"mailforge_smtp_messages_accepted_total", 1},
		{"mailforge_smtp_messages_rejected_total", 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			family := findMetric(t, m, tt.name)
			got := family.GetMetric()[0].GetCounter().GetValue()

			if got != tt.want {
				t.Fatalf("got %v, want %v", got, tt.want)
			}
		})
	}

	authFamilies := findMetric(t, m, "mailforge_smtp_authentications_total")
	authCounts := make(map[string]float64)
	for _, metric := range authFamilies.GetMetric() {
		for _, label := range metric.GetLabel() {
			if label.GetName() == "outcome" {
				authCounts[label.GetValue()] = metric.GetCounter().GetValue()
			}
		}
	}
	if authCounts["success"] != 1 || authCounts["failure"] != 1 {
		t.Fatalf("authentication outcome counts = %v, want success=1 failure=1", authCounts)
	}

	active := findMetric(
		t,
		m,
		"mailforge_smtp_connections_active",
	).GetMetric()[0].GetGauge().GetValue()

	if active != 0 {
		t.Fatalf("active connections = %v, want 0", active)
	}

	histogram := findMetric(
		t,
		m,
		"mailforge_smtp_submission_duration_seconds",
	).GetMetric()[0].GetHistogram()

	if histogram.GetSampleCount() != 2 {
		t.Fatalf(
			"histogram count = %d, want 2",
			histogram.GetSampleCount(),
		)
	}

	if math.Abs(histogram.GetSampleSum()-0.3) > 0.001 {
		t.Fatalf("unexpected histogram sum: %v", histogram.GetSampleSum())
	}
}

func TestMetricsHandler(t *testing.T) {
	m := NewMetrics()
	m.RecordConnectionOpened()

	handler := m.Handler()

	recorder := httptest.NewRecorder()
	request := httptest.NewRequest("GET", "/metrics", nil)

	handler.ServeHTTP(recorder, request)

	if recorder.Code != 200 {
		t.Fatalf("status = %d, want 200", recorder.Code)
	}

	if !strings.Contains(
		recorder.Body.String(),
		"mailforge_smtp_connections_total 1",
	) {
		t.Fatal("expected connection metric in response")
	}
}

func TestOperationalMetricsRecording(t *testing.T) {
	m := NewMetrics()
	m.SetWorkerPoolStats(32, 12, 32)
	m.DeliveryJobsEnqueuedTotal.Add(7)
	m.DeliveryJobQueueWait.Observe(0.25)
	m.DeliveryAttemptsInFlight.Set(3)
	m.RecordDeliveryFailure("data_final", "ambiguous")
	m.DeliveryLeaseRenewFailures.Inc()
	m.RedisOperationFailuresTotal.WithLabelValues("pending_heartbeat").Inc()
	m.SetPostgresPoolStats(PostgresPoolSnapshot{
		TotalConnections:        10,
		AcquiredConnections:     7,
		IdleConnections:         3,
		ConstructingConnections: 0,
		MaxConnections:          20,
		AcquireCount:            100,
		AcquireDurationSeconds:  1.5,
		EmptyAcquireWaitSeconds: 0.75,
		CanceledAcquireCount:    2,
		EmptyAcquireCount:       9,
		NewConnectionsCount:     12,
	})
	m.SetOutboxStats(15, 2, 1, 45*time.Second)
	m.OutboxPublishBatchesTotal.WithLabelValues("partial_failure").Inc()
	m.OutboxPublishEventsTotal.WithLabelValues("redis_accepted").Add(4)
	m.OutboxPublishErrorsTotal.WithLabelValues("mark_published").Inc()
	m.OutboxPublishBatchSize.Observe(4)
	m.OutboxPublishDuration.Observe(0.08)

	checks := map[string]float64{
		"mailforge_delivery_worker_concurrency":              32,
		"mailforge_delivery_queue_depth":                     12,
		"mailforge_delivery_queue_capacity":                  32,
		"mailforge_delivery_jobs_enqueued_total":             7,
		"mailforge_delivery_attempts_in_flight":              3,
		"mailforge_postgres_pool_total_connections":          10,
		"mailforge_postgres_pool_acquired_connections":       7,
		"mailforge_postgres_pool_max_connections":            20,
		"mailforge_postgres_pool_empty_acquire_wait_seconds": 0.75,
		"mailforge_postgres_pool_empty_acquire_count":        9,
		"mailforge_postgres_pool_new_connections_count":      12,
		"mailforge_outbox_pending_events":                    15,
		"mailforge_outbox_publishing_events":                 2,
		"mailforge_outbox_dead_events":                       1,
		"mailforge_outbox_oldest_pending_age_seconds":        45,
	}
	for name, want := range checks {
		family := findMetric(t, m, name)
		if len(family.GetMetric()) != 1 {
			t.Fatalf("metric %q has %d samples, want 1", name, len(family.GetMetric()))
		}
		metric := family.GetMetric()[0]
		var got float64
		switch family.GetType() {
		case dto.MetricType_GAUGE:
			got = metric.GetGauge().GetValue()
		case dto.MetricType_COUNTER:
			got = metric.GetCounter().GetValue()
		default:
			t.Fatalf("metric %q has unexpected type %s", name, family.GetType())
		}
		if math.Abs(got-want) > 0.0001 {
			t.Errorf("metric %q = %v, want %v", name, got, want)
		}
	}

	failureFamily := findMetric(t, m, "mailforge_delivery_failures_total")
	if len(failureFamily.GetMetric()) != 1 {
		t.Fatalf("delivery failure samples = %d, want 1", len(failureFamily.GetMetric()))
	}
	labels := map[string]string{}
	for _, label := range failureFamily.GetMetric()[0].GetLabel() {
		labels[label.GetName()] = label.GetValue()
	}
	if labels["stage"] != "data_final" || labels["classification"] != "ambiguous" {
		t.Fatalf("delivery failure labels = %v", labels)
	}
}
