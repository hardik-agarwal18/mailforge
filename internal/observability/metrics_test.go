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
