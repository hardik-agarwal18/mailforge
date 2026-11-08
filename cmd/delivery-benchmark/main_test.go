package main

import (
	"strings"
	"testing"
	"time"
)

func TestSummarizeDurationsUsesNearestRankPercentiles(t *testing.T) {
	values := []time.Duration{
		10 * time.Millisecond,
		20 * time.Millisecond,
		30 * time.Millisecond,
		40 * time.Millisecond,
		50 * time.Millisecond,
		60 * time.Millisecond,
		70 * time.Millisecond,
		80 * time.Millisecond,
		90 * time.Millisecond,
		100 * time.Millisecond,
	}

	got := summarizeDurations(values)
	if got.Samples != 10 {
		t.Fatalf("samples = %d, want 10", got.Samples)
	}
	if got.P50 != 50 {
		t.Fatalf("P50 = %.2fms, want 50ms", got.P50)
	}
	if got.P95 != 100 {
		t.Fatalf("P95 = %.2fms, want 100ms", got.P95)
	}
	if got.P99 != 100 {
		t.Fatalf("P99 = %.2fms, want 100ms", got.P99)
	}
}

func TestDeliverySnapshotCompletesOnlyWhenAllRecipientsAreTerminal(t *testing.T) {
	tests := []struct {
		name     string
		snapshot deliverySnapshot
		want     bool
	}{
		{
			name:     "all delivered",
			snapshot: deliverySnapshot{Total: 3, Delivered: 3},
			want:     true,
		},
		{
			name:     "mixed terminal states",
			snapshot: deliverySnapshot{Total: 3, Delivered: 1, Bounced: 1, Unknown: 1},
			want:     true,
		},
		{
			name:     "still pending",
			snapshot: deliverySnapshot{Total: 3, Delivered: 2, Pending: 1},
			want:     false,
		},
		{
			name:     "deferred retry",
			snapshot: deliverySnapshot{Total: 3, Delivered: 2, Deferred: 1},
			want:     false,
		},
		{
			name:     "not all submitted recipients persisted",
			snapshot: deliverySnapshot{Total: 2, Delivered: 2},
			want:     false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := test.snapshot.complete(3); got != test.want {
				t.Fatalf("complete() = %t, want %t", got, test.want)
			}
		})
	}
}

func TestWrapBenchmarkBodyKeepsSMTPLinesWithinProtocolLimit(t *testing.T) {
	const requestedBytes = 1024
	body := strings.Repeat("x", requestedBytes)
	got := wrapBenchmarkBody(body)

	if strings.ReplaceAll(got, "\r\n", "") != body {
		t.Fatal("wrapped body does not preserve the requested payload bytes")
	}
	for i, line := range strings.Split(got, "\r\n") {
		if len(line)+2 > 1000 {
			t.Fatalf("line %d is %d bytes including CRLF; SMTP limit is 1000", i+1, len(line)+2)
		}
		if len(line) > 76 {
			t.Fatalf("line %d is %d bytes; MIME benchmark wrapping limit is 76", i+1, len(line))
		}
	}
}

func TestWrapBenchmarkBodyDoesNotWrapShortPayload(t *testing.T) {
	input := strings.Repeat("x", 76)
	if got := wrapBenchmarkBody(input); got != input {
		t.Fatalf("wrapBenchmarkBody() = %q, want unchanged payload", got)
	}
}
