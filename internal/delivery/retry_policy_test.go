package delivery

import (
	"testing"
	"time"
)

func TestDefaultRetryPolicy(t *testing.T) {
	policy := DefaultRetryPolicy()
	if err := policy.Validate(); err != nil {
		t.Fatalf("default policy invalid: %v", err)
	}
	if policy.InitialDelay != 30*time.Minute {
		t.Errorf("InitialDelay = %s, want 30m", policy.InitialDelay)
	}
	if policy.MaxDelay != 3*time.Hour {
		t.Errorf("MaxDelay = %s, want 3h", policy.MaxDelay)
	}
	if policy.MaxQueueAge != 5*24*time.Hour {
		t.Errorf("MaxQueueAge = %s, want 120h", policy.MaxQueueAge)
	}
}

func TestRetryPolicyDelayIsExponentialBoundedAndJittered(t *testing.T) {
	policy := DefaultRetryPolicy()
	bounds := []struct {
		attempt int
		base    time.Duration
	}{
		{attempt: 1, base: 30 * time.Minute},
		{attempt: 2, base: time.Hour},
		{attempt: 3, base: 2 * time.Hour},
		{attempt: 4, base: 3 * time.Hour},
		{attempt: 12, base: 3 * time.Hour},
	}

	for _, test := range bounds {
		for i := 0; i < 100; i++ {
			got := policy.DelayForAttempt(test.attempt)
			min := test.base - test.base/10
			if min < policy.InitialDelay {
				min = policy.InitialDelay
			}
			max := test.base + test.base/10
			if max > policy.MaxDelay {
				max = policy.MaxDelay
			}
			if test.attempt == 1 {
				min = test.base
			}
			if got < min || got > max {
				t.Fatalf("attempt %d delay %s outside [%s, %s]", test.attempt, got, min, max)
			}
		}
	}
}

func TestRetryPolicyRejectsInvalidConfiguration(t *testing.T) {
	tests := []RetryPolicy{
		{InitialDelay: 0, MaxDelay: time.Hour, MaxQueueAge: time.Hour, MaxAttempts: 1},
		{InitialDelay: time.Hour, MaxDelay: time.Minute, MaxQueueAge: time.Hour, MaxAttempts: 1},
		{InitialDelay: time.Minute, MaxDelay: time.Hour, MaxQueueAge: 0, MaxAttempts: 1},
		{InitialDelay: time.Minute, MaxDelay: time.Hour, MaxQueueAge: time.Hour, MaxAttempts: 0},
	}
	for i, policy := range tests {
		if err := policy.Validate(); err == nil {
			t.Errorf("policy %d unexpectedly valid", i)
		}
	}
}
