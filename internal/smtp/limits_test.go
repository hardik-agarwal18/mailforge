package smtp

import (
	"testing"
	"time"
)

func TestFixedWindowLimiterBlocksAfterLimit(t *testing.T) {
	limiter := newFixedWindowLimiter(10)
	key := limiterKey("auth-ip", "192.0.2.1")

	if !limiter.Allow(key, 2, time.Minute) {
		t.Fatal("first request unexpectedly blocked")
	}
	if !limiter.Allow(key, 2, time.Minute) {
		t.Fatal("second request unexpectedly blocked")
	}
	if limiter.Allow(key, 2, time.Minute) {
		t.Fatal("third request was not blocked")
	}
}

func TestFixedWindowLimiterBoundsKeyStorage(t *testing.T) {
	limiter := newFixedWindowLimiter(1)
	if !limiter.Allow("known", 1, time.Hour) {
		t.Fatal("first key unexpectedly blocked")
	}
	if limiter.Allow("attacker-controlled-new-key", 1, time.Hour) {
		t.Fatal("new key should be rejected when limiter storage is full")
	}
}

func TestDefaultServerLimitsAreBounded(t *testing.T) {
	limits := DefaultServerLimits()
	if limits.MaxConnections <= 0 || limits.MaxRecipients <= 0 || limits.MaxConcurrentAuth <= 0 || limits.MaxConcurrentData <= 0 {
		t.Fatalf("resource limits must be positive: %+v", limits)
	}
}
