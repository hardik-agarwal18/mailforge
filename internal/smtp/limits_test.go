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

func TestFixedWindowLimiterAllowPairIsAtomicAcrossDimensions(t *testing.T) {
	limiter := newFixedWindowLimiter(10)
	window := time.Minute

	if !limiter.AllowPair("ip:one", "user:one", 1, 5, window) {
		t.Fatal("first attempt unexpectedly blocked")
	}
	if limiter.AllowPair("ip:one", "user:two", 1, 5, window) {
		t.Fatal("second attempt should be blocked by the IP limit")
	}
	if !limiter.AllowPair("ip:two", "user:two", 1, 5, window) {
		t.Fatal("blocked IP attempt partially consumed the username counter")
	}

	if limiter.AllowPair("ip:three", "user:one", 5, 1, window) {
		t.Fatal("attempt should be blocked by username limit")
	}
	if !limiter.AllowPair("ip:three", "user:three", 1, 5, window) {
		t.Fatal("blocked username attempt partially consumed the IP counter")
	}
}

func TestFixedWindowLimiterAllowPairBoundsKeyStorage(t *testing.T) {
	limiter := newFixedWindowLimiter(1)
	if limiter.AllowPair("ip:one", "user:one", 2, 2, time.Hour) {
		t.Fatal("attempt requiring two keys should fail when storage limit is one")
	}
	if len(limiter.entries) != 0 {
		t.Fatalf("entries = %d, want no partial key insertion", len(limiter.entries))
	}
}
